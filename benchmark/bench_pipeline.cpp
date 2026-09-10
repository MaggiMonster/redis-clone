// ============================================================================
// Pipelining benchmark + RESP parser correctness test for resp_server.
// NOT part of the server — separate binary, one connection.
//
// Writes `depth` commands back-to-back in as few write() calls as possible
// without waiting for replies, then reads and validates `depth` replies before
// sending the next batch. The point is to exercise the server's ability to
// parse several commands out of a single read buffer.
//
// Workload defaults to ECHO with a unique token per command, because that
// makes every reply in a batch distinguishable — which is what lets this
// detect *reordering*, not just drops. SET replies are all "+OK", so a
// SET-based test cannot tell a reordered batch from a correct one. Use
// --cmd=set for a SET workload (comparable to the other benchmarks); it still
// validates reply count and well-formedness, just not ordering.
//
// Validation is hard-fail: if the server drops, merges, reorders, malforms, or
// over-produces a reply, this exits non-zero with a specific error.
//
// --adversarial splits each batch's byte stream across many small write()
// calls at arbitrary offsets — including mid-command and mid-argument — with
// small sleeps in between, to verify the stateful parser reassembles commands
// correctly under pipelining. Throughput in that mode is meaningless (the
// sleeps dominate); it is purely a correctness run.
//
// Build: clang++ -std=c++17 -O2 benchmark/bench_pipeline.cpp -o benchmark/bench_pipeline
// Usage: ./bench_pipeline <port> <pipeline_depth> <total_ops> <out_csv>
//                         [--adversarial] [--cmd=set|echo]
// ============================================================================
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <string>
#include <vector>
#include <random>
#include <thread>
#include <chrono>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <iomanip>

static int connect_to(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        perror("socket");
        exit(1);
    }

    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1)
    {
        std::cerr << "inet_pton failed\n";
        exit(1);
    }

    if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        perror("connect");
        exit(1);
    }
    return fd;
}

static std::string encode_set(const std::string &key, const std::string &val)
{
    std::string cmd;
    cmd += "*3\r\n";
    cmd += "$3\r\nSET\r\n";
    cmd += "$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
    cmd += "$" + std::to_string(val.size()) + "\r\n" + val + "\r\n";
    return cmd;
}

static std::string encode_echo(const std::string &msg)
{
    std::string cmd;
    cmd += "*2\r\n";
    cmd += "$4\r\nECHO\r\n";
    cmd += "$" + std::to_string(msg.size()) + "\r\n" + msg + "\r\n";
    return cmd;
}

// A single write() is not guaranteed to accept the whole buffer.
static bool write_all(int fd, const char *data, size_t len)
{
    size_t sent = 0;
    while (sent < len)
    {
        ssize_t n = write(fd, data + sent, len - sent);
        if (n < 0)
        {
            if (errno == EINTR)
                continue;
            return false;
        }
        if (n == 0)
            return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

// ---------------------------------------------------------------------------
// Incremental RESP reply reading.
//
// Under pipelining a single read() routinely returns several replies at once,
// or half of one — so bytes are buffered and replies pulled out one at a time.
// ---------------------------------------------------------------------------

struct Reply
{
    char type = 0;       // '+', '-', ':' or '$'
    std::string payload; // reply body, without the type byte or trailing CRLF
    bool is_null = false;
};

static const char *find_crlf(const char *data, size_t len, size_t from)
{
    for (size_t i = from; i + 1 < len; ++i)
        if (data[i] == '\r' && data[i + 1] == '\n')
            return data + i;
    return nullptr;
}

// 1 = parsed one reply, 0 = need more bytes, -1 = malformed.
static int try_parse_reply(const char *data, size_t len, Reply &r, size_t &consumed)
{
    if (len == 0)
        return 0;

    char t = data[0];
    if (t == '+' || t == '-' || t == ':')
    {
        const char *crlf = find_crlf(data, len, 1);
        if (crlf == nullptr)
            return 0;
        r.type = t;
        r.is_null = false;
        r.payload.assign(data + 1, static_cast<size_t>(crlf - (data + 1)));
        consumed = static_cast<size_t>(crlf - data) + 2;
        return 1;
    }

    if (t == '$')
    {
        const char *crlf = find_crlf(data, len, 1);
        if (crlf == nullptr)
            return 0;

        std::string len_str(data + 1, static_cast<size_t>(crlf - (data + 1)));
        long long n;
        try
        {
            n = std::stoll(len_str);
        }
        catch (...)
        {
            return -1;
        }

        size_t header = static_cast<size_t>(crlf - data) + 2;
        if (n < 0)
        {
            r.type = '$';
            r.is_null = true;
            r.payload.clear();
            consumed = header;
            return 1;
        }
        if (len < header + static_cast<size_t>(n) + 2)
            return 0;
        if (data[header + n] != '\r' || data[header + n + 1] != '\n')
            return -1;

        r.type = '$';
        r.is_null = false;
        r.payload.assign(data + header, static_cast<size_t>(n));
        consumed = header + static_cast<size_t>(n) + 2;
        return 1;
    }

    return -1; // arrays aren't expected from the commands this tool sends
}

struct ReplyReader
{
    int fd;
    std::string buf;
    size_t pos = 0;

    explicit ReplyReader(int f) : fd(f) { buf.reserve(256 * 1024); }

    // Blocks until one complete reply is available. False on EOF, socket
    // error, or a malformed reply.
    bool next(Reply &r, std::string &err)
    {
        while (true)
        {
            if (pos < buf.size())
            {
                size_t consumed = 0;
                int rc = try_parse_reply(buf.data() + pos, buf.size() - pos, r, consumed);
                if (rc == 1)
                {
                    pos += consumed;
                    return true;
                }
                if (rc < 0)
                {
                    err = "malformed reply from server";
                    return false;
                }
            }

            char tmp[65536];
            ssize_t n = read(fd, tmp, sizeof(tmp));
            if (n == 0)
            {
                err = "server closed the connection";
                return false;
            }
            if (n < 0)
            {
                if (errno == EINTR)
                    continue;
                err = std::string("read: ") + std::strerror(errno);
                return false;
            }
            buf.append(tmp, static_cast<size_t>(n));
        }
    }

    // True if bytes remain past the replies already consumed. After a batch's
    // expected replies have been read, that means the server sent more than it
    // should have.
    bool has_buffered() const { return pos < buf.size(); }

    void reset_between_batches()
    {
        buf.erase(0, pos);
        pos = 0;
    }
};

static void fail(const std::string &msg)
{
    std::cerr << "FAIL: " << msg << '\n';
    exit(1);
}

int main(int argc, char **argv)
{
    if (argc < 5)
    {
        std::cerr << "usage: " << argv[0]
                  << " <port> <pipeline_depth> <total_ops> <out_csv>"
                     " [--adversarial] [--cmd=set|echo]\n";
        return 1;
    }

    int port = std::atoi(argv[1]);
    long depth = std::atol(argv[2]);
    long total_ops = std::atol(argv[3]);
    std::string out_path = argv[4];

    bool adversarial = false;
    std::string cmd_kind = "echo";
    for (int i = 5; i < argc; ++i)
    {
        std::string arg = argv[i];
        if (arg == "--adversarial")
            adversarial = true;
        else if (arg == "--cmd=set")
            cmd_kind = "set";
        else if (arg == "--cmd=echo")
            cmd_kind = "echo";
        else
        {
            std::cerr << "unknown option: " << arg << '\n';
            return 1;
        }
    }

    if (port <= 0 || depth <= 0 || total_ops <= 0)
    {
        std::cerr << "port, pipeline_depth and total_ops must all be positive\n";
        return 1;
    }

    int fd = connect_to(port);
    ReplyReader reader(fd);

    std::ofstream out(out_path);
    if (!out)
    {
        std::cerr << "failed to open " << out_path << " for writing\n";
        return 1;
    }
    out << "batch_index,batch_latency_ns\n";

    // Fixed seed so an adversarial run is reproducible.
    std::mt19937 rng(12345);
    std::uniform_int_distribution<size_t> chunk_dist(1, 16);

    long long batch_index = 0;
    long long ops_done = 0;
    long long total_bytes_written = 0;
    long long write_calls = 0;

    auto t_start = std::chrono::steady_clock::now();

    while (ops_done < total_ops)
    {
        long this_batch = static_cast<long>(
            std::min<long long>(depth, total_ops - ops_done));

        // Build the whole batch as one contiguous buffer, and remember what
        // each command's reply must be so ordering can be verified.
        std::string batch;
        std::vector<std::string> expected;
        batch.reserve(static_cast<size_t>(this_batch) * 48);
        expected.reserve(static_cast<size_t>(this_batch));

        for (long i = 0; i < this_batch; ++i)
        {
            if (cmd_kind == "echo")
            {
                // Unique per command across the whole run — this is what makes
                // a reordered or duplicated reply detectable.
                std::string token = "p:" + std::to_string(batch_index) + ":" + std::to_string(i);
                batch += encode_echo(token);
                expected.push_back(token);
            }
            else
            {
                std::string key = "pkey:" + std::to_string(ops_done + i);
                batch += encode_set(key, "v");
                expected.push_back("OK");
            }
        }

        auto b0 = std::chrono::steady_clock::now();

        if (!adversarial)
        {
            // One write() for the whole batch (write_all only loops if the
            // kernel takes a partial buffer).
            if (!write_all(fd, batch.data(), batch.size()))
                fail("write failed on batch " + std::to_string(batch_index));
            write_calls++;
        }
        else
        {
            // Split the batch at arbitrary offsets — deliberately landing
            // mid-command and mid-argument — with a pause between each piece,
            // so the server is forced to hold partial state across reads.
            size_t off = 0;
            while (off < batch.size())
            {
                size_t chunk = std::min(chunk_dist(rng), batch.size() - off);
                if (!write_all(fd, batch.data() + off, chunk))
                    fail("write failed on batch " + std::to_string(batch_index));
                write_calls++;
                off += chunk;
                if (off < batch.size())
                    std::this_thread::sleep_for(std::chrono::microseconds(200));
            }
        }
        total_bytes_written += static_cast<long long>(batch.size());

        // Read back exactly this_batch replies and check each one.
        for (long i = 0; i < this_batch; ++i)
        {
            Reply reply;
            std::string err;
            if (!reader.next(reply, err))
            {
                fail("batch " + std::to_string(batch_index) + ", reply " + std::to_string(i) +
                     " of " + std::to_string(this_batch) + ": " + err +
                     " (server dropped or truncated a pipelined reply)");
            }

            if (reply.type == '-')
            {
                fail("batch " + std::to_string(batch_index) + ", reply " + std::to_string(i) +
                     ": server returned an error: " + reply.payload);
            }

            if (cmd_kind == "echo")
            {
                if (reply.type != '$' || reply.is_null)
                {
                    fail("batch " + std::to_string(batch_index) + ", reply " + std::to_string(i) +
                         ": expected a bulk string for ECHO, got type '" +
                         std::string(1, reply.type) + "'");
                }
                if (reply.payload != expected[static_cast<size_t>(i)])
                {
                    fail("batch " + std::to_string(batch_index) + ", reply " + std::to_string(i) +
                         ": out-of-order or wrong reply — expected \"" +
                         expected[static_cast<size_t>(i)] + "\", got \"" + reply.payload + "\"");
                }
            }
            else
            {
                if (reply.type != '+' || reply.payload != "OK")
                {
                    fail("batch " + std::to_string(batch_index) + ", reply " + std::to_string(i) +
                         ": expected +OK for SET, got type '" + std::string(1, reply.type) +
                         "' payload \"" + reply.payload + "\"");
                }
            }
        }

        // Nothing should be left over: the server must not have produced more
        // replies than the commands in this batch.
        if (reader.has_buffered())
        {
            fail("batch " + std::to_string(batch_index) + ": server sent more data than the " +
                 std::to_string(this_batch) + " replies this batch called for");
        }
        reader.reset_between_batches();

        auto b1 = std::chrono::steady_clock::now();
        out << batch_index << ','
            << std::chrono::duration_cast<std::chrono::nanoseconds>(b1 - b0).count() << '\n';

        ops_done += this_batch;
        batch_index++;
    }

    auto t_end = std::chrono::steady_clock::now();
    double wall_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(t_end - t_start).count();
    double throughput = (wall_seconds > 0.0) ? static_cast<double>(ops_done) / wall_seconds : 0.0;

    out.flush();
    out.close();
    close(fd);

    std::cout << std::fixed;
    std::cout << "pipeline_depth: " << depth << '\n';
    std::cout << "command: " << cmd_kind << '\n';
    std::cout << "mode: " << (adversarial ? "adversarial (split writes)" : "normal") << '\n';
    std::cout << "total_ops: " << ops_done << '\n';
    std::cout << "batches: " << batch_index << '\n';
    std::cout << "bytes_written: " << total_bytes_written << '\n';
    std::cout << "write_calls: " << write_calls << '\n';
    std::cout << "wall_seconds: " << std::setprecision(6) << wall_seconds << '\n';
    if (adversarial)
    {
        std::cout << "throughput_ops_sec: n/a (adversarial mode sleeps between writes)\n";
    }
    else
    {
        std::cout << "throughput_ops_sec: " << std::setprecision(2) << throughput << '\n';
    }
    std::cout << "validation: PASS (" << ops_done << " replies, all well-formed"
              << (cmd_kind == "echo" ? " and in order" : "") << ")\n";
    std::cout << "wrote " << out_path << '\n';

    return 0;
}
