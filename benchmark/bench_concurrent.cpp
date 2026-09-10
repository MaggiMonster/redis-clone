// ============================================================================
// Concurrent connection benchmark for resp_server.
// NOT part of the server — separate binary.
//
// Opens N concurrent TCP connections, one per thread, each sending SETs with
// its own key namespace so connections never collide. All threads are held at
// a start gate until every connection is established, so the measurement
// window is genuinely concurrent rather than staggered by connect() latency —
// connection setup is outside the timed window entirely.
//
// Reports aggregate throughput (total ops / wall-clock of the measurement
// window) on stdout, and writes per-op round-trip latencies to CSV in the same
// index,latency_ns shape bench_client.cpp uses, so latency_percentiles.py can
// consume it unchanged.
//
// Build: clang++ -std=c++17 -O2 -pthread benchmark/bench_concurrent.cpp -o benchmark/bench_concurrent
// Usage: ./bench_concurrent <port> <num_connections> <ops_per_connection> <out_csv>
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
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <fstream>
#include <iostream>
#include <iomanip>

static int connect_to(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;

    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1)
    {
        close(fd);
        return -1;
    }

    if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        close(fd);
        return -1;
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
// A single read() is not guaranteed to return a whole reply — it can return a
// fragment, or (once pipelining is in play) several replies at once. So bytes
// are buffered and replies are pulled out one at a time.
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

    return -1; // arrays aren't expected from the commands these tools send
}

struct ReplyReader
{
    int fd;
    std::string buf;
    size_t pos = 0;

    explicit ReplyReader(int f) : fd(f) { buf.reserve(64 * 1024); }

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
                    if (pos > 32 * 1024)
                    {
                        buf.erase(0, pos);
                        pos = 0;
                    }
                    return true;
                }
                if (rc < 0)
                {
                    err = "malformed reply from server";
                    return false;
                }
            }

            char tmp[16384];
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

    bool has_buffered() const { return pos < buf.size(); }
};

// ---------------------------------------------------------------------------
// Start gate: workers connect, then block here until every worker has a live
// connection, so the timed window starts for all of them at once.
// ---------------------------------------------------------------------------

struct StartGate
{
    std::mutex m;
    std::condition_variable cv_arrived;
    std::condition_variable cv_go;
    int arrived = 0;
    bool go = false;
};

struct WorkerResult
{
    std::vector<long long> latencies_ns;
    long completed_ops = 0;
    bool ok = true;
    std::string error;
};

static void worker_main(int port, int conn_id, long ops, StartGate &gate, WorkerResult &result)
{
    int fd = connect_to(port);
    if (fd < 0)
    {
        result.ok = false;
        result.error = "connection " + std::to_string(conn_id) + " failed to connect";
    }

    // Arrive at the gate even on failure, so a bad connection can't hang the
    // whole run, then wait for the release.
    {
        std::unique_lock<std::mutex> lock(gate.m);
        gate.arrived++;
        gate.cv_arrived.notify_one();
        gate.cv_go.wait(lock, [&gate] { return gate.go; });
    }

    if (fd < 0)
        return;

    result.latencies_ns.reserve(static_cast<size_t>(ops));
    ReplyReader reader(fd);
    const std::string val = "v"; // small fixed value, same as bench_client
    const std::string key_prefix = "c" + std::to_string(conn_id) + ":key:";

    for (long i = 0; i < ops; ++i)
    {
        std::string cmd = encode_set(key_prefix + std::to_string(i), val);

        auto t0 = std::chrono::steady_clock::now();

        if (!write_all(fd, cmd.data(), cmd.size()))
        {
            result.ok = false;
            result.error = "connection " + std::to_string(conn_id) + ": write failed";
            break;
        }

        Reply reply;
        std::string err;
        if (!reader.next(reply, err))
        {
            result.ok = false;
            result.error = "connection " + std::to_string(conn_id) + ": " + err;
            break;
        }

        auto t1 = std::chrono::steady_clock::now();

        if (reply.type != '+' || reply.payload != "OK")
        {
            result.ok = false;
            result.error = "connection " + std::to_string(conn_id) +
                           ": unexpected reply to SET: type='" + std::string(1, reply.type) +
                           "' payload='" + reply.payload + "'";
            break;
        }

        result.latencies_ns.push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        result.completed_ops++;
    }

    close(fd);
}

int main(int argc, char **argv)
{
    if (argc < 5)
    {
        std::cerr << "usage: " << argv[0]
                  << " <port> <num_connections> <ops_per_connection> <out_csv>\n";
        return 1;
    }

    int port = std::atoi(argv[1]);
    int num_conns = std::atoi(argv[2]);
    long ops_per_conn = std::atol(argv[3]);
    std::string out_path = argv[4];

    if (port <= 0 || num_conns <= 0 || ops_per_conn <= 0)
    {
        std::cerr << "port, num_connections and ops_per_connection must all be positive\n";
        return 1;
    }

    StartGate gate;
    std::vector<WorkerResult> results(static_cast<size_t>(num_conns));
    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(num_conns));

    for (int i = 0; i < num_conns; ++i)
    {
        threads.emplace_back(worker_main, port, i, ops_per_conn,
                             std::ref(gate), std::ref(results[static_cast<size_t>(i)]));
    }

    // Wait for every worker to have finished connecting, then start the clock
    // and release them together.
    std::chrono::steady_clock::time_point t_start;
    {
        std::unique_lock<std::mutex> lock(gate.m);
        gate.cv_arrived.wait(lock, [&gate, num_conns] { return gate.arrived == num_conns; });
        t_start = std::chrono::steady_clock::now();
        gate.go = true;
    }
    gate.cv_go.notify_all();

    for (auto &t : threads)
        t.join();
    auto t_end = std::chrono::steady_clock::now();

    long long total_ops = 0;
    bool any_error = false;
    for (const auto &r : results)
    {
        total_ops += r.completed_ops;
        if (!r.ok)
        {
            any_error = true;
            std::cerr << "error: " << r.error << '\n';
        }
    }

    double wall_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(t_end - t_start).count();
    double throughput = (wall_seconds > 0.0) ? static_cast<double>(total_ops) / wall_seconds : 0.0;

    std::ofstream out(out_path);
    if (!out)
    {
        std::cerr << "failed to open " << out_path << " for writing\n";
        return 1;
    }
    out << "index,latency_ns\n";
    long long index = 0;
    for (const auto &r : results)
        for (long long ns : r.latencies_ns)
            out << index++ << ',' << ns << '\n';
    out.flush();
    out.close();

    std::cout << std::fixed;
    std::cout << "connections: " << num_conns << '\n';
    std::cout << "ops_per_connection: " << ops_per_conn << '\n';
    std::cout << "total_ops: " << total_ops << '\n';
    std::cout << "wall_seconds: " << std::setprecision(6) << wall_seconds << '\n';
    std::cout << "throughput_ops_sec: " << std::setprecision(2) << throughput << '\n';
    std::cout << "wrote " << out_path << '\n';

    return any_error ? 1 : 0;
}
