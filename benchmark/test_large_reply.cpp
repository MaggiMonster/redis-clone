// ============================================================================
// Regression test: large pipelined replies must not be dropped.
//
// This exists because of a real bug. The server used to discard write()'s
// return value when sending a reply on a non-blocking socket, so once the
// kernel send buffer filled (~555KB on the machine this was found on) every
// reply past that point was silently thrown away and the client waited
// forever. Small replies never reached the cliff, so ordinary pipelining tests
// passed while the bug sat there.
//
// The shape of the test is therefore deliberate:
//   1. SET a batch of keys with large values.
//   2. Pipeline a GET for every one of them without reading anything back, so
//      the server must produce far more reply bytes than a socket buffer holds.
//   3. Stall, guaranteeing the server hits a full send buffer mid-flush.
//   4. Read everything and assert every reply arrives, complete, in order.
//
// Values are distinct per key, so a dropped, truncated, reordered or
// duplicated reply all fail loudly rather than silently passing.
//
// Non-interactive: exits 0 on pass, non-zero on any failure. A server that
// drops replies shows up as a read timeout, not a hang.
//
// Build: clang++ -std=c++17 -O2 benchmark/test_large_reply.cpp -o benchmark/test_large_reply
// Usage: ./test_large_reply <port> [num_keys] [value_bytes]
//        defaults: 512 keys x 4096 bytes = ~2MB of replies
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
#include <chrono>
#include <iostream>

static int connect_to(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        perror("socket");
        exit(2);
    }

    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1)
    {
        std::cerr << "inet_pton failed\n";
        exit(2);
    }

    if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        perror("connect");
        exit(2);
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

static std::string encode_get(const std::string &key)
{
    std::string cmd;
    cmd += "*2\r\n";
    cmd += "$3\r\nGET\r\n";
    cmd += "$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
    return cmd;
}

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

struct Reply
{
    char type = 0;
    std::string payload;
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

    return -1;
}

struct ReplyReader
{
    int fd;
    std::string buf;
    size_t pos = 0;

    explicit ReplyReader(int f) : fd(f) { buf.reserve(1 << 20); }

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
                    if (pos > (1 << 20))
                    {
                        buf.erase(0, pos);
                        pos = 0;
                    }
                    return true;
                }
                if (rc < 0)
                {
                    err = "malformed reply";
                    return false;
                }
            }

            char tmp[262144];
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
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    err = "timed out waiting for a reply (server dropped it)";
                    return false;
                }
                err = std::string("read: ") + std::strerror(errno);
                return false;
            }
            buf.append(tmp, static_cast<size_t>(n));
        }
    }
};

static int fail(const std::string &msg)
{
    std::cerr << "FAIL: " << msg << '\n';
    return 1;
}

// Distinct, self-identifying value for each key, so truncation or reordering
// cannot slip through.
static std::string make_value(long i, size_t value_bytes)
{
    std::string marker = "val:" + std::to_string(i) + ":";
    std::string v = marker;
    v.resize(value_bytes, static_cast<char>('a' + (i % 26)));
    return v;
}

int main(int argc, char **argv)
{
    int port = (argc > 1) ? std::atoi(argv[1]) : 6379;
    long num_keys = (argc > 2) ? std::atol(argv[2]) : 512;
    size_t value_bytes = (argc > 3) ? static_cast<size_t>(std::atol(argv[3])) : 4096;

    if (port <= 0 || num_keys <= 0 || value_bytes < 16)
    {
        std::cerr << "usage: " << argv[0] << " <port> [num_keys] [value_bytes>=16]\n";
        return 2;
    }

    long long expected_reply_bytes = static_cast<long long>(num_keys) *
                                     static_cast<long long>(value_bytes + 16);
    std::cout << "large-reply regression test: " << num_keys << " keys x " << value_bytes
              << " bytes (~" << (expected_reply_bytes / 1024) << " KB of replies)\n";

    int fd = connect_to(port);
    ReplyReader reader(fd);

    // A server that drops replies must fail here, not hang.
    struct timeval tv;
    tv.tv_sec = 10;
    tv.tv_usec = 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // ---- Phase 1: load the keys (small +OK replies, read as we go) ----
    for (long i = 0; i < num_keys; ++i)
    {
        std::string key = "big:" + std::to_string(i);
        std::string cmd = encode_set(key, make_value(i, value_bytes));
        if (!write_all(fd, cmd.data(), cmd.size()))
            return fail("write failed during SET phase");

        Reply r;
        std::string err;
        if (!reader.next(r, err))
            return fail("SET " + key + ": " + err);
        if (r.type != '+' || r.payload != "OK")
            return fail("SET " + key + ": expected +OK, got type '" + std::string(1, r.type) + "'");
    }
    std::cout << "loaded " << num_keys << " keys\n";

    // ---- Phase 2: pipeline every GET without reading a single reply ----
    std::string batch;
    batch.reserve(static_cast<size_t>(num_keys) * 32);
    for (long i = 0; i < num_keys; ++i)
        batch += encode_get("big:" + std::to_string(i));

    if (!write_all(fd, batch.data(), batch.size()))
        return fail("write failed while pipelining GETs");
    std::cout << "pipelined " << num_keys << " GETs (" << batch.size()
              << " bytes out), stalling before reading\n";

    // ---- Phase 3: stall, so the server is mid-flush against a full buffer ----
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // ---- Phase 4: every reply must arrive, complete and in order ----
    for (long i = 0; i < num_keys; ++i)
    {
        Reply r;
        std::string err;
        if (!reader.next(r, err))
        {
            return fail("GET reply " + std::to_string(i) + " of " + std::to_string(num_keys) +
                        ": " + err + " — " + std::to_string(num_keys - i) +
                        " replies never arrived");
        }

        if (r.type != '$' || r.is_null)
            return fail("GET reply " + std::to_string(i) + ": expected a bulk string, got type '" +
                        std::string(1, r.type) + "'" + (r.is_null ? " (null)" : ""));

        std::string expected = make_value(i, value_bytes);
        if (r.payload.size() != expected.size())
            return fail("GET reply " + std::to_string(i) + ": truncated — expected " +
                        std::to_string(expected.size()) + " bytes, got " +
                        std::to_string(r.payload.size()));
        if (r.payload != expected)
            return fail("GET reply " + std::to_string(i) +
                        ": wrong or out-of-order payload (starts \"" + r.payload.substr(0, 24) +
                        "\", expected \"" + expected.substr(0, 24) + "\")");
    }

    close(fd);
    std::cout << "PASS: all " << num_keys << " large replies arrived complete and in order\n";
    return 0;
}
