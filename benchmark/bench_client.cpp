// ============================================================================
// Standalone benchmark client for resp_server / resp_server_naive.
// NOT part of the server — separate binary, one persistent TCP connection.
//
// Sends SET commands with unique keys (key:0 .. key:N-1), timing each full
// round trip (send + read reply) with steady_clock, and writes the samples
// to a CSV as index,latency_ns.
//
// Usage: ./bench_client <port> [num_ops] [out_csv]
//   num_ops defaults to 1000000, out_csv defaults to latencies.csv
// ============================================================================
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <chrono>
#include <fstream>
#include <iostream>

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

// SET always gets back the fixed simple-string "+OK\r\n" from this server,
// so a trailing CRLF is sufficient to know the reply is complete.
static void read_reply(int fd)
{
    char buf[256];
    size_t total = 0;
    while (true)
    {
        ssize_t n = read(fd, buf + total, sizeof(buf) - total);
        if (n <= 0)
        {
            perror("read");
            exit(1);
        }
        total += static_cast<size_t>(n);
        if (total >= 2 && buf[total - 2] == '\r' && buf[total - 1] == '\n')
            return;
        if (total == sizeof(buf))
        {
            std::cerr << "reply exceeded buffer\n";
            exit(1);
        }
    }
}

int main(int argc, char **argv)
{
    if (argc < 2)
    {
        std::cerr << "usage: " << argv[0] << " <port> [num_ops] [out_csv]\n";
        return 1;
    }
    int port = std::atoi(argv[1]);
    long num_ops = (argc >= 3) ? std::atol(argv[2]) : 1000000;
    std::string out_path = (argc >= 4) ? argv[3] : "latencies.csv";

    int fd = connect_to(port);

    std::ofstream out(out_path);
    if (!out)
    {
        std::cerr << "failed to open " << out_path << " for writing\n";
        return 1;
    }
    out << "index,latency_ns\n";

    const std::string val = "v"; // small fixed value

    for (long i = 0; i < num_ops; ++i)
    {
        std::string key = "key:" + std::to_string(i);
        std::string cmd = encode_set(key, val);

        auto t0 = std::chrono::steady_clock::now();

        size_t sent = 0;
        while (sent < cmd.size())
        {
            ssize_t n = write(fd, cmd.data() + sent, cmd.size() - sent);
            if (n <= 0)
            {
                perror("write");
                return 1;
            }
            sent += static_cast<size_t>(n);
        }
        read_reply(fd);

        auto t1 = std::chrono::steady_clock::now();
        long long latency_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

        out << i << ',' << latency_ns << '\n';
    }

    out.flush();
    out.close();
    close(fd);

    std::cout << "done: " << num_ops << " ops -> " << out_path << '\n';
    return 0;
}
