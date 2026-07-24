// ============================================================================
// Phase 1 — Blocking TCP echo server.
//
// This is the FIRST version: it serves one client at a time and freezes on a
// blocking read() while a client is idle. That limitation is what motivates the
// non-blocking version (and ultimately the kqueue event loop).
//
// Build (macOS):  clang++ -std=c++17 blocking_server.cpp -o blocking_server
// Run:            ./blocking_server
// Test:           nc localhost 6379      (type a line, it echoes back)
// ============================================================================

#include <sys/socket.h>   // socket(), setsockopt(), bind(), listen(), accept()
#include <netinet/in.h>   // struct sockaddr_in, INADDR_ANY, htons/htonl
#include <unistd.h>       // read(), write(), close()
#include <cstring>        // memset
#include <cstdio>         // perror
#include <cstdlib>        // exit
#include <iostream>

constexpr int PORT    = 6379;        // Redis's default port
constexpr int BACKLOG = SOMAXCONN;   // max pending connections the kernel queues

int main() {
    // 1. Create the listening socket (IPv4, TCP). Returns an fd, or -1 on error.
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        exit(1);
    }

    // 2. Allow immediate reuse of the port after restart (avoids EADDRINUSE
    //    while a previous connection is still in TIME_WAIT).
    int yes = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        perror("setsockopt");
        exit(1);
    }

    // 3. Describe the address: all interfaces (INADDR_ANY), port 6379.
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY); // 0.0.0.0 -> bind ALL interfaces
    addr.sin_port        = htons(PORT);       // host->network byte order

    // 4. Bind the socket to that (address, port).
    if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        exit(1);
    }

    // 5. Switch into passive (listening) mode with a backlog queue.
    if (listen(listen_fd, BACKLOG) < 0) {
        perror("listen");
        exit(1);
    }

    std::cout << "listening on 0.0.0.0:" << PORT << '\n';

    // ------------------------------------------------------------------------
    // The blocking accept loop: accept() blocks until a client connects, and
    // the inner read() blocks until that client sends data. While serving one
    // client, no other client can be accepted.
    // ------------------------------------------------------------------------
    while (true) {
        int client_fd = accept(listen_fd, nullptr, nullptr);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        std::cout << "client connected (fd " << client_fd << ")\n";

        char buf[4096];
        ssize_t n;
        // > 0 bytes read -> echo; == 0 -> peer closed; < 0 -> error.
        while ((n = read(client_fd, buf, sizeof(buf))) > 0) {
            if (write(client_fd, buf, n) < 0) {
                perror("write");
                break;
            }
        }

        std::cout << "client disconnected (fd " << client_fd << ")\n";
        close(client_fd);
    }

    close(listen_fd);
    return 0;
}