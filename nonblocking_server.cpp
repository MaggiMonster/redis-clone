// ============================================================================
// Phase 1 — Networking foundation: a blocking TCP echo server.
//
// This is the "ceremony" layer of the Redis clone. It does NOT yet use an
// event loop, so it can only serve ONE client at a time — that limitation is
// exactly what motivates Phase 2 (kqueue). Type this out by hand once and make
// sure you can explain every line; don't just paste it.
//
// Build (macOS):  clang++ -std=c++17 server.cpp -o server
// Run:            ./server
// Test:           nc localhost 6379      (type a line, it echoes back)
// ============================================================================
#include <sys/socket.h>   // socket(), setsockopt(), bind(), listen(), accept()
#include <netinet/in.h>   // struct sockaddr_in, INADDR_ANY, htons/htonl
#include <unistd.h>       // read(), write(), close(), fcntl()
#include <fcntl.h>        // fcntl() flags for non-blocking mode
#include <cstring>        // memset / strerror
#include <cstdio>         // perror
#include <cstdlib>        // exit
#include <cerrno>
#include <iostream>
#include <vector>

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

constexpr int PORT    = 6379;
constexpr int BACKLOG = SOMAXCONN;

int main() {
    // 1. Create the listening socket.
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); exit(1); }

    // 2. Allow immediate reuse of the port after restart.
    //    Without SO_REUSEADDR, the OS holds the port in TIME_WAIT for a minute
    //    or two after the server exits, and rebinding fails with EADDRINUSE.
    int yes = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        perror("setsockopt"); exit(1);
    }

    // 3. Describe the address we want to bind to.
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(PORT);

    // 4. Bind the socket to that (address, port).
    if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind"); exit(1);
    }

    // 5. Switch the socket into passive (listening) mode.
    if (listen(listen_fd, BACKLOG) < 0) { perror("listen"); exit(1); }

    set_nonblocking(listen_fd);
    std::cout << "listening on 0.0.0.0:" << PORT << '\n';

    std :: vector<int> clients;

    while (true) {
        // Drain the entire backlog queue before servicing clients.
        // Since listen_fd is non-blocking, accept() returns EAGAIN once the
        // queue is empty — that's our signal to stop and move on.
        while (true) {
            int client_fd = accept(listen_fd, nullptr, nullptr);
            if (client_fd < 0) {
                if (errno != EAGAIN && errno != EWOULDBLOCK)
                    perror("accept");
                break;
            }
            set_nonblocking(client_fd);
            clients.push_back(client_fd);
            std::cout << "client connected (fd " << client_fd << ")\n";
        }

        // Round-robin: service every connected client.
        char buf[4096];
        for (int i = 0; i < (int)clients.size(); ) {
            ssize_t n = read(clients[i], buf, sizeof(buf));
            if (n > 0) {
                write(clients[i], buf, n);
                ++i;
            } else if (n == 0) {
                // Peer closed the connection gracefully.
                std::cout << "client disconnected (fd " << clients[i] << ")\n";
                close(clients[i]);
                clients[i] = clients.back();
                clients.pop_back();
                // Don't increment i — swapped-in element needs checking.
            } else {
                // EAGAIN means no data right now, not a real error.
                if (errno != EAGAIN && errno != EWOULDBLOCK)
                    perror("read");
                ++i;
            }
        }
    }

    close(listen_fd);
    return 0;
}
