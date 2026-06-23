// ============================================================================
// Phase 2 — Event-driven server using kqueue (macOS).
//
// Replaces the busy-poll round-robin with a kernel event loop.
// kevent() blocks until at least one fd is ready, so we burn no CPU
// when all clients are idle.
//
// Build (macOS):  clang++ -std=c++17 kevent_server.cpp -o kevent_server
// Run:            ./kevent_server
// Test:           nc localhost 6379
// ============================================================================
#include <sys/socket.h>   // socket(), setsockopt(), bind(), listen(), accept()
#include <sys/event.h>    // kqueue(), kevent(), EV_SET, EVFILT_READ
#include <netinet/in.h>   // struct sockaddr_in, INADDR_ANY, htons/htonl
#include <unistd.h>       // read(), write(), close(), fcntl()
#include <fcntl.h>        // fcntl() flags for non-blocking mode
#include <cstring>        // memset / strerror
#include <cstdio>         // perror
#include <cstdlib>        // exit
#include <cerrno>
#include <iostream>

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Register an fd for EVFILT_READ events with the kqueue.
void kq_add(int kq, int fd) {
    struct kevent ev;
    EV_SET(&ev, fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
    if (kevent(kq, &ev, 1, nullptr, 0, nullptr) < 0)
        perror("kevent EV_ADD");
}

// Remove an fd from the kqueue and close it.
void kq_remove_and_close(int kq, int fd) {
    struct kevent ev;
    EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    kevent(kq, &ev, 1, nullptr, 0, nullptr);
    close(fd);
}

constexpr int PORT     = 6379;
constexpr int BACKLOG  = SOMAXCONN;
constexpr int MAX_EVTS = 64;   // events to collect per kevent() call

int main() {
    // 1. Listening socket.
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); exit(1); }

    int yes = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        perror("setsockopt"); exit(1);
    }

    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(PORT);

    if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind"); exit(1);
    }
    if (listen(listen_fd, BACKLOG) < 0) { perror("listen"); exit(1); }
    set_nonblocking(listen_fd);

    // 2. Create the kqueue and register listen_fd.
    int kq = kqueue();
    if (kq < 0) { perror("kqueue"); exit(1); }
    kq_add(kq, listen_fd);

    std::cout << "listening on 0.0.0.0:" << PORT << '\n';

    struct kevent events[MAX_EVTS];
    while (true) {
        // Block until at least one fd is ready.
        int n = kevent(kq, nullptr, 0, events, MAX_EVTS, nullptr);
        if (n < 0) { perror("kevent wait"); continue; }

        for (int i = 0; i < n; ++i) {
            int fd = static_cast<int>(events[i].ident);

            if (events[i].flags & EV_ERROR) {
                std::cerr << "kevent error on fd " << fd << '\n';
                if (fd != listen_fd) kq_remove_and_close(kq, fd);
                continue;
            }

            if (fd == listen_fd) {
                // New connection(s) — drain the entire backlog.
                while (true) {
                    int client_fd = accept(listen_fd, nullptr, nullptr);
                    if (client_fd < 0) break;  // EAGAIN: queue is empty
                    set_nonblocking(client_fd);
                    kq_add(kq, client_fd);
                    std::cout << "client connected (fd " << client_fd << ")\n";
                }
            } else {
                // the section below is something i cant test. cuz itll echo back before i close the connection. but in a laggy server. some data 
                // not yet echoed back will never be read because it will detect eof.
                
                // if (events[i].flags & EV_EOF) {
                //     std::cout << "client disconnected (fd " << fd << ")\n";
                //     kq_remove_and_close(kq, fd);
                //     continue;
                // }

                char buf[4096];
                ssize_t nr = read(fd, buf, sizeof(buf));
                if (nr > 0) {
                    write(fd, buf, nr);
                } else if (nr == 0) {
                    std::cout << "client disconnected (fd " << fd << ")\n";
                    kq_remove_and_close(kq, fd);
                } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    perror("read");
                    kq_remove_and_close(kq, fd);
                }
            }
        }
    }

    close(listen_fd);
    close(kq);
    return 0;
}
