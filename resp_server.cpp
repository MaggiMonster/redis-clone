// ============================================================================
// Phase 3 — RESP (Redis Serialization Protocol) on top of kqueue.
//
// TCP is a byte stream — a command may arrive split across many read() calls,
// or many commands may arrive in one call (pipelining). Each client gets a
// read buffer; we append into it and parse complete RESP arrays out in a loop.
//
// RESP array wire format (what redis-cli sends):
//   *<nargs>\r\n
//   $<len>\r\n<arg>\r\n
//   ...
//
// Build (macOS):  clang++ -std=c++17 resp_server.cpp -o resp_server
// Run:            ./resp_server
// Test:           redis-cli -p 6379 ping
//                 redis-cli -p 6379 echo hello
//                 redis-cli -p 6379 set foo bar
//                 redis-cli -p 6379 get foo
// ============================================================================
#include <sys/socket.h>
#include <sys/event.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <cstddef>
#include <algorithm>
#include <cctype>

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

void set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void kq_add(int kq, int fd)
{
    struct kevent ev;
    EV_SET(&ev, fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
    if (kevent(kq, &ev, 1, nullptr, 0, nullptr) < 0)
        perror("kevent EV_ADD");
}

void kq_remove_and_close(int kq, int fd)
{
    struct kevent ev;
    EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    kevent(kq, &ev, 1, nullptr, 0, nullptr);
    close(fd);
}
// dictionary to store data using incremental hashing to prevent latency spikes.
struct Entry
{
    std::string key;
    std::string value;
    Entry *next;
};
struct Table
{
    std::vector<Entry *> buckets;
    size_t count;
};
struct Dict
{
    Table ht[2];
    long rehash_idx;
    Dict()
    {
        ht[0].buckets.assign(8, nullptr);
        ht[0].count = 0;
        rehash_idx = -1;
    }
    void rehash_step()
    {
        if (rehash_idx == -1)
            return;
        Entry *cur = ht[0].buckets[rehash_idx];
        while (cur != nullptr)
        {
            Entry *next = cur->next;
            size_t i = std::hash<std::string>{}(cur->key) % ht[1].buckets.size();
            cur->next = ht[1].buckets[i];
            ht[1].buckets[i] = cur;
            ht[1].count++;
            ht[0].count--;
            cur = next;
        }
        ht[0].buckets[rehash_idx] = nullptr;
        rehash_idx++;
        if (rehash_idx == (long)ht[0].buckets.size())
        {
            ht[0] = ht[1];
            ht[1] = Table{};
            rehash_idx = -1;
        }
    }
    Entry *get(const std::string &key)
    {
        rehash_step();
        size_t index = std::hash<std::string>{}(key) % ht[0].buckets.size();
        Entry *cur = ht[0].buckets[index];
        while (cur != nullptr)
        {
            if (cur->key == key)
                return cur;
            cur = cur->next;
        }
        if (rehash_idx != -1)
        {
            size_t idx = std::hash<std::string>{}(key) % ht[1].buckets.size();
            Entry *current = ht[1].buckets[idx];
            while (current != nullptr)
            {
                if (current->key == key)
                    return current;
                current = current->next;
            }
        }
        return nullptr;
    }
    void set(const std::string &key, const std::string &value)
    {
        rehash_step();
        size_t index = std::hash<std::string>{}(key) % ht[0].buckets.size();
        Entry *cur = ht[0].buckets[index];
        while (cur != nullptr)
        {
            if (cur->key == key)
            {
                cur->value = value;
                return;
            }
            cur = cur->next;
        }
        if (rehash_idx != -1)
        {
            size_t index = std::hash<std::string>{}(key) % ht[1].buckets.size();
            Entry *cur = ht[1].buckets[index];
            while (cur != nullptr)
            {
                if (cur->key == key)
                {
                    cur->value = value;
                    return;
                }
                cur = cur->next;
            }
        }
        Table &t = (rehash_idx == -1) ? ht[0] : ht[1];
        size_t idx = std::hash<std::string>{}(key) % t.buckets.size();
        Entry *node = new Entry({key, value, t.buckets[idx]});
        t.buckets[idx] = node;
        t.count++;

        if (rehash_idx == -1 && ht[0].count >= ht[0].buckets.size())
        {
            ht[1].buckets.assign(ht[0].buckets.size() * 2, nullptr);
            ht[1].count = 0;
            rehash_idx = 0;
        }
        return;
    }
    void del(const std::string &key)
    {
        rehash_step();
        size_t index = std::hash<std::string>{}(key) % ht[0].buckets.size();
        Entry *cur = ht[0].buckets[index];
        Entry *prev = nullptr;
        while (cur != nullptr)
        {
            if (cur->key == key)
            {
                if (prev == nullptr)
                {
                    ht[0].buckets[index] = ht[0].buckets[index]->next;
                    delete cur;
                    ht[0].count--;
                    return;
                }
                else
                {
                    prev->next = cur->next;
                    delete cur;
                    ht[0].count--;
                    return;
                }
            }
            prev = cur;
            cur = cur->next;
        }
        if (rehash_idx != -1)
        {
            size_t index = std::hash<std::string>{}(key) % ht[1].buckets.size();
            Entry *cur = ht[1].buckets[index];
            Entry *prev = nullptr;
            while (cur != nullptr)
            {
                if (cur->key == key)
                {
                    if (prev == nullptr)
                    {
                        ht[1].buckets[index] = ht[1].buckets[index]->next;
                        delete cur;
                        ht[1].count--;
                        return;
                    }
                    else
                    {
                        prev->next = cur->next;
                        delete cur;
                        ht[1].count--;
                        return;
                    }
                }
                prev = cur;
                cur = cur->next;
            }
        }
    }
};
//

// ---------------------------------------------------------------------------
// RESP parser
//
// Tries to extract one complete RESP array from the front of buf.
// Returns true and fills args if a full command is present, consuming those
// bytes from buf. Returns false and leaves buf untouched if more data is
// needed (incomplete command).
// ---------------------------------------------------------------------------

bool parse_command(std::string &buf, std::vector<std::string> &args)
{
    if (buf.empty() || buf[0] != '*')
        return false;

    size_t pos = 0;

    // *<nargs>\r\n
    size_t crlf = buf.find("\r\n", pos);
    if (crlf == std::string::npos)
        return false;

    int nargs;
    try
    {
        nargs = std::stoi(buf.substr(1, crlf - 1));
    }
    catch (...)
    {
        return false;
    }
    if (nargs < 0)
        return false; // *-1 null array — not a valid command
    pos = crlf + 2;

    std::vector<std::string> out;
    out.reserve(nargs);

    for (int i = 0; i < nargs; ++i)
    {
        // $<len>\r\n
        if (pos >= buf.size() || buf[pos] != '$')
            return false;
        crlf = buf.find("\r\n", pos);
        if (crlf == std::string::npos)
            return false;

        int len;
        try
        {
            len = std::stoi(buf.substr(pos + 1, crlf - pos - 1));
        }
        catch (...)
        {
            return false;
        }
        if (len < 0)
            return false; // $-1 null bulk — guard before size_t cast
        pos = crlf + 2;

        // <arg>\r\n
        if (pos + (size_t)len + 2 > buf.size())
            return false;
        out.push_back(buf.substr(pos, len));
        pos += len + 2;
    }

    buf.erase(0, pos); // consume exactly the bytes we parsed
    args = std::move(out);
    return true;
}

// ---------------------------------------------------------------------------
// RESP response helpers
// ---------------------------------------------------------------------------

static std::string make_bulk(const std::string &s)
{
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}

static const std::string NULL_BULK = "$-1\r\n";
static const std::string OK = "+OK\r\n";

// ---------------------------------------------------------------------------
// Command dispatcher
// ---------------------------------------------------------------------------

std::string handle_command(const std::vector<std::string> &args, Dict &store)
{
    if (args.empty())
        return "-ERR empty command\r\n";

    std::string cmd = args[0];
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

    if (cmd == "PING")
    {
        if (args.size() == 1)
            return "+PONG\r\n";
        return make_bulk(args[1]);
    }

    if (cmd == "ECHO")
    {
        if (args.size() < 2)
            return "-ERR wrong number of arguments for 'echo'\r\n";
        return make_bulk(args[1]);
    }

    if (cmd == "SET")
    {
        if (args.size() < 3)
            return "-ERR wrong number of arguments for 'set'\r\n";
        store.set(args[1], args[2]);
        return OK;
    }

    if (cmd == "GET")
    {
        if (args.size() < 2)
            return "-ERR wrong number of arguments for 'get'\r\n";
        Entry *e = store.get(args[1]);
        if (!e)
            return NULL_BULK;
        return make_bulk(e->value);
    }
    if (cmd == "DEL")
    {
        if (args.size() < 2)
            return "-ERR wrong number of arguments for 'del'\r\n";
        store.del(args[1]);
        return ":1\r\n"; // RESP integer; real Redis returns count deleted
    }

    return "-ERR unknown command '" + args[0] + "'\r\n";
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

constexpr int PORT = 6379;
constexpr int BACKLOG = SOMAXCONN;
constexpr int MAX_EVTS = 64;

int main()
{
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0)
    {
        perror("socket");
        exit(1);
    }

    int yes = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
    {
        perror("setsockopt");
        exit(1);
    }

    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(PORT);

    if (::bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
    {
        perror("bind");
        exit(1);
    }
    if (listen(listen_fd, BACKLOG) < 0)
    {
        perror("listen");
        exit(1);
    }
    set_nonblocking(listen_fd);

    int kq = kqueue();
    if (kq < 0)
    {
        perror("kqueue");
        exit(1);
    }
    kq_add(kq, listen_fd);

    Dict store;

    // Per-client read buffers. Incomplete RESP data stays here until the
    // rest arrives in a future read().
    std::unordered_map<int, std::string> buffers;

    struct kevent events[MAX_EVTS];
    std::cout << "listening on 0.0.0.0:" << PORT << '\n';

    while (true)
    {
        int n = kevent(kq, nullptr, 0, events, MAX_EVTS, nullptr);
        if (n < 0)
        {
            perror("kevent wait");
            continue;
        }

        for (int i = 0; i < n; ++i)
        {
            int fd = static_cast<int>(events[i].ident);

            if (events[i].flags & EV_ERROR)
            {
                std::cerr << "kevent error on fd " << fd << '\n';
                if (fd != listen_fd)
                {
                    buffers.erase(fd);
                    kq_remove_and_close(kq, fd);
                }
                continue;
            }

            if (fd == listen_fd)
            {
                // Drain the entire backlog.
                while (true)
                {
                    int client_fd = accept(listen_fd, nullptr, nullptr);
                    if (client_fd < 0)
                        break;
                    set_nonblocking(client_fd);
                    kq_add(kq, client_fd);
                    buffers[client_fd] = {};
                    std::cout << "client connected (fd " << client_fd << ")\n";
                }
            }
            else
            {
                // Append new bytes into the client's buffer.
                char tmp[4096];
                ssize_t nr = read(fd, tmp, sizeof(tmp));
                if (nr > 0)
                {
                    buffers[fd].append(tmp, nr);
                }
                else if (nr == 0 || (nr < 0 && errno != EAGAIN && errno != EWOULDBLOCK))
                {
                    std::cout << "client disconnected (fd " << fd << ")\n";
                    buffers.erase(fd);
                    kq_remove_and_close(kq, fd);
                    continue;
                }

                // Parse and dispatch every complete command in the buffer.
                // Incomplete trailing bytes stay in buffers[fd] for next time.
                std::vector<std::string> args;
                while (parse_command(buffers[fd], args))
                {
                    std::string resp = handle_command(args, store);
                    write(fd, resp.data(), resp.size());
                }
            }
        }
    }

    close(listen_fd);
    close(kq);
    return 0;
}
