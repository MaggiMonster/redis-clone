# redis-clone

> Rename this to whatever you want to call the project.

A from-scratch reimplementation of core Redis internals in modern C++ on macOS.
The goal isn't just a working server — it's to understand and be able to defend
every systems-level design decision behind a high-performance in-memory data
store: the networking layer, the event loop, the wire protocol, the data store,
key expiration, and persistence.

## Why I built this

I wanted to understand how an in-memory data store actually works under the
hood — not just use one. Each piece is implemented by hand so I can explain the
trade-offs behind it. <!-- edit this into your own words -->

## Build & run

Requires macOS and a C++17 compiler (clang++ ships with the Xcode Command Line
Tools).

```bash
clang++ -std=c++17 server.cpp -o server
./server
```

Then connect a client in another terminal:

```bash
nc localhost 6379
```

## Roadmap

| Phase | Component | Status |
|-------|-----------|--------|
| 1 | Networking foundation — TCP, non-blocking sockets, `INADDR_ANY` | in progress |
| 2 | Event loop — `kqueue`, readiness scaling with active connections | next |
| 3 | RESP protocol — parsing the wire format, command dispatch | planned |
| 4 | Core data store — hash table with incremental rehashing | planned |
| 5 | Expiration — passive + active probabilistic sampling | planned |
| 6 | Persistence — RDB via `fork()`/copy-on-write, and AOF | planned |

## Design decisions

This is the part I actually care about being able to explain:

- **Two-socket model.** The listening socket's only job is to manufacture
  connections via `accept()`; each per-client connection fd is a separate
  point-to-point pipe that carries the actual data. They have distinct jobs.
- **Why an event loop instead of a polling loop.** A non-blocking round-robin
  poll works, but it spins a CPU core at ~100% even when every client is idle,
  because nothing ever makes the thread sleep. `kqueue` lets the kernel wake the
  thread only when an fd is actually ready, so the cost scales with *active*
  connections, not total ones, and an idle server sits near 0% CPU.
- **`SO_REUSEADDR`.** Set defensively so the port can be rebound immediately on
  restart; only strictly needed when a prior connection is lingering in
  `TIME_WAIT`.

<!-- Add to this section as you build each phase — it's your interview script. -->

## Benchmarks

TODO: numbers from `redis-benchmark` against this server vs. real Redis
(throughput in ops/sec and p99 latency on GET/SET).

## Notes

Developed and tested on macOS (Apple Silicon). The event loop uses `kqueue`,
the BSD/macOS equivalent of Linux's `epoll`.