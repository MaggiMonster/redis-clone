# redis-clone

A from-scratch reimplementation of core Redis internals in modern C++ on macOS.
The goal isn't just a working server — it's to understand and be able to defend
every systems-level design decision behind a high-performance in-memory data
store: the networking layer, the event loop, the wire protocol, the data store,
key expiration, and persistence.

## Why I built this

I wanted to understand how an in-memory data store actually works under the
hood — not just use one. Each piece is implemented by hand so I can explain the
trade-offs behind it.

## Repo layout

```
src/            server implementations, one per phase, plus the hand-rolled
                hash table (embedded in resp_server.cpp) and its test clients
benchmark/      latency benchmarking tool + results comparing incremental vs.
                stop-the-world rehashing
```

## Build & run

Requires macOS and a C++17 compiler (clang++ ships with the Xcode Command Line
Tools).

```bash
clang++ -std=c++17 -O2 src/resp_server.cpp -o resp_server
./resp_server
```

Then connect a client in another terminal:

```bash
redis-cli -p 6379 set foo bar
redis-cli -p 6379 get foo
```

(or `nc localhost 6379` and type RESP by hand.)

## Roadmap

| Phase | Component | Status |
|-------|-----------|--------|
| 1 | Networking foundation — TCP, non-blocking sockets, `INADDR_ANY` | done |
| 2 | Event loop — `kqueue`, readiness scaling with active connections | done |
| 3 | RESP protocol — parsing the wire format, command dispatch | done |
| 4 | Core data store — hash table with incremental rehashing | done |
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
- **Incremental rehashing.** The hash table (`Dict` in `src/resp_server.cpp`)
  keeps two tables (`ht[0]`, `ht[1])`. Once the load factor hits 1.0, it
  allocates a table at 2x size and migrates exactly one bucket per subsequent
  `get`/`set`/`del` call instead of migrating everything at once — so no single
  operation ever pays for the full resize. See Benchmarks below for what this
  actually buys in the worst case.

## Benchmarks

`src/resp_server_naive.cpp` is a byte-for-byte copy of `resp_server.cpp` with
one change: the moment a resize triggers, it drains the migration to
completion synchronously in that same call (stop-the-world), instead of
spreading it across future operations. Everything else — hash function, load
factor, growth factor, bucket structure — is identical between the two.

`benchmark/bench_client.cpp` opens one TCP connection and sends SETs with
unique keys, timing each full round trip with `steady_clock` (nanosecond
resolution). `benchmark/latency_percentiles.py` turns the resulting CSVs into
a percentile table.

**1,000,000 keys:**

| percentile | incremental (ns) | naive (ns) |
|---|---|---|
| p50 | 12,125 | 12,083 |
| p90 | 14,459 | 14,041 |
| p99 | 18,542 | 18,250 |
| p99.9 | 35,959 | 37,292 |
| p99.99 | 68,541 | 79,000 |
| **p100 (max)** | **1,433,042** | **15,154,417** |

**5,000,000 keys:**

| percentile | incremental (ns) | naive (ns) |
|---|---|---|
| p50 | 12,084 | 12,041 |
| p90 | 13,916 | 14,375 |
| p99 | 18,375 | 18,250 |
| p99.9 | 24,125 | 23,708 |
| p99.99 | 58,041 | 60,208 |
| **p100 (max)** | **8,540,250** | **158,292,458** |

Through p99.99 the two implementations are statistically indistinguishable —
incremental rehashing costs nothing in the common case. The entire difference
is concentrated in the true worst case, and that gap **widens with data size**:
~10.6x at 1M keys, ~18.5x at 5M keys (1.43ms vs. 15.15ms, then 8.54ms vs.
158.29ms). Stop-the-world's worst-case latency scales with table size;
incremental's is decoupled from it.

Reproduce:

```bash
clang++ -std=c++17 -O2 src/resp_server.cpp -o resp_server
clang++ -std=c++17 -O2 src/resp_server_naive.cpp -o resp_server_naive
clang++ -std=c++17 -O2 benchmark/bench_client.cpp -o benchmark/bench_client

./resp_server &                                          # then, separately:
./benchmark/bench_client 6379 1000000 incremental.csv
kill %1

./resp_server_naive &
./benchmark/bench_client 6379 1000000 naive.csv
kill %1

python3 benchmark/latency_percentiles.py incremental.csv naive.csv
```

## Notes

Developed and tested on macOS (Apple Silicon). The event loop uses `kqueue`,
the BSD/macOS equivalent of Linux's `epoll`.
