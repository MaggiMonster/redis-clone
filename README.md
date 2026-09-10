# redis-clone

A from-scratch reimplementation of core Redis internals in modern C++ on macOS.
The goal isn't just a working server — it's to understand and be able to defend
every systems-level design decision behind a high-performance in-memory data
store: the networking layer, the event loop, the wire protocol, the data store,
key expiration, and persistence.

## Why I built this

I liked the design choices that show up in systems-level projects — the kind
of thinking you only run into when you're working close to the OS and the
network stack instead of a framework on top of them. This was a chance to get
hands-on with that: sockets, event loops, wire protocols, and the data
structures underneath a store like Redis. It was a genuinely good way to learn,
including from the places I got wrong on the first pass.

## Repo layout

```
src/            server implementations, one per phase, the hand-rolled hash
                table's standalone first draft, and a RESP parser test client
benchmark/      latency benchmarking tool + results comparing incremental vs.
                stop-the-world rehashing
```

`src/` keeps every phase's server rather than overwriting it: `blocking_server.cpp`
→ `nonblocking_server.cpp` → `kevent_server.cpp` → `resp_server.cpp`, each one
tinkering with the networking model one step further (blocking sockets, then
non-blocking with plain `accept()` polling, then `kqueue`, then the full RESP
protocol on top). `dict.cpp` is the hash table's first draft — built and tested
standalone with its own `main()` before the same `Dict` was folded into
`resp_server.cpp` as the real store. `test_client.cpp` exists to test the RESP
parser against a split command: it writes a `PING` across two separate
`write()` calls with a `sleep()` in between, to confirm the parser correctly
reassembles a command that arrives in fragments instead of one clean read.

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

Supported commands:

| Command | Syntax | Notes |
|---|---|---|
| `PING` | `PING [message]` | replies `PONG`, or echoes `message` |
| `ECHO` | `ECHO message` | |
| `SET` | `SET key value` | clears any existing TTL on the key |
| `GET` | `GET key` | nil bulk reply if the key doesn't exist (or has expired) |
| `DEL` | `DEL key` | |
| `EXPIRE` | `EXPIRE key seconds` | `1` if the key exists, `0` otherwise |
| `TTL` | `TTL key` | seconds left, `-1` if no TTL, `-2` if the key doesn't exist |
| `PERSIST` | `PERSIST key` | removes a key's TTL; `1` if it had one, `0` otherwise |
| `DBSIZE` | `DBSIZE` | number of live keys |

## Roadmap

| Phase | Component | Status |
|-------|-----------|--------|
| 1 | Networking foundation — TCP, non-blocking sockets, `INADDR_ANY` | done |
| 2 | Event loop — `kqueue`, readiness scaling with active connections | done |
| 3 | RESP protocol — parsing the wire format, command dispatch | done |
| 4 | Core data store — hash table with incremental rehashing | done |
| 5 | Expiration — passive + active probabilistic sampling | done |
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
- **Draining the whole accept backlog, not one connection per cycle.** The
  first non-blocking pass called `accept()` once per event-loop iteration —
  fine at low load, but under a real connection burst, extras just sit queued
  in the kernel's backlog until later iterations happen to catch up to them
  one at a time. Looping `accept()` until it returns `EAGAIN` drains the
  entire backlog every time the listening socket is readable, so nothing
  waits longer than it has to.
- **Not trusting `EV_EOF` as the close signal.** kqueue sets `EV_EOF` on the
  read filter once the peer sends FIN — but that can be true at the same
  moment there's still unread data sitting in the socket's receive buffer (a
  client that writes then closes right away). Closing the connection as soon
  as `EV_EOF` is set would silently drop that data on a loaded or laggy
  server. `read()`'s own return value is the only signal actually trusted to
  mean "done": a `0` only means EOF *after* everything the peer sent has been
  drained, which is the guarantee that actually matters.
- **Partial writes on a non-blocking socket.** Same lesson as `EV_EOF`, on the
  way out instead of the way in: trust what the syscall returns, not a side
  channel that looks equivalent. `write()` on a non-blocking socket takes only
  what fits in the kernel send buffer right now — maybe part of a reply, maybe
  none of it — and says so in its return value. The server used to discard
  that value, which reads as "the reply was sent" and is right up until the
  buffer fills. The pipelining validator in `benchmark/` found it: past
  ~555KB of queued replies on this machine — consistent with autoscaled send
  and receive buffers sitting between the 128KB `net.inet.tcp.sendspace` base
  and the 4MB autoscaling ceiling, so it is a property of the buffers under
  that workload rather than a fixed constant you could read off a single
  sysctl — every further reply was silently dropped and the client waited
  forever for answers that no longer existed. It held steady across command
  counts and payload sizes, which is what identified it as the buffer filling
  rather than anything about the workload. Small replies never reach that
  cliff, which is why ordinary request-response traffic never noticed. Now every reply goes
  through a per-connection output buffer, flushed as the socket accepts it,
  with `EVFILT_WRITE` armed only while something is actually pending — leaving
  it armed on an empty buffer would spin the loop at 100% CPU, the exact thing
  the event loop above exists to avoid. That buffer is capped (32MB, per
  connection) and a client that blows past it gets closed, because an
  unbounded output buffer is a memory-exhaustion vector: one client that never
  reads its replies could otherwise take the server down.
- **Incremental rehashing.** The hash table (`Dict` in `src/resp_server.cpp`)
  keeps two tables (`ht[0]`, `ht[1])`. Once the load factor hits 1.0, it
  allocates a table at 2x size and migrates exactly one bucket per subsequent
  `get`/`set`/`del` call instead of migrating everything at once — so no single
  operation ever pays for the full resize. See Benchmarks below for what this
  actually buys in the worst case.
- **Passive + active expiration, not just one.** Each `Entry` carries its own
  `expire_at`. Passive expiration — checking and evicting lazily on `GET`,
  `EXPIRE`, `TTL`, `PERSIST` — is nearly free, but on its own it leaks memory:
  a key nobody ever accesses again just sits there forever, TTL or not. So the
  event loop also gives `kevent()` a bounded ~100ms timeout instead of blocking
  forever, and on every wake — even with zero client I/O — runs a bounded
  active-expiration pass: sample a handful of random buckets, evict anything
  expired, and if more than a quarter of the TTL-bearing entries sampled were
  expired, assume there's more and sample again (capped at a few passes so one
  tick can never run unbounded work). Same amortized-over-time shape as the
  rehashing above, applied to a different problem.

## Benchmarks

All numbers below were measured with client and server on the **same machine
over loopback**. That's deliberate: loopback takes the network out of the
picture so what's left is the server — its event loop, RESP parsing, and data
structures. These measure the server, **not a network path**. Across a real
network RTT would dominate every number here, so treat these as relative
figures for comparing implementation choices against each other, not as
absolute performance claims.

Hardware and toolchain:

| | |
|---|---|
| CPU | Apple M4, 10 cores (10 physical / 10 logical, no SMT) |
| Memory | 16 GB |
| OS | macOS 15.5 (build 24F74) |
| Compiler | Apple clang 17.0.0, `arm64-apple-darwin24.5.0`, `-O2` |
| TCP buffers | `sendspace`/`recvspace` 128 KB base, autoscaling to a 4 MB ceiling |

Methodology for the concurrent and pipelining sweeps below: **5,000,000 ops per
config, three runs each, reporting the median**. Runs were interleaved (run 1
of every config, then run 2, then run 3) rather than three-in-a-row, so drift
over the session spreads across configs instead of landing on one. The server
is restarted before every run, so no run inherits another's hash table. The
large-reply regression gate runs first. Run-to-run throughput spread was
**≤6.6% everywhere** and is quoted per config below.

**† Quote p50 and p99; treat p99.9 as indicative only.** Throughput (≤6.6%) and
the p50/p99 latencies were stable run to run, but p99.9 swung between roughly
10% and 78% depending on config — worst at pipelined depth 64, where it ranged
39,042–70,667 ns across three runs. That is a sample-count artifact, not the
server behaving erratically: at depth 64, 5,000,000 ops is only 78,125 batches,
and latency is recorded per batch, so p99.9 is decided by about **78 samples**.
A handful of scheduler hiccups moves it a long way. The fix would be more
batches, not a quieter machine, so the figures are left in as shape rather than
removed — just don't quote them as measurements.

### Incremental vs. stop-the-world rehashing

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
./benchmark/bench_client 6379 1000000 benchmark/incremental.csv
kill %1

./resp_server_naive &
./benchmark/bench_client 6379 1000000 benchmark/naive.csv
kill %1

python3 benchmark/latency_percentiles.py benchmark/incremental.csv benchmark/naive.csv
```

### Concurrent connections

`benchmark/bench_concurrent.cpp` opens N simultaneous connections, one thread
each, every connection writing SETs into its own key namespace so they never
collide. All threads are held at a start gate until every connection is
established, so the measurement window is genuinely concurrent instead of being
smeared out by connection setup — `connect()` happens entirely outside the
timed window.

This is the thing a single-connection benchmark structurally cannot show: with
one connection the server is never asked to multiplex, so the `kqueue` loop is
doing nothing the blocking server from Phase 1 couldn't. Latency here is
per-op round trip, so it *should* climb with connection count — each client is
queueing behind more work on a single-threaded server — while total throughput
is the number that matters.

These are **unpipelined** round trips — one command in flight per connection at
a time.

| connections | throughput (ops/sec) | p50 (ns) | p99 (ns) | p99.9 (ns, indicative†) | run-to-run spread |
|---|---|---|---|---|---|
| 1 | 76,910 | 12,250 | 18,792 | 23,167 | 1.7% |
| 10 | **268,531** | 35,625 | 65,375 | 91,041 | 3.4% |
| 50 | 259,361 | 169,750 | 361,083 | 429,542 | 4.6% |
| 100 | 256,916 | 387,125 | 474,917 | 773,458 | 4.7% |
| 200 | 252,675 | 785,250 | 973,625 | 1,551,500 | 6.6% |

**Reading the shape.** Throughput climbs 3.5x from 1 to 10 connections
(76.9k → 268.5k), and then stops. **The knee is at 10 connections** — that is
where this server saturates. Everything past it is flat to very slightly
declining: 50, 100 and 200 connections land within 6% of the peak, drifting
down rather than up. Meanwhile latency grows almost exactly in proportion to
connection count: p50 goes 35.6µs → 169.8µs → 387.1µs → 785.3µs across
10 → 50 → 100 → 200, roughly 2x per doubling, and p99 follows the same shape.

That is the expected and correct behavior for this architecture, not a
shortfall. The server is **single-threaded by design** — one `kqueue` loop, no
locks anywhere, which is what lets the hash table be lock-free and the
incremental rehashing above be reasoned about at all. Once that single thread
is busy, additional connections cannot add throughput; they can only queue. So
work per second flattens and each client's wait grows linearly with how many
clients are ahead of it. Multiplying throughput past the knee would mean
sharding the keyspace across threads or processes, which is a different design
with its own costs — not a tuning knob on this one.

**Caveat on 100 and 200 connections.** The client threads and the server share
the same 10 cores, so at those counts a meaningful part of what's being
measured is client-side thread scheduling rather than server capacity.
**1, 10 and 50 are the clean measurements**; 100 and 200 are directional —
enough to show the plateau continues and latency keeps scaling, not precise
figures for server capacity.

### Pipelining

`benchmark/bench_pipeline.cpp` writes `depth` commands back-to-back in a single
`write()` without waiting for replies, then reads and validates `depth` replies
before sending the next batch. It exists to test two separate things:

**Throughput.** Pipelining amortizes the per-round-trip syscall and wakeup cost
over many commands, so this measures how much of the single-connection number
is protocol overhead rather than actual work. Latency percentiles for this one
are per *batch*, not per op — a batch at depth 64 doing more work than a batch
at depth 1 is expected, so batch latency rising with depth is not a regression.

Every throughput figure in this table is **pipelined at the stated depth**, and
is not comparable to the unpipelined numbers in the concurrent table above
without that qualifier — they measure different things.

| depth | throughput (ops/sec, pipelined at this depth) | speedup vs. pipelined depth 1 | p50 batch (ns) | p99 batch (ns) | run-to-run spread |
|---|---|---|---|---|---|
| 1 | 74,794 | 1.00x | 12,250 | 18,000 | 1.5% |
| 2 | 148,390 | 1.98x | 12,292 | 18,000 | 0.6% |
| 4 | 288,686 | 3.86x | 12,500 | 18,042 | 0.5% |
| 8 | 506,688 | 6.77x | 13,584 | 20,375 | 3.1% |
| 16 | 922,024 | 12.33x | 14,375 | 21,209 | 1.0% |
| 32 | 1,433,464 | 19.17x | 17,125 | 23,709 | 1.2% |
| 64 | **1,912,995** | 25.58x | 23,834 | 31,584 | 1.5% |

Pipelined depth 1 (74,794 ops/sec) lands within noise of the 1-connection
unpipelined figure (76,910 ops/sec), which is the expected sanity check: at
depth 1 there is nothing to batch, so pipelining is a no-op.

**What the output-buffer fix changed here.** Measured at the same 5M ops, three
runs, against the pre-fix server built from the parent commit:

| | pre-fix | post-fix | change |
|---|---|---|---|
| pipelined depth 1 | 75,303 | 74,794 | −0.7% (within the 1.5% run-to-run spread) |
| pipelined depth 64 | 996,904 | 1,912,995 | **1.92x** |

The gain is **syscall amortization, not the server doing less work**. Before the
fix, each reply in a batch got its own `write()` — 64 syscalls per batch at
depth 64. After it, replies accumulate in the connection's output buffer and
the batch is flushed in one pass. Depth 1 is the control: it batches nothing, so
if the speedup came from cheaper work per command it would have moved too, and
it didn't.

The pre-fix comparison is valid, i.e. the old number is not inflated by replies
being silently dropped: at depth 64 the workload is ECHO with short tokens, so
pending replies peak at **1,088 bytes** (17 bytes per reply x 64) — roughly
500x below the ~555KB cliff at which the pre-fix server started losing them. Every pre-fix run also passed the
validator on all 5,000,000 replies, complete and in order, which confirms it
empirically rather than by argument.

**Correctness.** It's also a parser test, and hard-fails on any violation. The
default workload is `ECHO` with a unique token per command precisely *because*
every reply is then distinguishable — `SET` replies are all `+OK`, so a
SET-based test can't tell a reordered batch from a correct one. Each batch is
checked for exactly `depth` replies, each well-formed, each matching its
command's token in order, with nothing left over in the buffer afterward
(which would mean the server produced more replies than commands).

`--adversarial` goes further: it splits each batch's byte stream across many
small `write()` calls at arbitrary offsets — landing mid-command and
mid-argument — with a pause between each, forcing the server to hold partial
parse state across many reads. Throughput is meaningless in that mode and isn't
reported; it's purely a correctness run.

Reproduce the whole sweep — builds everything, restarts the server before every
run so no run inherits another's hash table, and cleans up after itself:

```bash
./benchmark/run_benchmarks.sh
python3 benchmark/summarize_benchmarks.py
```

## Notes

Developed and tested on macOS (Apple Silicon). The event loop uses `kqueue`,
the BSD/macOS equivalent of Linux's `epoll`.
