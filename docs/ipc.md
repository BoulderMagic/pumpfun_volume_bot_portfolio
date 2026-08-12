# Inter-Process Communication (IPC) — Learning Notes

Reference notes on IPC mechanisms: how independent OS processes exchange
data and coordinate. Kept here as background material, not something this
repo's bot currently uses (it runs as a single Node.js process).

## Why IPC exists

Each OS process has its own isolated address space, so two processes
can't just share variables like two threads in the same process can. IPC
mechanisms are the OS-provided ways to move data (or a signal) across
that isolation boundary safely.

## Core mechanisms

- **Pipes** — a unidirectional byte stream between two related processes
  (e.g. parent/child), backed by a kernel buffer. Anonymous pipes (`|` in
  a shell, `pipe()` syscall) only work between related processes; **named
  pipes / FIFOs** (`mkfifo`) let unrelated processes connect via a
  filesystem path.
- **Unix domain sockets** — like TCP sockets but addressed by a filesystem
  path instead of an IP:port, for IPC on the same machine. Support both
  stream (`SOCK_STREAM`) and datagram (`SOCK_DGRAM`) modes, and can pass
  file descriptors between processes (`SCM_RIGHTS`).
  This is what Docker (`/var/run/docker.sock`), most databases, and
  Node's `net.Server` with a socket path use for local-only channels.
- **TCP/UDP sockets** — the same socket API but over the network stack;
  usable for IPC between processes on different machines, at higher
  overhead than a Unix socket.
- **Shared memory** — a region of physical memory mapped into multiple
  processes' address spaces (`shm_open`/`mmap` on POSIX,
  `CreateFileMapping` on Windows). Fastest IPC mechanism since there's no
  kernel copy per message, but requires external synchronization
  (semaphores/mutexes) since the OS doesn't order the writes for you.
- **Message queues** — kernel-managed queues of discrete messages
  (POSIX `mq_open`, or higher-level brokers like ZeroMQ/RabbitMQ used
  between processes or hosts). Decouples sender/receiver timing —
  the sender doesn't block on the receiver being ready.
- **Signals** — asynchronous, one-bit-of-information notifications
  (`SIGTERM`, `SIGINT`, `SIGUSR1`, …) delivered to a process, handled via
  a signal handler. No payload beyond which signal fired (plus optional
  small data with `sigqueue`).
- **Memory-mapped files** — a file mapped into memory (`mmap`) that
  multiple processes open, giving shared-memory-like access with
  persistence and OS-managed paging, at the cost of disk-backed latency
  unless it's on tmpfs.

## Choosing a mechanism

| Need | Good fit |
|---|---|
| Simple one-way byte stream, related processes | Anonymous pipe |
| Simple one-way byte stream, unrelated processes | Named pipe / FIFO |
| Local RPC-style request/response | Unix domain socket |
| Cross-machine communication | TCP/UDP socket |
| Maximum throughput, same machine | Shared memory + a sync primitive |
| Decoupled producer/consumer, buffering | Message queue |
| "Something happened," no payload | Signal |

## In Node.js specifically

- `child_process.fork()` sets up a built-in IPC channel between parent
  and child automatically; use `child.send(msg)` / `process.on('message',
  ...)` to exchange JSON-serializable messages.
- `child_process.spawn()`/`exec()` communicate via the child's stdio
  streams (pipes) unless `stdio: 'ipc'` is added to a stdio array entry.
- `net.createServer({ path: '/tmp/foo.sock' })` and `net.connect({ path
  })` give a raw Unix domain socket for structured local IPC (e.g. an
  admin/control channel into a long-running bot process).
- `worker_threads` (a different model — threads, not processes) use
  `postMessage`/`MessagePort`, which is not OS IPC but solves a similar
  coordination problem within one process.

## Where this would matter for a bot like this one

Not used today (this bot is a single process), but relevant if it grew
into multiple cooperating processes — e.g. a wallet/signing process kept
separate from the swap-submission process for isolation, talking over a
Unix domain socket or `fork()`-based IPC channel rather than sharing a
private key in the same address space.

## Further reading

- `man 7 pipe`, `man 7 unix`, `man 7 shm_overview` (Linux man pages)
- Node.js docs: https://nodejs.org/api/child_process.html, https://nodejs.org/api/net.html
