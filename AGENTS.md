# Design north stars

These rules apply to every change in this repository, from people and agents alike.
[CONTRIBUTING.md](CONTRIBUTING.md) covers building, testing and the DCO.

## Goals, in order

1. **Highest performance**, on every platform, using each OS's native mechanisms.
2. **Best guarantees**, stated in public contracts and proved where possible.
3. **Least code.** Deleting code is as valuable as adding it.

## Build on the primitives

- **Use the core primitives.** Streams, Objects, Actors and Auth are the foundation. Higher layers are compositions of them, not parallel systems.
- **An agent is an actor, plus a stream subtree, plus a volume.**
  - Forking an agent is a stream fork.
  - Children are stream children.
  - Exploring a tree of any size is lazy paging over `children_page` and `follow`.
- **When a primitive almost fits, change the primitive.** Don't build a workaround in a consumer. API changes that make the design cleaner are welcome.
- **Generality.** Every optimisation lives in the most general layer: a primitive, or a shared core such as the stream reader or the harness. A smart behaviour locked inside one consumer (GraphCoder, the filesystem, one service) belongs in a shared layer, so every downstream use gets it.

## One implementation per mechanism

- Each mechanism has one implementation, shared across services and operating systems. Per-OS code is a thin adapter over one core. A second path for the same concept is a defect.
- The harness library holds all agent logic.
  - GraphCoder is a small preset plus a thin binary.
  - Host plugins (Claude Code, Codex, Pi) keep their hosts' loops and tools, and only align coordination with the harness: cwd, filesystem commits, fork and merge.
- **Remote serving is one `server::serve`.** A managed deployment and a self-deployed one run the same code.
- Pluggable backends are small adapters, about 300 lines each, that pass the one shared conformance suite.

## Scale and latency

- **Scale target:** about 10^5 agents on one machine, at O(1) cost per agent. Per-agent threads, mounts and scans are not allowed.
- **Lazy loading:** everything that can grow is lazy, paged and bounded by what is visible or subscribed, never by total size. Clients prefetch and cache, so a swarm of any size feels immediately available.
- **Control requests:** minimise them. The steady-state path aims for zero.
- **The hot path is accounted for.** Each round trip, quorum read, fsync, leader hop, lock, auth check and copy on a request path must name the public guarantee it buys. If it buys none, remove it.

## No arbitrary limits

- Everything is unlimited by default: agents, children, steps, volumes, sessions, history, queue depth, sizes and counts.
- Three kinds of bound are allowed:
  - chunking or page sizes, which split work;
  - backpressure or credit, which delays work;
  - protective limits (frame size, decompression bombs) set so high they can never legitimately be hit.
- Real capacity is configured by the user, through reservations or approvals.
- A preset `None` means truly unbounded, with no hidden fallback constant.

## No polling

- Product code and test harnesses never poll. That rules out sleep loops, interval rechecks, retry-until-ready and periodic probes.
- Wait on the primitive's event instead: a stream follow or watch, a conditional-commit result, an actor alarm or delivery, a filesystem notification, an OS wait (pidfd, kqueue, `WaitForSingleObject`, inotify) or a channel.
- Backoff is allowed only for transient transport failures, never as a readiness check.
- If a primitive lacks the event you need, add the event to the primitive.

## Filesystem-driven

Harness features and the whole UI are files in volumes plus live queries. Add a file or a query, not a bespoke tool or API.

## Fix classes, not instances

- Fix bugs structurally, so the whole error class becomes impossible.
- None of these are fixes:
  - `#[allow]` (except scoped `unsafe_code` on necessary FFI);
  - raising a limit, timeout or stack size to hide a failure;
  - masking retries;
  - special-casing tests;
  - compatibility or migration shims.
- Persisted formats are versioned from v1, with no migration code.

## Tests and proofs

- Tests are fast and deterministic, and wait on events. A suite should take well under 60 s, and over 10 s is a problem to fix in code, not by dropping coverage.
- Every test must earn its place. Delete tests that check:
  - deleted code;
  - behaviour another test already covers;
  - the implementation restated (getters, defaults, constants, derives);
  - something the compiler already guarantees;
  - incidental formatting;
  - only a mock.
- Proofs (kani, loom, deterministic simulation, fuzzing), generators and their checks, the shared conformance suites, and real-binary end-to-end tests are kept.

## Agent surface

- The model-visible command tool is named `shell` on every OS.
- Secrets never enter guest code, streams or volumes.
