# Native execution capabilities

G02 is being implemented at the native process launch owner, alongside G01.
This document describes the draft contract, including work still required
before it can be advertised as the stock harness execution boundary.

The authenticated host decides an `ExecutionGrant` after Auth and the existing
approval chain. A grant binds one workspace, read-only roots, workspace write
or read-only access, and network authority. It is not a model tool parameter,
environment override, additional configuration hierarchy or sandbox service.
The existing approval/decision files remain authoritative. No separate system
event path is introduced; lifecycle consumers use the sessions lane's Notice.

`ExecutionGrant::attenuate` permits narrower workspace/read roots, removal of
writes, and narrower networking. It refuses an increase of authority.
`ProcessTree::spawn_granted` prepares active enforcement and enters the same
process creation, custody, capture, cancellation and retirement implementation.
Enforcement is inherited by descendants. A preparation or launch refusal is
`tool execution failed: <reason>`; it never retries without confinement.
Native host plugins continue to run their host's own loop and sandbox.

Network `Off` denies new sockets. `On` admits Internet networking subject to
inherited restrictions. `TcpPorts` grants outbound TCP destination ports on
any address, with no listeners, UDP, raw or local IPC sockets. An empty set
grants nothing. This is a **port allowlist**, not a hostname or IP allowlist;
an address/domain policy must never be translated to a broader port grant.
Neither mode weakens a sandbox inherited from the host.

Linux uses Landlock ABI 6 or later: all filesystem rights through ABI 5,
TCP bind/connect restrictions, and signal/abstract-UNIX scoping. Seccomp
requires the native x86_64 or aarch64 syscall ABI and seals namespace,
process-group, cross-process, device, kernel and filesystem-metadata escape
mechanisms. `clone3` returns `ENOSYS` to permit libc's checked `clone` path.
Metadata changes are conservatively denied even in writable workspaces.
Read roots are explicit; host home, procfs, temporary directories and the
filesystem root are never implicitly added. `/dev/null` and `/dev/urandom`
provide null I/O and entropy. Only pipes and null may cross as stdio;
nonstdio descriptors become close-on-exec. Landlock rule roots are held during
active launch. Missing native facilities refuse execution.

macOS compiles a deny-by-default Seatbelt profile before fork, then applies
the compiled policy after the original custody hook in the final child.
Workspace writes and read-only grants are separate profile rules; network
port selectors preserve the shared grant meaning. Mach services, external
process signaling and arbitrary local IPC are not granted. The Seatbelt SPI
must be validated against the supported macOS runtime before publication.

Outstanding acceptance work: Windows AppContainer integration at G01's native
launch seam; cross-platform inherited-handle and root-identity fencing;
authenticated approval/role grants at all shell, executable-tool and process
entry points; the shared OS-backed conformance suite; remote compilation of
all affected targets. The current Windows adapter refuses confined execution.
The stock shell is not yet wired to this draft API. No cross-platform
confinement or performance qualification is claimed by this draft.

Idle grants share immutable policy only. Enforcement objects exist for active
executions, with no per-agent process, mount, timer, watcher, thread or daemon.
Landlock rule construction and seccomp installation buy filesystem, network
and escape enforcement. Seatbelt compilation/application buys the equivalent
native confinement. Custody remains the existing host-loss/retirement owner.
