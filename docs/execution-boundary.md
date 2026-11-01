# Native execution capabilities

The native process launch owner accepts explicit execution capabilities.
This milestone provides the Linux launch core and typed refusals on unsupported
platforms. Stock harness tool admission is separate follow-up work; existing
shell calls do not acquire confinement merely by using this library.

The authenticated host decides an `ExecutionGrant` after Auth and the existing
approval chain. A grant binds one workspace, read-only roots, workspace write
or read-only access, and network authority. It is not a model tool parameter,
environment override, additional configuration hierarchy or sandbox service.
The existing approval/decision files remain authoritative. No separate system
event path is introduced; lifecycle consumers use the shared session Notice.
Confined launches require an explicit environment (`env_clear` plus selected
variables) and an explicit workspace working directory. They refuse ambient
environment inheritance before allocating native enforcement objects, so host
secrets are not exposed by a command's default environment. Selection of any
explicit variable remains the authenticated host's responsibility.
Working-directory admission borrows the explicit absolute path, rejects parent
traversal and checks workspace membership without filesystem I/O or path copying.
Linux resolves that working directory beneath the verified workspace object
with `openat2`, refusing substituted symlinks. Preparation holds the directory
through the original launch; subsequent path replacement cannot select a
different object. The direct child uses `fchdir` instead of resolving the path
again. Custodied launches reuse their existing working-directory descriptor
role and omit the redundant path string; the decoder refuses a confined
request that also selects a path-based directory. Descriptor count, warden
ordering and the single request/acknowledgement remain unchanged.

`ExecutionGrant::attenuate` permits narrower workspace/read roots, removal of
writes, and narrower networking. It refuses an increase of authority. Linux
resolves narrower roots with `openat2` beneath the verified parent object,
refusing symlink substitution and resolution races. Other platforms currently
refuse subtree attenuation until an equivalent native operation is available;
retaining the same verified roots can still remove writes or network authority.
Explicit read-root matches are verified before overlapping subtree proofs, so
retaining a granted root does not require subtree support from its ancestor.
`ProcessTree::spawn_granted` prepares active enforcement and enters the same
process creation, custody, capture, cancellation and retirement implementation.
`spawn_granted_controlled` borrows the original invocation's launch meter and
recovery slot. It refuses an occupied slot before any native preparation and
checks cancellation before preparing enforcement. Admission errors preserve
any acquired tree or custody record in that slot. Recovery remains a separately
admitted operation; the controlled entry performs no hidden cleanup. Original
control interruption remains interruption, rather than a definitive refusal.
Linux preparation charges each root open and identity query, device metadata, ABI
query, ruleset creation and rule insertion to that same invocation ledger.
Every operation reserves before the native call. Successful handles acquire
RAII ownership before a late recording failure is returned, so preparation
failure closes them without launching a child. No preparation wait is added.
Grant construction and remaining launch allocations
still require resource accounting; operation charging is not a complete
active-memory reservation contract.
The granted entry consumes the original `ProcessCommand`; ordinary command conversion refuses
to discard an attached boundary. Linux custodied launches carry the prepared
ruleset descriptor and native filter in their original launch request. Both
direct and custodied children call one allocation-free enforcement entry after
identity and standard-stream setup. Custodied enforcement runs only after the
warden has forked and admitted the final guest; the warden retains its original
custody authority. The child borrows filter bytes from the original request
mapping, with no extra policy allocation or copy.
The immutable grant owns its compiled seccomp filter. Reusing that grant for
another launch rebuilds no filter and allocates no filter storage. The direct
child hook retains the same grant owner until launch completes; the custodied
child continues to borrow its bytes from the original launch request.
Enforcement is inherited by descendants. A preparation or launch refusal is
`tool execution failed: <reason>`; it never retries without confinement.
Refusals have a concrete native error type. The shared capture completion
returns the refusal alongside its original custody; only a genuinely interrupted
control check has no process result. The ordinary tool renderer owns its single
failure prefix and preserves unauthorized or unsupported classifications.
Native host plugins continue to run their host's own loop and sandbox.

The original `StdioMcpTransport::spawn` also accepts an optional host-selected
grant. A supplied grant enters the same native launch boundary before the MCP
initialization exchange; refusal never reconnects without it. The existing
connection and process owner still retain the child through transport cleanup.
The command retains its host-selected stderr (inherited by default); confined
callers must select a descriptor admitted by the native boundary, such as null.
Piped stderr is refused before launch because this transport consumes stdout
only. A caller-owned stderr descriptor requires its caller-owned live consumer;
the transport creates no separate stderr reader or task.
Configuration-based MCP installation explicitly supplies no grant
until authenticated admission provides one. Server names, configuration and
tool arguments confer no execution authority.

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
Unclassified syscalls are denied by default, including System V shared memory,
message queues and semaphores, POSIX message queues and other externally named
process-control operations. Descriptor control admits only descriptor-local
state and file locking, and resource-limit changes name only the current
process. Common reads and writes return before the control checks. Port grants
reject `MSG_FASTOPEN` in each send API, including on kernels whose Landlock
implementation does not check that implicit connection path.
They also reject the `TCP_FASTOPEN_CONNECT` socket option: the
[kernel TCP path](https://github.com/torvalds/linux/blob/v6.12/net/ipv4/tcp.c)
can enter Fast Open from deferred socket state during ordinary writes.
No socket survives the initial exec fence, so blocking creation of that state
and explicit Fast Open sends seals both entry paths. Ordinary TCP options
such as `TCP_NODELAY` remain available.
The [kernel's Fast Open fix](https://kernel.googlesource.com/pub/scm/linux/kernel/git/david/linux/+/33cb713db0161b54f04fe830e062c9e102c29a04)
documents why a reported Landlock ABI alone does not establish that check.
Metadata changes are conservatively denied even in writable workspaces.
Read roots are explicit; host home, procfs, temporary directories and the
filesystem root are never implicitly added. `/dev/null` and `/dev/urandom`
provide null I/O and entropy. Only pipes and null may cross as stdio;
nonstdio descriptors become close-on-exec. Each Landlock root is opened and
checked against the original grant, then closed after its rule is inserted.
The kernel ruleset retains its own object references; the immutable grant
also pins the original roots. Preparation keeps only one temporary root
descriptor, with no root-descriptor vector or copied root collection.
The selected working directory is the one retained directory descriptor;
binding another root temporarily needs one additional descriptor. Selecting
the workspace itself reuses its existing open. A nested directory adds one
metered `openat2` and one temporary NUL-terminated relative-path allocation
and copy, with no extra metadata query. That buffer borrows the original
allocation guard and meter until the synchronous open finishes. Its operation
credit is reserved before allocation; allocation refusal refunds that unused
credit and records no native attempt. Capacity is reconciled with the actual
buffer and released after its physical destruction on success or error. No
reference-counted owner allocation, extra buffer or wait is added. This charges
the existing path buffer, not all remaining native launch storage. On the custodied
path this replaces the original open of the host working directory and removes
the child's second `chdir`. Direct launches replace `chdir` with `fchdir`.
Before launch, the shared native file primitive relocates either retained
setup descriptor out of slots 0–2 if the host had closed standard streams.
Otherwise child stdio installation could overwrite the ruleset or directory.
Descriptors already above those slots require no syscall or allocation.
Each relocation performs one metered `F_DUPFD_CLOEXEC` and retires its old
descriptor through RAII. Both handles have ownership before a late recording
failure, so refusal cannot leak either handle or continue to guest execution.
Implicit descriptor-close and complete active-resource accounting remain
outstanding; operation charging is not a claim that all cleanup is metered.
The two implicit computation-device rules verify the opened object's character
device type and device number, using the metadata already read during
preparation. A directory, ordinary file or different device at either name
refuses before rule insertion. The same type/number predicate checks inherited
null standard streams in both final-child adapters. This adds no native call,
allocation or copy. Device numbers follow the
[Linux device registry](https://www.kernel.org/doc/Documentation/admin-guide/devices.txt).
Root file types are bound with their identities at grant construction. A
pinned inode's type cannot change; launch reuses that type after checking
identity, removing one repeated metadata syscall per declared root.
Missing native facilities return a typed unsupported refusal before execution.
The public preparation and consuming launch entries use the same refusal
mapper, including failures in preparation or its invocation meter. Cancellation
retains its `Interrupted` classification. Refusals carry exactly one
`tool execution failed: <reason>` prefix.
Conformance checks verify that refusal and the absence of guest effects on
unsupported kernels; kernel enforcement requires a supported kernel.

macOS and Windows currently return a typed unsupported refusal before creating
a confined process. A path-based Seatbelt profile alone does not establish the
object-bound grant or fence inherited descriptors. No partial macOS adapter is
exposed as an enforced boundary.

Follow-up work: Windows AppContainer integration at the native
launch seam; macOS/Windows inherited-handle and root-identity race fencing;
authenticated approval/role grants at all shell, executable-tool and process
entry points; complete active-resource accounting; the shared OS-backed
conformance suite and qualification of all affected targets. The current Windows
adapter refuses confined execution. The stock shell is not yet wired to this
API. No cross-platform confinement or runtime performance qualification is
claimed by this milestone.

Native capture invocations can carry a host-selected `execution_grant` through
the existing owned completion, output retention and process custody. A selected
grant reaches the same consuming native launch; capture never retries a refusal
without it. Recorded output substitution cannot establish kernel enforcement,
so granted invocations always reach the native owner. This transport does not
issue authority: the authenticated host must select the grant after approval.
Existing stock shell admission still supplies no execution grant.

Idle grants share immutable policy and one native handle per declared root.
The handles pin object identities across deletion and numeric identity reuse;
cloning a grant adds no handles. Enforcement objects exist for active
executions, with no per-agent process, mount, timer, watcher, thread or daemon.
Landlock rule construction and seccomp installation buy filesystem, network
and escape enforcement. Custody remains the existing host-loss/retirement owner.
