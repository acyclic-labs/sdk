# Signing custody

Each durable native store owns one independent random 32-byte signing seed.
Its versioned public reference commits to that seed and the selected custody
namespace. Reopening reads the original seed; missing, substituted or
wrong-store custody is refused before authority bootstrap. No replacement
key is generated for an existing owner.

Production selects native custody: Windows Credential Manager, Apple
Keychain, or Linux Secret Service when a session bus is configured, otherwise
the kernel keyring. The native service is
acyclic.authority.signing-seed.v2. Each store has its own opaque entry.
Failure never changes the selected provider. Existing legacy entries are
neither read for migration nor removed by this protocol.

For a host without an interactive credential service, explicitly select
ACYCLIC_SIGNING_CUSTODY=private-file. Private entries live under the
application configuration directory; ACYCLIC_SIGNING_CUSTODY_DIRECTORY
selects an absolute directory for this policy. It is invalid with native
custody. Keep this choice and directory with the original state.

File custody relies on filesystem ownership and access control. Directories
are private (0700 on Unix); entry files are private before the first write
(0600 on Unix, protected current-user DACL on Windows). Held-file validation
refuses public access, foreign owners, symlinks and hard links. Exactly 32
bytes are admitted and checked against the public commitment. Secret buffers
are cleared after bootstrap and failure; owner records contain no seed.

A provisional entry is retired if owner publication is abandoned. Its custody
transfers at the actual durable owner-record installation, including a lost
acknowledgement. Explicit store deletion removes that owner's credential
before its payload and removes the owner record last. Interrupted payload
cleanup retains the owner record for retry; after record removal, retry only
removes an empty directory. Removing files outside the store-deletion
operation bypasses credential cleanup; there is no filesystem deletion watcher.

SigningStore is the credential injection boundary. Tests use the zeroizing
MemorySigningStore, and test/test-support builds cannot construct a native
credential backend. Automatically temporary stores retain memory custody
through their existing resource ownership anchor. Shipping-binary fixtures
explicitly select private-file custody under their own scratch directory.

The agent journal's persisted policy permits read-only consumption of published
top-level `inputs/` files, published `inputs/mentions/` manifests and copies,
pinned `inputs/skills/*/SKILL.md` bodies, conversation
documents and tool results. Private epoch manifests remain under host custody,
including for broad journal readers holding their file reference. Reading an input still
requires its original file grant and the caller's ceiling; an exact reference
does not grant another version or file. Input publication grants no directory
listing or writes. Other journal control data requires host custody even when
the caller holds a broad journal capability.

Host journal admission checks the current kernel policy for its journal and
selected retained journals it administers, including after reopening. Read-only
foreign sources retain their owner's policy and ordinary read checks. Delegated
source administration preserves an existing agent binding and source ceilings.
A differing owned policy requires authenticated write and administrator authority
and uses the ordinary fenced workspace publication. The original input references and
bytes stay fixed. A matching policy publishes no generation and does not require
a source writer. The journal format is unchanged; no older-format reader or
conversion is involved.

CustodyFailure preserves the operation and CustodyCause, including native
numeric statuses. Apple interaction-not-allowed is
InteractionRequired(-25308); it cannot distinguish a locked keychain from an
unavailable prompt. Credential contents never enter diagnostics.

The current owner.v14 record admits external native preparation, host-owned
journals, pinned hook configurations, frozen manifests and context delivery
frontiers with read-only selected inputs, retains the same store-owned seed
reference and v1 actor records, and excludes native-only opaque subtrees from
captured generations. Native skill-source metadata is a versioned indexed
record in the original owner's Streams state. Settings decisions, including
invalid and rejected startup, are versioned indexed records in that same owner
state. Ordinary bounded reads and follows expose the decision query without
creating an agent root or publishing private admission files. Lifecycle markers
and outcomes
are in the host journal. There are no native source-metadata sidecars or
private hook-outcome readers. If that record is absent but owner.json,
owner.v2, owner.v3, owner.v4, owner.v5,
owner.v6, owner.v7, owner.v8, owner.v9, owner.v10, owner.v11, owner.v12 or owner.v13
exists, both opening and explicit deletion return Error::Conflict with:
local state at <dir> uses an older format; remove
that directory to start fresh. Refusal happens before decoding the older
record or accessing credentials, and leaves its bytes unchanged. There is
no compatible reader, migration, adoption, or silent key regeneration.
This refusal precedes filesystem attachment, lazy discovery and checkout
capture: a v3 owner can retain interrupted v5 target/backup data inside the
checkout, and a v4 owner can retain interaction records in an authored volume.
Owner v5 uses the earlier journal policy. Owner v6 retains private hook
configuration. Owner v7 has journal-pinned hooks and a private delivery
frontier. Owner v8 has a journal delivery frontier and a private frozen
manifest. Owner v9 pins all three in the host journal. Treating older state
as v9 could discard recorded hook settings, lose or repeat context changes,
or select a different frozen prefix.
Finish or discard that recovery using its original runtime before
starting fresh. A former recovery basename in a workspace with current state
is an ordinary authored name; admission never probes or filters it.

The journal and authored-volume authority boundaries are described in
[volume composition](volume-composition.md#authored-path-authority).
