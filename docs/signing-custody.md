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

CustodyFailure preserves the operation and CustodyCause, including native
numeric statuses. Apple interaction-not-allowed is
InteractionRequired(-25308); it cannot distinguish a locked keychain from an
unavailable prompt. Credential contents never enter diagnostics.

The store-owned seed reference is persisted only in owner.v3. If that record
is absent but owner.json or owner.v2 exists, both opening and explicit deletion
return Error::Conflict with: local state at <dir> uses an older format; remove
that directory to start fresh. Refusal happens before decoding the older
record or accessing credentials, and leaves its bytes unchanged. There is
no compatible reader, migration, adoption, or silent key regeneration.