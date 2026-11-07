# Signing custody

Every native consumer uses one shared protocol: each OS account has exactly
one credential entry, a random 32-byte signing root (service
`acyclic.authority.signing-root.v2`, account `root`). An owner's signing seed
is derived from that root and the owner's public reference with
`blake3::derive_key`; the reference is a random owner nonce plus a commitment
to the root. Creating owners never adds entries, and an abandoned owner leaves
nothing to retire. Production defaults to native custody: Windows Credential
Manager, Apple Keychain, or Linux Secret Service when a session bus is
configured, otherwise the kernel keyring. Failure never changes the selected
store or generates replacement authority.

For hosts without an interactive credential service, explicitly select
`ACYCLIC_SIGNING_CUSTODY=private-file` before starting the application. The
root then lives in `acyclic/signing-root.v2/root` under the application
configuration directory. `ACYCLIC_SIGNING_CUSTODY_DIRECTORY` optionally
selects an absolute configuration directory for this policy. It is invalid
with native custody. Keep this choice and directory with the original state.
A missing or replaced root refuses bootstrap; changing policy does not migrate
or copy authority. Repository scripts that launch shipping binaries in
temporary workspaces select private-file custody inside their own scratch
directory, so they never touch the account's native store. Test-support
builds use the existing fixture custodian.

File custody explicitly relies on filesystem ownership and access control
rather than the OS secret service. Directories are private (0700 on Unix); the
root file is private before the first write (0600 on Unix, protected
current-user DACL on Windows). Held-file validation refuses public access,
foreign owners, symlinks and hard links. Exactly 32 bytes are admitted, and
the root must match the reference's commitment. Secret buffers are cleared
after bootstrap and failure. Owner records contain only the public reference.

The root is created once, if absent, and adopted when present. Native stores
overwrite on write, so the first creators serialize on one lock file in the
account's data directory; the file store installs without replacement and a
losing creator adopts the winner. Reopening takes one read and no lock, write,
flush or listing. Creation adds one write and one verification read, once per
account. There is no polling, retry or asynchronous producer.

`CustodyFailure` preserves the failing operation and `CustodyCause`, including
native numeric statuses for unavailable providers, absent stores, missing user
sessions and access refusals. Apple interaction-not-allowed is reported as
`InteractionRequired(-25308)`: that status alone cannot prove whether the
keychain is locked or its prompt is unavailable. Other statuses retain their
exact number instead of being collapsed into an unknown cause. Credential
blobs and malformed secret encodings never enter diagnostics.

Local harness state stores its owner record as `owner.v2`. A state directory
that still holds the v1 `owner.json` is refused with `local state at <dir>
uses an older format; remove that directory to start fresh`; nothing is
migrated.
