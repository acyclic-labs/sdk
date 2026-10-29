# Signing custody

Every native consumer uses one shared seed-entry protocol. Production defaults
to native custody: Windows Credential Manager, Apple Keychain, or Linux Secret
Service when a session bus is configured, otherwise the kernel keyring. Failure
never changes the selected store or generates replacement authority.

For hosts without an interactive credential service, explicitly select
`ACYCLIC_SIGNING_CUSTODY=private-file` before starting the application. Seeds
then live in `acyclic/signing-seeds` under the application configuration
directory. `ACYCLIC_SIGNING_CUSTODY_DIRECTORY` optionally selects an absolute
configuration directory for this policy. It is invalid with native custody.
Keep this choice and directory with the original state. A missing original
entry refuses bootstrap; changing policy does not migrate or copy authority.
Standalone acceptance fixtures make this choice explicitly within their owned
settings directory. Test-support builds use the existing fixture custodian.

File custody explicitly relies on filesystem ownership and access control
rather than the OS secret service. Directories are private (0700 on Unix);
seed files are private before the first write (0600 on Unix, protected
current-user DACL on Windows). Held-file validation refuses public access,
foreign owners, symlinks and hard links. Exactly 32 bytes are admitted and
must match the opaque content-bound reference. Secret buffers are cleared
after bootstrap and failure. Owner records contain only the public reference.

The shared publication primitive stages one write and flushes only at commit,
then installs without replacement and synchronizes the parent. A provisional
owner retires only its own unpublished entry; actual installation transfers
custody even if a later acknowledgement is lost. Imports retain the canonical
cross-process identity lock. New entropy needs no identity lock.

The selected backend is probed once, with no candidate vector or fallback scan.
Reopen uses that observed value: one read, zero writes, flushes, identity locks
or listings. Creation uses one absence read, one write and one verification
read; the former loop could pay these costs in multiple stores. File commit
barriers remain at publication. There is no polling, retry or asynchronous
producer introduced by selection. Native service calls retain their OS progress
requirements.

`CustodyFailure` preserves the failing operation and `CustodyCause`, including
native numeric statuses for unavailable providers, absent stores, missing user
sessions and access refusals. Apple interaction-not-allowed is reported as
`InteractionRequired(-25308)`: that status alone cannot prove whether the
keychain is locked or its prompt is unavailable. Other statuses retain their
exact number instead of being collapsed into an unknown cause. Credential
blobs and malformed secret encodings never enter diagnostics.

Safety controls reopen committed custody, reject replacement and refuse public
permissions and aliases. A refused selected backend must not create a file
entry. Headless startup explicitly selects private files, reopens the original
identity and refuses changed permissions without producing another identity.
