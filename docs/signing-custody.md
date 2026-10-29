# Signing custody

Generated signing seeds use the shared native-runtime custody entry protocol.
Native credential storage is preferred: Windows Credential Manager, Apple
Keychain, or Linux Secret Service followed by the kernel keyring. If native
storage is unavailable, new identities use `acyclic/signing-seeds` beneath the
application configuration directory (`XDG_CONFIG_HOME`, otherwise `~/.config`
on Linux). Existing keys remain in their original store even when native
availability changes. The application must retain its configuration directory
along with its state directory; a missing or inaccessible existing seed produces
a typed I/O failure rather than replacement authority.

The file adapter uses the same private-file primitives as other native owners.
Directories are created private (0700 on Unix); seeds are created private (0600)
before any bytes are written. Windows uses the shared protected current-user
DACL adapter. Reads validate the held regular file's owner, permissions and
single-link identity, refusing symlinks, hard links, foreign owners and public
access. Exactly 32 bytes are admitted, and their content must match the public
reference. Secret buffers are zeroized after bootstrap or failure.

A new entry is staged, written once and durably committed before the host
publishes its reference. The shared publication primitive flushes at commit,
then atomically installs without replacement and synchronizes the parent.
Reopening performs no write, flush, identity lock or directory listing. A
provisional owner removes only its own unpublished entry on abandonment;
observing namespace installation transfers custody even if the later durability
acknowledgement fails. Imports retain the existing shared identity lock.

Selection probes a fixed platform-defined set of backends; it never scans keys
or grows with history. An existing selected entry supplies its observed bytes
directly, removing the former repeated credential-store reads. Creation checks
one stored value after writing. A native store that can report absence but
cannot accept a write cannot prevent selection of file custody for a new key.
Each backend is visited at most once; there is no polling or retry loop waiting
for a keychain. This adds no asynchronous wait; native service calls and import
locks retain their platform progress requirements.

Safety controls exercise private-file publication and reopen, reject replacement,
and refuse public permissions and symlinks. The headless startup control runs the
shipping GraphCoder with no session bus and denied kernel-keyring syscalls,
reopens its original state and checks the resulting seed's 0600 mode without
reading its contents. Changing that file to 0644 must refuse startup without
generating replacement custody; restoring 0600 must reopen successfully.
