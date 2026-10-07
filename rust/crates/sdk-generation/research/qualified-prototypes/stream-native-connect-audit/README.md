# Installed native connection cancellation

Run `node run.mjs ABSOLUTE_BINDING_CJS_PATH` against an extracted package's
generated native binding. The probe records the actual loaded `.node` path and
SHA-256 from Node's module cache, reaches a stalled TLS endpoint, cancels through
the generated Rust handle, and requires a typed cancellation result, no remaining
accepted sockets, no unhandled errors, and exit zero.

The fixture drains ClientHello bytes with `socket.resume()` without sending any
TLS response. A paused Node socket retains unread data and delays EOF/close
observation, producing a false cleanup failure.

On 2026-10-07 the coordinator measured:

- Archive: `stream-assembly-input-4de1458/acyclic-labs-stream-0.2.0.tgz`,
  SHA-256 `7e595289d8ba3eccb4a0569a9bf78881fdcd3b83bd58732366c6821eb25b7221`.
- Extracted native artifact:
  `f9382db8dbfbcb1fcd9b1deb651cba2f99eb51ea0c15edb4678b7f3c5eb9fd08`.
  The corrected probe passed against these actual loaded bytes.
- The earlier review scratch installation instead loaded
  `931d92546e5b94e2a2d7ed690f58ce57cdb67201902ace17d6c487c9dd361518`,
  the older lazy connection artifact. Its direct connect returned a client
  before the fixture accepted a connection.

The final candidate must run this probe and the public default-provider probe
against its own freshly installed archive, with archive/install/native hash
equality checked before and after execution. Credential admission, healthy
channel reuse, browser fallback, and operation tests remain separate gates.
