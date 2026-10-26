Enable `acyclic-harness/oidc` for cached verification and ordinary turn admission.
Native hosts can enable `oidc-http` for `oidc::HttpSource`; other platforms supply
one streamed GET through `JwksSource`. No identity server or account is required
by the harness.

The trusted host pins `Config::{new,loopback}` to an exact issuer, audience,
JWKS URI and one asymmetric algorithm (RS256, ES256 or Ed25519). Production
configuration requires HTTPS. `loopback` additionally permits HTTP on literal
127.0.0.1 or ::1. Token-supplied key URLs, embedded keys, critical extensions,
`none` and all symmetric algorithms are refused. RSA keys must be 2048–8192 bits.

Create a `Verifier`, refresh its keys outside request admission, then publish that
snapshot to request handlers. `verify(token, trusted_utc_seconds)` returns a
verified issuer/subject for the host's own policy lookup. An `AdmissionPolicy`
binds an exact subject to an already authenticated, agent-bound parent `Scope`
and its audience. `admit(token, &authority_root, &policy, trusted_utc_seconds)`
attenuates that parent through the existing issuer and returns
`TurnAdmission::Admitted`. Pass it to the existing `FilesystemTurnHost`; the
existing agent, volume, audience, revocation and expiry checks still apply.
Claims cannot select grants, agents, roots or a local-root fallback.

The minted scope expires at the earliest of token expiry, parent expiry and the
host's maximum lifetime. The default maximum is 900 seconds; hosts may narrow it
to 1–900 seconds. Token expiry is strict. Issuer, subject, audience, multiple-
audience `azp`, signature and optional `nbf` are checked before turn I/O. The
host supplies its trusted UTC clock, never a time supplied by the token caller.

Default bounds: 64 KiB JWKS, 16 keys, 16 KiB compact token, 30 seconds between
refresh attempts (including failures), a 10-second fetch deadline, one-hour
last-good key lifetime and zero `nbf` clock skew. Configuration permits bounded
changes: at most 1 MiB, 64 keys, 64 KiB tokens, one-day key lifetime and 30 seconds
of `nbf` skew. Refresh validates the complete replacement before publishing it.
Failed refresh retains the last-good snapshot without extending its lifetime;
a stale snapshot refuses admission. Unknown keys refuse without fetching.
Refresh uses the shared platform executor. The native HTTP source disables
redirects and retries and implements only one GET (44 physical lines).

Validation runs the unchanged agent-host suite, including identity vectors for
forgery, expiry, audience, subject, algorithm confusion, rotation/removal, stale
keys, bounded refresh and last-good preservation. Invalid tokens perform zero
turn-provider reads/writes and no executor calls; cached admission performs no
JWKS I/O. A disposable local Dex issuer additionally exercises real RS256
verification and admitted scope expiry against the same host implementation.

Run on bb2:

```powershell
pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/be-edge-oidc-verify.sh
```

Dex binds only 127.0.0.1:15482 (OIDC) and 127.0.0.1:15483 (telemetry), uses memory
storage and a public disposable test account, and stops on script exit. Raw
responses, hashes, capture time and container image provenance are retained in
`rust/crates/conformance/fixtures/identity/dex`. Its short-lived test token is
validated during capture; later executions start a fresh issuer and recapture.
This proves local integration; it does not qualify a particular production IdP's
configuration, discovery service, user interface or account lifecycle.
