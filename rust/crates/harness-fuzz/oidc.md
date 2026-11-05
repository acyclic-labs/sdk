# OIDC signature admission

Run the public OIDC verifier with its original Dex key and ID token, at the
host-injected capture issuance time, then at the original expiry:

```sh
cargo +nightly fuzz run --dev --fuzz-dir rust/crates/harness-fuzz --no-default-features --features oidc oidc -- -runs=1000 -seed=1 -max_len=4096
```

`corpus/oidc/dex-jwks` is the unchanged public JWKS response in
`harness/fixtures/conformance/identity/dex/jwks.body`; `dex-id-token` is the
original `id_token` field of that capture's `token.body`. Its capture manifest
and README retain issuer, source, original response hashes and subject. These
expired disposable tokens are replayed against the injected original clock,
without contacting an issuer or changing signed bytes.

An internal ES256 vector uses the maintained P-256 signer and the captured
claim bytes; invalid curve-point replacement must retain its last-good key.
This generated cryptographic vector is not an external-server response.

Every invocation requires original signature/identity/expiry acceptance and
refusal of an actual corrupted signature. Arbitrary input reaches the compact
header, signature and claims decoders and all three pinned JWKS families.
Rejected refreshes must preserve the original authenticated snapshot. Existing
shared AgentHost conformance retains Ed25519 issuance, audience/issuer/subject,
rotation, stale-key, malformed replacement and zero-turn-I/O controls.

The retained negative control changes the real admission call to discard a
signature refusal. The same fuzzer must fail its unchanged signature-binding
assertion, then pass after byte-identical restoration:

```sh
python3 rust/crates/harness-fuzz/oidc-control.py
```
