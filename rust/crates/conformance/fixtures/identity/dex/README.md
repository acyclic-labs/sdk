Captured on bb2 at 2026-10-26T21:24:55.592659+00:00 from a real disposable
Dex v2.44.0 server, using `scripts/be-edge-oidc.sh`. The image ID is recorded
in `image.txt`; its pulled manifest digest was
`sha256:5d0656fce7d453c0e3b2706abf40c0d0ce5b371fb0b73b3cf714d05f35fa5f86`.

The server bound only 127.0.0.1:15482, with telemetry on 127.0.0.1:15483,
and used in-memory storage. Its account, password and client secret are public
disposable test inputs declared in the script. No production credentials or
private signing keys were captured. The server stopped and its container was
removed after qualification.

The four response bodies are unchanged HTTP output. Headers and the two public
test token requests accompany them. `manifest.json` records original URLs,
statuses and body SHA-256 hashes. Its subject records Dex's actual subject for
the configured disposable account. Token output is short-lived and expires;
reruns start a fresh server and capture fresh output, rather than skipping expiry.

bb2 job `1793049881274-118323-345abc30`, client log
`1793049864792-118323-8ed2763b.log`, source
`672da92f5eab72e2a116a45474afdf926a3230f0` plus the runner/documentation change
retained in `f192f1fa86`: the CLI's complete host suite, real RS256 verification,
ordinary parent-bound turn admission and exact scope deadline all passed in
2403 ms. Compilation (2m06s) is outside suite timing. The separate complete
native and guest-wire host run passed 2 tests / 0 failed / 0 ignored / 0 filtered
in 2.96 seconds, job `1793049520298-116676-d405e08e`.

The portable identity vectors generate real Ed25519 signatures with public test
seeds and deliberately mutate tokens/keys for negative controls. Those internal
cryptographic vectors are separate from these captured external-server responses.
They include forgery, expiry, audience, subject, algorithm confusion, key rotation,
removed/stale keys, malformed replacement, bounded refresh and zero-read refusal.
