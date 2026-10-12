# Identity client contract

The customer CLI calls the public Edge origin from `ACYCLIC_API_URL` or
`--endpoint`. It refuses redirects and accepts cleartext HTTP only on loopback
for fake-server tests. The contract follows the published Global handlers on
`acyclic-labs/cloud` branch `codex/c5-identity-06-orgs`.

Credential files reject terminal symlinks and, on Unix, hard links. Directory aliases resolve to one canonical parent before credential locking.

## Device flow and sessions

All POST bodies are JSON objects, as required by Global's `readBody`.

- `POST /v1/identity/device/code`: `client_kind` (`cli` or `mcp`) and a PKCE
  S256 `code_challenge`. The verifier is a random 32-byte base64url value,
  retained privately until polling completes. The response contains
  `device_code`, `user_code`, `verification_uri`, optional
  `verification_uri_complete`, `expires_in`, and `interval` in seconds.
- `POST /v1/identity/token`: device grant type
  `urn:ietf:params:oauth:grant-type:device_code`, `device_code`, and
  `code_verifier`. Refresh sends `grant_type=refresh_token` and `refresh_token`.
- Token responses contain `access_token`, `refresh_token`, `token_type=Bearer`,
  and `expires_in`. Non-2xx responses contain `error`.
- `authorization_pending` repeats at the advertised interval; `slow_down`
  increases the interval by five seconds. Other errors end polling.
- `acyclic whoami` calls `GET /v1/identity/session`; `acyclic orgs` calls
  `GET /v1/identity/orgs`. Both use a session bearer and return the server JSON.
  Expiring or refused access triggers one refresh. Refresh durably removes the saved session before sending its rotating token. An interrupted refresh requires device login; the spent token is never replayed. If consuming the session fails, no request is sent.

## Fake-server validation

`cargo test -p acyclic-plugin --test identity` builds the native CLI and runs
the Node process suite with `ACYCLIC_TEST_BINARY` supplied by Cargo.
