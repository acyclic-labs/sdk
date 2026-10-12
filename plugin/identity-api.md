# Identity client contract

The customer CLI calls the public Edge origin from `ACYCLIC_API_URL` or
`--endpoint`. It refuses redirects and accepts cleartext HTTP only on loopback
for fake-server tests. The contract follows the published Global handlers on
`acyclic-labs/cloud` branch `codex/c5-identity-06-orgs`, plus the agreed
per-public-key signup/renew idempotency extension.

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
  Expiring or refused access triggers one refresh. Refresh durably removes the saved session before sending its rotating token, retaining any agent key. An interrupted refresh requires device login; the spent token is never replayed. If consuming the session fails, no request is sent.

## Agent signup and upgrade

- `GET /v1/identity/signup/challenge` returns `challenge`, `difficulty` in
  leading zero bits, and `expires_at` in Unix seconds. Global advertises
  difficulty from 22 through 28; test servers can use lower difficulty.
- `POST /v1/identity/signup` sends `public_key` (raw 32-byte Ed25519 public key,
  base64url without padding), `challenge`, and `nonce` (decimal text).
  The body also includes an Ed25519 signature over
  `acyclic-agent-signup-v1\nPUBLIC_KEY\nCHALLENGE\nNONCE`.
  Hashcash is SHA-256 of `challenge.public_key.nonce`, requiring the advertised
  number of leading zero bits. The client stops at challenge expiry.
- The response contains `user_id`, `org_id`, `key_id`, `account_id`, `birth`,
  `certificate`, `tier`, and `expires_at`. The private PKCS#8 key is stored
  locally and never returned through CLI or MCP output.
- Signup accepts a new credential file or a pending agent key; it refuses active
  credentials. Before submission, the client saves the pending key. Re-running
  signup after a lost response reuses that key with a fresh challenge, proof and
  signature. Global is idempotent per public key: creation returns HTTP 201;
  retry returns HTTP 200 with the same receipt. The client only marks the key
  active after validating the receipt, preserving it after errors or invalid
  responses. No separate recovery route is used.
- `refresh` with agent-only credentials gets a fresh challenge and posts to
  `/v1/identity/signup/renew` with `org_id`, `key_id`, `challenge`, and an
  Ed25519 `signature` over `acyclic-agent-renew-v1\nORG\nKEY\nCHALLENGE`.
  Within Global's renewal window, retrying a lost response returns the same
  renewed certificate for that org/key.
- `login` with active agent credentials adds `agent_org_id`, `agent_key_id`,
  and a signature over `acyclic-agent-upgrade-v1\nORG\nKEY\nCODE_CHALLENGE`
  to the device request. Successful approval retains the agent key and stores
  the new OAuth session. Subsequent refresh rotates that session.

The published session and org endpoints accept OAuth sessions only. Agent
signup returns its registration directly; `whoami` and `orgs` require device
login. The client does not synthesize server identity reads from local files.

## Fake-server validation

`cargo test -p acyclic-plugin --test identity` builds the customer binary and
runs the Node CLI/MCP process tests with `ACYCLIC_TEST_BINARY` set by Cargo.
The fakes verify JSON requests, PKCE, hashcash and Ed25519 signatures. They
also exercise credential rotation, pending signup preservation and repeated
login after an agent upgrade. No live identity service is required.
