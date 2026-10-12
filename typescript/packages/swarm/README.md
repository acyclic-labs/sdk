# @acyclic-labs/swarm

Client for Cloud's public Swarm HTTP contract at
`0c900150c3c7cda2c7b02434838f6c410cd74018`, from
`managed-agents/crates/host/src/swarm/{routes,protocol}.rs`.
Request and response fields retain the server's JSON names.

```ts
import { SwarmClient } from "@acyclic-labs/swarm";

const client = new SwarmClient({ endpoint: "https://swarms.example", token });
const created = await client.create({ task: "Fix the test", repo }, { idempotencyKey });
let lastEventId = "0";
for await (const event of client.watch(created.swarm_id, { lastEventId, signal })) {
  lastEventId = event.id;
  console.log(event.data);
}
```

The client sends each request once. Keep the original idempotency key after a lost
create/upload response. Resume a disconnected subscription by calling `watch`
with its last delivered event ID. IDs stay strings, including values beyond
JavaScript's safe integer range. Breaking iteration or aborting its signal
cancels the subscription. The client neither reconnects nor cancels the swarm
automatically when an event stream ends.

| Method | Public HTTP operation |
| --- | --- |
| `health`, `healthEvents` | GET `/health`, `/health/sse` |
| `create`, `list` | POST/GET `/v1/swarms` |
| `upload` | POST `/v1/swarms/uploads` (gzip, required idempotency key) |
| `detail`, `watch`, `trace` | GET `/v1/swarms/{id}`, `/events`, `/trace` |
| `artifact`, `artifactFiles`, `artifactPatch`, `artifactTree` | GET `/v1/swarms/{id}/artifact`, `/files`, `/patch`, `/tree` |
| `memory` | GET `/v1/swarms/{id}/memory` |
| `cancel`, `openPullRequest` | POST `/v1/swarms/{id}/cancel`, `/pull-request` |
| `createToken`, `listTokens`, `revokeToken` | POST/GET `/v1/tokens`, DELETE `/v1/tokens/{id}` |
| `whoami`, `metrics` | GET `/v1/whoami`, `/v1/metrics` |

Operator callers may supply `org` and `user` for `X-Acyclic-Org` and
`X-Acyclic-User`; scoped tokens derive these from server authority. Token
administration and metrics require operator authority. Each call accepts a
signal and optional `traceparent`. `SwarmHttpError` retains the HTTP status,
server error payload and `X-Trace-Id`.

`artifactTree` returns the gzip `Response`, preserving its stream and the
`X-Acyclic-Files`/`X-Acyclic-Left-Out-*` headers. The other calls return parsed
JSON or text. JSON/text responses and individual SSE frames have a configurable
`maximumResponseBytes` bound (64 MiB by default). HTTPS is required except for
HTTP loopback; redirects reject instead of forwarding authentication.

Compile and run the fake-server tests:

```sh
bun x tsc -p typescript/packages/swarm
node --test typescript/packages/swarm/test/http.test.mjs
```
