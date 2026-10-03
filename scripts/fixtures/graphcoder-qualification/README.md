# GraphCoder package qualification fixtures

These scripts qualify a packed `@acyclic-labs/graphcoder` artifact from outside
its source tree. `host.mjs`, `negative-host.mjs`, and `epoch-host.mjs` are
protocol-only JSON-lines peers. They are deliberately fake backends for bridge
conformance and are not a production Harness, PersistentLocalSwarm, model
runner, filesystem, or worker implementation.

The consumer resolves every module through the installed package's
`package.json` exports. It does not import `src`, use a workspace alias, or
reach into `dist` by path. Prepare a clean directory with the packed artifact
and install it there, then run:

```text
node scripts/fixtures/graphcoder-qualification/consumer.mjs --root <installed-consumer> --host scripts/fixtures/graphcoder-qualification/host.mjs --log <request-log>
node scripts/fixtures/graphcoder-qualification/negative-conformance.mjs --root <installed-consumer> --host scripts/fixtures/graphcoder-qualification/negative-host.mjs --epoch-host scripts/fixtures/graphcoder-qualification/epoch-host.mjs
node scripts/fixtures/graphcoder-qualification/harness-model-consumer.mjs --root <installed-consumer-with-harness>
```

The fixture IDs are stable qualification references:

| ID | Black-box coverage |
| --- | --- |
| `PKG-NATIVE-01` | Installed package exports; native snake_case wire fields; summary-only listing and bounded `limit`; lazy activity, messages, approvals, changes, diff, and file reads; approval and writeback receipt; cancel/resume; terminal adapter commands. |
| `NEG-CANCEL-01` | A hung history request must not prevent an explicit cancellation from reaching the transport. |
| `NEG-EPOCH-01` | A canceled process request ID is quarantined so a delayed old reply cannot settle a later request with the same ID. |
| `NEG-PATH-01` | Empty, traversal, absolute, separator, NUL, and 4097-byte paths are rejected before the bridge; a 4096-byte relative path is the accepted boundary case. |
| `NEG-ERROR-01` | Malformed envelopes, bridge error-code retention, and structured terminal error output/status. |
| `NEG-APPROVAL-01` | Session identity on approval responses and operation/generation/session identity on writeback receipts. |
| `NEG-BYTES-01` | Invalid octets and a process response line over an explicitly configured 256-byte limit. |
| `NEG-IDENTITY-01` | Snapshot, page, and diff response identities are checked against the request. |
| `PKG-HARNESS-CONTRACT-01` | Installed `@acyclic-labs/harness/proto` and `/protocol` subpaths expose their generated descriptors and public message schemas. |
| `PKG-HARNESS-MODEL-01` | Installed `@acyclic-labs/harness` model dispatch retains provider/name/revision/options, including a full-width integer, in one frozen canonical request; request and manifest digests agree with the canonical bytes and differ when model identity changes. |
| `NEG-MODEL-PAIR-01` | An unpaired context tool call is rejected before a model provider callback runs. |
| `NEG-MODEL-BOUNDS-01` | An aggregate model context over the configured byte bound is rejected before provider dispatch. |

The negative host intentionally returns wrong identities or malformed values;
the expected result is a typed `transport` or `invalid_input` failure. The
line limit exercised by `NEG-BYTES-01` is the test's configured
`maximumLineBytes: 256`, while the path boundary is 4096 ASCII bytes. These
fixtures do not prove durable cancellation of a real worker, storage policy,
model behavior, production swarm orchestration, N-API/Cargo bindings, or all
possible native field combinations. `harness-model-consumer.mjs` uses a local
assertion provider only to observe the installed Harness boundary; it does not
qualify any production model provider. The installed Harness artifact must
include the generated native `prepareModelRequest` export; a missing export is
a qualification failure rather than a skipped model scenario.
