# Implementation status

Base: 31b9ff52d63c91f2b9bf87e16b78ad682d26546f.
Branch: codex/graphcoder-sdk. No merge/publication.

## Completed foundation
- Shared versioned model-input admission in stock executor and live task dispatch.
- Ordered manifests bind message bytes, roles, file references, model and tool revisions.
- Aggregate input bounds reject overflow without truncation.
- Exact persisted model prefixes reject content/order/binding mutations and corruption.
- Fork-prefix capture rejects incomplete tool exchanges.
- Stock executor persists input manifests before model dispatch.
- Provider-capture integration verifies actual received input against the persisted manifest.
- Provider admission enforces pinned prefixes before dispatch and recovered attempts.
- Memory and persistent compositions share the same provider-neutral storage implementation.
- Persistent session descriptors pin identities, authority keys, model and limits.
- Reopening replays completed turns without model redispatch; changed prompt/configuration fails.

## Verification
- Existing Harness baseline: 176 passed.
- Shared-input integration: 181 passed.
- Filesystem-local suite after manifest and provider-capture integration: 195 passed.
- Durable local composition and prefix admission: 197 passed.
- None of these results qualify the complete swarm or terminal product.

## Next
Connect prefix enforcement to child execution and existing fork publication.
Extend durable composition with scoped swarm communication and git integration,
effect recovery, terminal app, and installed-artifact acceptance evidence.

The locked requirements matrix remains authoritative. No Cloud, web UI,
production models, migration or sandbox. Arbitrary host commands and root
writeback require approval; workspace routing is not process confinement.
