# Actors live update (v1)

`live_update` owns the public contract used by uploads, deployment volume files
and host-bound call metadata. TypeScript imports it through `liveUpdate` from
`@acyclic-labs/actors`; its declarations are emitted from these same Rust types.
`encode`/`decode` and TypeScript's `encodeLiveUpdate` use canonical UTF-8 JSON
`{"v":1,"value":...}`. All 64-bit counters are decimal strings. Unknown fields,
noncanonical bytes and other versions are refused; there are no migrations.

Upload `Publication` through `DeploymentRequest::with_publication`. Its
`expectedVersion` is the last allocated publication, independently of the code
currently selected. Cloud atomically admits the predecessor and immutable
publication identity. Each namespace artifact binds code, full configuration
and public schema digests. `CompatibilityManifest` declares RPC request/reply
schemas, message schemas, deployment-scoped peer ranges, readable state and
original-journal codecs, durable await support, and explicit application adapter
artifacts. An absent upper peer bound is unbounded. SDK validation checks the
declaration; Cloud must prove bidirectional compatibility against every reachable
retained inbox, turn, receipt and pinned or hibernated peer, including rollback.
An adapter is authenticated code, not a guessed schema conversion.

`Selection` is a version split at a deployment epoch. The default `Pin::Actor`
uses the canonical complete Actor address; `Pin::Cohort` uses the owner-admitted
cohort ID across namespaces and hosts. Cloud serializes membership and future
child enrollment with the selection. Selection hashes the domain
`acyclic.actors.pin.v1\0`, length-prefixed account and deployment name, then a
pin-kind byte and pin bytes. The first eight digest bytes form a big-endian
unsigned rank. A rational wave selects `rank * denominator < numerator * 2^64`,
so advancing a wave retains a stable prefix and never splits one cohort.
No Actor address changes. `promote` and `rollback` produce a `SelectionChange`
conditional on the previous epoch; every change takes the next epoch, even
when it selects older code. Neither rollback nor promotion rewinds retained data.

`CallEnvelope` identifies the original caller deployment, pin, epoch, immutable
version and schema contract. RPC requests/replies and durable `Invocation`
records retain its canonical bytes; `CallEnvelope::message` binds it to an
ordinary Stream message header without changing the payload or indexed keys.
Forwarding retains the original envelope. External callers have no deployment
envelope; that absence is not epoch zero. Cloud supplies and authenticates
metadata for deployment traffic, refuses missing/forged metadata there, and
checks the actual caller/callee compatibility relation before guest entry.
Supplying these serializable values never grants execution authority.

`Handler::handoff` defaults to unsupported. A committed `DurableAwait` names the
exact continuation and the pending original journal operations; completion
stays with those original producers and stable operation identities. An
arbitrary in-memory await or non-yielding handler cannot claim zero-pause
cutover. Cloud must suspend old continuations and effect admission, and resolve
or durably hand off in-flight intra-cohort work before one logical cohort cut.
Physical adoption remains lazy. `VersionedHandler` validates and shares the
artifact context once per host/version. `FencedHandler` composes that Handler
with the admitted selection Stream head. The shared local/hosted runtime puts
this condition on successful and failed delivery publications, atomically with
consumption, checkpoint, effects and response. A conflict returns to the owner
without reentering old code; unknown publications retain their original request.
This publication fence does not itself prevent an external effect from starting.

Cloud materializes `deployment/versions/`, `deployment/selection.json`,
`deployment/selection.request.json`, `deployment/rollout-policy.json` and
`deployment/rollout.json` in the deployment volume. Controls are conditional file
writes; progress and guard decisions are ordinary live file queries or Stream
follows from a retained cursor. `RolloutStatus` includes lazy wave paging,
adoption, directional deployment/version-pair aggregates, guard decisions and
the fresh rollback epoch. Zero traffic is missing coverage, not a passing guard.
No per-Actor metric labels, scan, periodic probe or bespoke status RPC is needed.

`tests/live_update.rs` checks cyclic traffic, retained outcomes and late success
and failure fencing using real MemoryStream and LocalStream providers and event
channels. This is SDK substrate evidence. The full multi-member local-cell
acceptance requires Cloud's upload gate, membership/admission authority,
quiescent handoff and real guard; [Cloud #7868](https://github.com/acyclic-labs/cloud/pull/7868) describes those prerequisites.
That acceptance must additionally retain every original request/receipt across
rollout and fresh-epoch rollback and prove no loss, duplication or reordering.
The SDK fixture cannot substitute for that service acceptance or establish
physical external-provider exactly-once behavior.
