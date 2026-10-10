# Harness client views

Pure, bounded hypotheses over demanded immutable authoritative references. The
kernel has no IO, authority issuer, domain reducer, journal, admission route,
retry policy, worker, network protocol or inverse-patch rollback.

`Client::new` installs a typed `Domain` and explicit finite limits. `observe`
consumes domain-validated evidence for one demanded key. `begin` creates a local
hypothesis; its dependency list expresses a fork from older hypotheses.
`view` explicitly selects overlays and returns provenance. `discard` removes a
hypothesis and invalidates its descendants without cancelling or undoing any
effect. `advance` applies monotonic host-supplied logical retention ticks.
`release` frees canonical demand after its hypotheses have been discarded.
Dropping the client drops all of its references and indexes.

The domain uses existing typed key, basis, operation, assumption, evidence and
projection types. Its basis must pin authority, generation, revision and content.
The adapter identity pins both the implementation and semantic revision; changing
it requires a new client. Prediction correspondence can require stronger evidence
than projection equality. Construction and sharing grant no authority.

```rust,ignore
// domain supplies the application's existing validated reducer/journal boundary.
let mut client = Client::new(domain, namespace, recovered_sequence, finite_limits)?;
let changes = client.observe(key.clone(), &trusted_evidence)?;
let branch = client.begin(Begin {
    key: key.clone(), basis: exact_basis, operation: original_operation,
    assumption: typed_assumption, predicted: shared_projection,
    dependencies: vec![], expires: logical_expiry,
})?;
let snapshot = client.view(&key, &[branch])?;
client.discard(branch)?; // hypothesis disposal, never provider cancellation
```

Run `cargo run -p acyclic-harness --example client_views --no-default-features`. The client primitive is `acyclic_harness::client`; its tests and examples use the existing Harness package and qualification infrastructure. This concrete public
consumer uses Harness `Reducer`, `ConversationMessage`, `FileRef`, original
`OperationId`, `Scheduler` and lease fences. Domain validation/reduction remains
in Harness. Its private in-process snapshot envelope represents a trusted host
boundary; it is not a remote authentication scheme. The example shows message
prediction alongside a concurrent authoritative message, provisional dequeue
of ready work losing to another worker's real lease, and a predicted tool result
invalidating a pure continuation input when canonical content differs. No model
or provider invocation occurs. Exact content/call linkage is required; visible
text equality does not imply tokenizer, runtime, model or KV equivalence.

Prediction and operation outcomes are independent. Admission does not confirm
a prediction. Indeterminate effects remain uncertain even if a delayed admission
arrives. A matching completed result confirms; a different result replaces the
overlay and invalidates descendants. Rejection/cancellation removes an eligible
overlay; invalidated hypotheses never resurrect on late completion. The real
operation remains recoverable through its original domain identity. Competing
overlays are explicitly ambiguous; there is no universal last-writer-wins rule.

`Hypothesis` is a data-only serde export with identity, adapter pin, original
operation, exact basis, assumptions, edges, prediction, expiry and both outcomes.
Export authorization and storage are host policy. Exporting or sharing never
creates a fact. There is intentionally no automatic import, replay, journal or
promotion: a future adapter must restore authoritative demand, validate current
pins, recover original operation identity and revalidate hypotheses. Namespace
uniqueness and atomic durable sequence checkpointing are host obligations. The
kernel never allocates operation identities or submits retries. Idempotent
effect admission and crash recovery continue to belong to domain journals.

Facts, basis pins and predictions hold `Arc` references; views copy references and
provenance, not histories. Exact duplicate basis observations retain the same
value identity. Caller-held snapshots survive reconciliation. Per-key and
per-operation indexes target affected roots; reverse edges traverse descendants
only. Old records are not retained after release. There is no automatic speculative
fanout. Confirmation leaves descendants valid until their pinned source changes;
invalidation requires explicit consumer reevaluation through a new `begin`.

Limits bound records, branches, edges, accounted bytes, mutation work, retention
and visible overlays. Adapter work and branch/edge visits are reported by
`Changes`; bounded map lookups additionally cost logarithmic time. Retention
maintenance scans only the bounded resident branch set. Byte accounting charges
adapter-declared deep allocations plus conservative sparse index metadata, not
allocator RSS. Adapters account cloned keys/operations and immutable projections;
external domain journals and caller snapshots have their own limits. Large bodies
remain content references. All fallible planning precedes publication, so explicit
Budget/Unsupported/Conflict/StaleBasis outcomes leave client state unchanged.
If correspondence is unavailable, callers can discard the unsupported hypothesis
and observe the evidence through a supported adapter; the kernel never guesses.

The generic kernel trusts the installed adapter's determinism, purity, finite
work, conservative sizes, evidence authentication, monotonic/conflict validation,
original-operation correlation and correspondence. Rust types do not prove these
obligations. There is no public `verified = true` escape hatch. A malicious adapter
can manufacture claims; this core is not an authority proof system. `advance`
must be called by the host for expiry; construction starts no timer. Local data
has no ability to dispatch dependent effects. Any real continuation must obtain
normal authoritative admission and recheck its domain assumptions there.

Qualification and simplification evidence is recorded in [QUALIFICATION.md](QUALIFICATION.md).
Framework bindings, transport/checkpoints, shared/durable adapters and inference
prefill/promotion belong to later CL2–CL5 owners.
