# Persisted event projection corpus

`event_projection` calls the public client `EventProjection` deserializer,
validation and selection methods, plus `EventCursor::wire`. Native headless
projection opens use these same methods; there is no consumer-only cursor.

The target compares decoded fields with the original input before checking
operation/version/opaque admission, source selection, selected agent identity,
execution advancement, refusal without mutation and persisted writer replay.
Opaque bytes are compared with the existing base64 codec; no second codec is
implemented. Original writer samples also have to decode, so rejecting every
input fails the oracle.

The kept control first captures eight samples from the original client serializer
and public state methods: empty/different operation, opaque cursor, resource and
execution positions, and explicit version/opaque corruptions of those bytes.
The resource name comes from the original AgentPath and stream resource codec.
Caller-chosen bytes exercise opaque copying; they are not recorded provider
continuations, and no provider-authorization claim is made. Empty input is an
authored mutation seed, not external output.

Capture uses `ACYCLIC_FUZZ_CORPUS` only in the unmodified writer process. The
control then removes the production operation comparison and requires the SAME
oracle's exact identity assertion to fail. Source restoration precedes the final
corpus replay. Run from the repository root:

```
python3 rust/crates/harness-fuzz/graphcoder-args-control.py event_projection
```

This target uses `--no-default-features --features client` to avoid native runtime
composition. It does not qualify file replacement, stream transport, multiparty
authorization, restart/upgrade or physical retirement. Existing real session
projection/drain tests remain. Replay edges are not complete source branch
coverage or a fuzz campaign; retain an actual source coverage report before
claiming those obligations.
