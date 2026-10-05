# Opaque host capability boundary

`ExecutionResolutionCapability`, `SwarmUsageLimiter`, and `SwarmDispatchToken`
are native Harness authorization objects. They bind authenticated host state,
owner fences, operation identities, resource ceilings, and secret proofs. Rust
deliberately keeps these types opaque: they do not implement public JSON
serialization, and they are never accepted as model, provider, or WASM input.

The WASM and TypeScript public contracts carry only the data needed for
validation and provider dispatch. They must not expose the capability names or
credential fields such as an operator principal, a completed boundary digest,
or a workspace generation digest. A host adapter may retain these values in its
private native state and use them to authorize one exact operation; it must
not put them in a public JSON envelope, model-visible event, or generated WASM
declaration.

The negative contract test in
`typescript/packages/harness/test/opaque-host-capabilities.test.ts` scans the
generated WASM declarations, generated Harness JSON declarations, and the
public package entry point for these names and credential fields. A failure is
a public ABI leak and must be fixed by removing the field from the projection,
not by redacting it after serialization.
