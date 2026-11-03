The `capability/public-certificates` and `capability/lineage-chain` captures
retain the original v1 writer bytes as unsupported-format inputs.

`capability/v2-signed-capability` and `capability/v2-attenuated-capability` are
byte-for-byte copies of the corresponding `rust/codec-fuzz/corpus/auth` captures
produced by `rust/codec-fuzz/examples/capture.rs` on 2026-10-31. They use the
original `testing::root` and `VerifiedCapability::attenuate`/`encode` producers
with deterministic SDK test keys. Their source provenance and exact bytes are
also recorded in `compatibility/frames/golden.hex` and the shared corpus
manifest. The contract test checks current admission and historical refusal
without changing either captured format.

The `addressing/{actor,resource,resource-hostname,canonical-alabel-hostname}`
seeds are byte-for-byte copies of `rust/codec-fuzz/corpus/identities` captures.
`corpus/SOURCES.tsv` in that shared project records their lengths, SHA-256 and
original producers: `ActorReference::encode`, `ResourceIdentity::encode`,
`ResourceIdentity::hostname` and `CanonicalHost::parse`.
They exercise the binary identity and plain hostname inputs consumed directly
by this target. Shared `route-*` captures include a NUL-separated URL context
for the shared route oracle; that tuple is not an input format of this target.
These original captures use public deterministic SDK identities. Copying them
does not establish a new campaign, branch-coverage or mutation-test result.

The `credential` target and shared `auth` target use the same
`acyclic_auth::credential::testing::check` oracle as the credential property.
It checks the real provider's admission and immediate completion, preserves
the original bytes through the bearer writer and requires injected transport
delimiters to fail. The original `auth/v2-opaque-bearer` shared capture remains
owned by `rust/codec-fuzz/examples/capture.rs`. Its byte-for-byte copy in
`credential/v2-opaque-bearer` is also admitted by the credential property, so
ordinary property validation exercises the retained real-writer input.
