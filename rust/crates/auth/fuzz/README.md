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
