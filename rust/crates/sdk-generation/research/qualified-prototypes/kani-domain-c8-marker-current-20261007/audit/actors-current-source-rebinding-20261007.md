# Actors current source rebinding audit — 2026-10-07

This records source identity only. It does not retag any historical Kani result or assert a new proof.

## Current checkout observation

- Checkout: `Q:/sdk/work/sdkgen-actors-c8-minimal`
- HEAD at audit: `1ac5b398644d9d7056a95cd579c6a5315dd98db8`
- Production `rust/crates/actors/src/domain.rs` SHA-256: `361C8584C3C9C5A4E1974B97B9341226D4A2C816A1E1684D18A3BE5D8BFDB257`
- Current proof module `rust/crates/actors/src/domain/kani_proofs.rs` SHA-256: `FC6C30F64E9056E8D1C80A5CB15AF90824B00A301F68D99DACF1ACE388F4CD57`
- The checkout has an uncommitted proof-module modification; production `domain.rs` has no reported working-tree modification.

The production `domain.rs` hash matches the exact signed source inventory used by the signed `1269511d636b8ceeea1604a23919a9c1448241fa` qualification and bounded direct-production receipt (`361C...`). The current proof-module hash differs from each historical proof-module hash, so this observation does not retag those receipts or establish a new result. Future ReadonlyBytes/factory changes require rechecking `domain.rs` and binding a new receipt if its hash changes.
