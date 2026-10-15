# Real Cargo timing report excerpt

Source: SDK Release [38665239762](https://github.com/acyclic-labs/sdk/actions/runs/38665239762), source commit 9843fe0dbd1c74eb2098c1ae39904c359ced8e8a, job Build win32-arm64, artifact native-build-timings-win32-arm64-38665239762-1, file product-cargo-timing.html.

Toolchain: rustc/Cargo 1.98.1, x86_64-pc-windows-msvc host, aarch64-pc-windows-msvc product, dist profile. No local build generated this fixture.

Original report SHA-256: dea20ed991f933052ee186294dc7b9ae490b5dcfaad68b9164a0f110995062c4

This is a data excerpt, not a replacement for Cargo's interactive report. Keep the original summary table verbatim. Parse the original UNIT_DATA array, sort by descending duration and retain the five slowest records without changing their fields. Retain the first two and last two CONCURRENCY_DATA and CPU_USAGE samples. Serialize these arrays with JSON.stringify(value, null, 2), preserving Cargo's const declarations inside a script element. Omit the original CSS and plotting JavaScript. Unit IDs and dependency references remain the original IDs; omitted units are intentionally not reconstructed.

The real binary target label is ' acyclic "bin"', not the hand-written fixture's ' bin "acyclic"'. Its sections value is null, so no separate codegen or linker duration is established. The fs and objects library records contain observed frontend/codegen sections, including zero-duration codegen; these values are preserved exactly.

Refresh from an authorized Release carrying the repository's pinned toolchain. Download the artifact and apply the same extraction, then record the new run/source/hash here. Never fabricate unavailable phase boundaries.
