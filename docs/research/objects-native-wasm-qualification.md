# Objects and Stream Rust bridge qualification

This record covers the Rust-owned Objects browser bridge and the Rust-owned Stream
browser/native bridge. It records executable checks only; a loader fixture is not treated
as a compiled platform binary qualification.

## Executed checks

From the isolated SDK checkout:

```text
cargo check --manifest-path rust/crates/objects-wasm/Cargo.toml --target wasm32-unknown-unknown --locked --offline
cargo check --manifest-path rust/crates/stream-wasm/Cargo.toml --target wasm32-unknown-unknown --locked --offline
cargo check --manifest-path rust/crates/objects/Cargo.toml --locked --offline
cargo test --manifest-path rust/crates/objects/Cargo.toml --locked --offline --lib
node scripts/build-objects-wasm.mjs C:\Users\varun\AppData\Local\Temp\acyclic-objects-wasm-20261004001352
node scripts/build-stream-wasm.mjs C:\Users\varun\AppData\Local\Temp\acyclic-stream-wasm-20261004001352
```

The native Objects library passed its check and 18 unit tests. Both standalone WASM
binding crates compiled for `wasm32-unknown-unknown`. Both pinned wasm-bindgen scripts
completed with wasm-bindgen `0.2.117`.

The generated browser wrappers were then initialized in Node from their temporary wasm
bytes. The smoke consumer exercised `objects_v2_http_type`, HTTPS and loopback endpoint
validation, `ObjectsV2Memory.invoke("buckets/create", ...)`,
`WasmMemoryStream.dispatch("append", ...)`, `dispatch("tail", ...)`, and Stream path
validation. It passed with:

```json
{"objects":"ok","stream":"ok","appendBytes":40}
```

The temporary generated wasm hashes were:

```text
Objects acyclic_objects_wasm_bg.wasm 134A3E262637103B147FBC2141BF9594C770CC20D6528DB73B2544FAD0500E4E
Stream  acyclic_stream_wasm_bg.wasm  5B38C68F765DE77D240D32D34B23D70C1366EF40FD5B0FFF9A26216CE430CD2D
```

## Source and packaging boundaries

`acyclic-objects-wasm` directly depends on `acyclic-objects` with `default-features =
false` and the Rust `json` feature. Its exported `ObjectsV2Memory` and validation helpers
call the canonical Objects provider and wire validators. The Objects packaging script
builds this crate directly.

The generated Stream browser package is built by `scripts/build-stream-wasm.mjs` from the
`acyclic-stream` crate with `--no-default-features --features wasm`. Its source is
`rust/crates/stream/src/wasm.rs`. The separate `acyclic-stream-wasm` crate also compiles
for wasm32 and contains a Rust binding surface, but the packaging script does not build
that crate or produce a package from it. Therefore its successful `cargo check` is a
compile qualification only; it must not be presented as the source of the generated
Stream browser artifact without changing the packaging workflow.

The native Stream npm directories contain six platform metadata and loader fixtures. The
fixture test now checks each package name, version, license, OS/CPU restriction, main
entrypoint, binary name, and exact loader. The installed-package TLS runtime qualifies
Windows x64 only; the other five directories remain loader-shape fixtures until matching
`.node` binaries are built and exercised.

## Source fingerprints

These fingerprints identify the Rust inputs reviewed in this run:

```text
rust/crates/objects-wasm/Cargo.toml 59D0B4213B8987E76F2185750A0EA759DE847F102CA3595BB1262E937AC93A4C
rust/crates/objects-wasm/src/lib.rs F744A336F6A8090AFCD196BA578667EF0026C53904A4AF4094759A5A3860962E
rust/crates/objects-wasm/src/v2.rs 77E7A531F7D1F159D0C5164A38D3347E2D9C7FAAB34CD173BC454E7AACE222BF
rust/crates/objects/Cargo.toml 3C1565F041931D5F479CC74FCC9B3E2CBE722CDC15A46A9393FDE78B740EE732
rust/crates/stream-wasm/Cargo.toml 52F4571F17E826B7C8582897AEEEAB502C551CFBAAF1763A00DE7451C0724FA6
rust/crates/stream-wasm/src/lib.rs F0CFA19825EE2C121C38512BE0A52BC96447B5344A0BF3BD7057ED2BA1DAC649
rust/crates/stream/Cargo.toml C982FD458187602493D8ED82DE3BBFCA76ED2DFC5ED41BC4B3BD4F0F103969B0
rust/crates/stream/src/wasm.rs 00B4A7E4EE5122000158EFD15148F3A36665EBF5B342A46E9F49428765DAE856
rust/crates/sdk-stream-native/Cargo.toml C6A81D30F47D3F5DF93C4842E6080F3C62AD6B9A328E7EEEDD6F3512FBA00449
rust/crates/sdk-stream-native/src/lib.rs 0CB559A1EB77A1C0E35B7C2EE525F5E0BB973261714B084ACB599E922B461E65
```
