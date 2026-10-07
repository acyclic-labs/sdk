# UniFFI version resolution

**Research date:** 2026-10-07  
**Registry snapshot:** the crates.io API was queried at `2026-10-07T01:47:34.818Z`. The review read registry metadata only; it did not download crates or change a lockfile.

## Decision

Keep the current generation cohort on **UniFFI 0.31.0**. Do not silently move the Rust facade, generated bindings, or generator tools to 0.32.x because a report calls 0.32.1 or 0.32.2 “latest”. The 0.32.x family is a forward qualification candidate and needs a complete regenerated-artifact and runtime qualification first.

The registry is the authority for the published crate version. At the snapshot time it reported 0.32.2, not 0.32.1, as the non-yanked maximum stable version for every queried UniFFI family crate:

| Crate | Max stable | Published at (UTC) | 0.32.2 checksum |
| --- | --- | --- | --- |
| `uniffi` | 0.32.2 | 2026-09-23T16:30:01.62697Z | `76407f5f396a2c949a069eff4a9fca9ce462410eb872bffef4c2b7b89804a77a` |
| `uniffi_bindgen` | 0.32.2 | 2026-09-23T16:28:58.867814Z | `3df3ced8f0eda3de99f6d50820e8628f443da8e4f447f477d1403ceba29dacbd` |
| `uniffi_core` | 0.32.2 | 2026-09-23T16:27:57.929744Z | `f7530ae8efeaaa488622865966763fe0294832779fff80508c36d69c6a874a6a` |
| `uniffi_meta` | 0.32.2 | 2026-09-23T16:27:54.449925Z | `af0c88664a9cb9559856f5a250283c9708923b303170e2e5c7db8f4ad65e9459` |
| `uniffi_pipeline` | 0.32.2 | 2026-09-23T16:27:49.576033Z | `d00f5c044b4a3dca0b1226c92ddaeea626f337470e22180dd182160c87a06b0b` |
| `uniffi_macros` | 0.32.2 | 2026-09-23T16:29:53.565411Z | `5b855a58da153739ce6e863f6e296edc3716a093768c4d3d957f17f5e4317c60` |
| `uniffi_build` | 0.32.2 | 2026-09-23T16:29:45.676975Z | `925be5c36297fff0d416fa2f03585ace718b6e6c635466754128d00f0862d5f1` |
| `uniffi_testing` | 0.32.2 | 2026-09-23T16:28:03.954371Z | `a15763458418c7534d47c98920c5a94cccb6b1083d96829536a9a02b81dbeaa5` |
| `uniffi_internal_macros` | 0.32.2 | 2026-09-23T16:27:43.888016Z | `0f4bad017164375d99450d70495014810d15a84ba9da6ed14b9628b1424223ba` |

This resolves the conflicting “0.32.1 latest” notes: those notes are stale relative to the registry snapshot. They must not be rewritten as a 0.32.2 adoption decision; the chosen cohort remains 0.31.0 until qualification is complete.

## Exact stable cohort pins

The maintained Go generator documents that it uses `uniffi-rs` 0.31.0 and instructs consumers to install the `v0.7.1+v0.31.0` tag. The exact source commits are:

| Input | Immutable reference | Commit |
| --- | --- | --- |
| Mozilla `uniffi-rs` | tag `v0.31.0` (annotated tag object `73f6aa1b0841198c271255f698cbcf2b76059f58`) | `309762f55db3f0548194a9ceba3027fa64b18a93` |
| NordSecurity `uniffi-bindgen-go` | tag `v0.7.1+v0.31.0` | `0b7fb4ceef12021bd7f790cc516fa9133e001813` |

The 0.31.0 registry records are non-yanked. The core package checksums used to audit a lockfile are:

| Crate | 0.31.0 checksum |
| --- | --- |
| `uniffi` | `b8c6dec3fc6645f71a16a3fa9ff57991028153bd194ca97f4b55e610c73ce66a` |
| `uniffi_bindgen` | `4ed0150801958d4825da56a41c71f000a457ac3a4613fa9647df78ac4b6b6881` |
| `uniffi_core` | `b0ef62e69762fbb9386dcb6c87cd3dd05d525fa8a3a579a290892e60ddbda47e` |
| `uniffi_meta` | `9df6d413db2827c68588f8149d30d49b71d540d46539e435b23a7f7dbd4d4f86` |
| `uniffi_pipeline` | `a806dddc8208f22efd7e95a5cdf88ed43d0f3271e8f63b47e757a8bbdb43b63a` |
| `uniffi_macros` | `db9d12529f1223d014fd501e5f29ca0884d15d6ed5ddddd9f506e55350327dc3` |
| `uniffi_build` | `b78fd9271a4c2e85bd2c266c5a9ede1fac676eb39fd77f636c27eaf67426fd5f` |
| `uniffi_testing` | `4802bed208a296657eef3451a961a6502e1d7aa8930b4b0cd952ed4f81a3957e` |
| `uniffi_internal_macros` | `98f51ebca0d9a4b2aa6c644d5ede45c56f73906b96403c08a1985e75ccb64a01` |

The `uniffi-rs` 0.31.0 changelog records a binding/runtime checksum change: checksums fail when bindings built with 0.30.x are combined with Rust code built with 0.31.x, or the reverse. That is direct evidence that the Rust runtime and generated bindings must be treated as one versioned cohort. It does not establish compatibility between 0.31.x and 0.32.x.

## Upgrade gate

An eventual 0.32.x change must be a separately recorded cohort:

1. Choose one exact `uniffi-rs` tag/commit and one exact version of every required UniFFI crate and external generator. Keep the Cargo.lock checksums and source commits together.
2. Regenerate every affected language output from the same Rust facade. Do not mix 0.31 generated output or Go `v0.7.1+v0.31.0` with a 0.32 runtime.
3. Compile and run native artifact tests for each language, including ownership, errors, async cancellation, streams, and the embedded/remote parity cases. A docs.rs page or a registry “latest” field is not runtime compatibility evidence.
4. Record the resulting generated-source and native-artifact hashes before changing the release cohort.

Until that gate passes, the only reproducible stable choice for this plan is the 0.31.0 cohort above.

## Primary records

- [crates.io `uniffi` registry record](https://crates.io/api/v1/crates/uniffi)
- [crates.io `uniffi_bindgen` registry record](https://crates.io/api/v1/crates/uniffi_bindgen)
- [Mozilla UniFFI 0.31.0 tag ref](https://api.github.com/repos/mozilla/uniffi-rs/git/ref/tags/v0.31.0)
- [Mozilla UniFFI 0.31.0 tag object](https://api.github.com/repos/mozilla/uniffi-rs/git/tags/73f6aa1b0841198c271255f698cbcf2b76059f58)
- [NordSecurity Go generator 0.31-compatible tag ref](https://api.github.com/repos/NordSecurity/uniffi-bindgen-go/git/ref/tags/v0.7.1%2Bv0.31.0)
- [Go generator README at the pinned commit](https://raw.githubusercontent.com/NordSecurity/uniffi-bindgen-go/0b7fb4ceef12021bd7f790cc516fa9133e001813/README.md)
- [UniFFI changelog at the pinned commit](https://raw.githubusercontent.com/mozilla/uniffi-rs/309762f55db3f0548194a9ceba3027fa64b18a93/CHANGELOG.md)
