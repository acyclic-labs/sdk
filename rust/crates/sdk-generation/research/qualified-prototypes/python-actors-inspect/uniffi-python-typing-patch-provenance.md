# UniFFI Python typing patch provenance

This file records the complete maintained `uniffi_bindgen` 0.31.0 source provenance used for Python typing qualification. The repository stores only the reviewed patch; generated Python modules, native libraries, wheels, Cargo targets, and virtual environments stay in the external `Q:\sdk\work` qualification area.

- Upstream: `https://github.com/mozilla/uniffi-rs`
- Crate: `uniffi_bindgen` `0.31.0`
- Upstream source revision: `309762f55db3f0548194a9ceba3027fa64b18a93`
- License declared by upstream `Cargo.toml`: `MPL-2.0`
- Upstream `Cargo.toml` SHA-256: `4815E130078F06D8B48582024E71ED8C035D75FB78A492594E30F0CDCD41E8CC`
- Upstream source `Cargo.lock` SHA-256: `3611136BD98D2C6A42FCE794655AE15915999CF01D04FCD6AF347E4370CC36EC`

The qualification patch is limited to these maintained generator sources:

| File | SHA-256 |
| --- | --- |
| `src/bindings/python/pipeline/types.rs` | `D6435ECE1A9031BD7DA82D20FE6BB6B0AB7798744F2D5A0696C644F5D19F15F1` |
| `src/bindings/python/pipeline/modules.rs` | `68EED062E3B295E2258D0395B96A1612D6895B526F85F31A9DECBE9BE3A49C72` |
| `src/bindings/python/templates/EnumTemplate.py` | `6DF470CE51EC90E457D3E7D39785432476536F646F92441CF4C9E0F017899217` |

The reproducible source-only entrypoint is `run-uniffi-python-typing-patch.sh`. It takes `SDK_ROOT`, `OUTPUT_DIR`, and `NATIVE_LIBRARY`; all generated output and build state must be directed to caller-selected external paths. The runner applies the downloaded and patched source through Cargo's `[patch.crates-io]` mechanism and never edits the product crate or checks generated output into this directory.

The compact repository reproduction uses the crates.io archive rather than vendoring this source tree:

- Archive URL: `https://crates.io/api/v1/crates/uniffi_bindgen/0.31.0/download`
- Archive SHA-256: `4ED0150801958D4825DA56A41C71F000A457AC3A4613FA9647DF78AC4B6B6881`
- Reviewed patch SHA-256: `7D61A970CAF75CF684E6B5EEA4CEF46C60D0392F258CC8C78F19054853F763B3`

`run-uniffi-python-typing-patch.sh` downloads and verifies that archive, checks the embedded upstream revision/version/license, applies `uniffi-python-typing.patch`, verifies the three changed source hashes, and runs the generator with all output paths externalized.

The measured generator, native-library, and wheel outputs are bound by
`minimal-patched-generator-receipt.json`. That receipt records hashes and byte
counts observed at the external producer paths after generation and packaging;
it does not accept caller-supplied artifact hashes. The Windows, WSL, and ivar
installation and all-eight qualification records remain the separate installed
consumer evidence in `cross-platform-qualification-receipt.json` and
`windows-wheel-qualification-receipt.json`.


