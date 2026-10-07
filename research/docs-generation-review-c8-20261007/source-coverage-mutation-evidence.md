# Source coverage mutation regression evidence (2026-10-07)

The active CLI integration suite was run against the fresh binary built in the
shared `sdk-generation` target. The run passed 9 tests, including the new source
coverage cases:

- `crate_rust_comment_and_markdown_guide_edits_invalidate_output`
- `plugin_rust_edits_invalidate_output`

Each case generated a baseline manifest, changed exactly one included source,
then verified both stale-output paths: `drift` rejected the existing manifest
with `source digest changed`, and a fresh generation rejected the unchanged
Rustdoc source attestation before writing a manifest. The exact original bytes
were restored; `drift` then passed and a new generation succeeded. The refreshed
manifest's source hash was checked against the restored file hash.

## Fresh manifest snapshots

Base fixture refreshed output:
`C:/Users/varun/AppData/Local/Temp/sdk-generation-cli-regression-63496-10-base/refreshed-after-source-restore/generation-manifest.v1.json`

- `sourceSha256`: `sha256:1d6cc2905e983bfae296fa9e9994f365badcf66dbfeff28142c6f03bdb59a4ab`
- `Cargo.lock`: `sha256:a6302849064e016e520e513a22aef99a2d874333e7fcbf0b2c2260cb6ffb42f6`
- `Cargo.toml`: `sha256:cab6517ef5b0fecf37a42e36ff12bef2ad314f2725fe2182ccdbec9d708d8d73`
- `docs/guide.md`: `sha256:e60080a3087edeca35e34bec6e29b20bbb70fc6a29af8fe5bf715e50aa20a729`
- `release/cargo-crates.json`: `sha256:894703640f4980dc2a3048544359af75f002c0c58d56bdee6e74caa398f4be31`
- `rust/crates/demo/Cargo.toml`: `sha256:4c983f6e31c82e6dfbeb7af4c9ea0b1a2ba2701193dd69faf3e0654270777627`
- `rust/crates/demo/README.md`: `sha256:f627fd13c34fc64018784b4b7ca7d6c11509013861e343f477eb55248d86b467`
- `rust/crates/demo/src/lib.rs`: `sha256:849fb7c5fe4b817c3619220e032ae9053dba21f38e05aaaeefb35c8ed8ff8ec6`

Plugin fixture refreshed output:
`C:/Users/varun/AppData/Local/Temp/sdk-generation-cli-regression-63496-0-plugin/refreshed-after-source-restore/generation-manifest.v1.json`

- `sourceSha256`: `sha256:73f29b1d2311b9c0b104c807b100d4bed88259a7769b119cc5d9d4787033c70c`
- `plugin/src/main.rs`: `sha256:536e506bb90914c243a12b397b9a998f85ae2cbd9ba02dfd03a9e155ca5ca0f4`
- The plugin snapshot also contains all base paths and hashes above.

These tests validate actual CLI source inventory behavior for crate Rust,
crate Markdown, workspace docs Markdown, and plugin Rust. No collector or
profile implementation was changed in this task.
