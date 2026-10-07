# SDK generation after-fix regression review (2026-10-07)

Checkout: `C:/Users/varun/.codex/worktrees/rust-profile-c8-port`, HEAD `c8bef0be476cbfd5d4fa30d1f8d017fa5336d964`. This is an evidence-only review; no shared production source was edited.

## Current CLI results

A fresh relocated fixture with Rustdoc format-60 JSON and a producer sidecar `demo_docs.source.sha256` was run through the actual `sdk-generation.exe`:

- Preview generation with the matching source attestation succeeded: `Q:/sdk/work/docs-generation-review-c8-output-20261007-11`.
- After changing `rust/crates/demo/src/lib.rs`, generation with the old receipt failed before writing output: `missing a matching source attestation (expected sha256:2867fc3e...)`. This closes the previous stale-Rustdoc-at-generate gap.
- Clean release generation with `--skip-scenarios` failed: `release generation cannot skip the registered scenario source closure` (`output-20261007-13`).
- Two concurrent identical preview generations to one new output both exited 0; subsequent drift passed. Evidence: `Q:/sdk/work/docs-generation-review-c8-output-20261007-concurrent`, `concurrent-1/2.out/.err`. The CLI now uses `fs::hard_link` for immutable publication, avoiding Unix rename-overwrite.

## Manifest tamper results

- Added unknown manifest field and recomputed `generation-manifest.v1.sha256`: rejected with serde `unknown field` (`output-20261007-15`).
- Changed `revision` and recomputed sidecar: rejected with `source revision changed since generation` (`output-20261007-17`).
- Changed only `sourceState` from `working-tree` to `captured-snapshot` and recomputed sidecar: **drift passed** (`output-20261007-16`). The drift path validates revision, source files, Rustdoc files, and artifacts, but does not validate `sourceState` against channel/checkout state. Add an explicit source-state invariant if this field is security-relevant.

The manifest integrity sidecar now protects against ordinary edits; an attacker able to edit both manifest and sidecar can still exploit the missing source-state invariant.

## Rust source closure

The generator inventory now includes every `.rs`, `.toml`, `.lock`, and `.md` below `rust/crates`, plus root `Cargo.toml`, `Cargo.lock`, and `release/cargo-crates.json`. A fixture containing `rust/crates/demo/src/lib.rs`, `examples/demo.rs`, `build.rs`, `README.md`, and `Cargo.toml` generated a source map containing all five files. This covers Rust crate guides, examples, and build scripts.

The inventory still excludes Rust files outside `rust/crates` (for example `plugin/src/main.rs`) and non-Rust scenario consumers. The scenario registry separately validates each registered example, package manifest, and workspace lockfile; its endpoint scenarios are compiled but not locally executed.

## Tests

After-fix `sdk-docs` library tests: 48 passed, 1 ignored. `sdk-generation` has no unit tests (0 tests), so the CLI regressions above remain external process evidence and should become focused CLI integration tests before qualification.

Evidence paths:

- `Q:/sdk/work/docs-generation-review-c8-output-20261007-11` matching generation
- `Q:/sdk/work/docs-generation-review-c8-output-20261007-12` stale source rejection (no manifest)
- `Q:/sdk/work/docs-generation-review-c8-output-20261007-13` release skip rejection (no manifest)
- `Q:/sdk/work/docs-generation-review-c8-output-20261007-15` unknown-field rejection
- `Q:/sdk/work/docs-generation-review-c8-output-20261007-16` sourceState tamper passing drift
- `Q:/sdk/work/docs-generation-review-c8-output-20261007-17` revision tamper rejection
- `Q:/sdk/work/docs-generation-review-c8-output-20261007-concurrent` concurrent publication

## Focused CLI regression test file

The shared checkout now contains `rust/crates/sdk-generation/tests/cli_regressions.rs` (SHA-256 `250B9873A1DB66FDC43A4759E5C52FBCA773795A9E9B1304912C6F6791EA8E57`). It runs the actual binary and records:

- `stale_source_attestation_is_rejected_before_generate`: active and passing; a source edit with an old Rustdoc producer sidecar fails before a manifest is written.
- `source_state_tamper_is_rejected`: intentionally ignored until the CLI compares the attested `sourceState` with the current checkout/channel. The existing external probe remains `output-20261007-16`.
- `plugin_source_is_part_of_manifest_closure`: intentionally ignored until the CLI inventory includes `plugin/src/main.rs` (the current external probe is `output-20261007-18`).

The integration test run used target directory `Q:/sdk/work/docs-generation-review-c8-target-cli-tests`: 1 passed, 2 ignored. The ignored tests are executable regression specifications, not qualification evidence until their owner-side CLI fixes land.

## Active integration gate rerun

The shared CLI regression file was updated against the final manifest field types. The real binary passes four cases: `source_state_tamper_is_rejected`, `stale_source_attestation_is_rejected_before_generate`, `release_generation_rejects_scenario_bypass`, and `concurrent_preview_publications_are_idempotent`. `plugin_source_is_part_of_manifest_closure` remains an active failing gate because the current source inventory excludes `plugin/src/main.rs`; its failure is the owner-facing production gap. The isolated target was `Q:/sdk/work/docs-generation-review-c8-target-profile-tests`.

The concurrent test requires one successful writer and a valid drift check; on Windows a contending writer can receive OS sharing error 33 from the SDK docs publication lock while the immutable bundle remains valid.
Integration source SHA-256: `8ABFC237234DFD31AC8F8095F6211053C725CE5738E5BA8A1AAA53BB6E5726F9`.

## Profile planner audit

`profile_planner.rs` (SHA-256 `EC13462D0A0A26A2A6AC4E7066878D814AD791C89F41A1BF21819EF366BE63CB`) passes against current Cargo metadata. It checks bounded host/WASM target identity for all six public feature-bearing roots and single default profiles for featureless public roots including native-runtime and plugin. The separate `profile-accuracy-review.md` records the unresolved external-receipt provenance gap: externally supplied Rustdoc still has no exact Cargo feature/default metadata and must not be presented as a known default profile.

## Plugin regression cause

A fresh `sdk-generation.exe` built in `Q:/sdk/work/docs-generation-review-c8-target-plugin-cause` reproduced the active plugin failure. The fixture attestation includes `plugin/src/main.rs` and hashes to `sha256:ff38aaa5760aee0d1f69ec4ecf3731491b932b5dbcf86a66f6f6260f1d6743cf`; the current collector computes `sha256:ba426e321fe85f66eebfe56c47b785904f559bc7c2dbac76d39672fdc9158dee`, which is exactly the same inventory with `plugin/src/main.rs` omitted. The fixture has the expected root Cargo files and Rust crate files, so this is an inventory mismatch rather than a fixture drift. The producer-side source attestation correctly exposes the missing plugin closure.
