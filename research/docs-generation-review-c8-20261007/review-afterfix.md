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

## Fresh binary input/provenance probe (supersedes stale summary claims)

On 2026-10-07, the fresh binary at
`Q:/sdk/work/docs-generation-review-c8-target-plugin-cause/debug/sdk-generation.exe`
accepted an unrecognized `--unknown accepted` pair and exited 0 after the
source-attestation sidecar was temporarily aligned to the binary's current
computed hash (`sha256:9f33f84392c25924dbab5e4039467ed67207649d996d3fa6da473c61b7888dc0`);
the original sidecar was restored. This confirms the parser still silently
accepts unknown flags. Evidence details and output paths are in
`external-profile-provenance-probe.md` (SHA-256
`8DD8EB675DBBA1C829A9F7D794A90422AD6687989D99346BBE00BB99B2B21A`).

That same fresh generation consumed an external Rustdoc receipt with no
producer profile sidecar and emitted `defaultFeatures: true`, `features: []`,
and `rustdocProfilesCovered: true`. Those values are not supported by the
receipt's provenance. The CLI must reject absent producer metadata or emit an
explicit unknown profile with coverage false; the bounded Cargo metadata
planner does not repair this external-receipt gap.

The current active integration rerun remains **4 passed, 1 failed**: the plugin
source-closure test fails because the production inventory excludes
`plugin/src/main.rs`. Earlier text saying all five passed was stale and is
superseded by the active run and the exact cause above. No production collector
change was made in this review.

## Strict CLI allowlist fix (parser ownership)

The maintained parser boundary now passes command-specific allowlists into
`parse_flags`: `generate` accepts only its documented path/version/channel,
Rustdoc mode, and scenario flags; `drift` accepts only root/output/Rustdoc
inputs. Unknown options fail with `unknown option `--name`` before command
execution. Boolean `--skip-scenarios` and `--execute-profiles` remain
value-less flags.

`cli_regressions.rs` adds active generation and drift cases that pass an
unknown option and assert a nonzero exit plus no output directory. The actual
binary test run passed all 7 tests, including plugin closure after the
owner-side collector update. Test source SHA-256 is
`D9637B65262709A36AD4C869F5C9F250E0C88FF7E6495879FE6187ED105A3693`.
The parser change is limited to `parse_generate`, `parse_path_flags`, and
`parse_flags`; collector/profile execution/provenance code was not edited by
this task.

## Source coverage mutation gate (latest)

The active CLI suite now passes 9 tests. New fixtures create included
`rust/crates/demo/src/lib.rs`, `rust/crates/demo/README.md`, `docs/guide.md`,
and (for the plugin fixture) `plugin/src/main.rs`. Each mutation test changes
one exact source, confirms existing drift fails with the changed relative path,
confirms fresh generation rejects the old Rustdoc source attestation before
writing, restores exact bytes, confirms drift passes, and regenerates a fresh
manifest.

The restored base fixture source snapshot is
`sha256:1d6cc2905e983bfae296fa9e9994f365badcf66dbfeff28142c6f03bdb59a4ab`; the
plugin snapshot is
`sha256:73f29b1d2311b9c0b104c807b100d4bed88259a7769b119cc5d9d4787033c70c`.
Detailed file hashes and paths are in
`source-coverage-mutation-evidence.md` (SHA-256
`65C9988D01E8C458BC37B9A231935524AC05F560CA641E9D67DCD07B54FB9D26`).
This task changed regression fixtures/tests only; the production collector and
profile execution paths were not edited.
