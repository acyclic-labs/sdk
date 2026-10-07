# Fresh CLI input and external-profile provenance probe (2026-10-07)

This is a fresh process probe of the relocated binary, recorded without changing
production source. Binary:
`Q:/sdk/work/docs-generation-review-c8-target-plugin-cause/debug/sdk-generation.exe`

## Unknown option handling

Fixture root:
`C:/Users/varun/AppData/Local/Temp/sdk-generation-cli-regression-39276-4-base`

The fixture's pre-existing Rustdoc source sidecar was from an earlier source
inventory (`sha256:ba426e321fe85f66eebfe56c47b785904f559bc7c2dbac76d39672fdc9158dee`),
while this fresh binary computed the current fixture inventory as
`sha256:9f33f84392c25924dbab5e4039467ed67207649d996d3fa6da473c61b7888dc0`.
For this parser-isolation probe only, the temporary sidecar value was changed to
that computed value and restored byte-for-byte after the run. The output was
written outside the fixture at:
`C:/Users/varun/AppData/Local/Temp/sdk-generation-unknown-flag-output-20261007`.

Command arguments included the unrecognized pair `--unknown accepted` after all
recognized `generate` options. The fresh binary exited `0` and printed:
`generated C:\Users\varun\AppData\Local\Temp\sdk-generation-unknown-flag-output-20261007`.
Therefore unknown options are silently accepted; `parse_flags` needs a strict
command-specific allowlist if rejection is required.

## External Rustdoc profile provenance

The same successful output contains this row in
`sdk-docs-rustdoc-profiles.v1.json`:

```json
{
  "package": "demo-docs",
  "target": "x86_64-unknown-linux-gnu",
  "defaultFeatures": true,
  "features": [],
  "profile": "p64656d6f2d646f6373;t7838365f36342d756e6b6e6f776e2d6c696e75782d676e75;d1;f",
  "rustdocFile": "rustdoc/demo_docs.json",
  "rustdocSha256": "sha256:12486583857f1325fccc5d70aa91d9daac6165609f01ca753dea05f02cdbdf11",
  "rustdocVersion": "1.0.0",
  "rustdocProfilesCovered": true,
  "installedRuntimeQualified": false
}
```

The external `rustdoc/demo_docs.json` input had a source attestation sidecar,
but no producer profile sidecar containing Cargo arguments, default-feature
state, feature closure, Cargo/rustc versions, manifest identity, or producer
source identity. The CLI nevertheless emitted `defaultFeatures: true`, an empty
feature set, and `rustdocProfilesCovered: true`. This is a provenance claim
created by the consumer, not an observed producer fact. The profile output must
reject missing producer metadata, or represent the profile as explicitly
unknown and leave `rustdocProfilesCovered` false.

The current Cargo-backed `profile_planner.rs` evidence remains separate: it
checks bounded profile planning against actual workspace metadata, but it does
not turn an external Rustdoc file into a qualified profile.
