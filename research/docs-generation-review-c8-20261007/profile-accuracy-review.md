# Docs generator profile accuracy review (2026-10-07)

This is an evidence-only review of the current `sdk-generation` CLI and
`sdk-docs::rustdoc_profiles` adapter. It records what the producer receipt
actually identifies; it does not promote receipt-only output to a qualified
profile.

## Receipt identity

`observe_rustdoc` reads the Rustdoc format version, crate name/version, target
triple, `includes_private`, and item count. The Rustdoc receipt has no
producer fields for Cargo arguments, default-feature state, enabled feature
set, binding mode, or toolchain identity.

The CLI then constructs every `ProfileSpec` from only the observed target:

```text
default_features = true
features = {}
```

The generated `sdk-docs-rustdoc-profiles.v1.json` consequently hardcodes the
same values and marks `rustdocProfilesCovered: true`. The current receipt also
sets `installedRuntimeQualified: false`; this is an honest non-qualification
flag, but it is not evidence that an installed runtime was tested.

The library already provides `profiles_for_package`, which can derive a
bounded feature matrix from Cargo metadata, and `execute_profile`, which runs
Cargo with exact target/feature arguments. The CLI's `--execute-profiles` mode
currently invokes only one default-feature host profile per package, plus a
default-feature WASM profile for a recognized WASM binding. It does not invoke
the feature matrix. When consuming externally supplied receipts, it also does
not consume producer Cargo arguments or feature metadata. Therefore a set of
16 receipts cannot be represented as 16 exact producer profiles by the current
schema; each receipt is projected as a default-feature profile unless an
additional producer sidecar/schema is consumed.

Observed demo output:

```text
package: demo-docs
target: x86_64-pc-windows-msvc
defaultFeatures: true
features: []
rustdocProfilesCovered: true
installedRuntimeQualified: false
```

This review does not claim that output as a feature-complete or installed
runtime qualification.

## Concrete producer/profiler fix

The producer should emit a sidecar next to each Rustdoc JSON receipt with the
exact profile invocation and tool identities, for example:

```json
{
  "schema": "sdk-docs-rustdoc-profile.v1",
  "package": "acyclic-fs",
  "target": "x86_64-pc-windows-msvc",
  "defaultFeatures": true,
  "features": ["local", "memory"],
  "cargoArgs": ["--locked", "--package", "acyclic-fs", "--target", "x86_64-pc-windows-msvc", "--features", "local,memory"],
  "cargoVersion": "...",
  "rustcVersion": "...",
  "manifestSha256": "sha256:...",
  "rustdocSha256": "sha256:...",
  "sourceSha256": "sha256:..."
}
```

The CLI should reject an external receipt when this sidecar is absent or has
any mismatched identity. If compatibility requires accepting it, the output
must use an explicit unknown provenance value and leave
`rustdocProfilesCovered` false; it must not synthesize
`defaultFeatures: true` and an empty feature set.

The feature planner should consume crate-owned Cargo metadata such as
`package.metadata.sdk-docs.public-features`, validate every listed feature
against Cargo's declared feature map, and execute only a bounded set: default,
no-default where meaningful, one closure per configured public capability, and
an explicitly configured all-capabilities profile. It should avoid arbitrary
feature powersets and exclude dev/test/internal-only combinations. The current
workspace has six public feature-bearing roots (`acyclic-fs` 7 declared
non-default features, `acyclic-objects` 3, `acyclic-stream` 4,
`acyclic-harness` 6, `acyclic-machines` 1, `acyclic-inference` 2) plus
featureless public roots such as native-runtime and plugin.

The independent `profile_planner.rs` tests cover the current Cargo metadata:
all six feature-bearing roots produce bounded target-exact host/WASM matrices,
and featureless public roots remain one default profile. These tests do not
claim that the generated API is qualified; each profile still needs its own
Rustdoc output and signature projection before availability is asserted.

## Other CLI gates exercised

The active integration file `cli_regressions.rs` runs the actual binary and
passes all five tests: source-attestation rejection, source-state tamper
rejection, plugin source closure, release scenario-bypass rejection, and
concurrent preview publication. The concurrency assertion requires at least
one successful writer and a valid post-race drift check; on Windows the
second writer may receive the OS sharing violation while the immutable output
remains valid.

The parser still accepts unknown `--name` flags because `parse_flags` collects
arbitrary keys and command parsers only read known keys. A strict allowlist is
needed if unknown CLI options are intended to be rejected.

