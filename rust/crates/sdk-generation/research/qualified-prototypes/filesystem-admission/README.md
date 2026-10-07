# Installed filesystem admission qualification

This prototype qualifies the runtimes that are actually present in an extracted
filesystem package. It does not rebuild Rust, regenerate WASM, or copy generated
assets into the repository. The runner takes the package-owned WASM files and a
package-owned N-API companion as inputs, then records their hashes beside the
observed admission results.

First freeze the candidate checkout, then create its source attestation with
the runner itself. The writer derives the actual Git `HEAD` and clean/dirty
state; it refuses a caller-supplied commit and never permits relabeling an
older manifest:

```text
node rust/crates/sdk-generation/research/qualified-prototypes/filesystem-admission/run.mjs \
  --write-source-manifest \
  --source-root <frozen-source-root> \
  --source-manifest <frozen-source-root>/target/qualification/source-attestation.json
```

The manifest path should be ignored build output or outside the checkout when
the source is clean. If it is an untracked path inside the checkout, Git will
correctly classify the snapshot as dirty on the next validation. Run the
runtime qualification only after package assembly has extracted the archive
and after the native companion has been installed:

```text
node rust/crates/sdk-generation/research/qualified-prototypes/filesystem-admission/run.mjs \
  --package-root <extracted-@acyclic-labs-fs> \
  --native-binding <installed-@acyclic-labs-fs-platform>/acyclic-fs-<version>-<target>.node \
  --native-archive <packed-@acyclic-labs-fs-platform.tgz> \
  --source-root <frozen-source-root> \
  --source-manifest <frozen-source-root>/source-attestation.json \
  --source-commit <checked-out-commit> \
  --archive <packed-@acyclic-labs-fs.tgz> \
  --receipt <qualification-directory>/filesystem-admission.json
```

`--source-commit` is required for runtime qualification and must equal the
actual checkout `HEAD`; `--native-archive` is required so the receipt binds
the installed companion to the exact package assembly that supplied it. The
validator also compares the manifest's
`source_state` with Git, checks the exact selector, and verifies the declared
lockfiles, toolchain file, and generator scripts. A dirty source snapshot is
allowed only when the manifest explicitly says `source_state: "dirty"`.

The runner hashes the package archive, native archive, extracted WASM files,
and installed native binding before and after the matrix. It indexes the
native tar archive, rejects unsafe or duplicate paths, extracts the one
binding entry, and compares those bytes with the installed `.node` file. Any
byte change during qualification fails the receipt. It also validates the
source attestation before and after execution. The runner invokes `liveRebase` through the generated WASM module and through
the installed N-API binding. Each call uses the same Rust-owned boundary
parameter. A value is classified as `boundary_rejected` only when the runtime
returns an exact typed-boundary message: `expected a finite integer in the u32
range`, `expected a JavaScript number`, or one of the seven exact N-API
`Failed to convert napi value <Type> into rust type \`f64\`` messages for
`Null`, `String`, `Boolean`, `Undefined`, `BigInt`, `Object`, and `Symbol`.
Any other conversion or policy string remains a downstream error, so stale or
coercing native artifacts cannot be mistaken for a typed boundary rejection.

The required matrix is:

| Class | Values |
| --- | --- |
| Numeric edges | `0`, `1`, `4294967295` |
| Numeric rejects | `-1`, `0.5`, `1.5`, `4294967296`, `NaN`, `Infinity`, `-Infinity` |
| Non-number rejects | `null`, `"1"`, `true`, `false`, `undefined`, `1n`, `new Number(1)`, `Symbol("1")` |

Both runtimes must reject every value in the two reject rows at the boundary.
The three edge values must pass boundary admission and reach the expected Rust core outcome for the non-fork fixture: zero reaches the configured-bound error, while one and u32::MAX reach the non-fork error. Unknown errors fail the gate.

The receipt binds the result to the bytes tested. It contains the checked-out
source commit, a complete source-attestation manifest, the manifest's canonical
source digest, the hash computed from the packed archive, package-relative WASM
JS/WASM hashes, native binding hash, matrix digest, and every observed result.
The manifest uses schema `acyclic.sdk.source-attestation.v1` and must declare
`inventory_complete: true`, `source_state: clean` or `dirty`, the matching
commit, the exact `required_source_roots` selector used by the runner, every
selected source-closure file with its SHA-256 and byte count, and a canonical
digest of sorted `path\0sha256\0bytes\n` records. The selector covers the
Rust filesystem/native/WASM dependency closure, its path-crate and transport
descriptor inputs, locked root/toolchain and
packaging inputs, protocol sources, and the TypeScript filesystem package's
source/test/example files while excluding generated package outputs. This
manifest also records the generator provenance: `Cargo.lock`, `bun.lock`,
`rust-toolchain.toml`, the filesystem generator scripts, and the package
metadata that selects them, plus the path-crate roots. This
permits an explicitly labelled dirty snapshot while still making the exact
tested source tree reviewable. The runner independently walks that selector,
rejects symlinks, and re-hashes every selected file. Removing a manifest entry
fails with a missing-selector error; mutating a selected source file fails with
a source-hash or byte-count mismatch before any receipt is written.

Generated declarations and runtime bytes are package identity rather than
source-closure inputs: the whole package and native archives are hashed, while
the generated WASM JavaScript, WASM binary, and native `.node` entry are each
compared byte-for-byte with their installed counterparts.

Release assembly must fail if any source-closure or package-owned byte differs
from the bytes used to create the receipt, or if either runtime has a non-number
that reaches downstream policy.

Before the expensive runtime qualification, the same runner can validate only
the source inventory:

```text
node rust/crates/sdk-generation/research/qualified-prototypes/filesystem-admission/run.mjs \
  --attestation-only \
  --source-root <frozen-source-root> \
  --source-manifest <frozen-source-root>/source-attestation.json \
  --source-commit <checked-out-commit>
```

This mode is used for the omission and mutation regression checks; it does not
load or execute either runtime.

Run the regression harness from the repository root with:

```text
node rust/crates/sdk-generation/research/qualified-prototypes/filesystem-admission/test.mjs
```

The harness invokes the real runner against the authoritative source selector
and requires a complete, canonically sorted, duplicate-free inventory whose
second write is byte-for-byte deterministic. It checks wrong commits, omitted
selector entries, a selected-source mutation, a fresh Git-state mismatch, a
package archive/install mismatch, an unknown error that merely contains the
canonical admission phrase, and mutations of source or package bytes during
qualification. Runtime cases use temporary package fixtures only to exercise
those failure paths; they still run the complete edge and reject matrix before
a receipt could be written.

This is a qualification input and receipt format. It intentionally contains no
generated WASM, native binary, or duplicated numeric validation logic.
