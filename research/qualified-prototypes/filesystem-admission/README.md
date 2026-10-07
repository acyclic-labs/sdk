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
node research/qualified-prototypes/filesystem-admission/run.mjs \
  --write-source-manifest \
  --source-root <frozen-source-root> \
  --validator-root <clean-signed-validator-checkout> \
  --source-manifest <frozen-source-root>/target/qualification/source-attestation.json
```

The manifest path should be ignored build output or outside the checkout when
the source is clean. If it is an untracked path inside the checkout, Git will
correctly classify the snapshot as dirty on the next validation. Run the
runtime qualification only after package assembly has extracted the archive
and after the native companion has been installed:

```text
node research/qualified-prototypes/filesystem-admission/run.mjs \
  --package-root <extracted-@acyclic-labs-fs> \
  --native-binding <installed-@acyclic-labs-fs-platform>/acyclic-fs-<version>-<target>.node \
  --native-archive <packed-@acyclic-labs-fs-platform.tgz> \
  --source-root <frozen-source-root> \
  --validator-root <clean-signed-validator-checkout> \
  --source-manifest <frozen-source-root>/source-attestation.json \
  --source-commit <checked-out-commit> \
  --archive <packed-@acyclic-labs-fs.tgz> \
  --receipt <qualification-directory>/filesystem-admission.json
```

`--source-commit` is required for runtime qualification and must equal the
actual checkout `HEAD`; `--native-archive` is required so the receipt binds
the installed companion to the exact package assembly that supplied it.
`--validator-root` identifies the clean, signed checkout containing this
validator. The receipt records its commit, tree, clean state, signer, and
signature fingerprint separately from the SDK source commit and runtime
artifact hashes; this permits a fixed external validator to qualify a clean
SDK checkout without relabeling the validator as SDK source. The validator
also compares the manifest's
`source_state` with Git, checks the exact selector, and verifies the declared
lockfiles, toolchain file, and generator scripts. A dirty source snapshot is
allowed only when the manifest explicitly says `source_state: "dirty"`.

The runner hashes the package archive, native archive, extracted WASM files,
and installed native binding before and after the matrix. It indexes the
native tar archive, rejects unsafe or duplicate paths, extracts the one
binding entry to a private temporary file, hashes it as a stream, and compares
those bytes with the installed `.node` file without buffering the native
artifact in memory. Any byte change during qualification fails the receipt.
The generated `BrowserWorkCounters` declaration supplies the expected counter
keys; both runtimes must return exactly those keys with bigint or decimal
values, so the check does not encode a field count. It also validates the
source attestation before and after execution. The runner invokes `liveRebase` through the generated WASM module and through
the installed N-API binding. Each call uses the same Rust-owned boundary
parameter. A value is classified as `boundary_rejected` only when the runtime
returns an exact typed-boundary message: `expected a finite integer in the u32
range`, `expected a JavaScript number`, or one of the seven exact N-API
`Failed to convert napi value <Type> into rust type \`f64\`` messages for
`Null`, `String`, `Boolean`, `Undefined`, `BigInt`, `Object`, and `Symbol`.
Any other conversion or policy string remains a downstream error, so stale or
coercing native artifacts cannot be mistaken for a typed boundary rejection.

For installed packages that expose the generated declarations, the runner also
creates one ephemeral volume in each runtime and validates its acquisition work
receipt. The expected counter names come from the installed
`BrowserWorkCounters` declaration; every WASM bigint and every N-API decimal
counter must be present, with no extras or omissions, and both runtimes must
expose the same generated field set. This follows additions to the Rust
counter model without a hardcoded field count.

The required matrix is:

| Class | Values |
| --- | --- |
| Numeric edges | `0`, `1`, `4294967295` |
| Numeric rejects | `-1`, `0.5`, `1.5`, `4294967296`, `NaN`, `Infinity`, `-Infinity` |
| Non-number rejects | `null`, `"1"`, `true`, `false`, `undefined`, `1n`, `new Number(1)`, `Symbol("1")` |

Both runtimes must reject every value in the two reject rows at the boundary.
The three edge values must pass boundary admission and reach the expected Rust core outcome for the non-fork fixture: zero reaches the configured-bound error, while one and u32::MAX reach the non-fork error. Unknown errors fail the gate.

The receipt binds the result to the bytes tested. It contains a separate
`validator` identity and the checked-out SDK source commit, a complete
source-attestation manifest, the manifest's canonical
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
node research/qualified-prototypes/filesystem-admission/run.mjs \
  --attestation-only \
  --source-root <frozen-source-root> \
  --source-manifest <frozen-source-root>/source-attestation.json \
  --source-commit <checked-out-commit>
```

This mode is used for the omission and mutation regression checks; it does not
load or execute either runtime.

Run the regression harness from the repository root with:

```text
node research/qualified-prototypes/filesystem-admission/test.mjs
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

The source-authority control mutates one canonical Rust `FileKind`,
`FilePayload`, or `WorkCounters` declaration in a detached worktree and runs
the maintained WASM/N-API Cargo graph:

```text
node research/qualified-prototypes/filesystem-admission/source-authority-mutation.test.mjs \
  --source-root <frozen-source-root> \
  --source-commit <checked-out-commit> \
  --output <external-evidence-directory> \
  --expect-propagation
```

Final candidates use `--expect-propagation`; the default mode retains the
historical manual-mirror blocker control for older source revisions.

This is a qualification input and receipt format. It intentionally contains no
generated WASM, native binary, or duplicated numeric validation logic.

## Isolated Linux and macOS native lane

`native-platform-lane.sh` builds the Linux or macOS companion in a separate
output tree. It requires a clean frozen checkout and an exact 40-character
source revision, and records the host tool versions beside the package and
admission receipt. It verifies the Rust toolchain declaration, the exact
`wasm-bindgen` pin in `Cargo.toml`, and the resolved N-API dependencies from
the frozen `Cargo.lock`. It also checks that the exact `@napi-rs/cli` pin in
`package.json` matches `bun.lock`; all resolved versions are recorded in
`toolchain.json` rather than copied from an older candidate.
The maintained lane currently pins Bun `1.4.2`; a host with another Bun
version is rejected before any build starts.

Run it on the native host with an output directory outside the Windows lane:

```text
bash research/qualified-prototypes/filesystem-admission/native-platform-lane.sh \
  <frozen-source-root> \
  /var/tmp/acyclic-fs-admission/<source-commit>-linux-x64 \
  <source-commit>
```

When the frozen checkout is a Windows worktree, stage the committed tree on
the WSL ext4 filesystem before building. A mounted `/mnt/c` worktree can make
Git's clean-state walk prohibitively slow and its `.git` file may contain a
Windows-only drive path. Export only the exact commit, then attach the staged
tree to the existing worktree Git directory so `HEAD` and status remain
authoritative:

```text
mkdir -p /var/tmp/acyclic-fs-source/<source-commit>
GIT_DIR=/mnt/q/sdk/.git/worktrees/filesystem-main-port-c8 \
  git archive --format=tar <source-commit> |
  tar -xf - -C /var/tmp/acyclic-fs-source/<source-commit>
printf 'gitdir: /mnt/q/sdk/.git/worktrees/filesystem-main-port-c8\n' \
  > /var/tmp/acyclic-fs-source/<source-commit>/.git
```

The staged tree must contain the committed runner and peer fixes. Do not copy
uncommitted files into it or use the staged tree to relabel a dirty checkout.
The output directory and the source staging directory must be distinct from
the Windows lane's target and package directories.

The lane builds `acyclic-fs-napi` and the package-owned WASM pair into isolated
Cargo/package directories, runs the maintained `check-filesystem-napi.mjs`
ABI and adapter checks, packs both `@acyclic-labs/fs` and the platform
companion, installs those exact archives into a private consumer, then invokes
the admission runner. The final matrix covers numeric boundary values,
non-number rejection, generated finite-kind declarations, and the complete
Rust-derived work-counter key set. `SHA256SUMS`, `toolchain.json`, the source
attestation, and the receipt bind all outputs to the frozen commit.

For a macOS host, run the same script over SSH or copy it to the host first;
the output path and source checkout must remain host-local so the Windows
qualification target is never reused:

```text
ssh <mac-host> 'bash -s -- <source-root> /var/tmp/acyclic-fs-admission/<source-commit>-darwin-arm64 <source-commit>' \
  < research/qualified-prototypes/filesystem-admission/native-platform-lane.sh
```

The script rejects Windows and unsupported architectures rather than silently
producing a mislabeled artifact. Do not attach a receipt to a dirty checkout
or reuse an archive from another source revision.

## Canonical kind/payload type prototype

`kind-payload-types.mjs` is a bounded source-only generator for the remaining
finite-kind type gap. It reads the maintained Rust `FileKind` and `FilePayload`
enum declarations, converts their variant names to the existing kebab-case wire
values, and emits one shared TypeScript type module. The generated module is a
review artifact; it does not modify production WASM or N-API bindings:

```text
node research/qualified-prototypes/filesystem-admission/kind-payload-types.mjs \
  --source-root <frozen-source-root> \
  --output <review-directory>/filesystem-kind-payload-types.d.ts
```

The prototype deliberately has no copied enum or hand-maintained literal list.
Its regression test copies only the two canonical Rust enum sources, verifies
the maintained values, then adds a fixture variant and requires the generated
union to change with that source:

```text
node research/qualified-prototypes/filesystem-admission/kind-payload-types.test.mjs
```

The production follow-up should make the WASM `file_kind`/`payload_kind` DTO
annotations and the N-API declaration overlay reference this one generated
module. The filesystem binding owner should choose the binding-specific
annotation mechanism; this prototype does not edit peer binding sources.
