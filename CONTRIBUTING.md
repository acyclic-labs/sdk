# Contributing

Run `node scripts/check-generated.mjs` to check generated contracts, model
fixtures, schemas and published projections against their source generators.
Pinned files are compared without rewriting them; ignored build outputs are
regenerated. `node scripts/check-generated.mjs --no-compile` checks README
examples, compatibility digests and packaged source copies without invoking
Rust. Regenerate changed artifacts with `bun run generate`.

Contributions are licensed under Apache-2.0. Every commit must include a
`Signed-off-by` trailer certifying the [Developer Certificate of Origin](DCO.md).

Before opening a pull request, run the Rust, Bun/TypeScript, Protobuf, provenance,
license, secret, and private-namespace checks used by CI. Imported code must add
an entry to `provenance/manifest.json` before it is merged.

## Build and test

Release tool installers accept `PINNED_ASSET_FETCHER`, an executable called
with the expected SHA-256, upstream URL and destination file. Configure an
offline provider to use preseeded inputs; a provider failure is terminal and
never falls back to a network download. Installers still verify the received bytes.

The Cargo configuration enables Tokio's `tokio_unstable` API for every workspace
build. Deterministic simulations use `acyclic_native_runtime::testing::seeded_runtime`
to seed randomized `select!` order, including selections in dependencies.
Consumers of `test-support` outside this workspace must also compile Tokio with
`--cfg tokio_unstable`. Replay requires the same locked dependencies and compiler.

`node scripts/qualify-mcp-loom.mjs --gate` retains the complete selected model
inventory. Individual selectors (for example `--native-owner`) run the same
obligations. The command emits `MODEL_CHECK_PHASE` build/search timings and one
terminal `MODEL_CHECK_RESULT` with `status: "PASS"` or `"FAIL"`; absence of that
terminal is incomplete execution. A zero-test or incomplete suite cannot pass.

Loom models compile only under `ACYCLIC_LOOM=1` together with the crate's
`model-checking` feature: each crate's build script then emits `cfg(loom)`. The
feature alone never implies loom, so `--all-features` and Bazel builds keep real
synchronization. Registry crates, including Tokio's own `cfg(loom)`, stay
untouched. The cfg travels in rustc's arguments, so compiler caches key on it.
`node scripts/check-test-support.mjs` rejects a crate with loom code but no
build-script opt-in.

An operator can set `SDK_CHECKER_RUNNER` to an installed shared Node runner for
libtest obligations. It receives `-- <binary> <original arguments>` after Cargo
builds the selected target, and must return the original complete test summary.
A cached PASS must cover the exact executable, arguments, environment, platform,
runner policy and observed input closure. Unknown closures always execute.
Mutation controls always execute directly. Keep the compatible warm Cargo target
and unchanged-source mtimes; the adapter does not create a second target cache.

Native x86_64 Linux Rust builds use clang and mold. On Debian or Ubuntu, install
them before running Cargo:

```sh
sudo apt-get update
sudo apt-get install --no-install-recommends clang mold ripgrep
```

Linux native-runtime policy tests execute `ls -l`, `stat`, `which -a`, `file`
and `rg --files` in the confined shell. Install these inspection tools even
when using a different linker; the shared Linux CI toolchain action provisions
the additional `ripgrep`, `debianutils` and `file` packages.

On Linux, sandbox syscall denials return `EPERM` to the calling process; they
do not terminate the command tree. Anonymous Unix stream/seqpacket pairs and process-group
or session changes are allowed. Custody follows descendants across those changes.
Descriptor-local FIONREAD and FIONBIO and read-only terminal queries are allowed.
If the root exits while descendants remain unfinished, cleanup terminates them
and reports a failed command tree rather than successful partial output. Join
background work before exiting the shell. Guest-generated SIGSYS remains an
ordinary failed process status, including without an execution grant.
The closed Unix tool environment supplies the already-selected `SHELL` path.
This avoids Bash looking up a login shell through host name-service IPC during
startup. The default workspace policy sets `HOME` to the workspace, so tools
such as ripgrep do not rediscover a host home through NSS. Unix socket creation
returns `EPERM` without contacting host services, so name-service probes
can fall back to numeric ownership. Datagram pairs remain closed because sendto/sendmsg can retarget them to host
services. Native policy tests exercise ordinary
inspection commands and force a socket probe independently of the host's NSS
configuration.

To use the system C linker instead, override the workspace linker for the command:

```sh
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=cc cargo build --locked -p acyclic-plugin
```

For isolated identity or filesystem protocol changes, validate the owning crate:

```sh
bazel test //plugin/identity:all //rust/crates/filesystem-wire:all
```

Identity's unit tests and main-thread entry contract live in the identity crate;
CLI integration tests exercise its executable. The filesystem wire crate owns
the protocol descriptor and message tests. Qualification also covers consumers.

```sh
bun install --frozen-lockfile
bun run check   # tsc -b --force, project-wide type-check (also emits build output)
bun run test    # type-check, then TypeScript and filesystem package tests
cargo test --all-features --locked
```

`node scripts/node-tests.mjs --all` discovers Node and Bun tests, runs the
root's `test:prepare` recipe once, and reports every native failure with a
`NODE_TEST_FAIL` identity and one terminal `NODE_TESTS_RESULT`. Pass changed
repository paths for affected selection, or `--only FILE_OR_DIRECTORY` for an
explicit scope. `--root tools/sdk-generator --all` runs the independent backend
inventory without SDK preparation. On Windows, set `E2E_SCRATCH_ROOT` and
`E2E_PROCESS_HOST` to absolute installed paths before invoking the runner.

Full nextest runs include the live PostgreSQL and S3 controls when their features
are enabled, and require real disposable services. Focused storage qualification
can select either capability module with its named profile:

```sh
cargo nextest run --locked -p acyclic-stream --no-default-features --profile postgres --no-tests=fail
cargo nextest run --locked -p acyclic-objects --no-default-features --profile s3 --no-tests=fail
```

Set `STORAGE_POSTGRES_URL` to a disposable PostgreSQL database. Verifying the SQL
observation counts also requires server statement logs with `log_statement=all`
and `%a` in `log_line_prefix`; the tests mark sessions with `publication-count-`
application names. The tests do not read or verify those server logs themselves.

For S3, supply `STORAGE_S3_BUCKET`, `STORAGE_S3_ENDPOINT`, `STORAGE_S3_REGION`,
`STORAGE_S3_ACCESS_KEY` and `STORAGE_S3_SECRET_KEY` for a disposable bucket with
conditional-write support. Set `STORAGE_S3_ALLOW_HTTP=1` only for an HTTP endpoint.
The caller owns service startup, observed SQL counts and final service teardown.
Selected controls fail if a required environment variable or service is missing;
they are never ignored. Direct `cargo test` commands do not use nextest profiles
and require the same services when selecting those modules.

The product workspace shares its root `Cargo.lock` with compatible fuzzers,
proofs and tools. Root commands include the SDK and the codec, Markdown and ts-rs
fuzzers, the patched ts-rs derive crate and ID codec proofs, so changes to their
dependencies are checked together.
Other tools remain explicitly selected with `-p PACKAGE`. `--workspace` also
includes compiler-negative fixtures, so use it only for that intended scope.
Documentation and generation packages, their file-identity helper and both fuzz
packages are workspace members using the root lock. They stay outside
`default-members`. Test documentation with `bazel test //rust/crates/sdk-docs:all`
and generation with `bazel test //rust/crates/sdk-generation:all`. Cargo-fuzz supports parent workspace
membership; run sanitizer fuzzing with `cargo +nightly fuzz` from the fuzzer
directory using the same root lockfile.
The captured proof-policy workspace under `scripts/tests/fixtures` is test data
with no root crate dependencies. It and vendored upstream packages retain their
original dependency inputs.

The TypeScript Stream models run the TypeScript sources and the WASM
implementation through Bun. After `bun install --frozen-lockfile`, nextest
prepares their generated WASM bindings once when selecting
`acyclic-stream-typescript-models`:

```sh
cargo nextest run --locked -p acyclic-stream-typescript-models
```

For `cargo test` or fuzzing, first run
`bun run --filter '@acyclic-labs/stream' build:wasm` to prepare the same
artifacts. The model drivers import the sources directly and never generate
substitutes.

Rust-only workspace tests also need the ignored Actors authoring contracts.
Generate them with the existing Bazel producer before running the Actors tests;
no Bun installation is needed for this prerequisite:

```sh
bazel run //rust/crates/actors:actors-http-routes_example -- "$PWD/typescript/packages/actors/src/generated/actors-service.ts"
bazel test //rust/crates/actors:main
```

The producer uses the output path's parent for `guest.ts` and service metadata.
Full TypeScript generation subsequently projects the metadata into
`actors-service.ts`. Rust qualification generates these files before its workspace
tests; drift checks only compare and never rewrite their inputs.

The `full-cloud` nextest profile exercises live Filesystem and Actors handlers.
Portable profiles leave these cases to this capability profile. List its cases with:

```sh
cargo nextest list --profile full-cloud --locked --all-features \
  -p acyclic-fs -p acyclic-harness
```

On Linux, build the Cloud checkout's binaries with
`node ops/local/e2e.mjs --build-only`. Run each listed case against a fresh
topology with the qualification entry below. Set absolute paths for
`CLOUD_SOURCE`, `CLOUD_BINARIES` and an existing `SDK_ARTIFACT_DIR` directory:

```sh
export CLOUD_SOURCE CLOUD_BINARIES SDK_ARTIFACT_DIR
cargo nextest run --profile full-cloud --locked --all-features \
  -p acyclic-fs -p acyclic-harness
```

The profile wraps every selected case with the shared local Cloud producer,
retaining service results under a fresh directory for each case.
The producer supplies `ACYCLIC_LOCAL_CLOUD`; selecting a case without its
endpoints fails. Each case needs fresh service state: the lifecycle oracle
migrates the deployment that the delivery oracle expects at its first revision.

The `dev` and `test` profiles use LLVM, panic unwinding and line tables for
backtraces. The independent `acyclic-fs-wire` and `acyclic-plugin-identity` crates
build at `opt-level = 0`; other crates retain `opt-level = 1` because their
unoptimized async state machines exceed default thread stacks. SHA-2 and BLAKE3
retain targeted optimization for executable readiness. Large asynchronous layers
use funded heap boundaries; `clippy::large_futures` rejects callers that embed a
large future. `--release` stays a
fast incremental build for development; the shipped `acyclic` executable comes
from `bazel build -c opt --remote_download_toplevel //plugin:acyclic` and is available under `bazel info -c opt bazel-bin`.
Qualifier and Cargo-install builds share the release artifact adapter;
see [release build reuse](docs/ci-releases.md#release-build-reuse) for its host
input-closure and persistent-target contract.

The optional gate and Windows adapters consume separately installed infrastructure.
Set `SDK_GATE_CLIPPY` to the external coordinator's `gate-clippy.py`,
`SDK_WINDOWS_GATE` to its `gate-checks.sh`, and `SDK_MSVC_COMPILER` to the
installed `msvc-compiler.py` when using nonstandard installation paths.
Infrastructure integration tests take `SDK_BUILD_TOOLS_DIR` as their external
source/fixture root; those sources are supplied by the operator, outside this
checkout. For example, `SDK_GATE_CLIPPY=/external/coordinator/gate-clippy.py
node --test scripts/main-gate-checks.test.mjs` exercises the SDK adapter against
that decoder. Missing required tools fail explicitly; normal Cargo checks do
not need these adapters.

## Commit messages

- Summary line: imperative mood ("Add", "Fix", "Rename", not "Added"/"Fixes"), no
  trailing period.
- Blank line, then a body explaining *why* the change is needed, not a restatement
  of the diff.
- No AI attribution trailers or "Generated with ..." lines (`Co-Authored-By:` naming
  an agent, session links, etc.) — the commit should read as yours, whatever tools
  helped write it. `Signed-off-by:` is still required.

## Before opening a PR

Run `node scripts/qualify-rustfmt.mjs` and use Bazel to build and test the
affected packages. Pass `--@rules_rust//rust/settings:extra_rustc_flag=-Dwarnings`
to deny warnings. Full release qualification covers the broader suites.
Write formatting with `node scripts/qualify-rustfmt.mjs --write`, the
same mechanism the check uses. Every PR should get review and approval from a
code owner (`.github/CODEOWNERS`) before it merges; branch protection does not yet
require this, so treat it as a norm until it's enforced (not yet tracked in a
dedicated issue). An automated Greptile review already runs on every PR.

## Code quality rules

Beyond rustfmt and clippy's defaults, the workspace enables an additional lint set in
`Cargo.toml` (`[workspace.lints.clippy]`, thresholds in `clippy.toml`):

- **No hidden panics in non-test code.** `unwrap`, `expect`, and `panic!` are lint
  errors outside tests. Reach for `?`, `get`, `let ... else`. The few documented
  exceptions carry `#[allow(..., reason = "...")]` naming the invariant that makes
  the panic unreachable.
- **`unsafe` is opt-in per function**, carrying `#[allow(unsafe_code, reason = "...")]`
  naming the invariant.
- **Tracing follows [docs/observability.md](docs/observability.md)**: span names, levels,
  forbidden fields, cost rules, and no `tracing` in wasm32 builds.
- **Benchmarks are on-demand [divan](docs/observability.md#benchmarks) `benches/`**, never run in CI.
- **Duplication under 3% of tokens** (`jscpd`, config in `.jscpd.json`, not yet wired
  into CI — run manually with `npx jscpd@4.3.0 --config .jscpd.json .`).

A wider lint set from the same source — no identical match arms, functions under 100
lines/cognitive complexity under 30, no lossy `as` casts, no out-of-bounds indexing or
string slicing outside tests — is tracked in [#59](https://github.com/acyclic-labs/sdk/issues/59)
rather than enabled here: the workspace currently has ~866 pre-existing hits against
that set, so it lands together with the fixes in a follow-up PR instead of breaking
`-D warnings` on `main`.

## Reporting bugs and security issues

Open a GitHub issue for regular bugs. For security vulnerabilities, follow
[SECURITY.md](SECURITY.md) instead of filing a public issue.

## CI and releases

Pull requests and main pushes run **SDK Static**, which checks lockfile and
manifest consistency without compiling. Heavy checks run only through
**SDK Release** on main; [CI and releases](docs/ci-releases.md) documents
its dispatch and recovery steps.

Qualification runs in the Foreman build system on Bazel. The repository's GitHub
Actions are deploy and the single SDK Static job; there are no Cargo
qualification lanes. Bazel's disk and repository caches reuse compiler and
dependency work without skipping qualification. `MODULE.bazel.lock` is
committed: after changing `MODULE.bazel` or `cargo.MODULE.bazel`, run
`bazel mod deps --lockfile_mode=update` and commit the lockfile. Every other
Bazel command runs with `--lockfile_mode=error`, so a stale lockfile fails
instead of being rewritten.

Linux retains tested Rust and TypeScript archives with exact-source receipts
and SHA-256 inventories. Native binding producers retain their host-qualified
ABI-tested companions; cross-target checks do not produce those artifacts.
SDK Release builds its release targets with Bazel before assembly, which
verifies native receipts, installation and evidence. Publication is explicit
and consumes that run's admitted bytes without rebuilding. Retry failed jobs in
the same run to preserve source and artifact identity.

Public contracts originate here. A service implementation may validate a
candidate commit, but it must not maintain a competing customer schema.

Every extraction must follow [ARCHITECTURE.md](ARCHITECTURE.md) and include a
deletion summary for the private repository. Copying code without centralizing
ownership is not accepted.
