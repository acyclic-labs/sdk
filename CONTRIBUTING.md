# Contributing

Contributions are licensed under Apache-2.0. Every commit must include a
`Signed-off-by` trailer certifying the [Developer Certificate of Origin](DCO.md).

Before opening a pull request, run the Rust, Bun/TypeScript, Protobuf, provenance,
license, secret, and private-namespace checks used by CI. Imported code must add
an entry to `provenance/manifest.json` before it is merged.

## Build and test

```sh
bun install --frozen-lockfile
bun run check   # tsc -b --force, project-wide type-check (also emits build output)
bun run test    # type-check, then TypeScript and filesystem package tests
cargo test --workspace --all-features --locked
```

The `dev` and `test` profiles build at `opt-level = 1`, which keeps backtraces
usable while the heavy fork tests run several times faster. `--release` stays a
fast incremental build for development; the shipped `acyclic` executable comes
from `node scripts/build-product.mjs`, which uses the `dist` profile (fat LTO,
one codegen unit, stripped) and writes `target/[<triple>/]dist/acyclic`.

## Commit messages

- Summary line: imperative mood ("Add", "Fix", "Rename", not "Added"/"Fixes"), no
  trailing period.
- Blank line, then a body explaining *why* the change is needed, not a restatement
  of the diff.
- No AI attribution trailers or "Generated with ..." lines (`Co-Authored-By:` naming
  an agent, session links, etc.) — the commit should read as yours, whatever tools
  helped write it. `Signed-off-by:` is still required.

## Before opening a PR

Run `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --all-features
--locked -- -D warnings`, and `cargo test --workspace --all-features --locked`
locally — CI enforces all of them. Every PR should get review and approval from a
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

`.github/workflows/qualification.yml` is the only authored qualification graph;
`.github/qualification-lanes.json` lists its lanes, which run on Blacksmith and
form a bounded graph of at most four concurrent matrix lanes. Pull requests and
`main` pushes run the lanes scoped `core` or `both`, in parallel:

- `gate`: `cargo nextest run --workspace --all-features --locked` (unit and
  integration tests, without the ignored live-mount and fork/join suites) and
  the standalone `sdk-docs` crate.
- `policy`: `cargo clippy --workspace --all-targets --all-features --locked -- -D
  warnings`, rustfmt, and the planner and preflight script tests.
- `typescript`: `bun run check:generated` and `bun run test` (WASM and
  TypeScript builds, type tests, package tests, and protocol contracts).

Release, scheduled, and manual runs run the `full` and `both` lanes instead:
coverage, policy (cargo-deny, rustdoc, feature sets, workflow lint), packaging
on Linux, native suites on Linux ARM64 and macOS, Windows, musl, and browser.
A lane whose observed inputs (see `ignored` in
`scripts/plan-qualification.mjs`) already qualified is reused, so a pull request
touching no TypeScript, WASM-compiled Rust, or generated bindings skips the
`typescript` lane. Every lane caps Cargo, CMake, Make, Rayon, and test parallelism at four
processes. The macOS runner is Blacksmith's smallest six-vCPU image but still uses
only four processes. Blacksmith's colocated dependency/tool cache and sccache make
cold and warm runs fast; Cargo target directories are never cached. Only `main`
saves compiler caches (main pushes run the core lanes; a manual run on `main`
warms the full lanes), and every other run restores main's newest entry.
Successful jobs and their declared outputs may be reused only for the identical
source tree, manifest semantics, toolchain, lockfile, operating system, and
architecture.

Every lane's job summary shows the wall time of each top-level `qualify-ci.sh`
command, the slowest Rust and TypeScript tests with per-binary and per-file
totals, and sccache hit rates (`scripts/ci-summary.mjs`); the raw JUnit, step,
and cache files are kept for seven days as the `timings-<lane>` artifact. Linux
and macOS run Rust tests with pinned cargo-nextest, whose `ci` profile
(`.config/nextest.toml`) flags tests slower than 30 seconds. Doctests, coverage,
the serial live-mount suites, and the Windows lane stay on `cargo test`.

The policy lane verifies and runs pinned cargo-deny and gitleaks archives from the
tool cache. The secret scanner keeps all default rules. Blacksmith's Windows image does
not enable the `Client-ProjFS` optional component: the Windows lane
executes the portable workspace and TypeScript suites and compiles all ProjFS paths,
while Linux and macOS execute native-mount behavior.

The Linux lane reuses a prior result only for an identical source tree and returns
the exact retained output inventory with that result. It retains the isolated, tested Rust
crates and all twelve public TypeScript package archives with a source-bound qualification
receipt and SHA-256 inventory in `packages-linux`. The filesystem archive runs
the existing public-export/WASM composition test outside the workspace; a missing
packaged WASM must fail. Cargo packages and verifies the public Objects, Streams,
and Filesystem dependency closure together with all features. Registry publication
must publish the exact Objects and Streams archives before Filesystem.
Publication consumes these exact successful-run bytes, never a rebuild. An operator
creates an immutable family or TypeScript GitHub release from the retained artifact
and points its tag at the matching qualified main commit. The crates.io publisher
remains a separate, tag-triggered publication boundary. Qualification runs on
Blacksmith; the small npm publication job uses a GitHub-hosted runner so npm trusted
publishing can exchange GitHub's OIDC identity without a stored token. After the exact
main commit qualifies, an operator creates an annotated `npm-v<VERSION>` tag at that
commit. The publisher downloads the exact retained archives, verifies their source and
integrity receipt, and publishes them directly to `latest` with provenance. Each
executable native lane
also retains the exact filesystem companion copy
loaded by its successful ABI child, named by package version/host OS/architecture with
a SHA-256 inventory. An existing output directory is rejected. These are host-qualified
debug binaries, not optimized or cross-target binaries; cross-target `cargo check` does
not produce a publishable companion. Retained archives are not published registry
versions, browser qualification, or qualification of other SDK families.

Public contracts originate here. A service implementation may validate a
candidate commit, but it must not maintain a competing customer schema.

Every extraction must follow [ARCHITECTURE.md](ARCHITECTURE.md) and include a
deletion summary for the private repository. Copying code without centralizing
ownership is not accepted.
