# Contributing

Contributions are licensed under Apache-2.0. Every commit must include a
`Signed-off-by` trailer certifying the [Developer Certificate of Origin](DCO.md).

Before opening a pull request, run the Rust, Bun/TypeScript, Protobuf, provenance,
license, secret, and private-namespace checks used by CI. Imported code must add
an entry to `provenance/manifest.json` before it is merged.

## Build and test

Native x86_64 Linux Rust builds use clang and mold. On Debian or Ubuntu, install
them before running Cargo:

```sh
sudo apt-get update
sudo apt-get install --no-install-recommends clang mold
```

To use the system C linker instead, override the workspace linker for the command:

```sh
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=cc cargo build --locked -p acyclic-plugin
```

```sh
bun install --frozen-lockfile
bun run check   # tsc -b --force, project-wide type-check (also emits build output)
bun run test    # type-check, then TypeScript and filesystem package tests
cargo test --workspace --all-features --locked
```

Rust-only workspace tests also need the ignored Actors authoring contracts.
Generate them with the existing locked Rust producer before running Cargo tests;
no Bun installation is needed for this prerequisite:

```sh
cargo run --locked -p acyclic-actors --example actors-http-routes -- typescript/packages/actors/src/generated/actors-service.ts
cargo test --locked -p acyclic-actors --test guest_contract generated_authoring_contract_has_no_drift -- --exact
cargo test --locked -p acyclic-actors --test timer_contract generated_timer_authoring_has_no_drift -- --exact
```

The producer uses the output path's parent for `guest.ts`, `timers.ts` and
service metadata. Full TypeScript generation subsequently projects the metadata
into `actors-service.ts`. Rust qualification generates these files before its
workspace tests; drift checks only compare and never rewrite their inputs.

The `dev` and `test` profiles use LLVM, panic unwinding and line tables for
backtraces. The independent `acyclic-fs-wire` and `acyclic-plugin-identity` crates
build at `opt-level = 0`; other crates retain `opt-level = 1` because their
unoptimized async state machines exceed default thread stacks. SHA-2 and BLAKE3
retain targeted optimization for executable readiness. `--release` stays a
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
locally for the affected crates; full release qualification enforces the broader
suites. Every PR should get review and approval from a
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

`.github/workflows/qualification.yml` executes every lane in
`.github/qualification-lanes.json` at the frozen release source. The graph
includes Rust, docs/generation, policy, TypeScript/package, native, musl and
browser checks. There is no fingerprinting or prior-run lane reuse. Runner and
worker counts in the registry control compilation and test parallelism.

Dependency/tool caches and sccache reuse compiler objects without skipping
qualification; only main saves compiler caches. Windows Rust caches dependency
build artifacts in its workspace target directory, keyed by the toolchain,
manifests and lockfiles. Other lanes use sccache without caching Cargo targets.
Job summaries report command/test timings and cache hit rates.
Timing artifacts expire after three days, or thirty for release runs; coverage
uploads expire after one. Windows qualification enables and exercises ProjFS.

Linux retains tested Rust and TypeScript archives with exact-source receipts
and SHA-256 inventories. Native binding producers retain their host-qualified
ABI-tested companions; cross-target checks do not produce those artifacts.
SDK Release requires all qualification and native builds before assembly,
which verifies native receipts, installation and evidence. Publication is
explicit and consumes that run's admitted bytes without rebuilding. Retry failed
jobs in the same run to preserve source and artifact identity.

Public contracts originate here. A service implementation may validate a
candidate commit, but it must not maintain a competing customer schema.

Every extraction must follow [ARCHITECTURE.md](ARCHITECTURE.md) and include a
deletion summary for the private repository. Copying code without centralizing
ownership is not accepted.
