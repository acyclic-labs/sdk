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

`.github/workflows/qualification.yml` is the only authored qualification graph.
Its independent Linux, Linux ARM64, Windows, macOS, browser, coverage, and policy
lanes run concurrently on Blacksmith. Per-lane caches restore the exact toolchain,
dependency, tool, and incremental-build state first by source commit and then by
locked dependency identity, so retries are exact and new commits can safely reuse
prior compilation. Heavy compilation uses 16-vCPU runners with 12 build jobs;
browser work uses 8 vCPUs, macOS uses 12, and the aggregate gate uses 2.

The policy lane verifies and runs pinned cargo-deny and gitleaks archives from the
tool cache. The secret scanner keeps all default rules. Blacksmith's Windows Server
2025 image does not enable the `Client-ProjFS` optional component by default, so the
Windows lane enables it online before executing the writable ProjFS lifecycle, live
USN continuity, N-API, daemon, and TypeScript runtime suites.

The Linux lane never reuses a prior job result because its Cargo archives bind the
exact source commit in `.cargo_vcs_info.json`. It retains the isolated, tested Rust
crates and the six core Objects, Stream, Inference, Machines, Filesystem, and SDK
TypeScript archives with a source-bound qualification receipt and SHA-256 inventory
in `packages-linux`. The filesystem archive runs
the existing public-export/WASM composition test outside the workspace; a missing
packaged WASM must fail. Cargo packages and verifies the public Objects, Streams,
and Filesystem dependency closure together with all features. Registry publication
must publish the exact Objects and Streams archives before Filesystem.
Publication consumes these exact successful-run bytes, never a rebuild. An operator
creates an immutable family or TypeScript GitHub release from the retained artifact
and points its tag at the matching qualified main commit. The crates.io publisher
stays on Blacksmith. npm trusted publishing is the sole CI exception: npm requires
a GitHub-hosted runner for OIDC, and its publisher is stage-only. CI submits exact
qualified archives with `npm stage publish`; a maintainer must review and approve
each staged package with 2FA before it becomes public. Each executable native lane
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
