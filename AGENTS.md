# Working in acyclic-labs/sdk

Instructions for AI coding agents and the humans who drive them. `CLAUDE.md`
imports this file; keep the two in step (`scripts/check-agent-docs.sh` fails
CI if they drift or if a path named here stops existing).

## The tree

- `rust/crates/<family>`: the SDK families (`filesystem`, `stream`, `objects`,
  `inference`, `machines`, `harness*`, `memory`, `sdk`, `conformance`,
  `native-runtime`, `cli`). Each owns its public contract; see
  `ARCHITECTURE.md` for the one-way dependency graph.
- `plugin/`: the `acyclic` CLI, daemon and MCP server, a product built on
  `acyclic-fs`. Its crates are workspace members (`plugin/crates/*`), its
  acceptance suite is `plugin/tests/acceptance/run-all.sh`, its public name
  is single-sourced in `plugin/product.toml`, and it releases as
  `plugin-v<version>` tags (`plugin/packaging/npm/RELEASING.md`). Read
  `plugin/README.md` before touching it; paths there are relative to `plugin/`.
- `plugins/acyclic-agent-workspaces`: the Codex-native workspace plugin.
- `typescript/packages/*`: the Bun workspace of TypeScript packages.
- `proto/`, `generated/`: wire schemas and their generated bindings
  (`bun run generate`; never edit `generated/` by hand).
- `provenance/manifest.json`: every imported source group, with its commit
  and audit result. Copying code in without an entry here fails CI.
- `scripts/qualify-ci.sh` (`qualify-ci.ps1` on Windows): every CI lane's
  exact commands. The lane list is the matrix in
  `.github/workflows/qualification.yml`.

## Rules that fail CI or reviews

- Every commit carries `Signed-off-by` (`git commit -s`) and a verified
  signature; `main` refuses unsigned commits. No AI attribution trailers,
  `Generated with` lines, or session links: commits read as the human's.
- `--locked` on every cargo command; a lockfile change is a deliberate diff.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
  with the workspace lint set: no `unwrap`/`expect`/`panic` outside tests,
  `unsafe` only behind `#[allow(unsafe_code, reason = "...")]`, docs on
  every public item.
- Review threads must be resolved before a merge; answer them, then let a
  maintainer resolve.
- Publication consumes the exact bytes a qualification run retained. Never
  add a workflow that rebuilds at release time.

## Before opening a PR

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
bun install --frozen-lockfile && bun run test
bun scripts/check-boundaries.mjs && bun scripts/check-metadata.mjs
```

To replay one CI lane locally:
`SDK_TEMP_DIR=/tmp/sdk SDK_ARTIFACT_DIR=/tmp/sdk-artifacts TOOLS_DIR=/tmp/sdk-tools bash scripts/qualify-ci.sh policy`.

## Plugin specifics

- Run the plugin's own gates from `plugin/`: `bash scripts/ci-local.sh
  --no-acceptance` for the static checks and unit tests, and
  `ACYCLIC_BIN=<release binary> bash tests/acceptance/run-all.sh` for the
  acceptance suite (Linux and macOS; `windows-smoke.sh` on Windows).
- The product name, npm package, repository and release-tag prefix come from
  `plugin/product.toml` only; `plugin/scripts/check-product-name.sh` guards
  the one file that mirrors them, `plugin/scripts/install.sh`.
- The plugin's design history lives in `plugin/docs/design/`; the launch
  status table is in `plugin/README.md`.
