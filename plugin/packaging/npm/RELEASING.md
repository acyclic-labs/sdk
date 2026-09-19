# Releasing to npm

`.github/workflows/release.yml` builds the `acyclic` binary for four targets,
attaches them to a GitHub release, and publishes five npm packages: the
launcher `@acyclic-labs/plugin` and one `@acyclic-labs/plugin-<os>-<cpu>`
platform package per target. Users run `npm i -g @acyclic-labs/plugin`; npm
installs only the platform package that matches their machine.

> **Qualified bytes.** In the sdk repository the release binaries are the ones
> the qualification lanes built and drove the acceptance suite against
> (`plugin-linux`, `plugin-macos`, `plugin-windows` artifacts, each with a
> `SHA256SUMS` and `SOURCE_COMMIT`). They are never rebuilt at release time.
> Two are retained without the acceptance suite: `darwin-x64` (no Intel
> macOS runner exists, so it is a cross-build from the macOS lane smoke-tested
> under Rosetta) and `linux-arm64` (built and version-checked on the ARM64
> lane; running the suite there waits on a lane-time measurement).

## How a release ships from this repository

1. **Bump.** Set the new version in all four `plugin/crates/*/Cargo.toml`
   (literal `version = "..."`), in `plugin/LATEST`, and add the `CHANGELOG.md`
   entry. Merge that PR; `scripts/test-plugin-publication.py` (linux lane)
   fails if the four manifests and `LATEST` disagree.
2. **Qualify.** The merge's push run on `main` builds the stripped release
   binary in each native lane, drives the acceptance suite (or the Windows
   smoke) against it, and retains it as `plugin-linux`, `plugin-linux-arm64`,
   `plugin-macos` (arm64 + the cross-built x64) and `plugin-windows`, each
   with `SHA256SUMS` and `SOURCE_COMMIT`. Artifacts live seven days; tag
   within that window or re-run the qualification on the same commit.
3. **Tag.** An annotated, signed tag on that exact `main` commit:
   `git tag -s stage/npm/plugin/<version> <sha> -m "plugin <version>"` and
   push it. Note the tag object SHA (`git rev-parse stage/npm/plugin/<version>`).
4. **Run `Publish plugin`** (`.github/workflows/publish-plugin.yml`) with the
   tag and its object SHA. It verifies the tag, the commit's presence on
   `main`, the version agreement, then downloads the retained binaries from
   the qualification run and checks every `SHA256SUMS` and `SOURCE_COMMIT`.
   Nothing is rebuilt. The `release` job attests build provenance, writes a
   per-binary dependency list from the locked graph, and creates the
   immutable GitHub release `plugin-v<version>` (asset names
   `acyclic-<os>-<cpu>[.exe]`, `SHA256SUMS`, `*.deps.txt`). The `stage` job,
   gated by the `npmjs` environment, assembles the platform packages and the
   launcher from those same bytes and stages them on npm with OIDC
   provenance, platform packages first.
5. **Approve on npm.** A maintainer reviews and approves each staged package
   with 2FA; the launcher last. Until approval nothing is public.
6. **Verify.** `plugin/scripts/install-smoke.sh` on a clean container, and
   `npm i -g @acyclic-labs/plugin@<version> && acyclic --version`.

No token secret exists anywhere in this flow: npm uses trusted publishing
(configured once per package on npmjs against `acyclic-labs/sdk` and
`publish-plugin.yml`), and GitHub's own token creates the release.

## Local dry runs

`packaging/npm/release-local.sh` still builds, packs and (with
`--dry-run`) exercises the packaging scripts on this machine; it is a
development aid, never a publication path.

## Deprecated packages

`@acyclic-labs/acyclic` and `@acyclic-labs/acyclic-darwin-arm64` were the
first attempt at the launcher and are deprecated on npm in favour of
`@acyclic-labs/plugin*`.
