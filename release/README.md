# Releasing Acyclic

All release tags must be annotated tags at the same qualified commit on `main`.
There is no staging registry or `next` promotion step.

1. Merge the release commit and wait for its **SDK Qualification** workflow run
   to succeed (every job, not only the required check).
2. Push `acyclic-v<VERSION>`. **Release Acyclic** builds and certifies every
   platform binary, assembles the universal plugin, and creates the GitHub
   release.
3. Push `npm-v<VERSION>` at the same commit. **Publish npm release** downloads
   the exact successful SDK qualification and universal-plugin artifacts,
   verifies their source commit and bytes, and publishes all packages in the
   order defined by [`npm-packages.json`](npm-packages.json).
4. Push `cargo-v<VERSION>` at the same commit. **Publish Cargo release** imports
   the exact qualified source bundle and publishes crates in the order defined
   by [`cargo-crates.json`](cargo-crates.json).

Both publishers are idempotent. npm requires identical registry integrity.
The npm publisher also verifies that `latest` points at the release version,
including on a retry after an exact archive was already published. If it does
not, repair the dist-tag interactively before retrying. npm trusted-publisher
OIDC authorizes `npm publish`, but not `npm dist-tag` or `npm deprecate`; do not
add a standing token to automate those account-management commands. After
publication, deprecate obsolete RC and plugin versions interactively and
verify their registry deprecation notices.
Cargo requires identical archive checksums, except for the already published
0.1.0 crates pinned in [`cargo-equivalent-archives.json`](cargo-equivalent-archives.json):
their registry checksums are accepted only while the crate source, workspace
manifest, and lockfile remain unchanged from the recorded source commits.
Those archives differ because Cargo embeds the packaging commit in
`.cargo_vcs_info.json`.

The GitHub release includes `SHA256SUMS` and `acyclic.spdx.json`. The release
workflow verifies the checksum manifest before upload and creates both SLSA
build-provenance and SBOM attestations for every subject in that manifest. A
consumer can verify downloaded files with `sha256sum --check SHA256SUMS` and
GitHub provenance with `gh attestation verify <file> --repo acyclic-labs/sdk`.

## npm trusted-publisher bootstrap

Configure every package in `npm-packages.json` with one GitHub Actions trusted
publisher:

- organization or user: `acyclic-labs`
- repository: `sdk`
- workflow: `publish-npm.yml`
- environment: `npmjs`

The plugin and SDK packages intentionally share this identity. Do not configure
`release-acyclic.yml` as a publisher. After trusted publishing works, select the
npm package setting that requires two-factor authentication and disallows token
publication.

npm cannot configure a trusted publisher until a package exists. For each new
package, publish its exact qualified archive once from an interactive maintainer
session with two-factor authentication, configure the publisher above, and then
enable token rejection. Do not add a bootstrap token to GitHub Actions.

## crates.io trusted-publisher bootstrap

Configure every crate in `cargo-crates.json` to trust:

- repository: `acyclic-labs/sdk`
- workflow: `publish-crate.yml`
- environment: `crates-io`

For a crate that does not yet exist, create a narrowly scoped, short-lived
crates.io token, publish the exact qualified crate once, revoke the token
immediately, configure the trusted publisher, and require trusted publishing for
future releases. No standing Cargo registry token belongs in repository or
environment secrets.
