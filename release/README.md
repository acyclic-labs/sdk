# Releasing Acyclic

All release tags must be annotated tags at the same qualified commit on `main`.
There is no staging registry or `next` promotion step.

1. Merge the release commit and wait for **SDK Qualification** to pass.
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

Both publishers are idempotent: an existing version is accepted only when its
registry checksum or integrity is identical to the qualified artifact.

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
- workflow: `publish-cargo.yml`
- environment: `crates-io`

For a crate that does not yet exist, create a narrowly scoped, short-lived
crates.io token, publish the exact qualified crate once, revoke the token
immediately, configure the trusted publisher, and require trusted publishing for
future releases. No standing Cargo registry token belongs in repository or
environment secrets.
