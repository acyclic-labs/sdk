# Releasing Acyclic

SDK npm packages are published directly from qualified artifacts; there is no
staging registry or `next` promotion step.

1. Merge the release commit to `main` and wait for **SDK Qualification** to pass.
2. Create and push an annotated `npm-v<VERSION>` tag at that exact commit.
3. The **Publish npm release** workflow downloads the successful Linux
   qualification artifact for the tagged commit, verifies every archive and
   publishes the packages in the dependency order defined by
   [`npm-packages.json`](npm-packages.json).
4. The publisher verifies npm's integrity value after each upload. A rerun skips
   an already-published package only when its registry bytes are identical.

Every package must trust `.github/workflows/publish-npm.yml` in the `npmjs`
GitHub environment. A package that has never been published must be bootstrapped
once before npm allows trusted publishing; remove that bootstrap credential as
soon as its first release exists.

The coding-agent plugin is released separately by the `acyclic-v<VERSION>` tag
through `.github/workflows/release-acyclic.yml` because its universal package is
assembled from platform binaries and native-mount certification receipts.

Cargo crates are published from an annotated `cargo-v<VERSION>` tag at the same
qualified `main` commit. The workflow validates every publishable workspace
crate and its dependency order, then publishes idempotently with crates.io
trusted publishing. Each crate must trust `.github/workflows/publish-cargo.yml`
and the `crates-io` GitHub environment. The ten crates that have never existed
on crates.io require a one-time manual first publication before that trusted
publisher can be configured; remove the bootstrap credential immediately.
