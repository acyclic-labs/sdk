# Changelog

All notable changes to Acyclic are recorded here. Every Rust crate, npm package,
and coding-agent integration in this repository shares one version and one
release commit.

## 0.1.1 - 2026-09-24

### Added

- A unified 0.1.1 release of the Rust and TypeScript SDK families and the
  `acyclic` coding-agent plugin, with one qualified source commit and
  platform-specific binaries.
- Native-view and projection improvements for source-backed workspaces, plus
  real-mount qualification across Linux FUSE, macOS NFS, and Windows ProjFS.

### Changed

- Package documentation now describes each family separately, including its
  provider and host guarantees.
- Release publishing uses exact qualified artifacts and short-lived trusted
  publisher credentials. Existing immutable 0.1.0 Cargo archives are retained;
  changed source is published under 0.1.1.

## 0.1.0 - 2026-09-21 (partial Cargo bootstrap)

### Added

- Published the first 0.1.0 Cargo archives for `acyclic-inference`,
  `acyclic-machines`, and `acyclic-native-runtime`. The remaining SDK packages
  and the binary release were not published at 0.1.0.

See [`release/README.md`](release/README.md) for the release and trusted-publisher
runbook.
