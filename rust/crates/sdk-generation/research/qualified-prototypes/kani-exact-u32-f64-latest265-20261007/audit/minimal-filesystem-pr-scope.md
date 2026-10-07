# Minimal filesystem PR scope and attestation

The direct theorem is scoped to the exported production symbol
acyclic_fs::exact_u32_from_f64 in
C:\Users\varun\.codex\worktrees\rust-sdk-docs-source\sdk\work\filesystem-main-port-latest265.

The successful receipt was launched against revision
f775b8465d836cf69b358f9fa9ebd8edbfa819db. A later checkout observation is
revision 42874b98e8079c14a32981216b0b2d19e365dcf6. The relevant production
files have byte-identical SHA-256 values at both observations:

- rust/crates/filesystem/src/numeric.rs:
  51276AA1FF76487DF226F6C6000F951842DBF0F2FEE7948796B4FBDE8FF05551
- rust/crates/filesystem/src/lib.rs:
  1BD52C28F32698CC7EF0D1C1F81616060461A531C7522BC2D2FAA90AC9F5B1DB

The source-bound theorem and expected negative control call the exported
production function through the path dependency. No function copy or
proof-only replacement is included. The current-commit observation is a
byte-identity attestation for these two files; receipt.json remains correctly
bound to its original launch revision and is not relabeled to 42874b.

For filesystem_port/fs_attestation review, the minimal evidence set is:
verification-manifest.json, receipt.json, receipt.negative-control.json, the
two raw .txt logs, audit/current-source-observation.json, this scope note,
and the production source hashes above. No target directory or global cache is
part of the scope.
