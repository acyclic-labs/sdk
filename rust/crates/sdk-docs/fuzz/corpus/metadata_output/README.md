# Native Cargo output corpus

Original bytes, captured on Windows x86_64 with Cargo 1.98.1
(797e8a9bc, 2026-08-05). Neither capture compiled Rust code.

- `native-workspace.json`: stdout of `cargo.exe metadata --locked --offline
  --no-deps --format-version 1 --manifest-path Q:/sdk/Cargo.toml`,
  2026-11-03T02:56:31.274Z–02:56:33.083Z, status 0, no signal, empty stderr.
  The original workspace has 48 packages and members. Source commit
  `3c281aebcec21a4388462749a691d96e8be9af6d`, tree
  `14d5ae33ee322cf295f0b0354f75db6df8ff9136`.
  341405 bytes, SHA-256
  `8f646000b530793455838ca44ebb36c7cf004432ebdc89cd3b4e46a349448de3`.
- `native-missing-manifest.stderr`: stderr of `cargo.exe metadata --offline
  --format-version 1 --manifest-path Q:/sdk/absent-metadata-manifest/Cargo.toml`,
  2026-11-03T03:28:14.835Z–03:28:15.988Z, status 101, no signal, empty stdout.
  81 bytes, SHA-256
  `275f229307f2b76df9e642c15e8c0be73451e0fa6f9afddf6c0505beb1f9f4f5`.

The property additionally composes a diagnostic prefix and mutates truncation
and UTF-8 from these captured bytes. Arbitrary fuzz bytes are passed unchanged
as success stdout and failure stderr through the production output decoder.
The same oracle checks all typed metadata fields against the maintained Cargo
schema and checks exact failure bytes and error classes. The former authored
empty-workspace seeds and result-discarding target are removed.

This corpus covers output admission, not Cargo process termination, signals,
Rustdoc production, external corpus admission or a complete fuzz campaign.
