# Native Cargo package capture

`native-package` is the two fuzz configuration lines `sdk-file-identity` and
`0.0.0`, followed by the unmodified original Cargo archive bytes. The fuzz
configuration varies package/version independently from the external archive.
The production decoder receives exactly the original archive slice.

Cargo 1.98.1 (797e8a9bc, 2026-08-05), Windows x86_64, produced the archive using
`cargo.exe package -p sdk-file-identity --no-verify --offline --allow-dirty
--target-dir <output>` from source commit
`0a7fef982604b61678eb9dd50b956a9a197a0997`,
2026-11-03T03:45:33.858824Z through 03:45:39.410653Z, status 0, no signal.
No Rust compilation or verification was requested. The archive is 3279 bytes,
SHA-256 `4a62fb5907e82395b2501885424ebdb95b1f4e84bd9f8bc8b15bb52cab4c8179`.

The five-file inventory in `../../archive-members-native.json` was recorded
independently with Python 3.12 tarfile and hashlib over this actual Cargo output.
Its names were checked against verbatim native `tar -tf <archive>` output,
SHA-256 `35adfa9a350ab28f52157caa712ca2d79ad09cf5c2163c310912cc0ba42ac949`.
Canonical LF inventory JSON SHA-256:
`cd8c9aa56e9c8d4ca9593f9c0808e1a6502296bc0f7bb592f91e8920f296002c`.
The former raw corpus had no recorded provenance and is replaced by this input.
Malformed/truncated archives remain arbitrary fuzz inputs and generated controls.

The shared oracle compares every accepted original body and the complete
inventory through the maintained TAR/gzip libraries. It also checks package
binding, member kinds and portable paths. It replaces the copied old SDK
admission algorithm; it does not copy the protective-limit reader. Existing
limit and immutable-publication controls are retained. Instrumented source
coverage, a fuzz campaign and real registry import/publication remain separate
qualification obligations.
