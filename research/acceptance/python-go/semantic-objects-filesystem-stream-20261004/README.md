# Python and Go semantic object/filesystem/stream probe

This receipt records a fresh remote gRPC run of the installed Python and Go consumers against the Rust fixture built from the isolated `codex/rust-sdk-docs-source` source tree.

Both consumers passed the same five checks:

- `ObjectsService/PutObject`: three request frames (header, body `hello`, complete).
- `ObjectsService/GetObject`: two response frames, header `fixture-object-etag`, size 25, body `rust-owned-object-payload`.
- `FilesystemService/Export`: one terminal chunk with cursor `fixture-export-cursor-1`, object ID `fixture-export-object-1`, and contents `rust-owned-filesystem-export`.
- `StreamService/Follow`: actual cancellation observed with the client call in the cancelled state / gRPC `Canceled`.
- `StreamService/Append`: recovery append completed after cancellation.

Provenance captured in both language logs:

- fixture binary SHA-256: `988C78B31A3B0CB1B9A3BBFEC297B3A7A5233CF7C87C7E7F9DAC9B4446184AB3`
- fixture source file SHA-256: `450D36071754E385CB1495A76F54D4B836C5B5688EB85C3787750C810A0D2A0A`
- installed Python wheel SHA-256: `19DC8E788379B6FF5624BFCE4A18398FBA31883FC542FC95C407396783BDD544`
- Rust-owned filesystem/harness graph: `fs35-rust-seed-graph.json`, 35 ordered steps

This is semantic evidence for the five checks, not the canonical 106-method completion receipt. The installed Python/Go artifacts predate the current fixture source snapshot, and the source-owned `sdk-examples` generator currently fails to compile in the shared branch because `filesystem_harness.rs` calls an older `Workspace::checkout` signature and compares wire/core authority types. The full gate remains pending until one clean source/model/package/fixture snapshot regenerates and reruns all modeled methods.
