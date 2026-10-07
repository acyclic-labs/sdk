# Kani observation `u64` projection prototype

This prototype is source-bound to the current production Actors generated wire
module through `include!`: `rust/crates/actors/src/generated/acyclic.actors.v1.rs`.
The harnesses use symbolic full-width `u64` values for subscription delivered,
completed, recoverable, and optional failed cursors; optional checkpoint time;
checkpoint epoch; actor configuration revision; and the Update expected
configuration revision. They also cover optional actor presence in
`CreateActorResponse` and `UpdateActorResponse`.

The projection reads fields by reference and does not clone a whole wire value.
Strings are initialized by defaults and are outside the mathematical claims;
transport, serialization, service behavior, and arbitrary-string semantics are
outside the claims as well.

The production source inventory contains generated wire types but no
production `project_*` function. The theorem therefore checks the
prototype-local projection against symbolic values in those actual production
types. It does not claim identity with a production projection implementation.
The negative audit mutates one local field mapping and records the resulting
Kani failure in `audit/negative-delivered-cursor-proof.raw.txt`.

## Environment

The intended run uses Kani 0.68.0, CBMC 6.11, and nightly-2026-08-21. The
cached Kani bundle and dependencies are on Linux ext4 under `/home/var`; run
from a Linux-ext4 copy of this directory rather than the Windows/WSL drvfs
mount. The source path remains the current checkout's generated Actors file.

## Run

```sh
cargo kani --bin kani_observation --harness actor_projection_preserves_selected_u64_and_option_fields
cargo kani --bin kani_observation --harness create_response_actor_optional_presence_is_preserved
cargo kani --bin kani_observation --harness update_expected_revision_and_actual_projection_are_preserved
```

The result is only conclusive when Kani reports `VERIFICATION SUCCESSFUL` for
the source-bound harness. Cargo metadata, registry, mount, or toolchain
failures are environment outcomes and are not proof results.
