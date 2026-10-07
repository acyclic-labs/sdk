# Installed CXX artifact qualification

Captured 2026-10-07 from the authoritative checkout
`C:\Users\varun\.codex\worktrees\rust-source-foundation\sdk`.

## Artifact

The evidence crate is `cpp-actors-oss-qualification` version `0.1.0`, using
CXX/cxx-build `1.0.202`. The generated bridge header and native Rust artifact
were built from the following exact inputs:

| file/artifact | SHA-256 |
|---|---|
| `Cargo.toml` | `8C0802E0BA0273C3E701B6AA841DD31E26B83C08735EB886FB5F2FE534F709BA` |
| `Cargo.lock` | `BFBA77A4026E975AEF5593E914469EE18BC8D41A9A7331C0CBAFD7FC1EB64E20` |
| `build.rs` | `CA31A73DC13021A2B7037F273D7FBE6824D4A55156E9B8F3AD60E1B5CA01727E` |
| `src/lib.rs` | `E81406D5F9B9E886572AB0FEDA1B13FF30835616AEA0AACECF4C67AA8B58F363` |
| `consumer/live-remote.cc` | `01447D04091E44E16AB7B1AD1169925A90B969C18C2AE46A01646CF43E0CFB1A` |
| `consumer/live-cancel-fixture.mjs` | `11B50C82B7AC1678D54E2AB913078C36769387D597BE68B05529BF05EE687B35` |
| `consumer/positive.cc` | `D8308A1185C1314DD779261EEC3267546865970025415891FB33DBD9316EE82D` |
| `consumer/negative.cc` | `015F2D8DD964C75CDEF5A48911D7A6C2247366D499AB478955016D31F72A9EF0` |
| `consumer/negative-nominal.cc` | `44E1C7DB09B21D4DBA17BEB834367A6D0B586DBCBA5F5FF8D5F512847C7F055F` |
| `generated/lib.rs.h` | `F302DDDE6C4D2FE594B3C41EA45505C2EDCEA85DECD591EBD0AF45935BC5A713` |
| Windows static library | `A62B99A7BA3363FA0F40B7544595AA67B2EDFBA92B51638A7AA7DED307069639` |

Producer qualification passed with:

```text
cargo 1.98.1 (7976e8a9bc 2026-08-05)
rustc 1.98.1 (48a229cea 2026-09-01)
cargo build --locked --target x86_64-pc-windows-msvc
```

The generated header compiled with MSVC `14.44.35207`, and the C++ consumer
linked against the static library. The external consumer invoked all eight
typed operation entry points, checked optional response presence and the
canonical large `u64` cursor, rejected a wrong bearer token, and observed an
in-flight cancellation as a server-side HTTP/2 abort:

```text
live_cxx_operations:8 authentication_rejected:true cancellation:cancelled
{"inspectStarted":true,"inspectAborted":true}
```

The expected-negative consumers remain part of the installed-header check:
`negative.cc` rejects a `std::string` where the generated API requires
`uint64_t`, and `negative-nominal.cc` rejects passing `ActorsClient` where the
nominal opaque `ActorsOperation` type is required.

## Clean package boundary

The manifest uses `acyclic-actors = { version = "0.2.0", path = "../../../../actors" }`
so Cargo can validate the version declaration. `cargo package --allow-dirty
--no-verify` then fails while preparing the package because `acyclic-actors`
is not present in the crates.io index. The Actors workspace crate is unpublished;
Cargo does not vendor a path dependency into this archive. This direct command
remains an honest packaging failure; the reproducible qualification below uses
the separately produced Actors archive and an explicit external patch for the
offline extracted build.

The source-only installed artifact is therefore qualified through its exact
generated header, static library, and external consumer link. A publishable
package needs an approved internal registry or a separately published Actors
dependency; adding a second hand-written C++ contract would invalidate this
qualification.

The reproducible package qualification entrypoint is
`scripts/check-cpp-actors-package.sh`. It packages the authoritative Actors
crate first, extracts that produced `.crate`, and supplies the extracted path
through an external `[patch.crates-io]` configuration while packaging and
building this CXX crate with `--offline`. The extracted install is consumed by
an independent CMake project using `find_package(AcyclicActorsCXX CONFIG
REQUIRED)`. That project links the primitive smoke consumer and the full
eight-operation consumer; direct syntax checks retain the `uint64_t` and
nominal opaque-type expected negatives. CMake derives the package version with
`cargo metadata` and records the Cargo manifest and generated-header hashes in
the installed config. This test-only patch does not publish Actors and does
not remove or conceal the dependency.

`pcwalton/cxx-async` 0.1.4 remains a maintained future bridge option. Its C++20
executor dependencies (cppcoro/Folly) are unavailable here; that does not
inherently exclude Tokio-backed Rust futures, but no linked cxx-async artifact
is claimed.
