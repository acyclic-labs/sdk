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
Cargo does not vendor a path dependency into this archive. This is an honest
packaging blocker, so no extracted build is claimed and no dependency is hidden
with an exclude or configuration hack.

The source-only installed artifact is therefore qualified through its exact
generated header, static library, and external consumer link. A publishable
package needs an approved internal registry or a separately published Actors
dependency; adding a second hand-written C++ contract would invalidate this
qualification.

`pcwalton/cxx-async` 0.1.4 remains a maintained future bridge option. Its C++20
executor dependencies (cppcoro/Folly) are unavailable here; that does not
inherently exclude Tokio-backed Rust futures, but no linked cxx-async artifact
is claimed.
