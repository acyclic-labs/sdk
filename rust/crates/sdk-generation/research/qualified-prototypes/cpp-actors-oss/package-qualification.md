# Installed CXX artifact qualification

Captured 2026-10-07 from the authoritative checkout
`C:\Users\varun\.codex\worktrees\rust-source-foundation\sdk`.

The current receipt is the installed-package run
`/tmp/cpp-actors-package-final10`. The linked checkout did not expose a
resolvable Git worktree identity at that run, so the tested source is identified
by the exact `.crate` archives and the extracted file-level `SOURCE-INVENTORY`,
not by a possibly stale `HEAD`. The output also includes `SOURCE-IDENTITY`,
`SOURCE_COMMIT` (explicitly unavailable when Git cannot resolve the worktree),
and `SHA256SUMS` for the staged archives, header, native library, and inventory.

## Artifact

The evidence crate is `cpp-actors-oss-qualification` version `0.1.0`, using
CXX/cxx-build `1.0.202`. The generated bridge header and native Rust artifact
were built from the following exact inputs:

| file/artifact | SHA-256 |
|---|---|
| `Cargo.toml` | `A1565A09BE8B0A5982706694617B84105BCF09082EEFB13E369E34EC36B8CB35` |
| `Cargo.lock` | `BFBA77A4026E975AEF5593E914469EE18BC8D41A9A7331C0CBAFD7FC1EB64E20` |
| `build.rs` | `CA31A73DC13021A2B7037F273D7FBE6824D4A55156E9B8F3AD60E1B5CA01727E` |
| `src/lib.rs` | `DB02E523ABE82EDC10CD758E23F32A8826436E2FA9ED83DC11899F6B8791CAFE` |
| `consumer/live-remote.cc` | `68AEDFC16052657AC1D3FB2D4994B7A04A9AC798A26DBF52214003F278796EA4` |
| `consumer/live-cancel-fixture.mjs` | `7A362F198D99AA6034E7DDA0B329E5D2C97793C8BE9DDD7B53C04FDAF7BB168E` |
| `consumer/positive.cc` | `D8308A1185C1314DD779261EEC3267546865970025415891FB33DBD9316EE82D` |
| `consumer/negative.cc` | `015F2D8DD964C75CDEF5A48911D7A6C2247366D499AB478955016D31F72A9EF0` |
| `consumer/negative-nominal.cc` | `44E1C7DB09B21D4DBA17BEB834367A6D0B586DBCBA5F5FF8D5F512847C7F055F` |
| `generated/lib.rs.h` | `3D36055C5905A7AE32FFF56537BD08E1D9B2713C4C681010F2D4265D20366559` |
| Windows static library | `79D410934FCEB1331406764B9762E45CC6B7A1A533233CEAF4D866DBCD642203` |

The final installed run produced these package identities:

| artifact | SHA-256 |
|---|---|
| `acyclic-actors-0.2.0.crate` | `539528F67D7D0A7539902A8E9F4E3B55BB94812324FE3171F5F4F7B7F5E07753` |
| `cpp-actors-oss-qualification-0.1.0.crate` | `7EF0CB8A496CD8E79F3667E42045659659DB36ABD8ED2711781B5C9A428EAEB5` |
| Linux/WSL static library | `C5033BBEEEC6D022684B4318738000B4DAA93C778357DC869862CBA1CD513B2A` |
| macOS/ivar static library | `BE0AC8CF6AD674E4B38ABFD7FDB6AB3F03ADCBE68264D5E9B2BA243E76B12539` |

The CXX archive's extracted source inventory is staged as
`SOURCE-INVENTORY`; it hashes every file under both extracted crates. The macOS
run consumed the same two archives, with generated header SHA-256
`3D36055C5905A7AE32FFF56537BD08E1D9B2713C4C681010F2D4265D20366559`.
For the current receipt, `SOURCE-INVENTORY` SHA-256 is
`0F73BA3D8D1C9415C3C7FFCE57611903F58CD0C9AAFD623ECCB902945543333`.

Producer qualification passed with:

```text
cargo 1.98.1 (7976e8a9bc 2026-08-05)
rustc 1.98.1 (48a229cea 2026-09-01)
cargo build --locked --target x86_64-pc-windows-msvc
```

The Cargo producer built the `x86_64-pc-windows-msvc` artifact with the
installed MSVC toolchain, and an external CMake consumer compiled the installed
generated header and linked against the Windows static library. The installed
Windows consumer invoked all eight typed operation entry points against the
canonical authenticated TLS fixture, checked optional response presence and
the canonical large `u64` cursor, rejected a wrong bearer token, and observed
an in-flight cancellation as a server-side HTTP/2 abort:

```text
live_cxx_operations:8 authentication_rejected:true cancellation:cancelled
{"inspectStarted":true,"inspectAborted":true}
```

The expected-negative consumers remain part of the installed-header check:
`negative.cc` rejects a `std::string` where the generated API requires
`uint64_t`, and `negative-nominal.cc` rejects passing `ActorsClient` where the
nominal opaque `ActorsOperation` type is required.

## Cross-platform installed cohort

The Linux/WSL and Windows installed packages are live runtime qualifications:
clean external `find_package`, all eight operations, TLS/authentication, typed
error, in-flight cancellation, and server abort cleanup all passed. The Windows
consumer used clang 17 with the installed MSVC STL version-mismatch opt-out;
the Cargo producer and static library were built for `x86_64-pc-windows-msvc`.

The macOS/ivar cohort was rebuilt from the exact two package archives with
`cargo +1.98.1 build --locked` on Darwin 24.6.0 arm64. Apple clang 17 compiled
the installed generated header, linked the static library, and ran the positive
consumer; both strong-type negatives failed as expected. The current macOS
native library hash is recorded above. No macOS live TLS fixture is claimed because
the retained live fixture is the Linux/WSL installed run.

This matrix separates a local/embedded native artifact from a remote fixture:
the CXX bridge remains Rust-first and delegates transport, typed errors, u64,
optional presence, and cancellation to Actors. Other maintained OSS executor
routes (including `cxx-async` with cppcoro/Folly) remain candidates when their
dependencies are installed; an unavailable executor is recorded as an
environment gap rather than a broad language or platform exclusion.

## Minimality audit

`ActorOperationResult` is opaque across the CXX boundary; its fields remain
private to Rust and the generated header exposes only Rust-owned accessors used
by the qualification consumer. The eight named CXX operation methods are
one-to-one delegates to typed Rust client operations. No C++ request/response
DTO, transport algorithm, future representation, or duplicated field layout is
authored in this boundary.

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
