# Stream UniFFI source-minimality and cohort manifest

Date: 2026-10-07
Scope: isolated research prototype; no product source, registry, merge, publish, or deployment.

## Producer closure

The maintained Rust producer is `Q:\sdk\work\actors-stream-uniffi-kotlin-prototype-20261007\src\lib.rs` and imports the real `acyclic_stream::StreamClient`, `MemoryStream`, `grpc::Client`, `RecordStream`, `ChildStream`, and `StreamError` APIs from `Q:\sdk\work\sdkgen-actors-c8-minimal\rust\crates\stream`. The bridge exposes the same opaque cursor shape for local and TLS remote clients:

- `StreamClientBridge`: `new`, `append`, `read`, `follow`, `children`, `activeRecordCursors`.
- `RemoteStreamClientBridge`: `connect(endpoint, bearerToken, caCertificatePem)`, `append`, `read`, `follow`, `children`, `activeRecordCursors`.
- `RecordCursor` and `ChildCursor`: generated opaque objects with `suspend next()` and `suspend closeCursor()`.

The bridge has no Rust recovery, replay, buffering, ordering, semantic type mirror, operation registry, or transport reimplementation. The remote object calls the maintained gRPC `Client::connect_with_ca_certificate`; the server fixture calls the maintained gRPC `Service` over TLS. The only cancellation accounting is an RAII guard that records server stream drop for qualification.

## Kotlin handwritten closure

The only handwritten Kotlin integration is:

- `StreamFlows.kt`: one generic cursor-to-Flow helper and three local/remote overloads; `finally` invokes generated `closeCursor()` inside `NonCancellable`.
- `StreamFlowProbe.kt` and `RemoteStreamFlowProbe.kt`: qualification fixtures only.
- `PublicSurfaceProbe.kt`: compile-only proof that every public operation accepts full-width `ULong`/`UInt`, returns generated cursor/`ULong` types, and exhaustively exposes typed `StreamBridgeException` variants.

There are no handwritten Actors operation lists, Rust record mirrors, semantic predicates, callback stream implementations, or Kotlin recovery algorithms. A source search of the handwritten files found no `when`-based operation dispatch and no actors-domain type names.

## Maintained generator

The generator is task-local UniFFI 0.31.0 at `Q:\sdk\work\actors-uniffi-generator-kotlin-nominal-20261007-v2\uniffi_bindgen-0.31.0`, MPL-2.0. Its Kotlin template is generic: the only custom policy used here is `generate_immutable_records = true`, yielding `val` record fields; opaque constructors remain `internal` via the maintained opaque-constructor patch. The wrapper passes an explicit `uniffi.toml` config path so this policy is reproducible instead of relying on process working-directory discovery. A search of the maintained generator source contains no Actors or Stream domain names.

## Exact source and artifact identities

- Prototype `src/lib.rs`: `407A338212CF2B208D99280B0804C3F83FB4AE01315519CB671C3167B12F58D9`
- Prototype `Cargo.toml`: `2C186AAE94FCFF2D99774EF453507CE0DD861C314C939828E2AB9A014720B9BD`
- Prototype `Cargo.lock`: `7B2A751A8F5774031B5B9C9D71AF4670A1D8B46F77C5958CD66575E7B92FBCCB`
- `uniffi.toml`: `ADAB4F663D8E89E40167715922CBA7EAD525F9E110C9C55C7960E7C69A8CE824`
- Generator wrapper: `597B6BC68D6D7580861831DAFE24D1E41519E0D4C4674C28380B28E798C1BF02`
- Generated Kotlin (`val` records, `ULong` fields): `2302DFE9C777CF3247E81848C9DF21D11107AF78F955A51A9F8317229418F25B`
- Windows DLL: `394D30E10400F725A0BFB8DA85342BAD7301B2D4FE8EE7F64350E1BE1DCFFB96`
- Linux SO rebuilt from the same source/lock: `DF7E5BD07517D776EF9250F42ECEAC1D775458637F12A3C3881A4428053890B3`
- Windows installed JAR: `B28EEB3D12A43A2147CAE1FC718448B87EC4EF553DD44725A5C4D460C813249F`
- Linux installed JAR variant with standard `linux-x86-64/` JNA resource: `F548D44FE97A68A9F0074FA5A7B81C9889E03CA851C9A07B4ED0E14DE060A248`
- `StreamFlows.kt`: `569D4D6F3FD1AEDB58E2E93E01A380F94CA43204AF1222050F3AE5CB252C1751`
- `PublicSurfaceProbe.kt`: `02D5CC2247A8507C49637D496762B6184AFA949E25D874B07FD5D8DCE3617A87`

The native files were independently rebuilt for Windows and Linux. No older native bytes were retagged with the current lock. No macOS host or macOS native build is connected to this task, so macOS installed-JAR execution is explicitly pending rather than claimed.

## Qualification result

Windows and Linux installed JARs both pass ordered typed reads and coroutine cancellation cleanup. The Linux JAR uses the same generated classes and Rust source/lock cohort, with a standard JNA `linux-x86-64/` resource. The TLS remote fixture was rebuilt for Windows and Linux from the same fixture source/lock; both pass ordered reads, nonzero-cursor replay, post-abort recovery, and a server-side `follow-closed` marker after Kotlin cancellation. The compile-only surface probe passes Maven compilation and covers `ULong.MAX_VALUE`, `UInt.MAX_VALUE`, all public bridge operations, cursor methods, immutable records, and both typed error variants.
Raw installed-platform receipt: platform-installed-qualification-20261007.raw.txt (SHA256 205097E4250C42EFCA9DB2F77F646E35E67F9C8D4389F15D9B228F99B3E2982C).
Reusable Linux TLS/JAR runner: Q:\sdk\work\actors-stream-uniffi-kotlin-prototype-20261007\run-linux-remote.sh (hash recorded locally above).
Runner SHA256: 622C5FC0A29FC6DA85EFF3A35F964B7F131527A8306E04F18DBB132C001C2A66.
