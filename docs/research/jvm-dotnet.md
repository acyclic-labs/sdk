# JVM and .NET transport prototype research

## Decision

The first JVM and .NET artifacts are transport-only packages generated from
the Protobuf family closure enumerated by the Rust authority manifest. They preserve
the package names emitted by the existing Protobuf packages because adding
`java_package` or `csharp_namespace` would change the active descriptors.
Rust-owned facades and semantic helpers remain a follow-up generation surface.

Hosted gRPC is the qualified first transport. It preserves unary, server
streaming, and client streaming method shapes, metadata, status codes, and
cancellation. HTTP/OpenAPI is deferred until every HTTP route is represented by
the canonical descriptor rather than the current mixed Rust/TypeScript route
inventory.

## JVM prototype

`jvm/pom.xml` stages every Rust-emitted proto listed under an explicit
`acyclic.schema.root` property and invokes pinned `protoc` and
`protoc-gen-grpc-java` artifacts through the Maven protobuf plugin. The
generated Java classes and gRPC stubs
are packaged as `dev.acyclic:acyclic-sdk-jvm-transport`; the opt-in
`-DgenerateKotlin` profile invokes the pinned grpc-kotlin wrapper to emit
coroutine stubs. The Maven plugin's normal `pluginArtifact` path tries to
execute the grpc-kotlin jar as a native Windows process. The profile therefore
also accepts `-Dgrpc.kotlin.plugin.executable=PATH`, allowing a small local
`java -jar <pinned-grpc-kotlin-jar> %*` wrapper. That override generated
`StreamGrpcKt.kt` successfully on this Windows host; Linux/CI can use the
artifact path directly. The generated Java source is adapted only by the
repeatable `adapt-generated-harness.ps1` target-specific pass described below;
the `.proto` inputs and descriptors are never edited.

`jvm/src/test/java` checks exact `uint64` round trips, oneof selection, Harness
reflection/serialization after the generated-source accessor adapter, and
server-stream cancellation through an in-process gRPC server. The consumer
project under `jvm/consumer` depends only on the locally installed Maven
artifact and compiles a clean consumer against the generated Actors and Stream
types. The clean Java consumer was run from the installed Maven artifact.
Set `ACYCLIC_FIXTURE_ENDPOINT` to run the opt-in Rust fixture test, which
performs Actors unary creation plus Stream append/read/follow recovery against
the same generated stubs. The live probe commits an append, replays it by
idempotency key, inspects the recorded observation, rejects a mismatched reuse
with `FAILED_PRECONDITION`/`idempotency_mismatch`, cancels Read and Follow
streams, then appends at the expected tail and reads the resumed record. The
source-matched fixture identity and assertion output are recorded in the
staged JVM report. The manifest's nine-family
authority export was staged and hash-verified. All nine families (ten proto
inputs including validation options) compile through Java and Kotlin.
`harness/v2` is covered by a narrow post-generation adapter that changes only
the Java accessor spelling needed to avoid the `FileRef.descriptor`/static
`getDescriptor()` collision; field number, wire name, JSON name, reflection
metadata, builder behavior, and bytes remain unchanged.

The shared Rust cross-language fixture at
`staged-jvm-dotnet-evidence/golden/tmp-cross-language-family-fixtures.json`
contains one source-bound wire vector for each of the nine authority families.
The Java test parses and reserializes all nine messages byte-identically, and
the installed .NET package consumer performs the same check after restoring the
nupkg from a clean local source. This is serialization coverage for all
families; the live server probe remains bounded to Actors and Stream.


The shared Rust examples renderer was also exercised through the installed artifacts. Its source-bound bundle is recorded at `staged-jvm-dotnet-evidence/examples/sdk-examples-manifest.json` (manifest SHA-256 `43A8B872B7B4357D979D805EA50B36E99184FD8DB72D548B3557874B8DA787BB`; renderer source SHA-256 `2C14631A894FB6D0522364977F99A1580DD043D741C7DA4FCDBA755397540F09`). The exact rendered Actors and Stream Java snippets compiled against the installed JAR and executed against the Rust fixture, printing `fixture-actor` and `0 1`; the corresponding C# snippets compiled against the installed nupkg and printed `fixture-actor` and `0 1`. The Kotlin evidence remains the installed generated coroutine consumer suite. These rendered transport scenarios are intentionally bounded to `actors/v1` and `stream/v2`; all nine families still have generated, compiled, installed, and golden serialization evidence.

The current renderer closure is staged separately at `staged-jvm-dotnet-evidence/examples-current/sdk-examples-manifest.json` (manifest SHA-256 `C39461CA65B9301636721528631CFC1F976B800C512E11B10FBA90B30E805AFB`; closure SHA-256 `BCF658B8C802B365A2BE4B15B1B84761DC982424F1811DB870076D4A100605D9`). Its exact Java and C# Actors snippets include the validator-required nonzero digest and limits, and the installed artifacts compiled and executed both Actors and Stream snippets with outputs `fixture-actor` and `0 1`. The fixture dependency closure is recorded independently in `docs/research/jvm-dotnet-receipt.json`: excluding the renderer-only `src/main.rs`, its current ordered source closure hashes to `F27B9DA485C80008D66DE1C4EE65B0B6EDCA14632D0DD0F0AAD0E0A8F744706CA`. A fresh fixture build is currently blocked because `rust/crates/machines/build.rs` and `rust/crates/inference/build.rs` contain NUL bytes. The structured report therefore records that the current-renderer snippets ran against the retained fixture binary at source SHA-256 `2332A0BAB5CB6FC8B43C7275BAA39BCF761C73AF98CEFD0A1331293DC56080F4`; the fixture snapshot predates the renderer-only edits and is kept distinct from the current closure identity.

Official references: [gRPC Java generated code](https://grpc.io/docs/languages/java/generated-code/),
[gRPC Kotlin basics](https://grpc.io/docs/languages/kotlin/basics/),
[grpc-kotlin](https://github.com/grpc/grpc-kotlin), and the
[Protobuf Java generated-code guide](https://protobuf.dev/reference/java/java-generated/).

## .NET prototype

`dotnet/Acyclic.Sdk.Transport.csproj` uses pinned `Grpc.Tools`,
`Google.Protobuf`, and `Grpc.Net.Client` packages. Its recursive `<Protobuf>`
item points at a `SchemaRoot` MSBuild property containing the manifest's Rust
family closure and requests `Client` code only. The package target is `net8.0`
and the package id is `Acyclic.Sdk.Transport`.

`ProtoRoot` preserves cross-family imports. A PowerShell verifier rejects stale
or tampered family source and descriptor files before generation.

`dotnet/consumer` is a clean package consumer that exercises generated uint64,
oneof, and cancellation-token APIs. Its `NuGet.Config` resolves the produced
nupkg from a local source, so the install check does not use a project
reference. It is ready for `dotnet pack`, local NuGet installation, and
`dotnet run` in a .NET SDK environment. Docker Desktop is
unavailable, but a local .NET 8 SDK was installed under ignored `target/` for
this qualification. Against the source-matched live Rust fixture, the installed
package performed Actors unary creation plus Stream append idempotency
replay/inspection/mismatch rejection, Read and Follow cancellation, and
resumed append/read boundary checks. The staged .NET report records the
fixture source SHA-256 and each assertion output.
The same installed consumer also parses and reserializes all nine Rust golden
messages. The portable package receipt records both normalized nupkg copies at
`staged-jvm-dotnet-evidence/dotnet/` and `dotnet-second/`, with matching SHA-256
`45ED75F7A4967F81FF9D927FB720ABB5E904B43B4D27C668509F345D9B099891`.

Official references: [Protobuf C# generated code](https://protobuf.dev/reference/csharp/csharp-generated/),
[Microsoft gRPC C# basics](https://learn.microsoft.com/en-us/aspnet/core/grpc/basics?view=aspnetcore-10.0),
and [Grpc.Tools build integration](https://github.com/grpc/grpc/blob/master/src/csharp/BUILD-INTEGRATION.md).

## Embedded boundary

Embedded JVM access is a separate qualification: UniFFI has first-party Kotlin
support but requires a UDL or proc-macro interface and native per-platform
artifacts. UniFFI has no first-party C# backend; C# would require a reviewed C
ABI/PInvoke layer or a local gRPC sidecar. Neither is included in this
transport-only prototype. See the [UniFFI guide](https://mozilla.github.io/uniffi-rs/latest/)
and [supported-language notes](https://github.com/mozilla/uniffi-rs).

## Reproduction

From the repository root:

```text
mvn -f jvm/pom.xml clean verify
mvn -f jvm/pom.xml install
mvn -f jvm/consumer/pom.xml test
mvn -f jvm/kotlin-consumer/pom.xml test
cargo run --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --bin sdk-contract-wire -- generate --out target/sdk-contract
dotnet restore dotnet/Acyclic.Sdk.Transport.csproj /p:SchemaRoot=<absolute-repo>\target\sdk-contract
dotnet pack dotnet/Acyclic.Sdk.Transport.csproj --configuration Release --output target\dotnet-nupkg /p:SchemaRoot=<absolute-repo>\target\sdk-contract
dotnet run --project dotnet/consumer/Acyclic.Sdk.Transport.Consumer.csproj /p:SchemaRoot=<absolute-repo>\target\sdk-contract
```

For Windows Kotlin emission, create an ignored wrapper whose body is
`@echo off` followed by `java -jar "<m2-cache>\io\grpc\protoc-gen-grpc-kotlin\1.4.3\protoc-gen-grpc-kotlin-1.4.3-jdk8.jar" %*`,
then run:

```text
mvn -f jvm/pom.xml "-DgenerateKotlin" "-Dgrpc.kotlin.plugin.executable=<absolute-wrapper-path>" clean generate-sources
```

The default JVM commands were verified with JDK 17 and Maven 3.9.9. The
optional Kotlin profile is platform-dependent as described above; the Windows
wrapper invocation was verified against grpc-kotlin 1.4.3. The .NET
commands were verified with .NET SDK 8.0.425 installed only under ignored
`target/`; the resulting nupkg was written to `target/dotnet-nupkg/`.

With `-Dproject.build.outputTimestamp=2026-01-01T00:00:00Z`, two clean
nine-family JVM builds produced the same jar SHA-256:
`2E0CF7F1C88A1676F39D158E59E4AE5B623E0AAEE75F4F3A55F109A18AFD7D5F`.
Two clean nine-family portable .NET packs produced the same nupkg SHA-256:
`45ED75F7A4967F81FF9D927FB720ABB5E904B43B4D27C668509F345D9B099891`.
The Rust golden fixture SHA-256 is
`B5D9401A427FF477BE0B2A611DF66D4B755AEC519976F38C244BBC302AEEABE0`, and
the generated Harness source is identical across the two JVM builds
(`EBB8147877E566EA7551D5CCC5FC1DA3D9F01C2D671AC5C73F78D925B3595806`).
The installed Kotlin consumer ran three tests, including all nine Rust golden
round trips and live coroutine unary, stream collection, and cancellation. The installed .NET
consumer printed `Rust fixture idempotency replay, mismatch, follow cancellation,
and resume checks passed.` and `Generated Actors and Stream consumer checks
passed.` These live
transport assertions cover `actors/v1` and `stream/v2`; the remaining seven
families are represented by source-verified generated bindings, compilation,
package installation, and all-family golden serialization evidence in the
receipt, without a server-execution claim.





