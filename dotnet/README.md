# Acyclic .NET transport prototype

This package is generated from every Protobuf family enumerated by the Rust
authority manifest. `Grpc.Tools` generates client-only C# bindings during the
build; the package contains transport types and stubs, while Rust-owned
facades and embedded behavior remain future generated surfaces. The build
accepts only a `rust-authority.json` export from `sdk-contract-wire`, embeds
that marker in the package, and declares Apache-2.0 package metadata. The
build verifies every family source and descriptor SHA-256 before invoking
`Grpc.Tools`, rejecting stale or tampered imports.

The project deliberately uses the namespaces inferred from the existing
Protobuf packages (`Acyclic.Actors.V1` and `Acyclic.Stream.V2`). Adding a
`csharp_namespace` option to an active descriptor would change compatibility
inputs, so that migration is deferred until a versioned descriptor policy is
approved.

## Verify

```text
cargo run --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --bin sdk-contract-wire -- generate --out target/sdk-contract
dotnet restore dotnet/Acyclic.Sdk.Transport.csproj /p:SchemaRoot=<absolute-repo>\target\sdk-contract
dotnet pack dotnet/Acyclic.Sdk.Transport.csproj --configuration Release --output target\dotnet-nupkg /p:SchemaRoot=<absolute-repo>\target\sdk-contract
dotnet run --project dotnet/consumer/Acyclic.Sdk.Transport.Consumer.csproj
```

The generated API is checked by the consumer for `uint64`, oneof selection,
and cancellation-token behavior. Set `ACYCLIC_FIXTURE_ENDPOINT` to a local
Rust fixture server to run an actual Actors unary call, Stream server stream,
and cancellation check; with the variable unset the package consumer remains
offline and deterministic. A .NET 8 SDK is required; qualification used one
installed only under the ignored worktree `target/` directory.
The consumer resolves `Acyclic.Sdk.Transport` from the locally produced nupkg
through `consumer/NuGet.Config`, so the install check exercises the package
artifact rather than a project reference.

The pack target normalizes NuGet's generated relationship identifiers and ZIP
timestamps, so two clean packs from the same Rust authority root are
byte-for-byte reproducible.

## Native transport assets

Mutual TLS uses the Rust-owned `sdk-dotnet-transport` boundary. The package
producer stages one native library per .NET runtime identifier and NuGet
selects the matching asset automatically:

```text
powershell -File scripts/build-dotnet-native-transport.ps1 -Output target/native/sdk-dotnet-transport -Target x86_64-pc-windows-msvc
```

The release recipe uses `-All` after provisioning the pinned Rust targets and
linkers. The package includes `win-x64`, `win-arm64`, Linux GNU/musl, and macOS
x64/ARM64 layouts when those builds are present. Consumers call the same
`RemoteClientFactory` API on every RID; native library selection and TLS
configuration remain package and Rust runtime details.
