# Embedded Stream producer matrix

The Rust Stream implementation is the authority for embedded behavior. A
foreign package must load that implementation through a maintained native or
WASM boundary; generated remote stubs and a TLS helper do not qualify as an
embedded Stream SDK.

## Current producer routes

| Consumer family | Rust-owned boundary | Current package producer | Current evidence | Embedded Stream status |
| --- | --- | --- | --- | --- |
| Node.js / TypeScript | N-API `acyclic-stream-native` | `scripts/build-stream-native-package.mjs` | Eight OS/architecture/libc packages, automatic package loader, installed consumer, and ten-RPC native receipt in `.github/workflows/stream-native-packages.yml` | Local ten-RPC evidence and hosted qualification path cover the current matrix; the next corrected hosted run supplies the platform receipts |
| C/C++ | cbindgen C ABI from `sdk-embedded-prototype` | `cpp/embedded-consumer/portable-package-test.ps1` and `.sh` | Native package and clean-prefix consumers exercise the bounded embedded prototype | ABI prototype; it is not the Stream service producer |
| .NET | `sdk-dotnet-transport` C ABI | `dotnet/producer-adapter.ps1` and `scripts/build-dotnet-native-transport.ps1` | The adapter emits generated Grpc.Net.Client remote stubs. The native script maps eight Rust targets to NuGet RIDs and exports a Rustls byte stream for mutual TLS | Remote SDK qualified separately; embedded Stream producer is missing |
| JVM / Java / Kotlin | None yet | `jvm/producer-adapter.ps1` | The adapter emits a generated grpc-java transport JAR and installed remote consumer. There is no JNI, JNA, UniFFI, or WASM Stream package | Remote SDK qualified separately; embedded Stream producer is missing |

The .NET RID map is still useful infrastructure:

| Rust target | NuGet RID | Native file |
| --- | --- | --- |
| `x86_64-pc-windows-msvc` | `win-x64` | `sdk_dotnet_transport.dll` |
| `aarch64-pc-windows-msvc` | `win-arm64` | `sdk_dotnet_transport.dll` |
| `x86_64-unknown-linux-gnu` | `linux-x64` | `libsdk_dotnet_transport.so` |
| `aarch64-unknown-linux-gnu` | `linux-arm64` | `libsdk_dotnet_transport.so` |
| `x86_64-unknown-linux-musl` | `linux-musl-x64` | `libsdk_dotnet_transport.so` |
| `aarch64-unknown-linux-musl` | `linux-musl-arm64` | `libsdk_dotnet_transport.so` |
| `x86_64-apple-darwin` | `osx-x64` | `libsdk_dotnet_transport.dylib` |
| `aarch64-apple-darwin` | `osx-arm64` | `libsdk_dotnet_transport.dylib` |

Those files implement the native TLS stream used by
`dotnet/NativeTlsStream.cs`. They do not implement Stream RPCs, and the
current .NET producer does not turn them into a ten-RPC Stream facade.

## Promotion gates

The .NET route can be promoted only when one NuGet package contains the
Rust-owned Stream boundary, its RID assets, and an idiomatic facade that
selects the matching asset automatically. An installed consumer must execute
Append, Read, InspectIdempotency, Commit, ReadCommit, Fork, Children,
Follow, Tail, and recovery/cancellation checks against the same source
revision. The consumer must use the same API on every supported RID.

The JVM route needs the same result through a maintained JNI, JNA, UniFFI,
or WASM boundary. Maven classifier or runtime selection may carry the native
artifact, but application code must not choose a platform feature or provide
a second implementation. The first qualification should cover a native
Stream producer and an installed Java consumer; Kotlin can consume the same
boundary after the Java facade passes.

Until those receipts exist, the generation inventory must keep JVM and .NET
embedded capability at `ffi-or-wasm`/`unqualified`, even though their remote
protobuf packages are installable. This prevents a transport-only package
from being mistaken for a Rust behavior binding.
