# Acyclic embedded Stream JVM facade

This Maven package is a thin JNA facade over the Rust `sdk-embedded-prototype` ABI. Rust owns the
Stream implementation; Java maps handles and generated protobuf wire bytes, copies returned
buffers, and releases them. Native resources are selected from the runtime platform package.

The facade exposes the ten canonical operation names. Unary operations have named generated
wrappers (`inspectIdempotency`, `appendWire`, `tail`, `fork`, `childrenPage`, `commit`, and
`readCommit`) over the matching `acyclic.stream.v2` request/response bytes; `read`, `follow`, and
`children` use explicit reader handles so cancellation and bounded delivery remain Rust-owned.

Before packaging, stage the Rust producer assets and provenance into the JVM resource layout:

```powershell
pwsh scripts/stage-jvm-embedded-native.ps1 `
  -InputRoot target/dotnet-embedded `
  -OutputRoot target/native/sdk-embedded
mvn -Dembedded.native.root="$PWD/target/native/sdk-embedded" `
  -Dacyclic.embedded.native.qualification=true package
```

The package contains all eight desktop and musl resources under `native/<platform>/`, plus the Rust
source revision manifest. `RustEmbedded` selects the host resource and libc variant from the
classpath automatically, and
the release consumer gate exercises append/read through the installed JAR without a native path
override. Typed overloads accept generated `com.google.protobuf.Message` values and parser
responses, so callers use the generated `acyclic.stream.v2` classes directly.
