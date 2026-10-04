# Objective-C Apple SDK recipe

This recipe keeps Objective-C on the Rust-owned wire contract while using the maintained gRPC Objective-C generator and CocoaPods runtime. The target is a native Apple client; embedded behavior remains the Rust C ABI.

## Pinned tool closure

Use one gRPC release for the compiler plugin and the runtime pods:

- gRPC source and `grpc_objective_c_plugin`: tag `v1.62.2`, Apache-2.0.
- `!ProtoCompiler`: the `protoc` version required by the matching gRPC release.
- `!ProtoCompiler-gRPCPlugin`: `1.62.2` from the matching gRPC release.
- `gRPC-ProtoRPC`: `1.62.2`.
- `Protobuf`: the version selected by the matching `gRPC-ProtoRPC` pod.

The official plugin podspec downloads the release plugin and pins both `protoc` and `gRPC-ProtoRPC` to the same release family. The official RouteGuide podspec is the reference for the generation layout and pod dependencies.

## Generation

Generate the wire tree with the Rust authority binary before invoking `protoc`:

```sh
cargo run --locked --manifest-path rust/crates/sdk-contract-wire/Cargo.toml -- \
  generate --out build/objective-c-wire
```

The generator must consume every `.proto` below `build/objective-c-wire`, including the generated `rust-authority.json`; no separately authored Objective-C schema is accepted. CocoaPods' `!ProtoCompiler` supplies `protoc` and `!ProtoCompiler-gRPCPlugin` supplies `grpc_objective_c_plugin`:

```sh
PROTOC=Pods/\!ProtoCompiler/protoc
PLUGIN=Pods/\!ProtoCompiler-gRPCPlugin/grpc_objective_c_plugin
OUT=Pods/AcyclicSDK
mkdir -p "$OUT"
"$PROTOC" \
  --plugin=protoc-gen-grpc="$PLUGIN" \
  --objc_out="$OUT" \
  --grpc_out="$OUT" \
  -I build/objective-c-wire \
  $(find build/objective-c-wire -name '*.proto' -print)
```

The generated `*.pbobjc.{h,m}` files are owned by the protobuf pod. The generated `*.pbrpc.{h,m}` files are owned by `gRPC-ProtoRPC` and expose unary, client-streaming, server-streaming, and bidirectional methods. The SDK podspec should expose both generated groups and preserve the `GPB_USE_PROTOBUF_FRAMEWORK_IMPORTS=1` setting used by the official example.

## Qualification receipt

An Apple producer job must install the generated pod from a local artifact and write the central Rust qualification receipt. The receipt binds:

- Rust source revision and `sdk-contract-wire` revision;
- the digest of the Rust wire tree and generated Objective-C sources;
- gRPC tag, plugin digest, `protoc` digest, and pod lockfile digest;
- Xcode, SDK, deployment target, and architecture;
- generated consumer source digest; and
- passing unary, client-streaming, server-streaming, bidi, cancellation, recovery, bytes, presence, custom-option, descriptor-digest, and package-install vectors.

A macOS/iOS producer is required for this receipt because CocoaPods and Xcode are the package/runtime boundary. Until that producer runs, the inventory remains a candidate with a concrete pinned recipe rather than a qualified release target.

Sources: [official Objective-C gRPC language guide](https://grpc.io/docs/languages/objective-c/), [official RouteGuide podspec](https://github.com/grpc/grpc/blob/master/examples/objective-c/route_guide/RouteGuide.podspec), and [official plugin podspec](https://github.com/grpc/grpc/blob/master/src/objective-c/%21ProtoCompiler-gRPCPlugin.podspec).
