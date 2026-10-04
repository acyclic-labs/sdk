# Acyclic embedded Stream JVM facade

This Maven package is a thin JNA facade over the Rust `sdk-embedded-prototype` ABI. Rust owns the
Stream implementation; Java maps handles and generated protobuf wire bytes, copies returned
buffers, and releases them. Native resources are selected from the runtime platform package.

The facade exposes the ten canonical operation names. Unary operations have named generated
wrappers (`inspectIdempotency`, `appendWire`, `tail`, `fork`, `childrenPage`, `commit`, and
`readCommit`) over the matching `acyclic.stream.v2` request/response bytes; `read`, `follow`, and
`children` use explicit reader handles so cancellation and bounded delivery remain Rust-owned.

The checked-in Windows x64 DLL in this review patch is the pre-wire-call asset and therefore does
not load the new wrapper surface yet. A Rust rebuild must replace it before an installed consumer
can execute the ten-operation surface.

Typed overloads accept generated com.google.protobuf.Message values and Parser responses, so callers use their generated acyclic.stream.v2 classes without manually handling wire bytes.
