# Language SDK layout

Rust is the canonical implementation and TypeScript is the first additional
surface. Future first-class SDKs are added in top-level `go`, `jvm`,
`dotnet`, `swift`, `cpp`, `ruby`, `php`, and `dart` directories only when they
contain installable, tested packages.

Each language consumes the accepted Rust-derived descriptor and binding surface
through pinned maintained generators. Public validation, presence, error,
transport-selection, cancellation, and recovery semantics remain authored in Rust.
An idiomatic facade may adapt the generated interface, but must not implement
another copy of that behavior. Embedded clients call the canonical Rust runtime
through a maintained native or WASM binding.

Qualification requires reproducible generation, installation from a local package
artifact, and applicable positive and negative type controls and common
conformance vectors against that installed package. Record the exact source,
generator, runtime, package version, and artifact hashes. Track remote and embedded
coverage separately; a wire round trip, HTTP projection, or generated-source type
check proves only its stated scope. Registry publication is a separate action.
No empty language package is published to reserve a name.
