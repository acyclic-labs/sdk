# Language SDK layout

Rust is the canonical implementation and TypeScript is the first additional
surface. Future first-class SDKs are added in top-level `python`, `go`, `jvm`,
`dotnet`, `swift`, `cpp`, `ruby`, `php`, and `dart` directories only when they
contain installable, tested packages.

Each language generates private transport bindings from the same descriptor,
adds an idiomatic handwritten facade, runs common conformance vectors, and tests
installation from the produced registry artifact. No empty language package is
published to reserve a name.
