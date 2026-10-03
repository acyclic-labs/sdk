# JVM transport prototype

This package stages every proto family enumerated by the Rust authority
manifest into `target/` during Maven's `generate-sources` phase and generates
Java protobuf messages and gRPC stubs. A source/descriptor SHA-256 gate rejects
stale or tampered authority exports before staging. The existing package names are
preserved because the active descriptor bytes must remain unchanged. Kotlin
coroutine stub emission is available through the opt-in `kotlin-generation`
profile.

First emit the schema root from the Rust contract model:

```text
cargo run --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --bin sdk-contract-wire -- generate --out target/sdk-contract
```

Then run `mvn -f jvm/pom.xml clean verify`, or point Maven at another
Rust-emitted root with `-Dacyclic.schema.root=PATH`. Finally run
`mvn -f jvm/pom.xml install` and
`mvn -f jvm/consumer/pom.xml test` for a clean Java consumer check. On a
platform where the grpc-kotlin protoc wrapper is executable, add
`-DgenerateKotlin` to emit coroutine stubs, install that artifact, and run
`mvn -f jvm/kotlin-consumer/pom.xml test` against the installed JAR. On
Windows, pass `-Dgrpc.kotlin.plugin.executable=PATH` for a wrapper that runs
the pinned jar with `java -jar`:

```text
mvn -f jvm/pom.xml "-DgenerateKotlin" "-Dgrpc.kotlin.plugin.executable=PATH" clean verify
```

Set `ACYCLIC_FIXTURE_ENDPOINT` to a plaintext Rust fixture gRPC endpoint when
running the JVM test suite. The opt-in test performs Actors `CreateActor`, a
Stream `Read` server stream, and client cancellation; with the variable unset,
the suite remains deterministic and uses only the in-process transport test.
The authority export has nine JVM-compilable families. `harness/v2` contains a
message field named `descriptor` whose type is also `FileDescriptor`, which
collides with protoc Java's static `getDescriptor()` accessor. The build
applies a narrow generated-source adapter to `Harness.FileRef`: Java accessors
use `FileDescriptor`/`getFileDescriptor`, while the protobuf field number, wire
name, JSON name, reflection table, builder methods, and serialized bytes remain
unchanged. The adapter is rerun after each generation and is covered by the
reflection and round-trip test in `GeneratedTransportTest`.

The same test includes a Rust-produced golden fixture with one wire vector for
each of the nine authority families. It checks SHA-256 and byte-identical
parse/serialize results, while the live fixture probe remains limited to
Actors and Stream unary/server-stream/cancellation.
