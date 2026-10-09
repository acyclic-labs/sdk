# Java producer tooling

This backend generates protobuf messages and gRPC stubs directly from the
canonical Rust descriptor sets. It adds no service model, validation, transport
policy, retry or embedded runtime implementation. The generated package is a
transport binding package; a complete SDK still needs the accepted Rust runtime
boundary and its applicable conformance checks.

```text
java/
  generate.mjs          authority validation, descriptor snapshots and generation
  generate.test.mjs     offline admission and staging controls
  toolchain.json        maintained generator versions and published host hashes
  package/pom.xml      pinned Java package dependencies and build plugins
  testdata/consumer/    installed descriptor, wire, RPC-shape and type controls
```

From the repository root, with an immutable source snapshot containing
`LICENSE` and `NOTICE`, and a Rust authority manifest with digests for every
source and supplied descriptor:

```sh
node tools/sdk-generator/backends/java/generate.mjs \
  --source-root /immutable-source --authority /rust-export \
  --protoc /tools/protoc --grpc-java /tools/protoc-gen-grpc-java \
  --output /new-package
mvn -B -ntp -T 1 -Dmaven.repo.local=/owned-cache -f /new-package/pom.xml install
```

The output must be absent, have an existing parent and be disjoint from protected
inputs. The backend validates all inputs and tools before creating output. It
copies the exact verified descriptor bytes into an isolated temporary directory;
protoc receives no source include root. Descriptor sets must contain the complete
import closure, including any attested import entries without their own
descriptor declaration. Each family must produce Java source, and colliding generated
filenames fail instead of overwriting another family's bindings. Partial output
remains available if generation fails. Temporary descriptors are removed.
Input directories and their parents must not be concurrently replaced.

Protoc must report `libprotoc 28.3`. The grpc-java 1.75.0 executable must match
the host's SHA-256 in `toolchain.json`, obtained from the published
[Maven Central artifacts](https://repo.maven.apache.org/maven2/io/grpc/protoc-gen-grpc-java/1.75.0/).
The backend does not download tools. Its receipt records the manifest, all
inputs, generator/tool identities and every output digest. The POM pins protobuf
4.31.1 and grpc-java 1.75.0; the newer protobuf runtime is compatible with the
older generated code within the supported major version, as documented in the
[protobuf runtime guarantee](https://protobuf.dev/support/cross-version-runtime-guarantee/).

Run the lightweight generator controls with:

```sh
node --test tools/sdk-generator/backends/java/generate.test.mjs
```

These controls use a command double and establish staging behavior. Actual
installed qualification additionally compiles `testdata/consumer/InstalledConsumer.java`
against the installed JAR and its resolved dependencies, runs it with the
verified Actors, Workers and Stream descriptor paths, and requires all three
assignments in `NegativeConsumer.java` to fail compilation. Descriptor equality
retains all API fields and other unknown fields, excluding source comments and
Buf's file-level image metadata tag 8042. The positive controls cover bytes,
unsigned integer bounds, optional-zero presence, oneof and exact gRPC method
shapes. They do not exercise a remote service.

The immutable foundation cut `51f3fe070c7b48ff5c7413671638634086aabe75`
passed those installed controls on Java 17.0.14. Two clean builds, and a third
through this reusable backend, produced the same JAR SHA-256:
`86101bcb8ab71fdc95fa7472e2a4f8e684afb7b42fe816e7abd57c4cda96115b`.
That evidence covers these three transport families. Final-foundation
regeneration, remaining families, Rust-backed RPC, TLS/authentication,
cancellation/recovery and embedded runtime qualification remain outstanding.
