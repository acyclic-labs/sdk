# Java producer tooling

Rust manifest admission is shared in `../../shared/authority.mjs`.

This backend generates protobuf messages and gRPC stubs directly from the
canonical Rust descriptor sets. It adds no service model, validation, transport
policy, retry or embedded runtime implementation. The generated package is a
transport binding package; a complete SDK still needs the accepted Rust runtime
boundary and its applicable conformance checks.

```text
java/
  src/                  generation and installed Java/JVM qualification runners
  tests/                offline admission and runner controls
    fixtures/           Java, Kotlin and Scala installed-consumer sources
  templates/package/    pinned Maven package template
  toolchains/           generator and JVM compiler versions and checksums
```

From the repository root, with an immutable source snapshot containing
`LICENSE` and `NOTICE`, and a Rust authority manifest with digests for every
source and supplied descriptor:

```sh
node tools/sdk-generator/backends/java/src/generate.mjs \
  --source-root /immutable-source --authority /rust-export \
  --protoc /tools/protoc --grpc-java /tools/protoc-gen-grpc-java \
  --output /new-package
mvn -B -ntp -T 1 -Dmaven.repo.local=/owned-cache -f /new-package/pom.xml install dependency:build-classpath
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

Protoc must report `libprotoc 28.3` and match the host's SHA-256 in
`../../shared/protoc.json` before execution. Compiler pins come from the official
Maven Central artifacts and are checked against their published checksums.
The grpc-java 1.75.0 executable must match
the host's SHA-256 in `toolchains/toolchain.json`, obtained from the published
[Maven Central artifacts](https://repo.maven.apache.org/maven2/io/grpc/protoc-gen-grpc-java/1.75.0/).
The backend does not download tools. Its receipt records the manifest, all
inputs, generator/tool identities and every output digest. The POM pins protobuf
4.31.1 and grpc-java 1.75.0; the newer protobuf runtime is compatible with the
older generated code within the supported major version, as documented in the
[protobuf runtime guarantee](https://protobuf.dev/support/cross-version-runtime-guarantee/).

Run the lightweight generator controls with:

```sh
node --test --test-concurrency=1 tools/sdk-generator/backends/java/tests/*.test.mjs
```

These controls use a command double and establish staging and runner behavior.
Run actual installed qualification separately with JDK 17.0.14, Maven 3.9.9 and
an exclusively owned dependency cache prepared by the package build above:

```sh
node tools/sdk-generator/backends/java/src/qualify.mjs \
  --package /new-package --authority /rust-export \
  --java-home /jdk-17.0.14 --maven-home /maven-3.9.9 \
  --cache /owned-cache --output /new-qualification
```

The runner verifies and snapshots the receipt's payload into a fresh project,
builds and installs it offline, then compiles and runs `InstalledConsumer.java`
against a fresh copy of the installed JAR and its resolved dependencies. It
checks that the SDK classes actually load from that JAR. Each invalid assignment
is compiled independently and must fail for the intended byte/integer type
mismatch. Inherited Java/Maven options and user settings are excluded. The cache
must remain exclusively owned during the run. A receipt is written only after
all controls pass; it records the tools, cache inputs, installed artifacts,
controls and logs. Partial output remains on failure. Routine CI runs neither
Maven nor the actual installed runner.

Descriptor equality
retains all API fields and other unknown fields, excluding source comments and
Buf's file-level image metadata tag 8042. The positive controls cover bytes,
unsigned integer bounds, optional-zero presence, oneof and exact gRPC method
shapes. They do not exercise a remote service.

The accepted foundation `5f13157414a5f48425007ffd957ea7d09611e5fe`
passed those installed controls on Java 17.0.14. Clean offline builds through
this reusable backend produce JAR SHA-256
`e0c5aeba5042764c2aa73de65456f538c25e2723506eb3628044cf739557ba26`.
That evidence covers these three transport families. Remaining families,
Rust-backed RPC, TLS/authentication,
cancellation/recovery and embedded runtime qualification remain outstanding.

Kotlin and Scala consume the same installed Java bindings. Their opt-in runner
requires a successful Java qualification directory from the command above.
Prepare the maintained compiler closure once using Maven 3.9.9 and the same
owned cache (set `language` to `kotlin` or `scala`):

```sh
language=kotlin
mvn -B -ntp -T 1 -Dmaven.repo.local=/owned-cache \
  -f tools/sdk-generator/backends/java/tests/fixtures/$language/toolchain.pom.xml \
  dependency:build-classpath
node tools/sdk-generator/backends/java/src/qualify-jvm.mjs \
  --language "$language" --installation /new-qualification \
  --authority /rust-export --java-home /jdk-17.0.14 \
  --cache /owned-cache --output /new-language-qualification
```

`toolchains/jvm-toolchains.json` pins Kotlin 2.4.20 and Scala 3.10.0 compiler
JARs and their complete runtime closures by SHA-256, with official Maven Central
coordinates and published SHA-1 checksums retained as provenance. The runner
checks every JAR before execution and copies verified bytes into fresh output.
It also snapshots the qualified SDK JAR, POM and dependencies, excludes inherited
Java options and classpaths, and limits each compiler to one CPU and 512 MiB.

Both language consumers run the shared Java descriptor, wire, presence, oneof
and RPC-shape controls against the installed JAR, then exercise their own
builder and serialization calls. Three independent invalid assignments must
fail for the intended byte/integer type mismatch. A success receipt records
compiler/tool identities, source controls and logs. Compiler preparation and
actual installed runs are opt-in; routine CI runs eight offline runner tests.
This establishes Kotlin/Scala interoperability with the Java transport package.
Dedicated language APIs, Rust-backed RPC and embedded bindings remain pending.
