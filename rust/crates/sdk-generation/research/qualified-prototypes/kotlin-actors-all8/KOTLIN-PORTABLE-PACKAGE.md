# Kotlin/JVM classpath-native package qualification

This bounded package prototype uses maintained UniFFI 0.31.0 generated Kotlin
unchanged and JNA's own classpath resource extraction. Native files are stored
at JNA's `Platform.RESOURCE_PREFIX` paths (`linux-x86-64/` and
`win32-x86-64/`) at the JAR root. The generated binding calls
`Native.register("acyclic_actors_uniffi")`; JNA resolves, extracts, and loads
the matching resource without a consumer loader, `java.library.path`, native
search path, or system property.

The generated Kotlin source is SHA-256
`6E73BC8CA0D0C47DA51C362E98D06AFBC2B29E6F0A7F12385401B59521623A3B`.
The Linux x86_64 resource is
`AA6427DD1C6836F164CEB83BF0D14C83E6CD8BC1598683654F7619DB5BF76A24` and the
Windows x86_64 resource is
`A09452F273B201FA7E3D288F6C3B3FDFC4ABD979431392797D4C80D375D85241`.
The installed qualification JAR is
`Q:\sdk\work\actors-uniffi-kotlin-portable-20261007\consumer\target\actors-uniffi-kotlin-portable-0.0.0-qualification.jar`.
Its SHA-256 is
`01C1765F1260DC8644C3D965963561D040EE842AC56619FE61B35EC2CD10BE30`.

The Maven consumer pins Kotlin 1.9.21, kotlinx-coroutines 1.8.0, JNA 5.15.0,
and JDK target 17. A clean offline Maven package completed successfully. WSL
Java ran the installed JAR constructor probe and reported
`KOTLIN_JNA_STANDARD_RESOURCE_CONSTRUCTORS_PASS`; the probe directly called
the generated `uniffiEnsureInitialized()` with no setup helper. Windows Java
ran the same standard-layout JAR against the live TLS fixture and reported
`KOTLIN_ALL8_IMMUTABLE_PROBE_PASS` (all eight request operations, CA connect,
typed service error, and cancellation).

The same WSL JAR ran the pending fixture probe with a generated call passing a
null cancellation handle and reported
`KOTLIN_JNA_STANDARD_RESOURCE_PENDING_CANCELLATION_PASS`: the fixture observed
one active pending request, then one abort and zero active requests after
coroutine cancellation.

The Windows all-eight run used fixture endpoint `https://localhost:59663`
and a copied CA file only as test input. The JNA resource loader selected the
embedded DLL automatically. This is a two-platform asset qualification; macOS
and other architectures still require their own standard-prefix native assets
and receipts.

The WSL fixture script was attempted with its default Cargo 1.75.0 and was
rejected by the repository's edition-2024 manifest. The live Windows fixture
used Cargo 1.98.1. The maintained Rust and bindgen cohort remains pinned at
0.31.0; the separate Ruby 0.32.2 receipt is an external compatibility cohort.
