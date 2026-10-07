# Kotlin/JVM classpath-native package qualification

This bounded package prototype uses maintained UniFFI 0.31.0 generated Kotlin
unchanged and JNA's own classpath resource extraction. Native files are stored
at JNA's `Platform.RESOURCE_PREFIX` paths (`linux-x86-64/`, `win32-x86-64/`,
and `darwin-aarch64/`) at the JAR root. The generated binding calls
`Native.register("acyclic_actors_uniffi")`; JNA resolves, extracts, and loads
the matching resource without a consumer loader, `java.library.path`, or native
search path. UniFFI's generated optional
`uniffi.component.<name>.libraryOverride` property exists, but this package and
its probes do not set it.

The generated Kotlin source is SHA-256
`6E73BC8CA0D0C47DA51C362E98D06AFBC2B29E6F0A7F12385401B59521623A3B`.
The Linux x86_64 resource is
`AA6427DD1C6836F164CEB83BF0D14C83E6CD8BC1598683654F7619DB5BF76A24` and the
Windows x86_64 resource is
`A09452F273B201FA7E3D288F6C3B3FDFC4ABD979431392797D4C80D375D85241`.
The macOS arm64 resource is the already-built Python cohort asset at
`Q:\sdk\work\actors-uniffi-python-mac-wheel-current-20261007\libacyclic_actors_uniffi.dylib`,
SHA-256
`28D885561244BD2D682D1103B70AD8C1989AC341777F28499731C1140FF3B719`,
5,122,144 bytes. It is packaged at
`darwin-aarch64/libacyclic_actors_uniffi.dylib` without rebuilding the Rust
library. The Python receipt identifies this asset as built on Darwin 24.6.0
arm64 with Rust `1.98.1-aarch64-apple-darwin`; it is the exact native asset
also recorded at `/tmp/actors-uniffi-mac-target/release/libacyclic_actors_uniffi.dylib`
on `ivar`.
The installed qualification JAR is
`Q:\sdk\work\actors-uniffi-kotlin-portable-20261007\consumer\target\actors-uniffi-kotlin-portable-0.0.0-qualification.jar`.
After adding the Darwin resource, its SHA-256 is
`EAA6F40C0F5BDC7CBDFACBE74F25D03D10E70F57C9682E8036095FF1B88124BC`.

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
embedded DLL automatically. The Darwin asset is source-and-hash qualified for
the same generated 0.31.0 cohort and is present in the rebuilt JAR. A
temporary, task-local Eclipse Temurin JRE `17.0.20.1+1` was downloaded from
the official `adoptium/temurin17-binaries` release for `aarch64_mac`; its
published SHA-256 is
`190480874CCCEB358CBC840393207F77AC3E63A4C5F8129D0E23E9518B96AD05`.
Artifact URL:
`https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.20.1%2B1/OpenJDK17U-jre_aarch64_mac_hotspot_17.0.20.1_1.tar.gz`;
the release `.sha256.txt` URL was fetched and matched before extraction.
It was extracted under `/tmp/actors-kotlin-mac-jre-20261007` on `ivar`, with
no machine-global installation. Using that JRE and the same JAR, the Mac
constructor probe reported
`KOTLIN_JNA_STANDARD_RESOURCE_CONSTRUCTORS_PASS`; the all-eight probe reported
`KOTLIN_ALL8_IMMUTABLE_PROBE_PASS` (CA connect, inspect/create/update/add/
remove/resume/checkpoint/invoke, typed service error, and cancellation); and
the pending probe reported
`KOTLIN_JNA_STANDARD_RESOURCE_PENDING_CANCELLATION_PASS` with one additional
started and aborted request and zero active requests. The live Windows fixture
was reached through an SSH reverse tunnel to `ivar`; no Rust or native rebuild
was performed.

The WSL fixture script was attempted with its default Cargo 1.75.0 and was
rejected by the repository's edition-2024 manifest. The live Windows fixture
used Cargo 1.98.1. The maintained Rust and bindgen cohort remains pinned at
0.31.0; the separate Ruby 0.32.2 receipt is an external compatibility cohort.

## Patched-generator cohort

The Rust-owned generator patch
`rust/crates/actors-uniffi/generator-patches/uniffi-0.31.0-kotlin-opaque-constructors.patch`
changes the maintained UniFFI Kotlin template's raw-handle and `NoHandle`
constructors to `internal`. The same-module generated FFI converters and
lifting paths continue to use those constructors; external Kotlin source
cannot call them. The patch SHA-256 is
`275222CFE64FD25C3723F6619FA87929713D7950320D46777D9B732A3C41A474`.

The patched generated Kotlin source is SHA-256
`201C988FA9B679784EE12E6FFC5C72EE88D5AD001633D085FFEFD628E392AC58`.
The Cargo-derived Maven package is
`dev.acyclic:acyclic-actors-uniffi-kotlin:0.2.0`, built from
`Q:\sdk\work\actors-uniffi-kotlin-internal-20261007\consumer`; its JAR SHA-256
is `A5E5C1D76ABC11A97DCC96C32A5B5D4C51879C19BAFA3C794C20E9D76E662CFE`.
The native resources are unchanged: Linux
`AA6427DD...`, Windows `A09452F2...`, and macOS arm64 `28D88556...`.

An external Maven consumer compiled against the JAR failed both negative
checks: raw `ActorId(UniffiWithHandle, 1L)` and `ActorId(NoHandle)` were rejected
because the constructors are internal, and `SubscriptionStart.Cursor(true)`
was rejected because the payload must be `ULong`. The production cohort's
constructor probe, all-eight
operation/service-error/cancellation probe, and generated-null pending
cancellation probe all passed with the unchanged native resources.

## Fresh current-lock patched package

The standalone crate was rebuilt with the current shared lockfile rather than
reusing the earlier native output. The task-local package is
`Q:\sdk\work\actors-uniffi-kotlin-internal-current-20261007\consumer\target\acyclic-actors-uniffi-kotlin-0.2.0.jar`, SHA-256
`6A616B2A73774F150E36C050E6D6FBD1AA5E21C5CBE801F0AACE04C38C16C5E4`.
Its Cargo.lock SHA-256 is
`B1E42E6BA03A535A25A1E369AE3BAAE1BCCB3789360D709229C67581FEE5AA11`, and
its freshly rebuilt Windows x86_64 native resource is
`E78550ED0A5EE2A3365F9738D692B5952936536DA980DB3F9FB4BC4042D0DE1F`.
The generated Kotlin source remains the patched source-derived file with
SHA-256 `201C988FA9B679784EE12E6FFC5C72EE88D5AD001633D085FFEFD628E392AC58`.

The fresh JAR passed the standard-resource constructor probe under WSL, the
live all-eight and service-error probe on Windows, and the live all-eight,
generated-null pending server-abort, and continuation-map probes on macOS
arm64 using the task-local Temurin 17 runtime. The macOS continuation map was
0 at baseline, 1 while each real native request was pending, and 0 after each
of three coroutine cancellations. Linux, Windows, and macOS resources remain
under JNA's standard resource prefixes; no loader, `java.library.path`, or
custom native-path setup was introduced.
