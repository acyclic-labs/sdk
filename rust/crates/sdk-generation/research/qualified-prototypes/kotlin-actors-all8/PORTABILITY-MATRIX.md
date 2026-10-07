# UniFFI Actors portability and package-target matrix

This is a source-only portability record for the maintained Rust Actors facade. The production cohort remains pinned to UniFFI 0.31.0. The Ruby result below is an external 0.32.2 qualification cohort and does not change the shared Rust crate, root workspace, or 0.31.0 lockfile.

## Native target mapping

| Consumer | Native loader | Linux x86_64 artifact | Windows x86_64 artifact | macOS artifact strategy |
| --- | --- | --- | --- | --- |
| Python | generated `ctypes` loader chooses `.so`, `.dll`, or `.dylib` from the package directory | `libacyclic_actors_uniffi.so`; wheel tag `py3-none-linux_x86_64` | package-relative `.dll`; build a `win_amd64` wheel | package-relative `.dylib`; build per-architecture `macosx_*` wheels |
| Kotlin/JVM | generated JNA direct mapping; default basename `acyclic_actors_uniffi`, or `uniffi.component.acyclic_actors_uniffi.libraryOverride` | supply `libacyclic_actors_uniffi.so` on `java.library.path` or override | supply `.dll` or override; current installed probe used the override | supply `.dylib` or override; arm64 and x86_64 need separate native artifacts |
| Ruby | generated `ffi` loader plus package-relative native loader | package a Linux `.so` gem variant | current 0.32.2 receipt packages a relative `.dll` | package per-architecture `.dylib` gem variants |

A single cross-platform Kotlin JAR is not sufficient: the maintained generated code uses JNA, not JNI, and still needs one native binary per OS/architecture. Python wheels and Ruby gems likewise need platform-specific native assets. The Rust-owned request and response types remain identical across these assets.

## Installed Linux wheel receipt

The existing maintained Python 0.31.0 Linux wheel was built from the Rust-owned facade with cached Linux dependencies. The package is installable in the WSL Python 3.10 environment and contains the generated module, `py.typed`, and the ELF native library:

- wheel: `Q:\sdk\work\actors-uniffi-python-linux-wheel-current-20261007\dist\acyclic_actors_uniffi-0.2.0-py3-none-linux_x86_64.whl`
- wheel SHA-256: `0D96809BE41C62DC893EDA16CB8DC1A1F1602F7227622EF1A167BDDA5288D154`
- native `.so` SHA-256: `AA6427DD1C6836F164CEB83BF0D14C83E6CD8BC1598683654F7619DB5BF76A24`
- generated Python SHA-256: `AC3E8CD8FD86ECFDB7515D3D9195DFD79B021CECE1BEA279C420BDAA09456FB8`
- build log: `Q:\sdk\work\actors-uniffi-python-linux-wheel-current-20261007\build.log`
- installed import and Rust-owned nominal constructor probe: `LINUX_WHEEL_INSTALLED_NOMINAL_TYPES_PASS`
- probe coverage: `ActorId`, 32-byte `CodeSha256`, maximum `PositiveU64` (`2**64-1`), `ActorLimits`, `Binding`, `Header`, `SubscriptionStart.CURSOR(2**53+1)`, request records, and `CancellationHandle.cancel()`.

The probe ran from the installed wheel in `/mnt/q/sdk/work/actors-uniffi-python-linux-wheel-current-20261007/venv` and imported the package from its installed site-packages path. A live transport fixture was unavailable during this bounded rerun, so this receipt proves installability, native loading, and nominal Rust-owned construction only; the all-eight remote conformance receipt remains the existing Python owner artifact.

## Cross-cohort findings

The maintained Python generated module exposes nominal constructor annotations and `py.typed`, but current checker output has an enum-factory typing defect: both mypy and pyright reject the valid generated expression `SubscriptionStart.CURSOR(1)` as incompatible with `SubscriptionStart`. Negative nominal misuse cases are rejected correctly. This is a generator typing gap, not a Rust domain validation gap.

The external maintained UniFFI 0.32.2 Ruby cohort passed installed-gem all-eight runtime operations, typed service errors, cancellation, package-relative native loading, and the `u64` cursor `9007199254740993`. Its generated async runtime uses Fiber scheduler-aware waiting and calls the native future cancel function in `ensure` before draining and freeing the future. The receipt is at `Q:\sdk\work\actors-uniffi-ruby-0322-all8-current-20261007\receipt.json` with gem SHA `BE90127FFA88D1A647930B4492B1A7C026ED9B23EE90AC3D71B2B466068E70FA` and bindgen 0.32.2 SHA `335DD194AEC4D388D3DCEE534F0B03BB8701F17549F27323B860240917554AA4`.

Next bounded step: keep the 0.31.0 Rust-owned adapter unchanged, have the Python owner repair or explicitly gate the generated enum typing defect, and qualify a Linux Ruby 0.32.2 native package variant separately if a Linux Ruby runtime is required. Do not advertise one universal package artifact.
