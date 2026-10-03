# Rust package archive rejection receipt

This is a retained failure receipt for `Q:/sdk/work/rust-generation-output-64484b`, source revision `64484b5930e0029de16730a55e4144bd37c6553f`. It records the output validator result; it does not qualify the generation run.

The two Rust package paths in `sdk-examples-manifest.json` were:

| Scenario | Package artifact SHA-256 | Compile artifact SHA-256 | First 16 bytes | Receipt status |
| --- | --- | --- | --- | --- |
| `actors-create-roundtrip` | `sha256:8c3bf97dc0ff96496ff32707613736384f25728e1d835772369ffedfa21532b4` | `sha256:8c3bf97dc0ff96496ff32707613736384f25728e1d835772369ffedfa21532b4` | `4D5A90000300000004000000FFFF0000` | `qualified` |
| `stream-append-read` | `sha256:58701bfc6ad5fde075053cffce3c78e8157c58940a3e9d8a2ab05a8838d4899b` | `sha256:58701bfc6ad5fde075053cffce3c78e8157c58940a3e9d8a2ab05a8838d4899b` | `4D5A90000300000004000000FFFF0000` | `qualified` |

The first row's compile hash is copied exactly from the manifest receipt as `sha256:8c3bf97dc0ff96496ff32707613736384f25728e1d835772369ffedfa21532b4`; the package and compile hashes are identical. Both headers are the Windows PE `MZ` signature, while a `.tgz` must begin with gzip bytes `1F8B`. Running `tar -tzf` against both package paths returned `Unrecognized archive format`.

The manifest maps each package path to a package directory and `Cargo.toml`, and both package-resolution records repeat the same `.tgz` path and `qualified` status. The mapped package manifests and consumer manifests/locks exist, but they do not turn the PE bytes at the `.tgz` path into an archive. The package receipt therefore fails the archive-header, archive-listing, and package-versus-compile-byte checks before any consumer result can establish package qualification.

The bounded regression command was:

```text
SDK_GENERATION_OUTPUT=Q:/sdk/work/rust-generation-output-64484b SDK_GENERATION_EXPECTED_FAILURE=package-archive bun test research/acceptance/typescript-review/sdk-package-archive.acceptance.test.ts
```

Result: `1 pass, 1 skip, 0 fail`; the passing test rejected the two executable masquerades. The producer source and failed output were not modified.
