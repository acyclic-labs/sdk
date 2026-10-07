# Acyclic.Actors 0.2.0 NuGet-style package recipe

This directory is the source-only package recipe. It contains no managed DLL,
native DLL, Cargo target, or registry output. The external qualified artifact
was assembled at
`Q:\sdk\work\uniffi-bindgen-cs-sourcefix-20261007-package-20261007` and is
recorded by its `package-receipt.json`.

Inputs are the copied Rust `acyclic-actors-uniffi` Cargo 0.2.0 manifest,
lockfile, facade source, and exact `cargo metadata` under `../rust`, plus
`../generator/generator-source.patch` and `../generated/acyclic_actors_uniffi.cs`.
The generator checkout is external and pinned to NordSecurity
`uniffi-bindgen-cs` tag `v0.11.0+v0.31.0`, commit
`e10ce410eb3a10cc19c7928b93ea8d84e038c034`. Apply the patch to that clean
checkout, build the locked generator, and regenerate the C# source. The
expected generated source hash is
`1227BF526944F1C1FBDE38115C9FC4CC2EC01435FB9B4CFCBB8A9A16BF200734`.

The Windows qualification layout is `lib/net8.0/Acyclic.Actors.dll`,
`runtimes/win-x64/native/acyclic_actors_uniffi.dll`, and the supplied
`buildTransitive/net8.0/Acyclic.Actors.targets`, using the Rust Cargo version
`0.2.0` as package version. The cross-platform qualification cohort adds
`runtimes/linux-x64/native/libacyclic_actors_uniffi.so` to the same managed
assembly and targets. The targets file only copies the existing Rust native
library; it contains no transport, semantic, or cancellation runtime.

External receipt:

- corrected archive SHA256:
  `5A50242B7317018E550EA908487BE8C27D2E1A72A070B139DEC361798F1F2FAC`
- corrected managed assembly SHA256:
  `0CFC7C771B3A5C152FBB95D9B47C130D61DC987754036DDAADEE1CD4ED2B8269`
- clean external net8.0 MSBuild restore/build: PASS, 0 warnings, 0 errors
- installed corrected package all-eight/null-handle runtime: PASS
- installed corrected package three-iteration CancellationToken abort and cleanup: PASS
- installed corrected package external raw-handle forge: expected CS1729
- installed corrected package external strong-type misuse: expected CS1503
- cross-platform cohort archive SHA256:
  `CC94B745551B2236561A64DCCA3AC200AB9CCE6CB44AEB0B042CCA63197AECF6`
- cross-platform Linux native SHA256:
  `60B22ED3000A996DB97EAF66B8C293649F7C895DFC35AEEF2853F2BCE146AC258`
- the Linux producer was built from foundation revision
  `371bb4170e16aca973176b6756a261ee5add7297` with Rust `1.98.1` using
  `cargo build --manifest-path rust/crates/actors-uniffi/Cargo.toml --release --locked`;
  the producer output and packaged `.so` have the same SHA256 above
- clean external Linux net8.0 MSBuild restore/build: PASS, 0 warnings, 0 errors
- clean external Linux net8.0 runtime against a WSL-local fixture: PASS; all
  nine generated Actors methods completed
- macOS remains unqualified: the available `ivar` host has Rust 1.96.0 and no
  .NET SDK, while this source cohort uses Rust 1.98.1 and .NET SDK 8.0.425

The artifact remains external and unpublished. Keep this recipe and receipt
with the source qualification cohort when reviewing a future package build.
