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

Package layout is `lib/net8.0/Acyclic.Actors.dll`,
`runtimes/win-x64/native/acyclic_actors_uniffi.dll`, and the supplied
`buildTransitive/net8.0/Acyclic.Actors.targets`, using the Rust Cargo version
`0.2.0` as package version. The targets file only copies the existing Rust
native library; it contains no transport, semantic, or cancellation runtime.

External receipt:

- archive SHA256:
  `2E33DC70607AAA01FE2BFA979E8AF7782CE3A3AA70EDD6933F6CC9720E844A5D`
- installed package all-eight/null-handle runtime: PASS
- installed package three-iteration CancellationToken abort and cleanup: PASS
- installed package external raw-handle forge: expected CS1729

The artifact remains external and unpublished. Keep this recipe and receipt
with the source qualification cohort when reviewing a future package build.
