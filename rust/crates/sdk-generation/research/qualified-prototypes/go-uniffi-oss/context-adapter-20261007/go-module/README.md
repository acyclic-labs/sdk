# acyclic Actors Go module qualification artifact

This is an installable source module produced by the maintained `uniffi-bindgen-go` source patch recorded in the parent receipt. The module version is the Rust Cargo authority version `0.2.0`; Go module source has no independent release version.

Install the source module from this directory with `go get` or `go mod edit -replace`. The generated package is `generated/acyclic_actors_uniffi`. Native resources are retained under `native/` for Linux x86_64, Windows x86_64, and macOS arm64. Consumers must provide the platform library through their normal cgo linker configuration.

The package is qualification evidence only. The Rust Cargo manifest, lockfile, source inventory, generator commit, source patch, generated hashes, and native identities are recorded in `go-module-manifest.json`. The AA642 Linux resource is retained under its historical source inventory; it is not relabeled as a build from the current dirty Actors checkout.
