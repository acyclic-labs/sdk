# sdk-source-identity

This crate owns the canonical `acyclic.sdk.cargo-build-recipe.v1` encoding.
Rust producers pass the same Cargo metadata, source root, and explicit build
target to `normalized_build_recipe`. The resulting bytes are included in the
source closure digest, so producers and importers agree on local dependency
paths, registry identities, features, targets, and build target.
