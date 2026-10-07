# External CMake consumer

This project intentionally lives outside the package source target. It resolves
the installed artifact through `find_package(AcyclicActorsCXX CONFIG REQUIRED)`
and links both the primitive/ownership smoke test and the eight-operation live
consumer. The package config exposes the Cargo-derived version, Cargo manifest
SHA-256, and generated-header SHA-256 as `ACYCLIC_ACTORS_CXX_*` variables.
