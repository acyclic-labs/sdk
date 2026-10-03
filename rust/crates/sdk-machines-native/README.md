# sdk-machines-native

This private N-API addon exposes the Rust-owned `acyclic-machines` provider to native
JavaScript consumers. It accepts explicit mutual-TLS material or the canonical
`ACYCLIC_MACHINES_*` environment configuration and forwards all nineteen Machines operations,
including the bounded operation watch stream.
