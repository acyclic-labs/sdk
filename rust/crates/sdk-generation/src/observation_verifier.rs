// Shared Rust scenario-log verifier implementation.
//
// The standalone CLI includes this implementation through its public entry
// points. The sdk-generation binary also includes it as a module so aggregate
// qualification uses the same ordered-plan, frame, terminal, and provenance
// checks.
include!("bin/verify-observations.rs");
