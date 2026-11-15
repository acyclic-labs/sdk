//! The native-runtime integration tests share one test binary, so the library
//! and its dependencies link once. Loom models keep their own targets because
//! each replaces production modules with instrumented copies.
mod cached_flush;
mod native_claim_record;
mod native_owner_record;
mod native_report_port;
mod node_process;
mod seeded_runtime;
mod signing_custody;
