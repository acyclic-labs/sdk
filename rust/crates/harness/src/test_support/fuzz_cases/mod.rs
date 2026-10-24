//! Shared public-API exercises for mutation fuzzers and shrinking property tests.

/// Shared client offline exercise.
pub mod client_offline;
/// Shared client resume exercise.
pub mod client_resume;
/// Shared client views exercise.
pub mod client_views;
/// Shared command oracle exercise.
pub mod command_oracle;
/// Shared file client exercise.
pub mod file_client;
/// Shared file views exercise.
pub mod file_views;
/// Shared goal parser input exercise.
pub mod goal_parser_input;
/// Shared mentions exercise.
pub mod mentions;
/// Shared notification files exercise.
pub mod notification_files;
/// Shared queries exercise.
pub mod queries;
/// Shared recorded invocation input exercise.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use acyclic_harness_core::test_support::recorded_invocation_input;
/// Shared swarm limits exercise.
pub mod swarm_limits;
/// Shared swarm membership exercise.
pub mod swarm_membership;
/// Shared swarm views exercise.
pub mod swarm_views;
/// Shared workspace media exercise.
pub mod workspace_media;

/// Synthetic numeric client adapter and evidence fixtures.
pub mod client_numbers;

/// Goal scenario mutation oracle.
pub mod goal_scenario_input;
/// Shared goal scenario contract.
pub mod goals;
