//! Deterministic model scripts for harness consumers and scenario tests.

mod scripted;

pub use scripted::{ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn};

/// The composition's actual task provider, for scenario wire-frame assertions.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub fn local_task_stream(
    harness: &crate::LocalHarness,
) -> std::sync::Arc<acyclic_stream::LocalStream> {
    harness.task_stream()
}
