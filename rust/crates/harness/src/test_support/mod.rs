//! Deterministic model scripts for harness consumers and scenario tests.

mod scripted;
mod search;
mod task_volume_work;
pub use search::{ScriptedSearch, ScriptedSearchProvider};
pub use task_volume_work::measure as measure_task_volume_work;
pub(crate) use task_volume_work::{
    Files as TaskVolumeFiles, examined as task_volume_item_examined,
};

pub use scripted::{ScriptedEvent, ScriptedModelProvider, ScriptedReconciliation, ScriptedTurn};

/// The composition's actual task provider, for scenario wire-frame assertions.
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub fn local_task_stream(
    harness: &crate::LocalHarness,
) -> std::sync::Arc<acyclic_stream::LocalStream> {
    harness.task_stream()
}
