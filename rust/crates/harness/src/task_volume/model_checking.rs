//! Fault injection and assertions for the production task-file model.
use super::*;
use std::cell::Cell;

/// Mutations present only in the dedicated harness model build.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub enum Fault {
    /// Production behavior.
    #[default]
    None,
    /// Resample the follow cursor after producing the snapshot.
    ResampleFollow,
    /// Replace the caller's original generation fence with a fresh tail sample.
    ResamplePublicationFence,
    /// Keep an index from the replaced history.
    RetainObsoleteIndex,
    /// Apply an old acknowledgement over a rebuilt index.
    OverwriteRebuiltIndex,
    /// Discard caller-supplied authority conditions.
    OmitAuthorityConditions,
    /// Publish without the caller's deadline.
    IgnoreDeadline,
    /// Stop output on an unrelated task-file writer's generation advance.
    StopAfterGenerationConflict,
}

loom::thread_local! {
    static FAULT: Cell<Fault> = Cell::new(Fault::None);
}

/// Selects a mutation on the current modeled actor.
pub fn set_fault(fault: Fault) {
    FAULT.with(|current| current.set(fault));
}

pub(super) fn enabled(fault: Fault) -> bool {
    FAULT.with(|current| current.get() == fault)
}

/// Checks the cache before another observation can repair a stale acknowledgement.
pub async fn assert_index_matches<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    snapshot: &Snapshot,
) {
    let index = volume.index.lock().await;
    assert_eq!(
        volume.generation(&index),
        snapshot.generation,
        "acknowledgement overwrote rebuilt generation"
    );
    assert_eq!(
        index
            .files
            .range(..)
            .map(|(_, stored)| stored.file.clone())
            .collect::<Vec<_>>(),
        snapshot.files,
        "acknowledgement overwrote rebuilt files"
    );
}
