//! Fault injection and assertions for the production task-file model.
use super::*;
use std::cell::Cell;

mod journal;
pub(crate) use journal::Journal;

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
    /// Return from child control before its live effect future is drained.
    DropActiveChild,
    /// Admit child preparation before its staged inputs are durable.
    SkipChildInputFence,
    /// Send live child mail without acknowledging its result bound.
    SkipChildInputAck,
    /// Return a controller error while sibling effect futures are live.
    DropSiblingEffects,
}

loom::thread_local! {
    static FAULT: Cell<Fault> = Cell::new(Fault::None);
    static PAUSE_PUBLICATION: Cell<bool> = Cell::new(false);
}

/// Parks the next prepared publication at its provider IO boundary.
pub fn pause_next_publication() {
    PAUSE_PUBLICATION.with(|pause| pause.set(true));
}

pub(crate) async fn publication_ready() {
    let mut pause = PAUSE_PUBLICATION.with(|pause| pause.replace(false));
    std::future::poll_fn(|cx| {
        if std::mem::replace(&mut pause, false) {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(())
        }
    })
    .await
}

/// Selects a mutation on the current modeled actor.
pub fn set_fault(fault: Fault) {
    FAULT.with(|current| current.set(fault));
}

pub(crate) fn enabled(fault: Fault) -> bool {
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

#[cfg(test)]
pub(crate) use super::files::model_files as files;
