//! Shared fenced publication for one or several already-encoded file records.
use super::*;

#[cfg(all(test, not(acyclic_harness_loom)))]
thread_local! {
    static OMIT_INDEX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    // Lock order: index only; never hold it across provider I/O. An accepted
    // acknowledgement can update only the generation from which it was prepared.
    pub(super) async fn commit_append(
        &self,
        expected: Generation,
        request: CommitRequest,
        files: impl ExactSizeIterator<Item = FileValue>,
        next: u64,
        deadline_unix_millis: Option<u64>,
    ) -> Result<Generation> {
        if expected.position.checked_add(files.len() as u64) != Some(next) {
            return Err(Error::InvalidSegment);
        }
        #[cfg(acyclic_harness_loom)]
        let deadline_unix_millis = if model_checking::enabled(model_checking::Fault::IgnoreDeadline)
        {
            None
        } else {
            deadline_unix_millis
        };
        #[cfg(acyclic_harness_loom)]
        model_checking::publication_ready().await;
        #[cfg(acyclic_harness_loom)]
        let request = if model_checking::enabled(model_checking::Fault::ResampleCommitFence) {
            let mut request = request;
            let tail = self.provider.watermarks(self.path.clone()).await?.accepted;
            for condition in &mut request.conditions {
                if matches!(condition, CommitCondition::Head { path, .. } if path == &self.path) {
                    *condition = CommitCondition::Tail {
                        path: self.path.clone(),
                        expected: tail,
                    };
                }
            }
            request
        } else {
            request
        };
        let outcome = if let Some(deadline) = deadline_unix_millis {
            self.provider.commit_before(request, deadline).await?
        } else {
            self.provider.commit(request).await?
        };
        match outcome {
            CommitOutcome::Committed(envelope) => {
                let mut index = self.index.lock().await;
                let unchanged = self.generation(&index) == expected;
                #[cfg(all(test, not(acyclic_harness_loom)))]
                let unchanged = unchanged && !OMIT_INDEX.with(std::cell::Cell::get);
                #[cfg(acyclic_harness_loom)]
                let unchanged = unchanged
                    || model_checking::enabled(model_checking::Fault::OverwriteRebuiltIndex);
                if unchanged {
                    for (position, file) in (expected.position..next).zip(files) {
                        apply(&mut index, file, position + 1);
                    }
                    index.next = next;
                    index.commit = envelope.commit_id;
                }
                Ok(Generation {
                    volume: expected.volume,
                    epoch: expected.epoch,
                    position: next,
                    commit: *envelope.commit_id.as_bytes(),
                })
            }
            CommitOutcome::Conflict(_) => {
                self.refresh().await?;
                let index = self.index.lock().await;
                if expected == self.generation(&index) {
                    Err(Error::FenceConflict)
                } else {
                    Err(Error::Conflict(self.generation(&index)))
                }
            }
        }
    }
}

#[cfg(all(test, acyclic_harness_loom))]
mod models;
#[cfg(all(test, feature = "test-support", not(acyclic_harness_loom)))]
mod tests;
