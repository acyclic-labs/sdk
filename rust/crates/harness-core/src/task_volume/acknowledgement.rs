use super::*;

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    pub(super) async fn acknowledge(
        &self,
        expected: Generation,
        files: FileValue,
        next: u64,
        commit: CommitId,
    ) -> Generation {
        // Lock order: index only. Provider I/O has already completed.
        let mut index = self.index.lock().await;
        let unchanged = self.generation(&index) == expected;
        #[cfg(acyclic_harness_loom)]
        let unchanged =
            unchanged || model_checking::enabled(model_checking::Fault::OverwriteRebuiltIndex);
        if unchanged {
            apply(&mut index, files, next);
            index.next = next;
            index.commit = commit;
        }
        Generation {
            volume: expected.volume,
            epoch: expected.epoch,
            position: next,
            commit: *commit.as_bytes(),
        }
    }
}
