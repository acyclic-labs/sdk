struct LocalInner {
    provider: MemoryStream,
    // Immutable publication policy; reading it never takes the journal lock.
    durability: LocalDurability,
    commit_clock: Arc<PinnedCommitClock>,
    // Lock order: visibility -> publication -> journal -> synced watch; never acquire provider state while holding journal.
    journal: Arc<Mutex<Journal>>,
    // Serializes mutations and their snapshot/journal cut, including compaction.
    // Lock order: visibility -> publication -> provider state -> journal/watch.
    visibility: RwLock<()>,
    // Accepted readers use only this fence. It ends after the frame is written,
    // before compaction performs device work under the outer visibility fence.
    publication: RwLock<()>,
    /// Ordered and device-flushed cuts, published together under the journal lock.
    synced: watch::Sender<SyncProgress>,
    // Lock order: visibility -> publication -> journal -> failure watch. No failure borrow
    // crosses a visibility/publication wait or provider call. Only poison wakes this watch.
    failed: watch::Sender<bool>,
    // Lock order: visibility -> publication -> revision watch; no borrow crosses provider work.
    // Borrow-only admission fence: record/discovery cursors never subscribe.
    revision: watch::Sender<u64>,
    // Model-only control: omit the accepted-path wait registration.
    #[cfg(loom)]
    omit_follow_wake: bool,
    #[cfg(loom)]
    retain_pending_read: bool,
    // Lock order: publication -> durable_tails, after releasing provider state and journal.
    durable_tails: Mutex<std::collections::BTreeMap<StreamPath, u64>>,
    #[cfg(feature = "test-support")]
    persistence_cut: std::sync::OnceLock<Arc<dyn PersistenceCut>>,
}

impl LocalInner {
    fn required(&self) -> Result<u64, StreamError> {
        Ok(self
            .journal
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .required)
    }

    fn poison(&self) -> StreamError {
        #[cfg(loom)]
        let notify =
            !crate::verification::fault_is(crate::verification::Fault::OmitPoisonPublication);
        #[cfg(not(loom))]
        let notify = true;
        if notify {
            self.failed.send_replace(true);
        }
        // Wake every caller waiting for a flush, so it sees the poison.
        self.synced.send_modify(|_| {});
        StreamError::Unavailable
    }

    fn published(&self) {
        self.revision
            .send_modify(|revision| *revision = revision.saturating_add(1));
    }

    async fn unavailable(failed: &mut watch::Receiver<bool>) -> StreamError {
        loop {
            if *failed.borrow_and_update() {
                return StreamError::Unavailable;
            }
            if failed.changed().await.is_err() {
                return StreamError::Unavailable;
            }
        }
    }

    /// Publishes only the strength completed by the device operation.
    fn publish_synced(&self, journal: &mut Journal, target: u64, completion: SyncCompletion) {
        #[cfg(loom)]
        let completion =
            if crate::verification::fault_is(crate::verification::Fault::BarrierAsFlushed) {
                SyncCompletion::Flushed(DeviceFlushed)
            } else {
                completion
            };
        journal.synced = journal.synced.max(target);
        if matches!(completion, SyncCompletion::Flushed(_)) {
            journal.flushed_sequence = Some(journal.flushed_sequence.unwrap_or(0).max(target));
        }
        let synced = SyncProgress {
            ordered: journal.synced,
            flushed: journal.flushed_sequence,
        };
        self.synced.send_modify(|published| *published = synced);
    }

    /// Captures one journal/file cut in the worker immediately before I/O.
    /// Writes accepted while this worker was queued share its device operation;
    /// writes racing the operation never advance its certified cut.
    fn flush_through(
        &self,
        file: &acyclic_native_runtime::FileLease,
        durability: LocalDurability,
        work: Option<&crate::native::Work>,
    ) -> Option<(u64, u64, u64)> {
        #[cfg(test)]
        {
            let journal_identity = Arc::as_ptr(&self.journal) as usize;
            if let Ok(mut hook) = JOURNAL_PERSIST_BLOCKER.lock()
                && hook
                    .as_ref()
                    .is_some_and(|(expected_journal, _, _)| *expected_journal == journal_identity)
                && let Some((_, started, release)) = hook.take()
            {
                let _ = started.send(());
                let _ = release.recv();
            }
        }
        let (target, synced_before, journal_bytes) = {
            let Ok(journal) = self.journal.lock() else {
                self.poison();
                return None;
            };
            (journal.appended, journal.synced, journal.bytes)
        };
        let flushed = match &work {
            Some(work) => file.flush(|file| work.sync(|| sync_file_data(file, durability))),
            None => file.flush(|file| sync_file_data(file, durability)),
        };
        #[cfg(test)]
        {
            let identity = Arc::as_ptr(&self.journal) as usize;
            let gate = {
                let mut hook = JOURNAL_FLUSHED_BLOCKER
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if hook
                    .as_ref()
                    .is_some_and(|(expected, _, _)| *expected == identity)
                {
                    hook.take()
                } else {
                    None
                }
            };
            if let Some((_, started, release)) = gate {
                let _ = started.send(());
                let _ = release.recv();
            }
        }
        let Ok(mut journal) = self.journal.lock() else {
            self.poison();
            return None;
        };
        journal.syncing = false;
        if let Ok(completion) = flushed {
            self.publish_synced(&mut journal, target, completion);
        } else {
            drop(journal);
            self.poison();
        }
        Some((target, synced_before, journal_bytes))
    }
}
