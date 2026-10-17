struct LocalInner {
    provider: MemoryStream,
    commit_clock: Arc<PinnedCommitClock>,
    // Lock order: visibility -> publication -> journal -> synced watch; never acquire provider state while holding journal.
    journal: Arc<Mutex<Journal>>,
    // Serializes mutations and their snapshot/journal cut, including compaction.
    // Lock order: visibility -> publication -> provider state -> journal/watch.
    visibility: RwLock<()>,
    // Accepted readers use only this fence. It ends after the frame is written,
    // before compaction performs device work under the outer visibility fence.
    publication: RwLock<()>,
    /// The journal's `synced` sequence, published under the journal lock so
    synced: watch::Sender<u64>,
    // Lock order: visibility -> publication -> journal -> failure watch. No failure borrow
    // crosses a visibility/publication wait or provider call. Only poison wakes this watch.
    failed: watch::Sender<bool>,
    // Lock order: visibility -> publication -> revision watch; no borrow crosses provider work.
    // Borrow-only admission fence: record/discovery cursors never subscribe.
    revision: watch::Sender<u64>,
    // Model-only control: omit the accepted-path wait registration.
    #[cfg(acyclic_loom)]
    omit_follow_wake: bool,
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
        #[cfg(acyclic_loom)]
        let notify =
            !crate::verification::fault_is(crate::verification::Fault::OmitPoisonPublication);
        #[cfg(not(acyclic_loom))]
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

    /// Records that every frame through `target` is durable.
    fn publish_synced(&self, journal: &mut Journal, target: u64) {
        journal.synced = journal.synced.max(target);
        let synced = journal.synced;
        self.synced.send_modify(|published| *published = synced);
    }

    /// Flushes the journal through `target`, the latest frame written when
    fn flush_through(
        &self,
        target: u64,
        file: &File,
        durability: LocalDurability,
        work: Option<&crate::memory::Work>,
    ) {
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
        let flushed = match &work {
            Some(work) => work.sync(|| sync_file_data(file, durability)),
            None => sync_file_data(file, durability),
        };
        let Ok(mut journal) = self.journal.lock() else {
            self.poison();
            return;
        };
        journal.syncing = false;
        if flushed.is_ok() {
            self.publish_synced(&mut journal, target);
        } else {
            drop(journal);
            self.poison();
        }
    }
}
