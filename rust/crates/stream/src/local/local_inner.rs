struct LocalInner {
    provider: MemoryStream,
    commit_clock: Arc<PinnedCommitClock>,
    // Lock order: visibility -> journal -> synced watch; never acquire provider state while holding journal.
    journal: Arc<Mutex<Journal>>,
    /// Held exclusively while a mutation applies and writes its frame, and
    visibility: RwLock<()>,
    /// The journal's `synced` sequence, published under the journal lock so
    synced: watch::Sender<u64>,
    // None is terminal; publication and failure share one synchronized push state.
    changed: watch::Sender<Option<u64>>,
    /// Lock order: visibility -> durable_tails, after releasing provider state and journal.
    durable_tails: Mutex<std::collections::BTreeMap<StreamPath, u64>>,
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
        self.changed.send_replace(None);
        // Wake every caller waiting for a flush, so it sees the poison.
        self.synced.send_modify(|_| {});
        StreamError::Unavailable
    }

    fn published(&self) {
        self.changed.send_modify(|revision| {
            if let Some(revision) = revision {
                *revision = revision.saturating_add(1);
            }
        });
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
