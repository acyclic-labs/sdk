struct LocalInner {
    provider: MemoryStream,
    commit_clock: Arc<PinnedCommitClock>,
    journal: Arc<Mutex<Journal>>,
    /// Held exclusively while a mutation applies and writes its frame, and
    visibility: RwLock<()>,
    /// The journal's `synced` sequence, published under the journal lock so
    synced: watch::Sender<u64>,
    changed: watch::Sender<u64>,
    poisoned: AtomicBool,
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
        self.poisoned.store(true, Ordering::Release);
        // Wake every caller waiting for a flush, so it sees the poison.
        self.synced.send_modify(|_| {});
        StreamError::Unavailable
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
        work: Option<Arc<crate::memory::Work>>,
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
