// The original mutation retains completion custody while unrelated mutations
// append to another file. Abandoning that owner poisons rather than stranding it.
struct PendingCompaction {
    inner: Arc<LocalInner>,
    state: crate::memory::Snapshot,
    root: PathBuf,
    limits: LocalStreamLimits,
    epoch: u64,
    work: Option<Arc<crate::native::Work>>,
    complete: bool,
}

impl Drop for PendingCompaction {
    fn drop(&mut self) {
        if !self.complete {
            self.inner
                .fail(LocalStreamFailure::new("compact", "producer_lost"));
        }
    }
}

impl LocalStream {
    // Called under mutation visibility. Capture the immutable state and rotate
    // a flushed journal; neither encoding nor snapshot I/O owns this fence.
    async fn prepare_compaction(&self) -> Result<Option<PendingCompaction>, StreamError> {
        let (root, limits, through) = {
            let journal = self.inner.journal.lock().map_err(|_| self.inner.poison())?;
            if !journal.compaction_due() {
                return Ok(None);
            }
            (journal.root.clone(), journal.limits, journal.appended)
        };
        // A flush of the next file must not certify an unflushed previous file.
        self.await_flushed(through).await?;
        let state = self.inner.provider.capture_state().await;
        let inner = Arc::clone(&self.inner);
        let work = self.work.clone();
        let key = PersistenceKey::new(Arc::as_ptr(&inner.journal) as usize, 0);
        let epoch = spawn_blocking("streams", "rotate", key, move || {
            inner
                .journal
                .lock()
                .map_err(|_| LocalStreamError::Corrupt)?
                .rotate(work.as_ref())
        })
        .await
        .map_err(|_| LocalStreamError::Executor)
        .and_then(std::convert::identity)
        .map_err(|error| {
            self.inner.fail(LocalStreamFailure::local(
                "rotate",
                &error,
                self.work.as_deref(),
            ))
        })?;
        Ok(Some(PendingCompaction {
            inner: Arc::clone(&self.inner),
            state,
            root,
            limits,
            epoch,
            work: self.work.clone(),
            complete: false,
        }))
    }

    async fn finish_compaction(&self, mut pending: PendingCompaction) -> Result<(), StreamError> {
        let span = obs::OperationSpan::new(tracing::debug_span!("acyclic.stream.compact"));
        let key = PersistenceKey::new(Arc::as_ptr(&self.inner.journal) as usize, pending.epoch);
        spawn_blocking("streams", "compact", key, move || {
            span.in_scope(|| {
                let result = (|| {
                    let encoded = pending.state.encode();
                    #[cfg(test)]
                    {
                        let identity = Arc::as_ptr(&pending.inner.journal) as usize;
                        let gate = {
                            let mut hook = JOURNAL_COMPACT_BLOCKER
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
                    // No journal mutex, file lease or mutation fence spans this
                    // potentially large encode/write/flush/rename operation.
                    install_snapshot(
                        &pending.root,
                        pending.limits,
                        pending.epoch,
                        &encoded,
                        pending.work.as_ref(),
                    )?;
                    remove_archive(&pending.root, pending.work.as_ref())?;
                    let mut journal = pending
                        .inner
                        .journal
                        .lock()
                        .map_err(|_| LocalStreamError::Corrupt)?;
                    if !journal.compacting || journal.epoch != pending.epoch {
                        return Err(LocalStreamError::Corrupt);
                    }
                    journal.compacting = false;
                    Ok::<_, LocalStreamError>(())
                })();
                pending.complete = true;
                result.map_err(|error| {
                    pending.inner.fail(LocalStreamFailure::local(
                        "compact",
                        &error,
                        pending.work.as_deref(),
                    ))
                })
            })
        })
        .await
        .map_err(|_| {
            self.inner
                .fail(LocalStreamFailure::new("compact", "executor"))
        })?
    }
}
