impl LocalStream {
    /// Opens or creates one exclusive durable provider below `root`.
    pub async fn open(
        root: impl AsRef<Path>,
        limits: LocalStreamLimits,
    ) -> Result<Self, LocalStreamError> {
        Self::open_with_clock_and_anchor(root, limits, Arc::new(SystemUnixMillisClock), None).await
    }

    /// Opens a provider while retaining an external ownership gate through every provider clone.
    #[doc(hidden)]
    pub async fn open_with_ownership_anchor(
        root: impl AsRef<Path>,
        limits: LocalStreamLimits,
        ownership_anchor: OwnershipAnchor,
    ) -> Result<Self, LocalStreamError> {
        Self::open_with_clock_and_anchor(
            root,
            limits,
            Arc::new(SystemUnixMillisClock),
            Some(ownership_anchor),
        )
        .await
    }

    /// Opens a provider with an injected trusted clock.
    pub async fn open_with_clock(
        root: impl AsRef<Path>,
        limits: LocalStreamLimits,
        clock: Arc<dyn UnixMillisClock>,
    ) -> Result<Self, LocalStreamError> {
        Self::open_with_clock_and_anchor(root, limits, clock, None).await
    }

    /// Opens a provider with a trusted clock and an optional retained owner.
    #[doc(hidden)]
    pub async fn open_with_clock_and_anchor(
        root: impl AsRef<Path>,
        limits: LocalStreamLimits,
        clock: Arc<dyn UnixMillisClock>,
        ownership_anchor: Option<OwnershipAnchor>,
    ) -> Result<Self, LocalStreamError> {
        let span = obs::OperationSpan::new(tracing::info_span!(
            "acyclic.stream.open",
            outcome = Empty,
            error.kind = Empty
        ));
        span.scope(async {
            let root = root.as_ref().to_path_buf();
            // Dropping the caller must not release an external root gate while journal recovery is
            // still running on a blocking worker. The independently owned task retains every startup
            // input until the worker and bounded replay have both completed.
            run_owned_initialization(&span, async move {
                Self::open_owned(root, limits, clock, ownership_anchor).await
            })
            .await
        })
        .await
    }

    async fn open_owned(
        root: PathBuf,
        limits: LocalStreamLimits,
        clock: Arc<dyn UnixMillisClock>,
        ownership_anchor: Option<OwnershipAnchor>,
    ) -> Result<Self, LocalStreamError> {
        validate_limits(limits)?;
        let (recovered, mut receiver) = mpsc::channel(REPLAY_PIPELINE_COMMANDS);
        #[cfg(test)]
        let submitted_root = root.clone();
        let span = obs::OperationSpan::new(tracing::debug_span!(
            "acyclic.stream.recover",
            frames = Empty,
            bytes = Empty
        ));
        let open = spawn_blocking(move || {
            span.in_scope(|| Journal::open(&root, limits, &recovered, &span))
        });
        #[cfg(test)]
        {
            if let Ok(mut hook) = JOURNAL_OPEN_SUBMITTED.lock()
                && hook
                    .as_ref()
                    .is_some_and(|(expected_root, _)| expected_root == &submitted_root)
                && let Some((_, submitted)) = hook.take()
            {
                let _ = submitted.send(());
            }
        }
        let commit_clock = Arc::new(PinnedCommitClock {
            source: Arc::clone(&clock),
            pinned: Mutex::new(None),
        });
        let mut provider = MemoryStream::new_with_commit_clock(
            limits.memory,
            clock,
            Arc::clone(&commit_clock) as _,
        );
        let mut replay_error = None;
        while let Some(recovered) = receiver.recv().await {
            let replayed = match recovered {
                // Arrives first, before any state is installed or replayed.
                Recovered::Incarnation(scope, incarnation) => {
                    provider = provider.with_authority(scope, incarnation);
                    continue;
                }
                Recovered::Validate(complete) => {
                    let _ = complete.send(replay_error.is_none());
                    continue;
                }
                _ if replay_error.is_some() => continue,
                Recovered::State { encoded } => {
                    provider
                        .restore_state(&encoded)
                        .await
                        .map_err(|error| match error {
                            crate::memory::RestoreError::PreRevision => {
                                LocalStreamError::UnsupportedPreRevisionStore
                            }
                            crate::memory::RestoreError::State(StreamError::InvalidArgument) => {
                                LocalStreamError::Corrupt
                            }
                            crate::memory::RestoreError::State(error) => {
                                LocalStreamError::Replay(error)
                            }
                        })
                }
                Recovered::Command {
                    committed_at_millis,
                    command,
                } => {
                    commit_clock.pin(committed_at_millis);
                    replay(&provider, command)
                        .await
                        .map_err(LocalStreamError::Replay)
                }
            };
            if let Err(error) = replayed {
                replay_error = Some(error);
            }
        }
        let mut journal = open.await.map_err(|_| LocalStreamError::Executor)??;
        if let Some(error) = replay_error {
            return Err(error);
        }
        journal._ownership_anchor = ownership_anchor;
        commit_clock.unpin();
        let (failed, _) = watch::channel(false);
        let progress = SyncProgress {
            ordered: journal.synced,
            flushed: journal.flushed_sequence,
        };
        let handle = journal.claim.handle();
        Ok(Self {
            handle: Some(handle),
            inner: Arc::new(LocalInner {
                provider,
                durability: limits.durability,
                commit_clock,
                journal: Arc::new(Mutex::new(journal)),
                visibility: RwLock::new(()),
                publication: RwLock::new(()),
                synced: watch::channel(progress).0,
                failed,
                revision: watch::channel(0).0,
                #[cfg(loom)]
                omit_follow_wake: false,
                #[cfg(loom)]
                retain_pending_read: false,
                durable_tails: Mutex::new(Default::default()),
                #[cfg(feature = "test-support")]
                persistence_cut: Default::default(),
            }),
            work: None,
        })
    }

    /// This provider for its own detached work, which holds no user's
    /// handle: work a user left running drains as the store closes.
    fn worker(&self) -> Self {
        Self {
            handle: None,
            inner: Arc::clone(&self.inner),
            work: self.work.clone(),
        }
    }

    fn check_available(&self) -> Result<(), StreamError> {
        if *self.inner.failed.borrow() {
            Err(StreamError::Unavailable)
        } else {
            Ok(())
        }
    }

    async fn execute(
        &self,
        command: Command,
        deadline: Option<u64>,
    ) -> Result<crate::journal::Outcome, StreamError> {
        let cursor = self
            .inner
            .journal
            .lock()
            .map_err(|_| self.inner.poison())?
            .bytes;
        let clock = Arc::new(crate::replicated::ReplicaClock::default());
        let provider = self
            .inner
            .provider
            .with_commit_clock(Arc::clone(&clock) as _);
        let mut replica = crate::replicated::Replicated::new(
            port::LocalPort {
                stream: self.clone(),
            },
            provider,
            clock,
            cursor,
        );
        let result = replica
            .execute(command, deadline, REPLAY_PIPELINE_COMMANDS)
            .await;
        if matches!(
            &result,
            Ok(crate::journal::Outcome::Append(AppendOutcome::Committed(_))
                | crate::journal::Outcome::Commit(CommitOutcome::Committed(_))
                | crate::journal::Outcome::Fork(_))
        ) {
            // A retained success joins the original durability cut even when
            // the canonical decision correctly elides another journal frame.
            self.require_accepted::<true>()?;
        }
        if matches!(
            result,
            Err(StreamError::Unavailable | StreamError::InvalidResponse | StreamError::Unsupported)
        ) {
            self.inner.poison();
        }
        result
    }

    /// Encodes the command and pins its recorded commit timestamp.
    fn prepare(&self, command: &Command) -> Result<PreparedFrame, StreamError> {
        let committed_at_millis = self.inner.commit_clock.source.now_unix_millis();
        let frame = PreparedFrame::encode(command, committed_at_millis, self.work.clone())
            .map_err(|error| match error {
                LocalStreamError::InvalidLimits => StreamError::Capacity,
                LocalStreamError::Io(error) => match &self.work {
                    Some(work) => {
                        let _ = work.record::<()>(Err(error));
                        StreamError::Unavailable
                    }
                    None => StreamError::Unavailable,
                },
                _ => StreamError::Unavailable,
            })?;
        self.inner
            .journal
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .admit(&frame)
            .map_err(|error| match error {
                LocalStreamError::InvalidLimits => StreamError::Capacity,
                LocalStreamError::Io(error) => match &self.work {
                    Some(work) => {
                        let _ = work.record::<()>(Err(error));
                        StreamError::Unavailable
                    }
                    None => StreamError::Unavailable,
                },
                _ => StreamError::Unavailable,
            })?;
        self.inner.commit_clock.pin(committed_at_millis);
        Ok(frame)
    }

    /// Flushes every frame written without waiting for the storage device
    pub fn flush(&self) -> Result<(), StreamError> {
        let span = obs::OperationSpan::new(tracing::info_span!(
            "acyclic.stream.flush",
            batch_len = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        span.in_scope(|| obs::finish(&span, self.flush_all(&span)))
    }

    fn flush_all(&self, span: &tracing::Span) -> Result<(), StreamError> {
        #[cfg(feature = "test-support")]
        let written;
        let (target, file, durability) = {
            let journal = self.inner.journal.lock().map_err(|_| self.inner.poison())?;
            if journal.synced >= journal.appended {
                return Ok(());
            }
            #[cfg(feature = "test-support")]
            {
                written = (journal.epoch, journal.bytes);
            }
            span.record("batch_len", journal.appended - journal.synced);
            (
                journal.appended,
                journal.file.lease().map_err(|_| self.inner.poison())?,
                journal.limits.durability,
            )
        };
        let completion = file
            .flush(|file| sync_file_data(file, durability))
            .map_err(|_| self.inner.poison())?;
        #[cfg(feature = "test-support")]
        let device_flushed = matches!(completion, SyncCompletion::Flushed(_));
        let mut journal = self.inner.journal.lock().map_err(|_| self.inner.poison())?;
        self.inner.publish_synced(&mut journal, target, completion);
        // A restart in between already marked its own header flushed.
        #[cfg(feature = "test-support")]
        if device_flushed && journal.epoch == written.0 {
            journal.flushed = journal.flushed.max(written.1);
        }
        Ok(())
    }

    /// Writes one frame through the same ordered owner as a journal batch.
    async fn persist(&self, frame: PreparedFrame) -> Result<(), StreamError> {
        self.persist_frames(std::iter::once(frame)).await
    }

    /// The caller retains publication for the entire ordered proposal. The
    /// worker owns every original frame and its reservations until terminal I/O.
    async fn persist_frames(
        &self,
        frames: impl Iterator<Item = PreparedFrame> + Send + 'static,
    ) -> Result<(), StreamError> {
        #[cfg(feature = "test-support")]
        let cut = self.inner.persistence_cut.get().cloned();
        #[cfg(feature = "test-support")]
        let fail_cut_truncation = FAIL_CUT_TRUNCATION.with(std::cell::Cell::get);
        let required = !deferred(self);
        let stream = self.worker();
        spawn_blocking(move || {
            let mut written = false;
            for mut frame in frames {
                #[cfg(feature = "test-support")]
                if let Some(cut) = &cut
                    && !cut.admit()
                {
                    return Err(stream.cut_crash(cut.as_ref(), fail_cut_truncation));
                }
                {
                    let mut journal = stream
                        .inner
                        .journal
                        .lock()
                        .map_err(|_| stream.inner.poison())?;
                    if frame.deadline.is_some_and(|deadline| {
                        stream.inner.commit_clock.source.now_unix_millis() >= deadline
                    }) {
                        return Err(if written {
                            // Earlier frames may already be durable. Only reopen
                            // and replay can resolve this partially published proposal.
                            stream.inner.poison()
                        } else {
                            StreamError::DeadlineElapsed
                        });
                    }
                    journal
                        .append(&mut frame, required)
                        .map_err(|_| stream.inner.poison())?;
                    written = true;
                }
                #[cfg(feature = "test-support")]
                if let Some(cut) = &cut {
                    stream.cut_after_write(cut.as_ref(), fail_cut_truncation)?;
                }
                stream.inner.published();
            }
            Ok(())
        })
        .await
        .map_err(|_| self.inner.poison())?
    }

    // Native builds reduce this to drop. The modeled old variant returns the
    // guard so the caller retains it through maintenance, exposing the stall.
    fn release_publication<G>(&self, publication: G) -> Option<G> {
        #[cfg(loom)]
        if crate::verification::fault_is(
            crate::verification::Fault::HoldPublicationDuringMaintenance,
        ) {
            return Some(publication);
        }
        drop(publication);
        None
    }

    /// Writes the whole state as the snapshot and starts the journal again,
    async fn compact_if_due(&self) -> Result<(), StreamError> {
        if !self
            .inner
            .journal
            .lock()
            .map_err(|_| self.inner.poison())?
            .compaction_due()
        {
            return Ok(());
        }
        let state = self.inner.provider.encode_state().await;
        let inner = Arc::clone(&self.inner);
        let work = self.work.clone();
        let span = obs::OperationSpan::new(tracing::debug_span!(
            "acyclic.stream.compact",
            bytes = state.len()
        ));
        spawn_blocking(move || {
            span.in_scope(|| {
                let mut journal = inner
                    .journal
                    .lock()
                    .map_err(|_| LocalStreamError::Corrupt)?;
                #[cfg(test)]
                {
                    let identity = Arc::as_ptr(&inner.journal) as usize;
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
                let flushed = journal.compact(&state, work.as_ref())?;
                let appended = journal.appended;
                inner.publish_synced(&mut journal, appended, SyncCompletion::Flushed(flushed));
                Ok::<_, LocalStreamError>(())
            })
        })
        .await
        .map_err(|_| LocalStreamError::Executor)
        .and_then(|result| result)
        .map_err(|_| self.inner.poison())
    }

    // Called under visibility before releasing an ordinary replay. A deferred
    // original must become required, without appending a duplicate frame.
    fn require_accepted<const REQUIRE: bool>(&self) -> Result<(), StreamError> {
        if REQUIRE && !deferred(self) {
            let mut journal = self
                .inner
                .journal
                .lock()
                .map_err(|_| StreamError::Unavailable)?;
            if journal.synced < journal.appended
                && let Some(work) = &self.work
            {
                work.reserve_durability(1)?;
            }
            journal.required = journal.appended;
        }
        Ok(())
    }

    /// Pins the accepted prefix before joining the common group-commit wait.
    async fn seal_accepted(&self) -> Result<(), StreamError> {
        self.seal_accepted_with_reservation(false).await
    }

    async fn seal_accepted_with_reservation(&self, reserved: bool) -> Result<(), StreamError> {
        self.check_available()?;
        let target = {
            let mut journal = self.inner.journal.lock().map_err(|_| self.inner.poison())?;
            if journal
                .flushed_sequence
                .is_some_and(|cut| cut >= journal.appended)
            {
                return Ok(());
            }
            if !reserved && let Some(work) = &self.work {
                work.reserve_durability(1)?;
            }
            journal.required = journal.appended;
            journal.appended
        };
        self.await_flushed(target).await.map(|_| ())
    }

    /// Waits for the configured publication strength, sharing one group sync.
    async fn await_publication(&self, sequence: u64) -> Result<(), StreamError> {
        self.await_synced(sequence, self.inner.durability)
            .await
            .map(|_| ())
    }

    async fn await_flushed(&self, sequence: u64) -> Result<DeviceFlushed, StreamError> {
        match self
            .await_synced(sequence, LocalDurability::FullFlush)
            .await?
        {
            SyncCompletion::Flushed(flushed) => Ok(flushed),
            SyncCompletion::Ordered => Err(StreamError::Unavailable),
        }
    }

    async fn await_synced(
        &self,
        sequence: u64,
        durability: LocalDurability,
    ) -> Result<SyncCompletion, StreamError> {
        let mut synced = self.inner.synced.subscribe();
        loop {
            self.check_available()?;
            if let Some(completion) = synced.borrow_and_update().completion(sequence, durability) {
                return Ok(completion);
            }
            let lead = {
                let mut journal = self.inner.journal.lock().map_err(|_| self.inner.poison())?;
                let progress = SyncProgress {
                    ordered: journal.synced,
                    flushed: journal.flushed_sequence,
                };
                if progress.completion(sequence, durability).is_some() || journal.syncing {
                    None
                } else {
                    let Ok(file) = journal.file.lease() else {
                        drop(journal);
                        return Err(self.inner.poison());
                    };
                    journal.syncing = true;
                    Some((file, durability))
                }
            };
            match lead {
                Some((file, durability)) => {
                    let inner = Arc::clone(&self.inner);
                    let work = self.work.clone();
                    let flushed = spawn_blocking(move || {
                        inner.flush_through(&file, durability, work.as_deref())
                    })
                    .await
                    .map_err(|_| self.inner.poison())?;
                    // Group commit: one fsync covers every frame written since the last one.
                    if let Some((target, synced_before, journal_bytes)) = flushed {
                        tracing::debug!(
                            fsyncs = 1_u64,
                            batch_len = target.saturating_sub(synced_before),
                            journal_bytes,
                            waiters = self.inner.synced.receiver_count(),
                            "acyclic.stream.fsync"
                        );
                    }
                }
                None => {
                    if synced.changed().await.is_err() {
                        return Err(StreamError::Unavailable);
                    }
                }
            }
        }
    }

    /// Runs one mutation under exclusive visibility on its own task, so a
    async fn exclusive<T, F>(
        &self,
        span: &obs::OperationSpan,
        mutation: impl FnOnce(Self) -> F + Send + 'static,
    ) -> Result<T, StreamError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, StreamError>> + Send + 'static,
    {
        self.check_available().inspect_err(|error| {
            obs::failed(span, error.code());
        })?;
        let stream = self.worker();
        // The mutation owns completion as well as durability, even if its caller is dropped.
        let operation = span.clone();
        spawn(publication_scope(async move {
            let result = operation
                .scope(async {
                    let (result, required) = {
                        let _visibility = stream.inner.visibility.write().await;
                        stream.check_available()?;
                        let publication = stream.inner.publication.write().await;
                        let result = mutation(stream.clone()).await;
                        stream.inner.commit_clock.unpin();
                        let _retained = stream.release_publication(publication);
                        if result.is_ok() {
                            stream.compact_if_due().await?;
                        }
                        (result, stream.inner.required()?)
                    };
                    stream.await_publication(required).await?;
                    result
                })
                .await;
            obs::finish(&operation, result)
        }))
        .await
        .map_err(|_| {
            obs::failed(span, StreamError::Unavailable.code());
            StreamError::Unavailable
        })?
    }

    /// Observes accepted publications without joining their durability wait.
    async fn accepted<T>(&self, read: impl Future<Output = T>) -> Result<T, StreamError> {
        let read = self.accepted_snapshot(read);
        #[cfg(feature = "test-support")]
        return acyclic_native_runtime::testing::observe_read(read).await;
        #[cfg(not(feature = "test-support"))]
        read.await
    }

    // Snapshot operations have no asynchronous acknowledgement after mutation.
    // The shared stream driver owns observation outside its cancellable probe.
    async fn accepted_snapshot<T>(&self, read: impl Future<Output = T>) -> Result<T, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.publication.read().await;
        self.check_available()?;
        Ok(read.await)
    }

    /// Runs one read under shared visibility, then joins its durable watermark.
    async fn visible<T>(&self, read: impl Future<Output = T>) -> Result<T, StreamError> {
        let read = async {
            self.check_available()?;
            let (result, required) = {
                let _visibility = self.inner.visibility.read().await;
                self.check_available()?;
                let required = self.inner.required()?;
                (read.await, required)
            };
            self.await_publication(required).await?;
            Ok(result)
        };
        #[cfg(feature = "test-support")]
        return acyclic_native_runtime::testing::observe_read(read).await;
        #[cfg(not(feature = "test-support"))]
        read.await
    }

    async fn read_visible(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.accepted(self.inner.provider.read(request)).await?
    }
}
