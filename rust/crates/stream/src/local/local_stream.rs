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
        let provider = MemoryStream::new_with_commit_clock(
            limits.memory,
            clock,
            Arc::clone(&commit_clock) as _,
        );
        let mut replay_error = None;
        while let Some(recovered) = receiver.recv().await {
            let replayed = match recovered {
                Recovered::Validate(complete) => {
                    let _ = complete.send(replay_error.is_none());
                    continue;
                }
                _ if replay_error.is_some() => continue,
                Recovered::State { encoded } => provider
                    .install_state(&encoded)
                    .await
                    .map_err(|_| LocalStreamError::Corrupt),
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
        Ok(Self {
            inner: Arc::new(LocalInner {
                provider,
                commit_clock,
                journal: Arc::new(Mutex::new(journal)),
                visibility: RwLock::new(()),
                publication: RwLock::new(()),
                synced: watch::channel(0_u64).0,
                failed,
                revision: watch::channel(0).0,
                #[cfg(acyclic_loom)]
                omit_follow_wake: false,
                #[cfg(acyclic_loom)]
                retain_pending_read: false,
                durable_tails: Mutex::new(Default::default()),
                #[cfg(feature = "test-support")]
                persistence_cut: Default::default(),
            }),
            work: None,
        })
    }

    fn check_available(&self) -> Result<(), StreamError> {
        if *self.inner.failed.borrow() {
            Err(StreamError::Unavailable)
        } else {
            Ok(())
        }
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
                Arc::clone(&journal.file),
                journal.limits.durability,
            )
        };
        sync_file_data(&file, durability).map_err(|_| self.inner.poison())?;
        let mut journal = self.inner.journal.lock().map_err(|_| self.inner.poison())?;
        self.inner.publish_synced(&mut journal, target);
        // A restart in between already marked its own header flushed.
        #[cfg(feature = "test-support")]
        if journal.epoch == written.0 {
            journal.flushed = journal.flushed.max(written.1);
        }
        Ok(())
    }

    /// Writes `frame`, in the order its mutation applied, without waiting for
    async fn persist(&self, mut frame: PreparedFrame) -> Result<(), StreamError> {
        #[cfg(feature = "test-support")]
        let cut = self.inner.persistence_cut.get().cloned();
        #[cfg(feature = "test-support")]
        if let Some(cut) = &cut
            && !cut.admit()
        {
            return Err(self.cut_crash(cut.as_ref()).await);
        }
        let required = !deferred();
        let journal = self.inner.journal.clone();
        let persist = spawn_blocking(move || {
            journal
                .lock()
                .map_err(|_| LocalStreamError::Corrupt)?
                .append(&mut frame, required)
        });
        let result = persist
            .await
            .map_err(|_| LocalStreamError::Executor)
            .and_then(|result| result);
        if result.is_err() {
            return Err(self.inner.poison());
        }
        #[cfg(feature = "test-support")]
        if let Some(cut) = cut {
            return self.cut_after_write(cut).await;
        }
        self.inner.published();
        Ok(())
    }

    // Native builds reduce this to drop. The modeled old variant returns the
    // guard so the caller retains it through maintenance, exposing the stall.
    fn release_publication<G>(&self, publication: G) -> Option<G> {
        #[cfg(acyclic_loom)]
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
                journal.compact(&state, work.as_ref())?;
                let appended = journal.appended;
                inner.publish_synced(&mut journal, appended);
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
        if REQUIRE && !deferred() {
            let mut journal = self
                .inner
                .journal
                .lock()
                .map_err(|_| StreamError::Unavailable)?;
            journal.required = journal.appended;
        }
        Ok(())
    }

    /// Returns once every frame through `sequence` is durable: group commit.
    async fn await_durable(&self, sequence: u64) -> Result<(), StreamError> {
        let mut synced = self.inner.synced.subscribe();
        loop {
            self.check_available()?;
            if *synced.borrow_and_update() >= sequence {
                return Ok(());
            }
            let lead = {
                let mut journal = self.inner.journal.lock().map_err(|_| self.inner.poison())?;
                if journal.synced >= sequence || journal.syncing {
                    None
                } else {
                    journal.syncing = true;
                    Some((
                        journal.appended,
                        journal.synced,
                        journal.bytes,
                        Arc::clone(&journal.file),
                        journal.limits.durability,
                    ))
                }
            };
            match lead {
                Some((target, synced_before, journal_bytes, file, durability)) => {
                    let inner = Arc::clone(&self.inner);
                    let work = self.work.clone();
                    spawn_blocking(move || {
                        inner.flush_through(target, &file, durability, work.as_deref());
                    })
                    .await
                    .map_err(|_| self.inner.poison())?;
                    // Group commit: one fsync covers every frame written since the last one.
                    tracing::debug!(
                        fsyncs = 1_u64,
                        batch_len = target.saturating_sub(synced_before),
                        journal_bytes,
                        waiters = self.inner.synced.receiver_count(),
                        "acyclic.stream.fsync"
                    );
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
        let stream = self.clone();
        // The mutation owns completion as well as durability, even if its caller is dropped.
        let deferred = deferred();
        let operation = span.clone();
        spawn(DEFERRED.scope(deferred, async move {
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
                    stream.await_durable(required).await?;
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
            self.await_durable(required).await?;
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
