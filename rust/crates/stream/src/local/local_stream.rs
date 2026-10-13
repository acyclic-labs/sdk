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

    async fn open_with_clock_and_anchor(
        root: impl AsRef<Path>,
        limits: LocalStreamLimits,
        clock: Arc<dyn UnixMillisClock>,
        ownership_anchor: Option<OwnershipAnchor>,
    ) -> Result<Self, LocalStreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
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
        let span = obs::OwnedSpan::new(tracing::debug_span!(
            "acyclic.stream.recover",
            frames = Empty,
            bytes = Empty
        ));
        let open = tokio::task::spawn_blocking(move || {
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
        let (changed, _) = watch::channel(0_u64);
        Ok(Self {
            inner: Arc::new(LocalInner {
                provider,
                commit_clock,
                journal: Arc::new(Mutex::new(journal)),
                visibility: RwLock::new(()),
                synced: watch::channel(0_u64).0,
                changed,
                poisoned: AtomicBool::new(false),
            }),
            work: None,
        })
    }

    fn check_available(&self) -> Result<(), StreamError> {
        if self.inner.poisoned.load(Ordering::Acquire) {
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
                    Some(work) => work.record::<()>(Err(error)).unwrap_err(),
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
                    Some(work) => work.record::<()>(Err(error)).unwrap_err(),
                    None => StreamError::Unavailable,
                },
                _ => StreamError::Unavailable,
            })?;
        self.inner.commit_clock.pin(committed_at_millis);
        Ok(frame)
    }

    /// Flushes every frame written without waiting for the storage device
    pub fn flush(&self) -> Result<(), StreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
            "acyclic.stream.flush",
            batch_len = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        span.in_scope(|| obs::finish(&span, self.flush_all(&span)))
    }

    fn flush_all(&self, span: &tracing::Span) -> Result<(), StreamError> {
        let (target, file, durability) = {
            let journal = self.inner.journal.lock().map_err(|_| self.inner.poison())?;
            if journal.synced >= journal.appended {
                return Ok(());
            }
            obs::record(span, "batch_len", journal.appended - journal.synced);
            (
                journal.appended,
                Arc::clone(&journal.sync),
                journal.limits.durability,
            )
        };
        sync_file_data(&file, durability).map_err(|_| self.inner.poison())?;
        let mut journal = self.inner.journal.lock().map_err(|_| self.inner.poison())?;
        self.inner.publish_synced(&mut journal, target);
        Ok(())
    }

    /// Writes `frame`, in the order its mutation applied, without waiting for
    async fn persist(&self, mut frame: PreparedFrame) -> Result<(), StreamError> {
        let required = !deferred();
        let journal = self.inner.journal.clone();
        let persist = tokio::task::spawn_blocking(move || {
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
        self.inner.changed.send_modify(|revision| {
            *revision = revision.saturating_add(1);
        });
        self.compact_if_due().await
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
        let span = obs::OwnedSpan::new(tracing::debug_span!(
            "acyclic.stream.compact",
            bytes = state.len()
        ));
        tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                let mut journal = inner
                    .journal
                    .lock()
                    .map_err(|_| LocalStreamError::Corrupt)?;
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
                        Arc::clone(&journal.sync),
                        journal.limits.durability,
                    ))
                }
            };
            match lead {
                Some((target, synced_before, journal_bytes, file, durability)) => {
                    let inner = Arc::clone(&self.inner);
                    let work = self.work.clone();
                    tokio::task::spawn_blocking(move || {
                        inner.flush_through(target, &file, durability, work);
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
        span: &obs::OwnedSpan,
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
        tokio::spawn(DEFERRED.scope(deferred, async move {
            let result = operation
                .scope(async {
                    let (result, required) = {
                        let _visibility = stream.inner.visibility.write().await;
                        stream.check_available()?;
                        let result = mutation(stream.clone()).await;
                        stream.inner.commit_clock.unpin();
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

    /// Runs one read under shared visibility, then returns its result once
    async fn visible<T>(
        &self,
        read: impl Future<Output = Result<T, StreamError>>,
    ) -> Result<T, StreamError> {
        self.check_available()?;
        let (result, required) = {
            let _visibility = self.inner.visibility.read().await;
            self.check_available()?;
            (read.await, self.inner.required()?)
        };
        self.await_durable(required).await?;
        result
    }

    async fn read_visible(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.visible(self.inner.provider.read(request)).await
    }
}
