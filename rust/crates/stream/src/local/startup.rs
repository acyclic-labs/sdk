use super::*;

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
        root: impl AsRef<Path>, limits: LocalStreamLimits,
        clock: Arc<dyn UnixMillisClock>, ownership_anchor: Option<OwnershipAnchor>,
    ) -> Result<Self, LocalStreamError> {
        let span = obs::OperationSpan::new(tracing::info_span!("acyclic.stream.open", outcome = Empty, error.kind = Empty));
        let root = root.as_ref().to_path_buf();
        let operation = span.clone();
        span.scope(async {
            let context = operation.clone();
            tokio::spawn(context.scope(async move {
                let result = Self::recover(root, limits, clock, ownership_anchor).await;
                match &result {
                    Ok(_) => { operation.record("outcome", "ok"); }
                    Err(error) => obs::failed(&operation, error.kind()),
                }
                result
            })).await.map_err(|_| { obs::failed(&span, "executor"); LocalStreamError::Executor })?
        }).await
    }

    async fn recover(root: PathBuf, limits: LocalStreamLimits, clock: Arc<dyn UnixMillisClock>,
        ownership_anchor: Option<OwnershipAnchor>) -> Result<Self, LocalStreamError> {
        validate_limits(limits)?;
        let provider = MemoryStream::new_with_clock(limits.memory, clock);
        let (recovered, mut receiver) = mpsc::channel(REPLAY_PIPELINE_COMMANDS);
        #[cfg(test)]
        let submitted_root = root.clone();
        let span = obs::OperationSpan::new(tracing::debug_span!("acyclic.stream.recover", frames = Empty, bytes = Empty));
        let open = tokio::task::spawn_blocking(move || span.in_scope(|| Journal::open(&root, limits, &recovered, &span)));
        #[cfg(test)]
        if let Ok(mut hook) = JOURNAL_OPEN_SUBMITTED.lock()
            && hook.as_ref().is_some_and(|(expected, _)| *expected == submitted_root)
            && let Some((_, submitted)) = hook.take() { let _ = submitted.send(()); }
        let mut replay_error = None;
        while let Some(recovered) = receiver.recv().await {
            let result = match recovered {
                Recovered::Validate(complete) => { let _ = complete.send(replay_error.is_none()); continue; }
                _ if replay_error.is_some() => continue,
                Recovered::State { encoded } => provider.install_state(&encoded).await.map_err(|_| LocalStreamError::Corrupt),
                Recovered::Command { committed_at_millis, command } => replay(&provider, command, committed_at_millis).await.map_err(LocalStreamError::Replay),
            };
            if let Err(error) = result { replay_error = Some(error); }
        }
        let (journal, sync) = open.await.map_err(|_| LocalStreamError::Executor)??;
        if let Some(error) = replay_error { return Err(error); }
        Ok(Self { inner: Arc::new(LocalInner {
            provider, journal: Mutex::new(journal), sync, _ownership_anchor: ownership_anchor,
            visibility: Arc::new(RwLock::new(())), synced: watch::channel(0).0,
            changed: watch::channel(0).0, poisoned: AtomicBool::new(false),
        }) })
    }
}
