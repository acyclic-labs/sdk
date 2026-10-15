use super::*;
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitId, CommitOutcome,
    CommitRequest, CommittedEnvelope, ForkReceipt, ForkRequest, IdempotencyKey,
    IdempotencyObservation, RecordStream, StreamBounds, StreamError,
};

type StreamResult<T> = std::result::Result<T, StreamError>;

struct Restartable {
    provider: std::sync::RwLock<Arc<MemoryStream>>,
    after_read: std::sync::Mutex<Option<(u64, Arc<MemoryStream>)>>,
    before_commit: std::sync::Mutex<Option<Arc<MemoryStream>>>,
}

impl Restartable {
    fn new(provider: Arc<MemoryStream>) -> Self {
        Self {
            provider: std::sync::RwLock::new(provider),
            after_read: Default::default(),
            before_commit: Default::default(),
        }
    }

    fn install(&self, provider: Arc<MemoryStream>) -> StreamResult<()> {
        *self
            .provider
            .write()
            .map_err(|_| StreamError::Unavailable)? = provider;
        Ok(())
    }

    fn current(&self) -> StreamResult<Arc<MemoryStream>> {
        self.provider
            .read()
            .map(|provider| provider.clone())
            .map_err(|_| StreamError::Unavailable)
    }
}

#[async_trait::async_trait]
impl StreamProvider for Restartable {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> StreamResult<Option<IdempotencyObservation>> {
        self.current()?.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> StreamResult<u64> {
        self.current()?.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> StreamResult<StreamBounds> {
        self.current()?.bounds(path).await
    }
    async fn append(&self, request: AppendRequest) -> StreamResult<AppendOutcome> {
        self.current()?.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> StreamResult<ForkReceipt> {
        self.current()?.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> StreamResult<RecordStream> {
        let from = request.from;
        let records = self.current()?.read(request).await?;
        let mut swap = self
            .after_read
            .lock()
            .map_err(|_| StreamError::Unavailable)?;
        if swap.as_ref().is_some_and(|(at, _)| *at == from) {
            let (_, provider) = swap.take().ok_or(StreamError::Unavailable)?;
            self.install(provider)?;
        }
        Ok(records)
    }
    async fn follow(&self, path: StreamPath, from: u64) -> StreamResult<RecordStream> {
        self.current()?.follow(path, from).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
    ) -> StreamResult<RecordStream> {
        self.current()?.follow_checked(path, from, preceding).await
    }
    async fn children(&self, request: ChildrenRequest) -> StreamResult<ChildStream> {
        self.current()?.children(request).await
    }
    async fn read_commit(&self, id: CommitId) -> StreamResult<CommittedEnvelope> {
        self.current()?.read_commit(id).await
    }
    async fn commit(&self, request: CommitRequest) -> StreamResult<CommitOutcome> {
        let swap = self
            .before_commit
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .take();
        if let Some(provider) = swap {
            self.install(provider)?;
        }
        self.current()?.commit(request).await
    }
}

#[tokio::test]
async fn existing_empty_forks_initialize_and_racing_openers_share_the_header() -> Result {
    let (provider, _) = memory().await?;
    let path = StreamPath::new("empty")?;
    provider
        .fork(ForkRequest {
            source: StreamPath::new("task-volume")?,
            destination: path.clone(),
            at_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    assert_eq!(provider.tail(path.clone()).await?, 0);
    let (first, second) = tokio::join!(
        TaskVolume::open(provider.clone(), path.clone()),
        TaskVolume::open(provider.clone(), path.clone()),
    );
    let first = first?.snapshot(&Query::Root, 0).await?;
    let second = second?.snapshot(&Query::Root, 0).await?;
    assert_eq!(first.cursor, second.cursor);
    assert_eq!(provider.tail(path).await?, 1);
    Ok(())
}

#[tokio::test]
async fn reused_accepted_positions_reset_cached_heads_and_resume_cursors() -> Result {
    let (original, original_volume) = memory().await?;
    let path = StreamPath::new("task-volume")?;
    let mut records = original
        .read(ReadRequest {
            path: path.clone(),
            from: 0,
            limit: 1,
        })
        .await?;
    let header = records.next().await.ok_or("missing header")??.value;
    let provider = Arc::new(Restartable::new(original));
    let cached = TaskVolume::open(provider.clone(), path.clone()).await?;
    let initial = original_volume.snapshot(&Query::Root, 0).await?;
    original_volume
        .append(initial.generation, &write("GOAL.md", "lost")?)
        .await?;
    let stale = cached.snapshot(&Query::Root, 1).await?;
    let cached_resume = TaskVolume::open(provider.clone(), path.clone()).await?;

    let replacement = Arc::new(MemoryStream::default());
    replacement
        .append(AppendRequest {
            path: path.clone(),
            records: vec![header],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let recovered = TaskVolume::open(replacement.clone(), path).await?;
    let initial = recovered.snapshot(&Query::Root, 0).await?;
    let replacement_file = write("GOAL.md", "replacement")?;
    recovered
        .append(initial.generation, &replacement_file)
        .await?;
    let current = recovered.snapshot(&Query::Root, 1).await?;
    assert_eq!(stale.cursor.position, current.cursor.position);
    assert_ne!(stale.cursor.commit, current.cursor.commit);
    provider.install(replacement)?;

    assert!(matches!(
        cached.snapshot(&Query::Root, 1).await,
        Err(Error::ResetRequired)
    ));
    assert!(matches!(
        cached.heads(&Query::Root, None, 1).await,
        Err(Error::ResetRequired)
    ));
    assert!(matches!(
        cached
            .append(stale.generation, &write("GOAL.md", "stale")?)
            .await,
        Err(Error::ResetRequired)
    ));
    let mut follows = Vec::new();
    for (volume, cursor) in [(&cached, None), (&cached_resume, Some(stale.cursor))] {
        let observation = volume.observe(Query::Root, cursor, 1).await?;
        assert!(observation.reset);
        let snapshot = observation.snapshot.ok_or("missing replacement snapshot")?;
        assert_eq!(snapshot.files, vec![replacement_file.clone()]);
        assert_eq!(snapshot.cursor, current.cursor);
        let repaired = volume.snapshot(&Query::Root, 1).await?;
        assert_eq!(repaired.files, current.files);
        assert_eq!(repaired.cursor, current.cursor);
        assert_eq!(
            volume.heads(&Query::Root, None, 1).await?.generation,
            current.generation
        );
        follows.push(observation.changes);
    }
    let next = write("GOAL.md", "next")?;
    recovered.append(current.generation, &next).await?;
    for mut follow in follows {
        assert_eq!(
            follow.next().await.ok_or("missing follow")??.file,
            Some(next.clone())
        );
    }
    Ok(())
}

#[tokio::test]
async fn reset_replaces_cached_volume_identity_after_reinitialization() -> Result {
    let (original, _) = memory().await?;
    let provider = Arc::new(Restartable::new(original));
    let path = StreamPath::new("task-volume")?;
    let cached = TaskVolume::open(provider.clone(), path.clone()).await?;
    let stale = cached.snapshot(&Query::Root, 0).await?;
    let replacement = Arc::new(MemoryStream::default());
    let recovered = TaskVolume::open(replacement.clone(), path).await?;
    let current = recovered.snapshot(&Query::Root, 0).await?;
    provider.install(replacement)?;
    let observation = cached.observe(Query::Root, Some(stale.cursor), 0).await?;
    assert!(observation.reset);
    assert_eq!(
        observation.snapshot.ok_or("missing reset")?.cursor,
        current.cursor
    );
    assert_eq!(
        cached.snapshot(&Query::Root, 0).await?.cursor,
        current.cursor
    );
    let file = write("GOAL.md", "new epoch")?;
    cached.append(current.generation, &file).await?;
    assert_eq!(cached.snapshot(&Query::Root, 1).await?.files, vec![file]);
    Ok(())
}

async fn replacement_history(
    original: &MemoryStream,
    writes: &[Write],
) -> Result<(Arc<MemoryStream>, TaskVolume<MemoryStream>)> {
    let path = StreamPath::new("task-volume")?;
    let mut records = original
        .read(ReadRequest {
            path: path.clone(),
            from: 0,
            limit: 1,
        })
        .await?;
    let header = records.next().await.ok_or("missing header")??.value;
    let provider = Arc::new(MemoryStream::default());
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![header],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await?;
    let volume = TaskVolume::open(provider.clone(), path).await?;
    let initial = volume.snapshot(&Query::Root, 0).await?;
    append_all(&volume, initial.generation, writes).await?;
    Ok((provider, volume))
}

#[tokio::test]
async fn recovery_between_anchor_and_suffix_cannot_install_a_mixed_projection() -> Result {
    for suffix in [1, acyclic_stream::MAX_ITEMS] {
        let (original, original_volume) = memory().await?;
        let initial = original_volume.snapshot(&Query::Root, 0).await?;
        let cut = original_volume
            .append(initial.generation, &write("GOAL.md", "lost")?)
            .await?;
        let provider = Arc::new(Restartable::new(original.clone()));
        let cached = TaskVolume::open(provider.clone(), StreamPath::new("task-volume")?).await?;
        let writes = (0..suffix)
            .map(|n| write(&format!(".tasks/t/notifications/old{n}.json"), "old"))
            .collect::<Result<Vec<_>>>()?;
        append_all(&original_volume, cut, &writes).await?;
        let original_files = original_volume
            .snapshot(&Query::Root, usize::MAX)
            .await?
            .files;
        let writes = (0..=suffix)
            .map(|n| write(&format!(".tasks/t/notifications/new{n}.json"), "new"))
            .collect::<Result<Vec<_>>>()?;
        let (replacement, replacement_volume) = replacement_history(&original, &writes).await?;
        let expected = replacement_volume
            .snapshot(&Query::Root, usize::MAX)
            .await?;
        *provider
            .after_read
            .lock()
            .map_err(|_| "swap lock poisoned")? = Some((1, replacement));

        match cached.snapshot(&Query::Root, usize::MAX).await {
            // The complete page can linearize before recovery, but must never
            // combine its old cached goal with the replacement's notifications.
            Ok(snapshot) => assert_eq!(snapshot.files, original_files),
            Err(Error::ResetRequired) => {}
            Err(error) => return Err(error.into()),
        }
        assert!(matches!(
            cached.snapshot(&Query::Root, usize::MAX).await,
            Err(Error::ResetRequired)
        ));
        let reset = cached.observe(Query::Root, None, usize::MAX).await?;
        assert!(reset.reset);
        assert_eq!(
            reset.snapshot.ok_or("missing reset snapshot")?.files,
            expected.files
        );
        assert_eq!(
            cached.snapshot(&Query::Root, usize::MAX).await?.files,
            expected.files
        );
    }
    Ok(())
}

#[tokio::test]
async fn recovery_between_validation_and_commit_rejects_an_obsolete_generation() -> Result {
    let (original, volume) = memory().await?;
    let initial = volume.snapshot(&Query::Root, 0).await?;
    volume
        .append(initial.generation, &write("GOAL.md", "lost")?)
        .await?;
    let provider = Arc::new(Restartable::new(original.clone()));
    let cached = TaskVolume::open(provider.clone(), StreamPath::new("task-volume")?).await?;
    let obsolete = cached.snapshot(&Query::Root, 1).await?;
    let (replacement, recovered) =
        replacement_history(&original, &[write("GOAL.md", "new history")?]).await?;
    let before = recovered.snapshot(&Query::Root, 1).await?;
    assert_eq!(obsolete.generation.position, before.generation.position);
    *provider
        .before_commit
        .lock()
        .map_err(|_| "swap lock poisoned")? = Some(replacement);

    assert!(matches!(
        cached
            .append(
                obsolete.generation,
                &write(".tasks/t/notifications/stale.json", "forged")?
            )
            .await,
        Err(Error::ResetRequired)
    ));
    let after = recovered.snapshot(&Query::Root, usize::MAX).await?;
    assert_eq!(after.files, before.files);
    assert_eq!(after.generation, before.generation);
    Ok(())
}

#[tokio::test]
async fn recovery_between_cursor_validation_and_follow_requires_reset() -> Result {
    let (original, volume) = memory().await?;
    let initial = volume.snapshot(&Query::Root, 0).await?;
    volume
        .append(initial.generation, &write("GOAL.md", "lost")?)
        .await?;
    let provider = Arc::new(Restartable::new(original.clone()));
    let cached = TaskVolume::open(provider.clone(), StreamPath::new("task-volume")?).await?;
    let obsolete = cached.snapshot(&Query::Root, 1).await?;
    let (replacement, _) =
        replacement_history(&original, &[write("GOAL.md", "replacement")?]).await?;
    *provider
        .after_read
        .lock()
        .map_err(|_| "swap lock poisoned")? = Some((1, replacement));
    assert!(matches!(
        cached.follow(Query::Root, obsolete.cursor).await,
        Err(Error::ResetRequired)
    ));
    Ok(())
}
