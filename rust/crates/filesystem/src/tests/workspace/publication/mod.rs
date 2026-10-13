use super::*;

mod collection;
mod local_recovery;
mod provider_cuts;
mod reopened_join;

#[derive(Clone, Copy, Debug)]
enum ForkCut {
    Before,
    AfterCommit,
}

struct CutStream {
    inner: Arc<acyclic_stream::MemoryStream>,
    calls: std::sync::atomic::AtomicUsize,
    fail_at: std::sync::atomic::AtomicUsize,
    cut: std::sync::Mutex<ForkCut>,
}

impl CutStream {
    fn new(inner: Arc<acyclic_stream::MemoryStream>) -> Self {
        Self {
            inner,
            calls: std::sync::atomic::AtomicUsize::new(0),
            fail_at: std::sync::atomic::AtomicUsize::new(usize::MAX),
            cut: std::sync::Mutex::new(ForkCut::Before),
        }
    }

    fn arm(&self, fail_at: usize, cut: ForkCut) {
        use std::sync::atomic::Ordering;
        self.calls.store(0, Ordering::SeqCst);
        if let Ok(mut current) = self.cut.lock() {
            *current = cut;
        }
        self.fail_at.store(fail_at, Ordering::SeqCst);
    }

    fn disarm(&self) -> bool {
        use std::sync::atomic::Ordering;
        let fired = self.calls.load(Ordering::SeqCst) >= self.fail_at.load(Ordering::SeqCst);
        self.fail_at.store(usize::MAX, Ordering::SeqCst);
        fired
    }

    fn fails_now(&self) -> bool {
        use std::sync::atomic::Ordering;
        self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.fail_at.load(Ordering::SeqCst)
    }

    fn cut(&self) -> ForkCut {
        self.cut.lock().map_or(ForkCut::Before, |cut| *cut)
    }
}

#[async_trait::async_trait]
impl acyclic_stream::StreamProvider for CutStream {
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<Option<acyclic_stream::IdempotencyObservation>, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.inspect_idempotency(key).await
    }

    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<u64, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.tail(path).await
    }

    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.bounds(path).await
    }

    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.append(request).await
    }

    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.fork(request).await
    }

    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.read(request).await
    }

    async fn follow(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
    ) -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.follow(path, from).await
    }

    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.children(request).await
    }

    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
        let fails = self.fails_now();
        if fails && matches!(self.cut(), ForkCut::Before) {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        let outcome = self.inner.commit(request).await?;
        if fails {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        Ok(outcome)
    }

    async fn read_commit(
        &self,
        commit_id: acyclic_stream::CommitId,
    ) -> Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        if self.fails_now() {
            return Err(acyclic_stream::StreamError::Unavailable);
        }
        self.inner.read_commit(commit_id).await
    }
}

type CutFs = Fs<
    crate::distributed::StreamAuthorityStore<CutStream>,
    crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
>;

async fn first_fork_record(
    stream: &Arc<acyclic_stream::MemoryStream>,
    authority: crate::AuthorityId,
) -> Result<Option<crate::foundation::DurableCommit>, Box<dyn Error>> {
    use crate::AsyncAuthorityStore as _;
    let store = crate::distributed::StreamAuthorityStore::new(Arc::clone(stream));
    match store
        .replay(
            authority,
            crate::foundation::Sequence::GENESIS,
            crate::storage::ReplayLimit {
                records: 1,
                payload_bytes: 4 * 1024,
            },
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
    {
        Ok(records) => Ok(records.value.into_iter().next()),
        Err(failure) if matches!(failure.error, crate::storage::AuthorityStoreError::Missing) => {
            Ok(None)
        }
        Err(failure) => Err(failure.error.into()),
    }
}

async fn assert_fork_state_exact(
    stream: &Arc<acyclic_stream::MemoryStream>,
    source: &crate::Generation<
        crate::distributed::StreamAuthorityStore<CutStream>,
        crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
    >,
    source_volume: crate::foundation::VolumeId,
    destination: WorkspaceId,
) -> Result<bool, Box<dyn Error>> {
    let retention = crate::kernel::retention_authority_id(
        source_volume,
        crate::kernel::RetentionKind::ForkBase,
        &hex::encode(destination.into_bytes()),
    );
    let retained = first_fork_record(stream, retention).await?;
    if let Some(retained) = &retained {
        let record = crate::kernel::decode_retention_created(&retained.payload, 4 * 1024)?;
        assert_eq!(record.generation_root.digest, source.id().digest());
    }
    let created = first_fork_record(
        stream,
        crate::kernel::volume_authority_id(destination.volume_id()),
    )
    .await?;
    if let Some(created) = &created {
        let record = crate::kernel::decode_volume_created(&created.payload, 4 * 1024, &mut |_| {
            Ok::<_, crate::kernel::VolumeCreatedError>(())
        })?;
        assert_eq!(record.volume_id, destination.volume_id());
        assert!(
            retained.is_some(),
            "a workspace became durable before its source generation was retained"
        );
    }
    Ok(created.is_some())
}

async fn assert_fork_inheritance(
    source: &Generation<
        crate::distributed::StreamAuthorityStore<CutStream>,
        crate::MemoryObjectBackend,
    >,
    child: &Workspace<
        crate::distributed::StreamAuthorityStore<CutStream>,
        crate::MemoryObjectBackend,
    >,
    inherits_base: bool,
) -> Result<(), Box<dyn Error>> {
    let generation = unbounded!(child, head())?;
    assert_eq!(unbounded!(generation, parents())?, vec![source.id()]);
    let grandchild = fork_workspace(
        child,
        "grandchild",
        ForkOptions::from_generation(generation.clone(), IdempotencyKey::from_bytes([0x71; 16])),
    )
    .await?;
    let descendant = unbounded!(grandchild, head())?;
    assert_eq!(unbounded!(descendant, parents())?, vec![generation.id()]);
    let content = unbounded!(grandchild, read("/base.txt", 16));
    if inherits_base {
        assert_eq!(content?, OwnedBytes::from_static(b"base"));
    } else {
        assert!(content.is_err());
    }
    Ok(())
}

async fn fork_through_every_cut(
    advance_source: bool,
    paths: Option<Vec<String>>,
) -> Result<(), Box<dyn Error>> {
    let inherits_base = paths
        .as_ref()
        .is_none_or(|paths| paths.iter().any(|path| path == "/base.txt"));
    for cut in [ForkCut::Before, ForkCut::AfterCommit] {
        for fail_at in 1.. {
            let stream = Arc::new(acyclic_stream::MemoryStream::default());
            let cutting = Arc::new(CutStream::new(Arc::clone(&stream)));
            let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
            let fs: CutFs = Fs::new(
                crate::distributed::StreamAuthorityStore::new(Arc::clone(&cutting)),
                crate::LogicalObjectStore::new(Arc::new(objects), bucket),
                crate::EmbeddedCapabilities::MEMORY,
            );
            let main = unbounded!(fs, create_workspace("repo"))?;
            main.write_text("/base.txt", "base").await?;
            main.write_text("/excluded.txt", "excluded").await?;
            let source = unbounded!(main, head())?;
            if advance_source {
                main.write_text("/later.txt", "later").await?;
            }
            let key = IdempotencyKey::from_bytes([0x51; 16]);
            let options = || {
                let options = ForkOptions::from_generation(source.clone(), key);
                match &paths {
                    Some(paths) => options.inherit_paths(paths.clone()),
                    None => options,
                }
            };
            cutting.arm(fail_at, cut);
            let attempt = fork_workspace(&main, "agent", options()).await;
            let fired = cutting.disarm();
            let destination =
                value!(fs.workspace_id("agent", WorkBudget::UNBOUNDED, &CancellationToken::new()))?;
            let complete =
                assert_fork_state_exact(&stream, &source, main.id().volume_id(), destination)
                    .await?;
            if !fired {
                let fork = attempt?;
                assert!(complete);
                assert_fork_inheritance(&source, &fork, inherits_base).await?;
                assert!(unbounded!(fork, operation_generation(key))?.is_some());
                break;
            }
            assert!(
                attempt.is_err(),
                "a fault at call {fail_at} was not reported"
            );
            let retried = fork_workspace(&main, "agent", options()).await?;
            assert_eq!(retried.id(), destination);
            if inherits_base {
                assert_eq!(
                    unbounded!(retried, read("/base.txt", 16))?,
                    OwnedBytes::from_static(b"base")
                );
            } else {
                assert!(unbounded!(retried, read("/base.txt", 16)).is_err());
            }
            assert!(unbounded!(
                unbounded!(retried, head())?,
                matches_fork_selection(&source, paths.clone())
            )?);
            if paths.is_some() {
                assert!(unbounded!(retried, read("/excluded.txt", 16)).is_err());
            }
            assert!(
                assert_fork_state_exact(&stream, &source, main.id().volume_id(), destination)
                    .await?
            );
            let again = fork_workspace(&main, "agent", options()).await?;
            assert_eq!(
                unbounded!(again, head())?.id(),
                unbounded!(retried, head())?.id()
            );
            assert_fork_inheritance(&source, &retried, inherits_base).await?;
            assert!(unbounded!(retried, operation_generation(key))?.is_some());
        }
    }
    Ok(())
}

#[cfg(all(feature = "local", any(unix, windows)))]
async fn close_local(
    fs: crate::Fs<crate::LocalAuthorityBackend, crate::LocalObjectBackend>,
) -> Result<(), Box<dyn Error>> {
    let released = fs
        .local_root_release_barrier()
        .ok_or("local root has no release barrier")?;
    drop(fs);
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !released.is_released() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}
