use super::*;
use crate::model::{Lifecycle, VolumeConfig};
use crate::{CancellationToken, Fs, WorkBudget};
use crate::{FsError, OperationFailure, OperationReceipt, PublicationPermit, WorkCounters};
use acyclic_stream::OwnedBytes;
const B: WorkBudget = WorkBudget::UNBOUNDED;
fn allow<E>(
    _: WorkBudget,
) -> std::future::Ready<Result<OperationReceipt<()>, OperationFailure<E>>> {
    std::future::ready(Ok(OperationReceipt {
        value: (),
        work: WorkCounters::UNCHARGED,
    }))
}

use std::error::Error;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
use std::path::Path;
use std::sync::Arc;

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(48))]
    #[test]
    fn selected_fork_hardlink_aliases_keep_exact_content(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 1..64), carry_alias in proptest::prelude::any::<bool>()) {
        let result: Result<(), Box<dyn Error>> = futures::executor::block_on(async {
            let fs = Fs::memory();
            let parent = fs.create_workspace("hardlink-parent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            parent.write("/file", bytes::Bytes::copy_from_slice(&bytes)).await?;
            let mut transaction = parent.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            transaction.hard_link("/file", "/alias", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            let mut paths = vec!["/file".to_owned()];
            if carry_alias { paths.push("/alias".to_owned()); }
            let child = parent.fork("hardlink-child", ForkOptions::from_generation(parent.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?, IdempotencyKey::new()).inherit_paths(paths), crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| std::future::ready(Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }))).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
            assert_eq!(child.read("/file", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), bytes.as_slice());
            if carry_alias { assert_eq!(child.read("/alias", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), bytes.as_slice()); }
            else { assert!(child.read("/alias", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).is_err()); }
            child.write("/file", bytes::Bytes::from_static(b"changed")).await?;
            if carry_alias { assert_eq!(child.read("/alias", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), b"changed"); }
            assert_eq!(parent.read("/alias", 64, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.as_ref(), bytes.as_slice());
            Ok(())
        });
        proptest::prop_assert!(result.is_ok(), "{result:?}");
    }
}

struct SelectTheirsDriver;

impl crate::MergeDriver for SelectTheirsDriver {
    fn fingerprint(&self) -> &[u8] {
        b"workspace-select-theirs-v1"
    }

    fn mode(&self) -> crate::MergeDriverMode {
        crate::MergeDriverMode::Deterministic
    }

    fn resolve(
        &self,
        conflict: &crate::ConflictView,
    ) -> Result<crate::MergeResolution, crate::DriverError> {
        assert_eq!(conflict.kind, crate::ConflictKind::Text);
        assert_eq!(conflict.base, crate::ConflictValue::Text("base".into()));
        assert_eq!(conflict.ours, crate::ConflictValue::Text("main".into()));
        assert_eq!(conflict.theirs, crate::ConflictValue::Text("agent".into()));
        Ok(crate::MergeResolution::Select(crate::ConflictSide::Theirs))
    }
}

/// Where a fork's provider fault lands: before a call reaches the provider,
/// or after a commit applied but before its caller learned so.
#[derive(Clone, Copy, Debug)]
enum ForkCut {
    Before,
    AfterCommit,
}

/// A Stream provider that fails its `fail_at`-th call, and counts commits.
struct CutStream {
    inner: Arc<acyclic_stream::MemoryStream>,
    calls: std::sync::atomic::AtomicUsize,
    commits: std::sync::atomic::AtomicUsize,
    fail_at: std::sync::atomic::AtomicUsize,
    cut: std::sync::Mutex<ForkCut>,
}

impl CutStream {
    fn new(inner: Arc<acyclic_stream::MemoryStream>) -> Self {
        Self {
            inner,
            calls: std::sync::atomic::AtomicUsize::new(0),
            commits: std::sync::atomic::AtomicUsize::new(0),
            fail_at: std::sync::atomic::AtomicUsize::new(usize::MAX),
            cut: std::sync::Mutex::new(ForkCut::Before),
        }
    }

    fn arm(&self, fail_at: usize, cut: ForkCut) {
        use std::sync::atomic::Ordering;
        self.calls.store(0, Ordering::SeqCst);
        self.commits.store(0, Ordering::SeqCst);
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

    fn commits(&self) -> usize {
        self.commits.load(std::sync::atomic::Ordering::SeqCst)
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
        if matches!(outcome, acyclic_stream::CommitOutcome::Committed(_)) {
            self.commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
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

/// The first record of one authority, if it has one.
async fn first_fork_record(
    stream: &Arc<acyclic_stream::MemoryStream>,
    authority: crate::AuthorityId,
) -> Result<Option<crate::foundation::DurableCommit>, Box<dyn Error>> {
    use crate::AsyncAuthorityStore as _;
    let store = crate::distributed::StreamAuthorityStore::new(Arc::clone(stream));
    match ({
        let authority_cancellation = &CancellationToken::new();
        let mut __authority_work =
            crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation);
        let __authority_result = store
            .replay(
                authority,
                crate::foundation::Sequence::GENESIS,
                crate::storage::ReplayLimit {
                    records: 1,
                    payload_bytes: 4 * 1024,
                },
                &mut __authority_work,
            )
            .await
            .map(|(value, _allocation)| value);
        __authority_work.receipt(__authority_result)
    }) {
        Ok(records) => Ok(records.value.into_iter().next()),
        Err(failure) if matches!(failure.error, crate::storage::AuthorityStoreError::Missing) => {
            Ok(None)
        }
        Err(failure) => Err(failure.error.into()),
    }
}

/// Checks a fork's durable authority state after any interruption: the
/// workspace exists only with its source retained, and a record that
/// exists is exactly the fork's.
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

/// Whether the destination's creation record is found by the operation
/// that made it, as operation observation looks it up.
async fn creation_operation(
    stream: &Arc<acyclic_stream::MemoryStream>,
    destination: WorkspaceId,
    key: IdempotencyKey,
) -> Result<bool, Box<dyn Error>> {
    use crate::AsyncAuthorityStore as _;
    let found = ({
        let authority_cancellation = &CancellationToken::new();

        crate::distributed::StreamAuthorityStore::new(Arc::clone(stream))
            .find_operation(
                crate::kernel::volume_authority_id(destination.volume_id()),
                key.operation_id(),
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, authority_cancellation),
            )
            .await
            .map(|(value, _allocation)| value)
    })?;
    Ok(found.is_some_and(|commit| commit.sequence == crate::foundation::Sequence::new(1)))
}

/// Checks that a fork's authority lineage is exactly the one separate fork
/// and append commits would leave: the source's lineage through the source
/// generation, for a fork of the published head, then the fork's first
/// generation, located at its position. A fork of the fork's head then
/// shares that lineage in turn.
async fn assert_fork_lineage_exact(
    stream: &Arc<acyclic_stream::MemoryStream>,
    main: &crate::Workspace<
        crate::distributed::StreamAuthorityStore<CutStream>,
        crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
    >,
    source: &crate::Generation<
        crate::distributed::StreamAuthorityStore<CutStream>,
        crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
    >,
    fork: &crate::Workspace<
        crate::distributed::StreamAuthorityStore<CutStream>,
        crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
    >,
    independent: bool,
    inherits_base: bool,
) -> Result<(), Box<dyn Error>> {
    let store = crate::distributed::StreamAuthorityStore::new(Arc::clone(stream));
    let authority =
        |workspace: WorkspaceId| crate::kernel::volume_authority_id(workspace.volume_id());
    let head = fork
        .head(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value
        .id();
    let mut expected = if independent {
        Vec::new()
    } else {
        let source_lineage = store.generation_lineage(authority(main.id())).await?;
        let at = store
            .generation_locator(authority(main.id()), source.id())
            .await?;
        assert_eq!(
            store
                .generation_locator(authority(fork.id()), source.id())
                .await?,
            at
        );
        source_lineage[..usize::try_from(at)?].to_vec()
    };
    expected.push(head);
    assert_eq!(
        store.generation_lineage(authority(fork.id())).await?,
        expected
    );
    assert_eq!(
        store.generation_locator(authority(fork.id()), head).await?,
        u64::try_from(expected.len())?
    );
    // A fork of the fork's published head reuses that lineage.
    let grandchild = fork
        .fork(
            "grandchild",
            ForkOptions::from_generation(
                fork.head(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await?
                .value,
                IdempotencyKey::from_bytes([0x71; 16]),
            ),
            crate::PublicationPermit::Unrestricted,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
            |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: crate::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    if inherits_base {
        assert_eq!(
            grandchild
                .read(
                    "/base.txt",
                    16,
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new()
                )
                .await?
                .value,
            OwnedBytes::from_static(b"base")
        );
    } else {
        assert!(
            grandchild
                .read(
                    "/base.txt",
                    16,
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new()
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
                .is_err()
        );
    }
    let grandchild_lineage = store.generation_lineage(authority(grandchild.id())).await?;
    assert_eq!(
        grandchild_lineage.get(..expected.len()),
        Some(&expected[..])
    );
    Ok(())
}

/// Forks one generation through every provider cut, before calls and after
/// commits, checking the durable state after each interruption and that a
/// retry with the same key completes the same workspace. Returns the commits
/// an uninterrupted fork made.
async fn fork_through_every_cut(
    advance_source: bool,
    paths: Option<Vec<String>>,
) -> Result<usize, Box<dyn Error>> {
    let inherits_base = paths
        .as_ref()
        .is_none_or(|paths| paths.iter().any(|path| path == "/base.txt"));
    let mut uninterrupted_commits = None;
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
            let main = fs
                .create_workspace(
                    "repo",
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await?
                .value;
            main.write_text("/base.txt", "base").await?;
            main.write_text("/excluded.txt", "excluded").await?;
            let source = main
                .head(
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await?
                .value;
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
            let attempt = main
                .fork(
                    "agent",
                    options(),
                    crate::PublicationPermit::Unrestricted,
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                    |_| {
                        std::future::ready(Ok(crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::UNCHARGED,
                        }))
                    },
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error);
            let fired = cutting.disarm();
            let committed = cutting.commits();
            let destination = fs.workspace_id("agent")?;
            let complete =
                assert_fork_state_exact(&stream, &source, main.id().volume_id(), destination)
                    .await?;
            if !fired {
                let fork = attempt?;
                assert!(complete);
                assert_fork_lineage_exact(
                    &stream,
                    &main,
                    &source,
                    &fork,
                    advance_source,
                    inherits_base,
                )
                .await?;
                assert!(creation_operation(&stream, destination, key).await?);
                uninterrupted_commits.get_or_insert(committed);
                break;
            }
            assert!(
                attempt.is_err(),
                "a fault at call {fail_at} was not reported"
            );
            let retried = main
                .fork(
                    "agent",
                    options(),
                    crate::PublicationPermit::Unrestricted,
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                    |_| {
                        std::future::ready(Ok(crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::UNCHARGED,
                        }))
                    },
                )
                .await?
                .value;
            assert_eq!(retried.id(), destination);
            if inherits_base {
                assert_eq!(
                    retried
                        .read(
                            "/base.txt",
                            16,
                            crate::WorkBudget::UNBOUNDED,
                            &crate::CancellationToken::new()
                        )
                        .await?
                        .value,
                    OwnedBytes::from_static(b"base")
                );
            } else {
                assert!(
                    retried
                        .read(
                            "/base.txt",
                            16,
                            crate::WorkBudget::UNBOUNDED,
                            &crate::CancellationToken::new()
                        )
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| failure.error)
                        .is_err()
                );
            }
            assert!(
                retried
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new()
                    )
                    .await?
                    .value
                    .matches_fork_selection(
                        &source,
                        paths.clone(),
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new()
                    )
                    .await?
                    .value
            );
            if paths.is_some() {
                assert!(
                    retried
                        .read(
                            "/excluded.txt",
                            16,
                            crate::WorkBudget::UNBOUNDED,
                            &crate::CancellationToken::new()
                        )
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| failure.error)
                        .is_err()
                );
            }
            assert!(
                assert_fork_state_exact(&stream, &source, main.id().volume_id(), destination)
                    .await?
            );
            let again = main
                .fork(
                    "agent",
                    options(),
                    crate::PublicationPermit::Unrestricted,
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                    |_| {
                        std::future::ready(Ok(crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::UNCHARGED,
                        }))
                    },
                )
                .await?
                .value;
            assert_eq!(
                again
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new()
                    )
                    .await?
                    .value
                    .id(),
                retried
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new()
                    )
                    .await?
                    .value
                    .id()
            );
            assert_fork_lineage_exact(
                &stream,
                &main,
                &source,
                &retried,
                advance_source,
                inherits_base,
            )
            .await?;
            assert!(
                creation_operation(&stream, destination, key).await?,
                "the fork's creation is not found by its operation"
            );
        }
    }
    uninterrupted_commits.ok_or_else(|| "the fork never completed".into())
}

/// Truncates a local root's Stream journal to `length` bytes, as a power
/// loss before the rest was synchronized would leave it.
#[cfg(all(feature = "local", any(unix, windows)))]
fn cut_journal(root: &std::path::Path, journal: &[u8], length: usize) -> std::io::Result<()> {
    std::fs::write(
        root.join("stream").join("stream.journal"),
        journal.get(..length).unwrap_or(journal),
    )
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

fn selected_transaction_budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 1_000_000,
        authority_records_appended: 1_000_000,
        authority_bytes_read: 1_000_000,
        authority_bytes_written: 1_000_000,
        object_probes: 1_000_000,
        backend_read_operations: 1_000_000,
        backend_write_operations: 1_000_000,
        durability_operations: 1_000_000,
        page_reads: 1_000_000,
        page_writes: 1_000_000,
        object_bytes_read: 1_000_000,
        object_bytes_written: 1_000_000,
        bytes_hashed: 1_000_000,
        bytes_copied: 1_000_000,
        bytes_encoded: 1_000_000,
        source_bytes_read: 1_000_000,
        source_path_components: 1_000_000,
        source_entries_visited: 1_000_000,
        output_bytes: 1_000_000,
        items_examined: 1_000_000,
        items_returned: 1_000_000,
        allocation_operations: 1_000_000,
        peak_allocation_bytes: 64 * 1024 * 1024,
        materializations: 1_000_000,
    }
}

#[derive(Debug, thiserror::Error)]
enum OrdinaryOwnerError {
    #[error(transparent)]
    Sdk(WorkspaceError),
    #[error("ordinary owner denied publication")]
    Denied {
        task: [u8; 16],
        revision: u64,
        expired: bool,
    },
}

impl From<WorkspaceError> for OrdinaryOwnerError {
    fn from(error: WorkspaceError) -> Self {
        Self::Sdk(error)
    }
}

impl From<FsError> for OrdinaryOwnerError {
    fn from(error: FsError) -> Self {
        Self::Sdk(error.into())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, thiserror::Error)]
enum NativeCreationGuardTestError {
    #[error("the owner denied this creation phase: {0}")]
    Denied(u32),
    #[error(transparent)]
    Filesystem(#[from] crate::FsError),
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
}

#[path = "workspace/collection.rs"]
mod collection;
#[path = "workspace/discovery.rs"]
mod discovery;
#[path = "workspace/faults.rs"]
mod faults;
#[path = "workspace/joins/mod.rs"]
mod joins;
#[path = "workspace/materialization.rs"]
mod materialization;
#[path = "workspace/restore/mod.rs"]
mod restore;
#[path = "workspace/retention.rs"]
mod retention;
#[path = "workspace/retry.rs"]
mod retry;
#[path = "workspace/selection.rs"]
mod selection;
#[path = "workspace/siblings.rs"]
mod siblings;
#[path = "workspace/transactions.rs"]
mod transactions;
