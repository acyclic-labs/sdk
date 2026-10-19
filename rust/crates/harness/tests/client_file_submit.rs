//! Request files survive reconnects and checkpoint failures in publication order.
#![cfg(not(target_arch = "wasm32"))]
use acyclic_harness::{
    client::{FileClient, FileClientDurability, FileClientError, FileClientLimits},
    task_volume::{FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::*;
use futures::StreamExt as _;
use std::{
    future::{Future as _, poll_fn},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
};

#[path = "client_file_admission.rs"]
mod admission;
#[path = "../../harness-fuzz/support/file_client.rs"]
mod parser;

struct Provider {
    inner: MemoryStream,
    offline: AtomicBool,
    commits: AtomicUsize,
    fail_commit: AtomicUsize,
    durable_through: AtomicUsize,
    lose_ack: AtomicBool,
    pause_ack: AtomicBool,
    defer_commit: AtomicBool,
    release_after_watermarks: AtomicUsize,
    // A sequential fault-injection slot; never held during provider I/O.
    deferred: std::sync::Mutex<Option<CommitRequest>>,
}
impl Default for Provider {
    fn default() -> Self {
        Self {
            inner: MemoryStream::default(),
            offline: AtomicBool::new(false),
            commits: AtomicUsize::new(0),
            fail_commit: AtomicUsize::new(0),
            durable_through: AtomicUsize::new(usize::MAX),
            lose_ack: AtomicBool::new(false),
            pause_ack: AtomicBool::new(false),
            defer_commit: AtomicBool::new(false),
            release_after_watermarks: AtomicUsize::new(0),
            deferred: std::sync::Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl StreamProvider for Provider {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.inner.operation_claims()
    }

    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.inner.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.inner.bounds(path).await
    }
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        if self
            .release_after_watermarks
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            == Ok(1)
        {
            let request = { self.deferred.lock().unwrap().take() };
            if let Some(request) = request {
                self.inner.commit(request).await?;
            }
        }
        let tail = self.inner.tail(path).await?;
        Ok(StreamWatermarks {
            accepted: tail,
            durable: tail.min(self.durable_through.load(Ordering::SeqCst) as u64),
        })
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.inner.follow(path, from).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
    ) -> Result<RecordStream, StreamError> {
        self.inner.follow_checked(path, from, preceding).await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.inner.children(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let number = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
        if self.offline.load(Ordering::SeqCst) || self.fail_commit.load(Ordering::SeqCst) == number
        {
            return Err(StreamError::Unavailable);
        }
        if self.defer_commit.swap(false, Ordering::SeqCst) {
            *self.deferred.lock().unwrap() = Some(request);
            return futures::future::pending().await;
        }
        let outcome = self.inner.commit(request).await?;
        if self.pause_ack.swap(false, Ordering::SeqCst) {
            futures::future::pending::<()>().await;
        }
        if self.lose_ack.swap(false, Ordering::SeqCst) {
            return Err(StreamError::Unavailable);
        }
        Ok(outcome)
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}

const LIMITS: FileClientLimits = FileClientLimits {
    commands: 64,
    bytes: 4096,
    durability: FileClientDurability::Durable,
};
fn path(value: &str) -> StreamPath {
    StreamPath::new(value).unwrap()
}
fn request(id: u8) -> Write {
    Write::new(
        FileKey::parse(&format!(".tasks/task/requests/op{id}.json")).unwrap(),
        vec![id],
    )
}
async fn volume(provider: Arc<Provider>, name: &str) -> Arc<TaskVolume<Provider>> {
    Arc::new(TaskVolume::open(provider, path(name)).await.unwrap())
}

#[tokio::test]
async fn failed_admission_blocks_later_requests_across_reopen() {
    let local = Arc::new(Provider::default());
    let remote = Arc::new(Provider::default());
    let outbox = volume(local, "client/files").await;
    let target = volume(remote.clone(), "agent/files").await;
    let cut = target.cut(&Query::Tasks).await.unwrap();
    let mut client = FileClient::open(outbox.clone(), target.clone(), LIMITS)
        .await
        .unwrap();
    client.enqueue(request(9)).await.unwrap();
    remote.offline.store(true, Ordering::SeqCst);
    assert!(client.flush().await.is_err());
    client.enqueue(request(1)).await.unwrap();
    assert!(client.flush().await.is_err());
    assert_eq!(client.pending(), 2);
    assert!(
        target
            .snapshot(&Query::Tasks, 2)
            .await
            .unwrap()
            .files
            .is_empty()
    );
    drop(client);
    let mut client = FileClient::open(outbox, target.clone(), LIMITS)
        .await
        .unwrap();
    assert_eq!(client.pending(), 2);
    remote.offline.store(false, Ordering::SeqCst);
    assert_eq!(client.flush().await.unwrap(), 2);
    let mut changes = target.follow(Query::Tasks, cut).await.unwrap();
    for id in [9, 1] {
        assert_eq!(
            changes
                .next()
                .await
                .unwrap()
                .unwrap()
                .file
                .unwrap()
                .bytes
                .as_ref(),
            &[id]
        );
    }
    assert_eq!(client.flush().await.unwrap(), 0);
}

fn single_request_limit(byte_quota: bool) -> FileClientLimits {
    FileClientLimits {
        commands: if byte_quota { 2 } else { 1 },
        bytes: if byte_quota { 1 } else { 2 },
        ..LIMITS
    }
}

async fn indeterminate_enqueue_preserves_capacity(
    id: u8,
    mode: u8,
    byte_quota: bool,
    omit_reconciliation: bool,
) -> bool {
    let local = Arc::new(Provider::default());
    let outbox = volume(local.clone(), "client/files").await;
    let target = volume(Arc::new(Provider::default()), "agent/files").await;
    let cut = target.cut(&Query::Tasks).await.unwrap();
    let limits = single_request_limit(byte_quota);
    let mut client = FileClient::open(outbox.clone(), target.clone(), limits)
        .await
        .unwrap();
    if mode > 0 {
        if mode == 1 {
            local.pause_ack.store(true, Ordering::SeqCst);
        } else {
            local.defer_commit.store(true, Ordering::SeqCst);
        }
        let publication = client.enqueue(request(id));
        futures::pin_mut!(publication);
        // A cooperative yield is not the fault boundary. Keep following the
        // future's wakeups until commit consumes the requested injection.
        poll_fn(|cx| {
            assert!(publication.as_mut().poll(cx).is_pending());
            if local.pause_ack.load(Ordering::SeqCst) || local.defer_commit.load(Ordering::SeqCst) {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        })
        .await;
    } else {
        local.lose_ack.store(true, Ordering::SeqCst);
        assert!(client.enqueue(request(id)).await.is_err());
    }
    if mode == 2 {
        // The abandoned request lands after B's reconciliation, during its
        // identity read-back. B must retain the reconciled fence and conflict.
        local.release_after_watermarks.store(2, Ordering::SeqCst);
    }
    let later = id.wrapping_add(1);
    let result = if omit_reconciliation {
        // Deliberately broken admission: retain the pre-failure empty cache,
        // skip reconciliation, and replace its fence with the latest head.
        // Real volume writes let the same checker detect quota over-admission.
        let existing = outbox
            .snapshot(&Query::File(request(later).key), 1)
            .await
            .unwrap();
        assert!(existing.files.is_empty());
        let head = outbox.head().await.unwrap();
        outbox
            .append(head, &request(later))
            .await
            .map(|_| ())
            .map_err(|error| {
                FileClientError::Harness(acyclic_harness::Error::Storage(error.to_string()))
            })
    } else {
        client.enqueue(request(later)).await
    };
    let files = outbox.snapshot(&Query::Tasks, 2).await.unwrap().files;
    if files.len() != 1 || files[0].bytes.as_ref() != &[id] || client.pending() != 1 {
        return false;
    }
    let Err(error) = result else { return false };
    if mode == 2 {
        assert!(matches!(error, FileClientError::Harness(_)));
    } else if byte_quota {
        assert!(matches!(error, FileClientError::ByteQuota { limit: 1 }));
    } else {
        assert!(matches!(error, FileClientError::CommandQuota { limit: 1 }));
    }
    drop(client);
    let mut client = FileClient::open(outbox.clone(), target.clone(), limits)
        .await
        .unwrap();
    assert_eq!(client.pending(), 1);
    assert_eq!(client.flush().await.unwrap(), 1);
    client.enqueue(request(later)).await.unwrap();

    // A is acknowledged and no longer pending; B fills the quota. A's immutable
    // identity must still succeed without reserving capacity, including reopen.
    for reopen in [false, true] {
        if reopen {
            drop(client);
            client = FileClient::open(outbox.clone(), target.clone(), limits)
                .await
                .unwrap();
        }
        let before = outbox.head().await.unwrap();
        let acknowledged = client.cursor().clone();
        client.enqueue(request(id)).await.unwrap();
        assert_eq!(client.pending(), 1);
        assert_eq!(client.cursor(), &acknowledged);
        assert_eq!(outbox.head().await.unwrap(), before);
        let conflict = Write::new(request(id).key, vec![later]);
        assert!(matches!(
            client.enqueue(conflict).await,
            Err(FileClientError::Harness(acyclic_harness::Error::Conflict(
                _
            )))
        ));
        assert_eq!(outbox.head().await.unwrap(), before);
    }
    assert_eq!(client.flush().await.unwrap(), 1);
    assert_eq!(target.head().await.unwrap().position, cut.position + 2);
    let mut changes = target.follow(Query::Tasks, cut).await.unwrap();
    for value in [id, later] {
        assert_eq!(
            changes
                .next()
                .await
                .unwrap()
                .unwrap()
                .file
                .unwrap()
                .bytes
                .as_ref(),
            &[value]
        );
    }
    drop(client);
    let mut client = FileClient::open(outbox, target, limits).await.unwrap();
    client.enqueue(request(id)).await.unwrap();
    assert_eq!(client.pending(), 0);
    assert_eq!(client.flush().await.unwrap(), 0);
    true
}

#[tokio::test]
async fn lost_ack_recovery_and_acknowledged_retries_respect_each_quota() {
    for mode in 0..3 {
        for byte_quota in [false, true] {
            assert!(indeterminate_enqueue_preserves_capacity(7, mode, byte_quota, false).await);
        }
    }
}

#[tokio::test]
async fn negative_control_omitting_reconciliation_and_fencing_is_detected() {
    for mode in 0..3 {
        for byte_quota in [false, true] {
            assert!(!indeterminate_enqueue_preserves_capacity(7, mode, byte_quota, true).await);
        }
    }
}

#[tokio::test]
async fn capacity_rejections_are_typed_and_distinct_from_invalid_requests() {
    for commands in [1, 2] {
        let outbox = volume(Arc::new(Provider::default()), "client/files").await;
        let target = volume(Arc::new(Provider::default()), "agent/files").await;
        let mut client = FileClient::open(
            outbox,
            target,
            FileClientLimits {
                commands,
                bytes: 1,
                ..LIMITS
            },
        )
        .await
        .unwrap();
        client.enqueue(request(1)).await.unwrap();
        match (commands, client.enqueue(request(2)).await.unwrap_err()) {
            (1, FileClientError::CommandQuota { limit: 1 }) => (),
            (2, FileClientError::ByteQuota { limit: 1 }) => (),
            (_, error) => panic!("unexpected quota classification: {error:?}"),
        }
        assert!(matches!(
            client.enqueue(Write::new(FileKey::Goal, vec![])).await,
            Err(FileClientError::Harness(acyclic_harness::Error::Invalid(_)))
        ));
    }
}

#[tokio::test]
async fn recovery_classifies_capacity_and_identity_separately() {
    for commands in [1, 2] {
        let outbox = volume(Arc::new(Provider::default()), "client/files").await;
        let target = volume(Arc::new(Provider::default()), "agent/files").await;
        let mut client = FileClient::open(outbox.clone(), target.clone(), LIMITS)
            .await
            .unwrap();
        client.enqueue(request(1)).await.unwrap();
        client.enqueue(request(2)).await.unwrap();
        drop(client);
        let limits = FileClientLimits {
            commands,
            bytes: if commands == 1 { 2 } else { 1 },
            ..LIMITS
        };
        let error = FileClient::open(outbox, target, limits)
            .await
            .err()
            .unwrap();
        match (commands, error) {
            (1, FileClientError::CommandQuota { limit: 1 }) => (),
            (2, FileClientError::ByteQuota { limit: 1 }) => (),
            (_, error) => panic!("unexpected recovery classification: {error:?}"),
        }
    }
    let provider = Arc::new(Provider::default());
    let outbox = volume(provider.clone(), "client/files").await;
    let target = volume(Arc::new(Provider::default()), "agent/files").await;
    let limits = FileClientLimits {
        commands: 1,
        bytes: 1,
        ..LIMITS
    };
    let mut client = FileClient::open(outbox.clone(), target.clone(), limits)
        .await
        .unwrap();
    client.enqueue(request(1)).await.unwrap();
    let mut records = provider
        .read(ReadRequest {
            path: path("client/files"),
            from: 2,
            limit: 1,
        })
        .await
        .unwrap();
    let repeated = records.next().await.unwrap().unwrap().value;
    provider
        .append(AppendRequest {
            path: path("client/files"),
            records: vec![repeated],
            if_tail: None,
            idempotency_key: None,
        })
        .await
        .unwrap();
    drop(client);
    assert!(matches!(
        FileClient::open(outbox, target, limits).await,
        Err(FileClientError::Harness(acyclic_harness::Error::Conflict(
            _
        )))
    ));
}

#[tokio::test]
async fn checkpoint_failure_retains_an_already_published_request() {
    let local = Arc::new(Provider::default());
    let outbox = volume(local.clone(), "client/files").await;
    let target = volume(Arc::new(Provider::default()), "agent/files").await;
    let mut client = FileClient::open(outbox.clone(), target.clone(), LIMITS)
        .await
        .unwrap();
    client.enqueue(request(1)).await.unwrap();
    local
        .fail_commit
        .store(local.commits.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    assert!(client.flush().await.is_err());
    assert_eq!(client.pending(), 1);
    let accepted = target.head().await.unwrap();
    drop(client);
    let mut client = FileClient::open(outbox.clone(), target.clone(), LIMITS)
        .await
        .unwrap();
    assert_eq!(client.pending(), 1);
    assert_eq!(client.flush().await.unwrap(), 1);
    assert_eq!(
        target.head().await.unwrap(),
        accepted,
        "retry must not append another request"
    );
    drop(client);
    assert_eq!(
        FileClient::open(outbox, target, LIMITS)
            .await
            .unwrap()
            .pending(),
        0
    );
}

#[tokio::test]
async fn durable_admission_retains_intent_at_each_unconfirmed_prefix() {
    for stage in ["outbox", "target", "checkpoint"] {
        let local = Arc::new(Provider::default());
        let remote = Arc::new(Provider::default());
        let outbox = volume(local.clone(), "client/files").await;
        let target = volume(remote.clone(), "agent/files").await;
        let mut client = FileClient::open(outbox.clone(), target.clone(), LIMITS)
            .await
            .unwrap();
        client.enqueue(request(1)).await.unwrap();
        let pending_cut = outbox.cut(&Query::Tasks).await.unwrap().position;
        match stage {
            "outbox" => local
                .durable_through
                .store((pending_cut - 1) as usize, Ordering::SeqCst),
            "target" => remote.durable_through.store(0, Ordering::SeqCst),
            "checkpoint" => local
                .durable_through
                .store(pending_cut as usize, Ordering::SeqCst),
            _ => unreachable!(),
        }
        assert!(client.flush().await.is_err(), "{stage}");
        assert_eq!(client.pending(), 1, "{stage}");
        let admitted = target.snapshot(&Query::Tasks, 1).await.unwrap();
        assert_eq!(
            admitted.files.len(),
            usize::from(stage != "outbox"),
            "{stage}"
        );
        local.durable_through.store(usize::MAX, Ordering::SeqCst);
        remote.durable_through.store(usize::MAX, Ordering::SeqCst);
        assert_eq!(client.flush().await.unwrap(), 1, "{stage}");
        assert_eq!(client.pending(), 0);
        drop(client);
        assert_eq!(
            FileClient::open(outbox, target, LIMITS)
                .await
                .unwrap()
                .pending(),
            0
        );
    }
}

#[tokio::test]
async fn stale_openers_cannot_regress_the_checkpoint() {
    let outbox = volume(Arc::new(Provider::default()), "client/files").await;
    let target = volume(Arc::new(Provider::default()), "agent/files").await;
    let mut current = FileClient::open(outbox.clone(), target.clone(), LIMITS)
        .await
        .unwrap();
    current.enqueue(request(1)).await.unwrap();
    current.enqueue(request(2)).await.unwrap();
    let mut stale = FileClient::open(outbox.clone(), target.clone(), LIMITS)
        .await
        .unwrap();
    assert_eq!(current.flush().await.unwrap(), 2);
    let committed = current.cursor().clone();
    assert!(stale.flush().await.is_err());
    let reopened = FileClient::open(outbox, target, LIMITS).await.unwrap();
    assert_eq!(reopened.cursor(), &committed);
    assert_eq!(reopened.pending(), 0);
}

async fn reopen_requires_durable_checkpoint(id: u8) {
    let local = Arc::new(Provider::default());
    let outbox = volume(local.clone(), "client/files").await;
    let target = volume(Arc::new(Provider::default()), "agent/files").await;
    let mut client = FileClient::open(outbox.clone(), target.clone(), LIMITS)
        .await
        .unwrap();
    client.enqueue(request(id)).await.unwrap();
    let intent = outbox.head().await.unwrap();
    local
        .durable_through
        .store(intent.position as usize, Ordering::SeqCst);
    assert!(client.flush().await.is_err());
    assert_eq!(client.pending(), 1);
    drop(client);
    // The target is durable, but the accepted checkpoint is not. Reopening may
    // not report its cursor as committed until its own publication is durable.
    assert!(
        FileClient::open(outbox.clone(), target.clone(), LIMITS)
            .await
            .is_err()
    );
    local.durable_through.store(usize::MAX, Ordering::SeqCst);
    let restored = FileClient::open(outbox, target, LIMITS).await.unwrap();
    assert_eq!(restored.pending(), 0);
    assert_eq!(restored.cursor().position, intent.position);
}

#[tokio::test]
async fn reopening_cannot_promote_an_accepted_checkpoint_to_durable() {
    for id in [0, 1, u8::MAX] {
        reopen_requires_durable_checkpoint(id).await;
    }
}

#[tokio::test]
async fn quotas_and_target_identity_fail_before_submission() {
    let outbox = volume(Arc::new(Provider::default()), "client/files").await;
    let target = volume(Arc::new(Provider::default()), "agent/files").await;
    let mut client = FileClient::open(
        outbox.clone(),
        target.clone(),
        FileClientLimits {
            commands: 1,
            bytes: 1,
            durability: FileClientDurability::Durable,
        },
    )
    .await
    .unwrap();
    client.enqueue(request(1)).await.unwrap();
    client.enqueue(request(1)).await.unwrap();
    let before = outbox.head().await.unwrap();
    assert!(client.enqueue(request(2)).await.is_err());
    assert!(
        client
            .enqueue(Write::new(FileKey::Goal, b"goal".to_vec()))
            .await
            .is_err()
    );
    assert_eq!(outbox.head().await.unwrap(), before);
    drop(client);
    let replacement = volume(Arc::new(Provider::default()), "agent/files").await;
    assert!(FileClient::open(outbox, replacement, LIMITS).await.is_err());
    assert!(
        target
            .snapshot(&Query::Tasks, 1)
            .await
            .unwrap()
            .files
            .is_empty()
    );
}

fn conserved(enqueued: usize, accepted: usize, pending: usize) -> bool {
    enqueued == accepted + pending
}

async fn earlier_ack_survives_unrelated_tail(id: u8, checkpoint: bool, whole_tail: bool) -> bool {
    let local = Arc::new(Provider::default());
    let remote = Arc::new(Provider::default());
    let outbox = volume(local.clone(), "client/files").await;
    let target = volume(remote.clone(), "agent/files").await;
    let mut client = FileClient::open(outbox.clone(), target.clone(), LIMITS)
        .await
        .unwrap();
    client.enqueue(request(id)).await.unwrap();
    if checkpoint {
        local.durable_through.store(
            outbox.head().await.unwrap().position as usize,
            Ordering::SeqCst,
        );
    } else {
        local
            .fail_commit
            .store(local.commits.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
    }
    assert!(client.flush().await.is_err());
    let accepted = target.head().await.unwrap();
    if checkpoint {
        let own_cut = outbox.head().await.unwrap();
        client.enqueue(request(id.wrapping_add(1))).await.unwrap();
        local
            .durable_through
            .store(own_cut.position as usize, Ordering::SeqCst);
        if whole_tail
            && outbox.cut(&Query::Tasks).await.unwrap().durable
                < outbox.head().await.unwrap().position
        {
            return false;
        }
        // A's checkpoint is durable; B's intent is not. A must be acknowledged
        // before flushing stops at B, regardless of the later accepted suffix.
        assert!(client.flush().await.is_err());
        client.pending() == 1 && target.head().await.unwrap() == accepted
    } else {
        target
            .append(accepted.clone(), &Write::new(FileKey::Goal, vec![id]))
            .await
            .unwrap();
        remote
            .durable_through
            .store(accepted.position as usize, Ordering::SeqCst);
        drop(client);
        let mut reopened = FileClient::open(outbox, target.clone(), LIMITS)
            .await
            .unwrap();
        if whole_tail
            && target.cut(&Query::Tasks).await.unwrap().durable
                < target.head().await.unwrap().position
        {
            return false;
        }
        let before_retry = target.head().await.unwrap();
        reopened.flush().await.unwrap() == 1
            && reopened.pending() == 0
            && target.head().await.unwrap() == before_retry
    }
}

#[tokio::test]
async fn each_file_uses_its_own_durable_publication_cut() {
    for checkpoint in [false, true] {
        assert!(earlier_ack_survives_unrelated_tail(1, checkpoint, false).await);
        // The former whole-head durability check stalls behind an unrelated
        // accepted write. The same progress property rejects that decision.
        assert!(!earlier_ack_survives_unrelated_tail(1, checkpoint, true).await);
    }
}

#[tokio::test]
async fn volume_identity_is_provider_scoped_instead_of_path_scoped() {
    let outbox = volume(Arc::new(Provider::default()), "files").await;
    let target = volume(Arc::new(Provider::default()), "files").await;
    let mut client = FileClient::open(outbox.clone(), target, LIMITS)
        .await
        .unwrap();
    client.enqueue(request(1)).await.unwrap();
    assert_eq!(client.flush().await.unwrap(), 1);
    assert!(
        FileClient::open(outbox.clone(), outbox, LIMITS)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn negative_control_acknowledging_before_target_publication_is_detected() {
    let outbox = volume(Arc::new(Provider::default()), "client/files").await;
    let target = volume(Arc::new(Provider::default()), "agent/files").await;
    let mut client = FileClient::open(outbox.clone(), target.clone(), LIMITS)
        .await
        .unwrap();
    client.enqueue(request(1)).await.unwrap();
    assert!(conserved(1, 0, client.pending()));
    drop(client);
    let key = FileKey::parse("workspace/client/outbox.json").unwrap();
    let state = outbox.snapshot(&Query::File(key.clone()), 1).await.unwrap();
    let mut checkpoint: serde_json::Value = serde_json::from_slice(&state.files[0].bytes).unwrap();
    checkpoint["cursor"] = serde_json::to_value(outbox.cut(&Query::Tasks).await.unwrap()).unwrap();
    outbox
        .append(
            state.generation,
            &Write::new(key, serde_json::to_vec(&checkpoint).unwrap()),
        )
        .await
        .unwrap();
    let broken = FileClient::open(outbox, target, LIMITS).await.unwrap();
    assert!(!conserved(1, 0, broken.pending()));
}

proptest::proptest! {
    #[test]
    fn quota_error_projection_preserves_kind_and_exact_limit(commands in proptest::num::usize::ANY, bytes in proptest::num::u64::ANY) {
        for (error, kind, limit) in [
            (FileClientError::CommandQuota { limit: commands }, "CommandQuota", commands as u64),
            (FileClientError::ByteQuota { limit: bytes }, "ByteQuota", bytes),
        ] {
            assert_eq!(serde_json::to_value(error).unwrap(), serde_json::json!({ "kind": kind, "detail": { "limit": limit } }));
        }
    }

    #[test]
    fn indeterminate_publication_never_bypasses_reconciled_quotas(
        id in proptest::num::u8::ANY, mode in 0u8..3, byte_quota in proptest::bool::ANY,
    ) {
        assert!(futures::executor::block_on(indeterminate_enqueue_preserves_capacity(id, mode, byte_quota, false)));
        assert!(!futures::executor::block_on(indeterminate_enqueue_preserves_capacity(id, mode, byte_quota, true)));
    }
    #[test]
    fn restoration_requires_its_own_checkpoint_durability(id in proptest::num::u8::ANY) {
        futures::executor::block_on(reopen_requires_durable_checkpoint(id));
    }

    // Unrelated accepted suffixes cannot hold back a durable request or its
    // checkpoint, for either publication stage and every request identity.
    #[test]
    fn durable_progress_ignores_later_tail(id in proptest::num::u8::ANY, checkpoint in proptest::bool::ANY) {
        assert!(futures::executor::block_on(earlier_ack_survives_unrelated_tail(id, checkpoint, false)));
    }

    #[test]
    fn checkpoint_codec_and_recovery_preserve_request_identity(bytes in proptest::collection::vec(proptest::num::u8::ANY, 0..256)) {
        parser::exercise(&bytes);
    }

    // Each first-seen immutable request is delivered once in enqueue order after
    // reopen; repeated identities neither reorder nor duplicate publication.
    #[test]
    fn recovery_preserves_order_and_idempotence(ids in proptest::collection::vec(0u8..16, 0..32)) {
        futures::executor::block_on(async {
            let outbox = volume(Arc::new(Provider::default()), "client/files").await;
            let target = volume(Arc::new(Provider::default()), "agent/files").await;
            let cut = target.cut(&Query::Tasks).await.unwrap();
            let mut client = FileClient::open(outbox.clone(), target.clone(), LIMITS).await.unwrap();
            let mut expected = Vec::new();
            for id in ids { client.enqueue(request(id)).await.unwrap(); if !expected.contains(&id) { expected.push(id); } }
            assert!(conserved(expected.len(), 0, client.pending()));
            drop(client);
            let mut client = FileClient::open(outbox, target.clone(), LIMITS).await.unwrap();
            assert_eq!(client.flush().await.unwrap(), expected.len());
            let mut changes = target.follow(Query::Tasks, cut).await.unwrap();
            for id in expected { assert_eq!(changes.next().await.unwrap().unwrap().file.unwrap().bytes.as_ref(), &[id]); }
            assert_eq!(client.pending(), 0);
        });
    }
}
