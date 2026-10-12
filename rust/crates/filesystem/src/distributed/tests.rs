use super::*;
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitId, CommittedEnvelope,
    CommittedMutation, ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation,
    IdempotencyOutcome, MemoryStream, ReadRequest, RecordStream, StreamBounds, StreamError,
    StreamProvider,
};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
type TestResult = Result<(), Box<dyn std::error::Error>>;
const BUDGET: WorkBudget = crate::performance::work_budget(1 << 28, 1 << 28);

fn proposed(id: u8) -> ProposedCommit {
    ProposedCommit {
        operation_id: OperationId::from_bytes([id; 16]),
        fingerprint: Digest::from_bytes([id; 32]),
        payload: Bytes::from_static(b"publication"),
    }
}
fn append(reservation: PublicationReservation, permit: PublicationPermit) -> crate::GuardedAppend {
    crate::GuardedAppend {
        authority_id: reservation.authority_id,
        epoch: reservation.expected.epoch,
        expected: reservation.expected,
        commit: proposed(3),
        permit,
    }
}
fn permit(reservation: PublicationReservation) -> PublicationPermit {
    PublicationPermit::Reservation {
        operation_id: reservation.operation_id.into_bytes(),
        gate_tail: reservation.gate_tail,
        expected: reservation.expected,
    }
}
async fn reserved()
-> Result<(Arc<MemoryStream>, PublicationReservation), Box<dyn std::error::Error>> {
    let stream = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(stream.clone());
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    let id = AuthorityId::from_bytes([1; 16]);
    store
        .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await?;
    let head = store.head(id, &mut work).await?;
    let ReservationOutcome::Reserved(reservation) = store
        .reserve_publication(id, head, OperationId::from_bytes([2; 16]), &mut work)
        .await?
    else {
        return Err("reservation denied".into());
    };
    Ok((stream, reservation))
}
fn gate_commit(
    reservation: PublicationReservation,
    body: acyclic_stream::OwnedBytes,
    key: IdempotencyKey,
) -> Result<CommitRequest, AuthorityStoreError> {
    let path = stream_path(reservation.authority_id, "/publication-gate")?;
    Ok(CommitRequest {
        conditions: vec![CommitCondition::Tail {
            path: path.clone(),
            expected: reservation.gate_tail,
        }],
        mutations: vec![CommitMutation::Append {
            path,
            records: vec![body],
        }],
        idempotency_key: key,
    })
}
fn release_key(reservation: PublicationReservation) -> Result<IdempotencyKey, AuthorityStoreError> {
    stream_key(
        b"release-publication",
        &[
            reservation.authority_id.into_bytes().as_slice(),
            reservation.operation_id.into_bytes().as_slice(),
            reservation.gate_tail.to_le_bytes().as_slice(),
        ]
        .concat(),
    )
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_owner_rejection_limits_and_cancellation_preserve_the_head() -> TestResult {
    let (stream, reservation) = reserved().await?;
    let store = StreamAuthorityStore::new(stream);
    let token = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(BUDGET, &token).map_err(|(error, _)| error)?;
    let request = append(reservation, permit(reservation));
    let ledger = original.clone();
    let reject = move |work: &crate::NativeMaterializationWork| {
        assert!(work.shares_ledger(&ledger));
        assert!(work.receipt().bytes_hashed > 0);
        std::future::ready(Err(AuthorityStoreError::Retired))
    };
    for committed in [false, true] {
        let mut work = AuthorityWork::native(&original, &token);
        let error = store
            .compare_and_append(
                request.clone(),
                &mut work,
                Some(&mut crate::NativeAuthorityGuard(reject.clone())),
            )
            .await;
        assert!(matches!(
            error,
            Err(AuthorityError::Owner(AuthorityStoreError::Retired))
        ));
        if !committed {
            let mut ordinary = AuthorityWork::new(BUDGET, &token);
            assert_eq!(
                store.head(reservation.authority_id, &mut ordinary).await?,
                reservation.expected
            );
            assert!(matches!(
                store
                    .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
                    .await?,
                FsAppendOutcome::Committed(_)
            ));
        }
    }
    let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let mut budget = BUDGET;
    budget.allocation_operations = 1;
    let original =
        crate::NativeMaterializationWork::new(budget, &token).map_err(|(error, _)| error)?;
    let mut work = AuthorityWork::native(&original, &token);
    assert!(matches!(
        store
            .create_first_record(
                reservation.authority_id,
                proposed(4),
                &mut work,
                None::<&mut ()>
            )
            .await,
        Err(AuthorityError::Authority(
            AuthorityStoreError::NativeOriginal(crate::NativeTreeMaterializationError::Work(_))
        ))
    ));
    token.cancel();
    assert!(matches!(
        store
            .create_first_record(
                reservation.authority_id,
                proposed(4),
                &mut work,
                None::<&mut ()>
            )
            .await,
        Err(AuthorityError::Authority(
            AuthorityStoreError::Cancelled
                | AuthorityStoreError::NativeOriginal(
                    crate::NativeTreeMaterializationError::Cancelled
                )
        ))
    ));
    let fresh = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &fresh);
    assert!(matches!(
        store.head(reservation.authority_id, &mut work).await,
        Err(AuthorityStoreError::Missing)
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_custom_provider_never_falls_back_to_ordinary_io() -> TestResult {
    let spy = Arc::new(ReleaseSpy::new(Arc::new(MemoryStream::default())));
    let store = StreamAuthorityStore::new(spy.clone());
    let token = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(BUDGET, &token).map_err(|(error, _)| error)?;
    let mut work = AuthorityWork::native(&original, &token);
    let id = AuthorityId::from_bytes([1; 16]);
    for result in [
        store.head(id, &mut work).await.map(|_| ()),
        store
            .replay(
                id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 8,
                },
                &mut work,
            )
            .await
            .map(|_| ()),
        store
            .fence(id, Head::genesis(Epoch::GENESIS), &mut work)
            .await
            .map(|_| ()),
        store
            .find_operation(id, OperationId::from_bytes([2; 16]), &mut work)
            .await
            .map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(AuthorityStoreError::NativeStream(StreamError::Unsupported))
        ));
    }
    assert!(!spy.reads.load(SeqCst));
    assert!(!spy.writes.load(SeqCst));
    original.receipt().verify(BUDGET)?;
    Ok(())
}

#[tokio::test]
async fn missing_and_corrupt_gates_fail_authority_reopen() -> TestResult {
    for gate in [None, Some(b"invalid-publication-gate".as_slice())] {
        let stream = Arc::new(MemoryStream::default());
        let id = AuthorityId::from_bytes([1; 16]);
        let mut request = CommitRequest {
            conditions: Vec::new(),
            mutations: Vec::new(),
            idempotency_key: stream_key(b"gateless", &id.into_bytes())?,
        };
        let epoch = Epoch::GENESIS.get().to_le_bytes();
        for (suffix, body) in [
            ("", AUTHORITY_MARKER.to_vec()),
            ("/records", [GENESIS_DOMAIN, epoch.as_slice()].concat()),
            ("/epochs", [EPOCH_DOMAIN, epoch.as_slice()].concat()),
        ] {
            let path = stream_path(id, suffix)?;
            request
                .conditions
                .push(CommitCondition::Absent { path: path.clone() });
            request.mutations.push(CommitMutation::Append {
                path,
                records: vec![body.into()],
            });
        }
        if let Some(gate) = gate {
            request.mutations.push(CommitMutation::Append {
                path: stream_path(id, "/publication-gate")?,
                records: vec![
                    acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE),
                    acyclic_stream::OwnedBytes::copy_from_slice(gate),
                ],
            });
        }
        stream.commit(request).await?;
        let token = CancellationToken::new();
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
        let store = StreamAuthorityStore::new(stream.clone());
        assert!(matches!(
            store
                .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
                .await,
            Err(AuthorityError::Authority(AuthorityStoreError::Corrupt(_)))
        ));
        if gate.is_none() {
            assert!(matches!(
                stream.tail(stream_path(id, "/publication-gate")?).await,
                Err(StreamError::NotFound)
            ));
        }
    }
    Ok(())
}

#[tokio::test]
async fn reservations_exclude_writers_and_recover_the_original_head() -> TestResult {
    let (stream, reservation) = reserved().await?;
    let store = StreamAuthorityStore::new(stream.clone());
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    let id = reservation.authority_id;
    let op = reservation.operation_id;
    assert_eq!(
        store
            .reserve_publication(id, reservation.expected, op, &mut work)
            .await?,
        ReservationOutcome::AlreadyReserved(reservation)
    );
    assert!(matches!(
        store
            .compare_and_append(
                append(reservation, PublicationPermit::Unrestricted),
                &mut work,
                None::<&mut ()>
            )
            .await?,
        FsAppendOutcome::Fenced { .. }
    ));
    assert!(matches!(
        store
            .compare_and_append(
                append(reservation, permit(reservation)),
                &mut work,
                None::<&mut ()>
            )
            .await?,
        FsAppendOutcome::Committed(_)
    ));
    let changed = store.head(id, &mut work).await?;
    assert!(
        matches!(store.reserve_publication(id,reservation.expected,op,&mut work).await?,ReservationOutcome::Conflict{actual} if actual==changed)
    );
    assert!(
        store
            .reserve_publication(id, changed, op, &mut work)
            .await
            .is_err()
    );
    let spy = Arc::new(ReleaseSpy::new(stream.clone()));
    let queried = StreamAuthorityStore::new(spy.clone());
    assert_eq!(
        queried.publication_reservation(id, op, &mut work).await?,
        Some(reservation)
    );
    assert!(work.spent().bytes_hashed > 0);
    assert!(!spy.writes.load(SeqCst));
    for hash_denied in [false, true] {
        let mut budget = WorkBudget::UNBOUNDED;
        if hash_denied {
            budget.bytes_hashed = 0;
        } else {
            budget.allocation_operations = 0;
        }
        let spy = Arc::new(ReleaseSpy::new(stream.clone()));
        let mut bounded = AuthorityWork::new(budget, &token);
        assert!(matches!(
            StreamAuthorityStore::new(spy.clone())
                .publication_reservation(id, op, &mut bounded)
                .await,
            Err(AuthorityStoreError::Work(_))
        ));
        assert_eq!(spy.reads.load(SeqCst), hash_denied);
        assert_eq!(bounded.spent().bytes_hashed, 0);
        assert!(!spy.writes.load(SeqCst));
    }
    let mut corrupt = ReleaseSpy::new(stream.clone());
    corrupt.corrupt_observation = 10;
    assert!(
        matches!(StreamAuthorityStore::new(Arc::new(corrupt)).publication_reservation(id,op,&mut work).await,
        Err(AuthorityStoreError::ReservationOriginalHeadUnavailable {authority_id,operation_id,gate_tail}) if authority_id==id&&operation_id==op&&gate_tail==reservation.gate_tail)
    );
    let mut denied = AuthorityWork::new(WorkBudget::default(), &token);
    assert!(matches!(
        store.release_publication(reservation, &mut denied).await,
        Err(AuthorityStoreError::Work(_))
    ));
    store.release_publication(reservation, &mut work).await?;
    store.release_publication(reservation, &mut work).await?;
    assert_eq!(
        queried.publication_reservation(id, op, &mut work).await?,
        None
    );
    let ReservationOutcome::Reserved(next) = store
        .reserve_publication(id, changed, op, &mut work)
        .await?
    else {
        return Err("reacquisition denied".into());
    };
    assert!(
        store
            .release_publication(
                PublicationReservation {
                    operation_id: OperationId::from_bytes([9; 16]),
                    ..next
                },
                &mut work
            )
            .await
            .is_err()
    );
    stream
        .commit(gate_commit(
            next,
            [PUBLICATION_GATE_ACTIVE, &op.into_bytes()].concat().into(),
            stream_key(b"advance", &id.into_bytes())?,
        )?)
        .await?;
    assert!(matches!(
        stream
            .commit(gate_commit(
                next,
                acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE),
                release_key(next)?
            )?)
            .await?,
        CommitOutcome::Conflict(_)
    ));
    assert!(store.release_publication(next, &mut work).await.is_err());
    let path = stream_path(id, "/publication-gate")?;
    let tail = stream.tail(path.clone()).await?;
    assert_eq!(
        decode_publication_gate(&read_one(stream.as_ref(), path, tail - 1).await?.value)?,
        Some(op)
    );
    Ok(())
}

#[tokio::test]
async fn release_lost_ack_retains_its_original_decision_and_rejects_forged_envelopes() -> TestResult
{
    let (stream, reservation) = reserved().await?;
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
    let mut spy = ReleaseSpy::new(stream.clone());
    spy.lose_ack = true;
    let spy = Arc::new(spy);
    assert!(
        StreamAuthorityStore::new(spy.clone())
            .release_publication(reservation, &mut work)
            .await
            .is_err()
    );
    assert!(spy.writes.load(SeqCst));
    let store = StreamAuthorityStore::new(stream.clone());
    let next_op = OperationId::from_bytes([3; 16]);
    assert!(matches!(
        store
            .reserve_publication(
                reservation.authority_id,
                reservation.expected,
                next_op,
                &mut work
            )
            .await?,
        ReservationOutcome::Reserved(_)
    ));
    for corrupt in 0..=10 {
        let mut spy = ReleaseSpy::new(stream.clone());
        spy.corrupt_observation = corrupt;
        let spy = Arc::new(spy);
        let result = StreamAuthorityStore::new(spy.clone())
            .release_publication(reservation, &mut work)
            .await;
        if corrupt == 0 {
            result?;
        } else {
            assert!(matches!(result, Err(AuthorityStoreError::Corrupt(_))));
        }
        assert!(!spy.writes.load(SeqCst));
    }
    let path = stream_path(reservation.authority_id, "/publication-gate")?;
    let tail = stream.tail(path.clone()).await?;
    assert_eq!(
        decode_publication_gate(&read_one(stream.as_ref(), path, tail - 1).await?.value)?,
        Some(next_op)
    );
    let mut spy = ReleaseSpy::new(stream);
    spy.cancel_inspect = Some(token.clone());
    assert!(matches!(
        StreamAuthorityStore::new(Arc::new(spy))
            .release_publication(reservation, &mut work)
            .await,
        Err(AuthorityStoreError::Cancelled)
    ));
    Ok(())
}

#[tokio::test]
async fn release_refuses_a_committed_different_request_or_mutation_family() -> TestResult {
    let token = CancellationToken::new();
    for wrong_family in [false, true] {
        let (stream, reservation) = reserved().await?;
        let key = release_key(reservation)?;
        let value = acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE);
        if wrong_family {
            assert!(matches!(
                stream
                    .append(AppendRequest {
                        path: stream_path(reservation.authority_id, "/publication-gate")?,
                        records: vec![value],
                        if_tail: Some(reservation.gate_tail),
                        idempotency_key: Some(key)
                    })
                    .await?,
                AppendOutcome::Committed(_)
            ));
        } else {
            assert!(matches!(
                stream.commit(gate_commit(reservation, value, key)?).await?,
                CommitOutcome::Committed(_)
            ));
        }
        let spy = Arc::new(ReleaseSpy::new(stream));
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &token);
        assert!(matches!(
            StreamAuthorityStore::new(spy.clone())
                .release_publication(reservation, &mut work)
                .await,
            Err(AuthorityStoreError::Corrupt(_))
        ));
        assert!(!spy.writes.load(SeqCst));
    }
    Ok(())
}

struct ReleaseSpy {
    inner: Arc<MemoryStream>,
    reads: AtomicBool,
    writes: AtomicBool,
    cancel_inspect: Option<CancellationToken>,
    lose_ack: bool,
    corrupt_observation: u8,
}

impl ReleaseSpy {
    fn new(inner: Arc<MemoryStream>) -> Self {
        Self {
            inner,
            reads: AtomicBool::new(false),
            writes: AtomicBool::new(false),
            cancel_inspect: None,
            lose_ack: false,
            corrupt_observation: 0,
        }
    }
}

#[async_trait::async_trait]
impl StreamProvider for ReleaseSpy {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.reads.store(true, SeqCst);
        let mut result = self.inner.inspect_idempotency(key).await?;
        if let Some(observation) = result.as_mut() {
            if self.corrupt_observation == 1 {
                observation.idempotency_key = stream_key(b"another-release", b"identity")
                    .map_err(|_| StreamError::InvalidArgument)?;
            }
            if let IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) =
                &mut observation.outcome
            {
                if let Some(CommittedMutation::Append(append)) = envelope.mutations.first_mut() {
                    match self.corrupt_observation {
                        2 => append.path = StreamPath::new("other/gate")?,
                        3 => append.start += 1,
                        4 => append.end += 1,
                        5 => append.tail += 1,
                        6 => append.records[0].sequence += 1,
                        7 => append.records[0].commit_id = CommitId::from_bytes([0xff; 32]),
                        8 => {
                            append.records[0].value =
                                acyclic_stream::OwnedBytes::from_static(b"not-free")
                        }
                        9 => append.records.push(append.records[0].clone()),
                        10 => observation.request_digest = [0; 32],
                        _ => {}
                    }
                }
            }
        }
        if let Some(cancellation) = &self.cancel_inspect {
            cancellation.cancel();
        }
        Ok(result)
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.reads.store(true, SeqCst);
        self.inner.tail(path).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.reads.store(true, SeqCst);
        self.inner.read(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.writes.store(true, SeqCst);
        let result = self.inner.commit(request).await?;
        if self.lose_ack {
            Err(StreamError::Unavailable)
        } else {
            Ok(result)
        }
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.inner.bounds(path).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.inner.follow(path, from).await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.inner.children(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}
