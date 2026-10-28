use super::*;
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    AppendOutcome, AppendRequest, CommitId, CommittedEnvelope, CommittedMutation, ForkReceipt,
    ForkRequest, IdempotencyKey, IdempotencyObservation, IdempotencyOutcome, MemoryStream,
    ReadRequest, RecordStream, StreamError, StreamProvider,
};
use futures::TryStreamExt;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
#[cfg(not(target_arch = "wasm32"))]
mod operation_tests;
#[cfg(not(target_arch = "wasm32"))]
mod policy_tests;
type TestResult = Result<(), Box<dyn std::error::Error>>;
const BUDGET: WorkBudget = crate::performance::work_budget(1 << 28, 1 << 28);

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_io_failure_preserves_the_real_cause_with_and_without_original_work()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(cause, _)| cause)?;
    for code in [2, 5, 13, 28] {
        let ordinary = AuthorityWork::new(BUDGET, &cancellation);
        let AuthorityStoreError::Io(error) =
            ordinary.native_io_failure(std::io::Error::from_raw_os_error(code))
        else {
            panic!("missing original funding must retain the I/O error");
        };
        assert_eq!(error.raw_os_error(), Some(code));
        let native = AuthorityWork::native(&original, &cancellation);
        let AuthorityStoreError::NativeOriginal(crate::NativeTreeMaterializationError::Io(error)) =
            native.native_io_failure(std::io::Error::from_raw_os_error(code))
        else {
            panic!("native funding must retain the original typed I/O failure");
        };
        assert_eq!(error.raw_os_error(), Some(code));
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_read_meter_rejects_missing_original_funding_before_work()
-> Result<(), Box<dyn std::error::Error>> {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let mut ordinary = AuthorityWork::new(BUDGET, &cancellation);
    let before = ordinary.spent();
    assert!(matches!(
        ordinary.native_meter(),
        Err(AuthorityStoreError::NativeOriginalUnsupported)
    ));
    assert_eq!(ordinary.spent(), before);

    let original =
        crate::NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(cause, _)| cause)?;
    let baseline = original.live_allocation_bytes();
    let mut native = AuthorityWork::native(&original, &cancellation);
    let bytes = native.bytes(b"retained native bytes")?;
    let alias = bytes.clone();
    drop(native);
    assert!(original.live_allocation_bytes() > baseline);
    drop(bytes);
    assert_eq!(alias.as_ref(), b"retained native bytes");
    assert!(original.live_allocation_bytes() > baseline);
    drop(alias);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

fn proposed(id: u8) -> ProposedCommit {
    ProposedCommit {
        publication: None,
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
        deadline_unix_millis: None,
        conditions: vec![CommitCondition::Tail {
            path: path.clone(),
            expected: reservation.gate_tail,
        }],
        mutations: vec![CommitMutation::Append {
            path,
            records: vec![body.into()],
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

#[tokio::test]
async fn atomic_first_record_recovers_main_two_step_creation() -> TestResult {
    let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &token);
    let id = AuthorityId::from_bytes([9; 16]);
    let commit = proposed(9);
    assert!(
        crate::async_storage::first_record(&store, id, commit.clone(), &mut work, None::<&mut ()>)
            .await?
    );
    assert!(
        store
            .create_first_record(id, commit.clone(), &mut work, None::<&mut ()>)
            .await?
    );
    let mut conflicting = commit;
    conflicting.fingerprint = Digest::from_bytes([10; 32]);
    assert!(
        !store
            .create_first_record(id, conflicting, &mut work, None::<&mut ()>)
            .await?
    );
    Ok(())
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
async fn replay_rejects_a_record_with_a_different_stream_sequence() -> TestResult {
    let (stream, reservation) = reserved().await?;
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &token);
    StreamAuthorityStore::new(stream.clone())
        .compare_and_append(
            append(reservation, permit(reservation)),
            &mut work,
            None::<&mut ()>,
        )
        .await?;
    let mut spy = ReleaseSpy::new(stream);
    spy.corrupt_observation = 11;
    let result = StreamAuthorityStore::new(Arc::new(spy))
        .replay(
            reservation.authority_id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 2,
                payload_bytes: 32,
            },
            &mut work,
        )
        .await;
    assert!(matches!(result, Err(AuthorityStoreError::Corrupt(_))));
    Ok(())
}

#[tokio::test]
async fn missing_and_corrupt_gates_fail_authority_reopen() -> TestResult {
    for gate in [None, Some(b"invalid-publication-gate".as_slice())] {
        let stream = Arc::new(MemoryStream::default());
        let id = AuthorityId::from_bytes([1; 16]);
        let mut request = CommitRequest {
            deadline_unix_millis: None,
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
            let path = stream_path(id, "/publication-gate")?;
            request
                .conditions
                .push(CommitCondition::Absent { path: path.clone() });
            request.mutations.push(CommitMutation::Append {
                path,
                records: vec![
                    acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE).into(),
                    acyclic_stream::OwnedBytes::copy_from_slice(gate).into(),
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
                        records: vec![value.into()],
                        if_tail: Some(reservation.gate_tail),
                        idempotency_key: key
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

pub(super) struct ReleaseSpy {
    inner: Arc<MemoryStream>,
    reads: AtomicBool,
    writes: AtomicBool,
    cancel_inspect: Option<CancellationToken>,
    lose_ack: bool,
    corrupt_observation: u8,
    before_commit: Vec<crate::GuardedAppend>,
    before_gate_release: Option<PublicationReservation>,
    before_lease_close: Option<StreamPath>,
    race_once: AtomicBool,
    deny_probes_after_commit: bool,
    after_conflict: Option<crate::GuardedAppend>,
    corrupt_recovered_locator: Option<(StreamPath, u8)>,
    commit_attempts: std::sync::atomic::AtomicUsize,
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
            before_commit: Vec::new(),
            before_gate_release: None,
            before_lease_close: None,
            race_once: AtomicBool::new(true),
            deny_probes_after_commit: false,
            after_conflict: None,
            corrupt_recovered_locator: None,
            commit_attempts: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    pub(super) fn losing_ack(inner: Arc<MemoryStream>) -> Self {
        Self {
            lose_ack: true,
            ..Self::new(inner)
        }
    }
    pub(super) fn commit_attempts(&self) -> usize {
        self.commit_attempts.load(SeqCst)
    }
    async fn interleave(&self) -> Result<(), StreamError> {
        self.commit_attempts.fetch_add(1, SeqCst);
        self.writes.store(true, SeqCst);
        if let Some(path) = &self.before_lease_close {
            if self.race_once.swap(false, SeqCst) {
                self.inner
                    .append(AppendRequest {
                        path: path.clone(),
                        records: vec![acyclic_stream::OwnedBytes::from_static(b"closed").into()],
                        if_tail: Some(1),
                        idempotency_key: stream_key(b"close-lease", path.as_str().as_bytes())
                            .map_err(|_| StreamError::InvalidArgument)?,
                    })
                    .await?;
            }
        }
        if !self.before_commit.is_empty() && self.race_once.swap(false, SeqCst) {
            for winner in &self.before_commit {
                let token = CancellationToken::new();
                let mut work = AuthorityWork::new(BUDGET, &token);
                let outcome = StreamAuthorityStore::new(self.inner.clone())
                    .compare_and_append(winner.clone(), &mut work, None::<&mut ()>)
                    .await
                    .map_err(|_| StreamError::InvalidArgument)?;
                assert!(matches!(outcome, FsAppendOutcome::Committed(_)));
            }
        }
        if let Some(reservation) = self.before_gate_release {
            if self.race_once.swap(false, SeqCst) {
                let token = CancellationToken::new();
                let mut work = AuthorityWork::new(BUDGET, &token);
                StreamAuthorityStore::new(self.inner.clone())
                    .release_publication(reservation, &mut work)
                    .await
                    .map_err(|_| StreamError::InvalidArgument)?;
            }
        }
        Ok(())
    }
    async fn finish_commit(&self, result: CommitOutcome) -> Result<CommitOutcome, StreamError> {
        if matches!(&result, CommitOutcome::Conflict(_))
            && let Some(next) = &self.after_conflict
        {
            let token = CancellationToken::new();
            let mut work = AuthorityWork::new(BUDGET, &token);
            StreamAuthorityStore::new(self.inner.clone())
                .compare_and_append(next.clone(), &mut work, None::<&mut ()>)
                .await
                .map_err(|_| StreamError::InvalidArgument)?;
        }
        if self.lose_ack {
            Err(StreamError::Unavailable)
        } else {
            Ok(result)
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl StreamProvider for ReleaseSpy {
    fn context(&self) -> acyclic_stream::ProviderContext<'_> {
        acyclic_stream::ProviderContext::new(self.inner.context().operation_claims())
    }

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
            if let IdempotencyOutcome::Commit(envelope) = &mut observation.outcome {
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

    async fn heads(
        &self,
        request: acyclic_stream::HeadsRequest,
    ) -> std::result::Result<acyclic_stream::Heads, acyclic_stream::StreamError> {
        self.reads.store(true, SeqCst);
        if self.deny_probes_after_commit && self.writes.load(SeqCst) {
            return Err(StreamError::InvalidArgument);
        }
        self.inner.heads(request).await
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.reads.store(true, SeqCst);
        if self.deny_probes_after_commit && self.writes.load(SeqCst) {
            return Err(StreamError::InvalidArgument);
        }
        self.inner.tail(path).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.reads.store(true, SeqCst);
        if self.writes.load(SeqCst)
            && let Some((path, corruption)) = &self.corrupt_recovered_locator
            && &request.path == path
        {
            if *corruption == 1 {
                return Err(StreamError::NotFound);
            }
            let mut records: Vec<_> = self.inner.read(request).await?.try_collect().await?;
            let locator = records.first_mut().ok_or(StreamError::NotFound)?;
            match corruption {
                2 => locator.value = acyclic_stream::OwnedBytes::from_static(b"invalid locator"),
                3 => locator.commit_id = CommitId::from_bytes([0xff; 32]),
                4 => locator.committed_at_micros = locator.committed_at_micros.wrapping_add(1),
                5 => {
                    let mut duplicate = locator.clone();
                    duplicate.sequence = 1;
                    records.push(duplicate);
                }
                _ => return Err(StreamError::InvalidArgument),
            }
            return Ok(Box::pin(futures::stream::iter(records.into_iter().map(Ok))));
        }
        let records = self.inner.read(request).await?;
        if self.corrupt_observation == 11 {
            return Ok(Box::pin(records.map(|record| {
                record.map(|mut record| {
                    record.sequence += 1;
                    record
                })
            })));
        }
        Ok(records)
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.interleave().await?;
        let result = self.inner.commit(request).await?;
        self.finish_commit(result).await
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }
    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> Result<acyclic_stream::FollowStream, StreamError> {
        self.inner.follow(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
}

fn competing_requests(
    authority: AuthorityId,
    grouped: bool,
) -> Result<Vec<crate::GuardedAppend>, Box<dyn std::error::Error>> {
    let mut losers = super::append::group_tests::requests(authority, if grouped { 2 } else { 1 })?;
    for (index, request) in losers.iter_mut().enumerate() {
        request.commit = proposed(
            u8::try_from(index)?
                .checked_add(90)
                .ok_or("operation overflow")?,
        );
    }
    // Recompute the dense loser history after giving it distinct operation
    // identities. The second member is only a prepared, non-durable root.
    let mut expected = Head::genesis(Epoch::GENESIS);
    for request in &mut losers {
        request.expected = expected;
        expected.sequence = expected.sequence.checked_next()?;
        expected.digest = authority_commit_digest(
            authority,
            expected.epoch,
            expected.sequence,
            request.commit.operation_id,
            request.commit.fingerprint,
            request.expected.digest,
            &request.commit.payload,
        );
    }
    Ok(losers)
}

#[tokio::test]
async fn rejected_single_and_group_appends_keep_the_reply_head_without_probing() -> TestResult {
    for grouped in [false, true] {
        let stream = Arc::new(MemoryStream::default());
        let plain = StreamAuthorityStore::new(stream.clone());
        let authority = AuthorityId::from_bytes([93; 16]);
        let token = CancellationToken::new();
        let mut work = AuthorityWork::new(BUDGET, &token);
        plain
            .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await?;
        let winners = super::append::group_tests::requests(authority, 2)?;
        let first = winners.first().ok_or("missing first winner")?.clone();
        let second = winners.get(1).ok_or("missing later winner")?.clone();
        let observed = second.expected;
        let mut spy = ReleaseSpy::new(stream.clone());
        spy.before_commit = vec![first];
        spy.after_conflict = Some(second);
        spy.deny_probes_after_commit = true;
        let store = StreamAuthorityStore::new(Arc::new(spy));
        let mut losers = competing_requests(authority, grouped)?;
        if grouped {
            let crate::AppendGroupOutcome::Published {
                receipts,
                allocation,
            } = store
                .compare_and_append_group(&losers, &[], &mut work, None::<&mut ()>)
                .await?
            else {
                return Err("group failed before the injected race".into());
            };
            assert_eq!(receipts.len(), 2);
            assert!(receipts.iter().all(|outcome| matches!(outcome, FsAppendOutcome::Conflict { actual } if *actual == observed)),
                "group members did not share the failed append's observation");
            drop(receipts);
            drop(allocation);
        } else {
            assert!(
                matches!(store.compare_and_append(losers.remove(0), &mut work, None::<&mut ()>).await?,
                FsAppendOutcome::Conflict { actual } if actual == observed),
                "single append probed a later head"
            );
        }
        assert_eq!(
            plain.head(authority, &mut work).await?.sequence,
            Sequence::new(2)
        );
    }
    Ok(())
}

#[tokio::test]
async fn rejected_append_recovers_operation_identity_from_the_same_suffix() -> TestResult {
    for wrong_fingerprint in [false, true] {
        let stream = Arc::new(MemoryStream::default());
        let plain = StreamAuthorityStore::new(stream.clone());
        let authority = AuthorityId::from_bytes([94; 16]);
        let token = CancellationToken::new();
        let mut work = AuthorityWork::new(BUDGET, &token);
        plain
            .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await?;
        let winners = super::append::group_tests::requests(authority, 2)?;
        let winner = winners.last().ok_or("missing winning retry")?.clone();
        let mut request = winner.clone();
        request.expected = Head::genesis(Epoch::GENESIS);
        if wrong_fingerprint {
            request.commit.fingerprint = Digest::from_bytes([95; 32]);
        }
        let mut spy = ReleaseSpy::new(stream);
        spy.before_commit = winners;
        spy.deny_probes_after_commit = true;
        let store = StreamAuthorityStore::new(Arc::new(spy));
        let outcome = store
            .compare_and_append(request, &mut work, None::<&mut ()>)
            .await?;
        if wrong_fingerprint {
            assert!(
                matches!(outcome, FsAppendOutcome::IdempotencyConflict { committed_fingerprint } if committed_fingerprint == winner.commit.fingerprint)
            );
        } else {
            assert!(
                matches!(outcome, FsAppendOutcome::AlreadyCommitted(existing) if existing.operation_id == winner.commit.operation_id && existing.fingerprint == winner.commit.fingerprint)
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn rejected_single_and_group_appends_require_atomic_operation_locator_proof() -> TestResult {
    for grouped in [false, true] {
        for corruption in 1..=5 {
            let stream = Arc::new(MemoryStream::default());
            let plain = StreamAuthorityStore::new(stream.clone());
            let authority = AuthorityId::from_bytes([96; 16]);
            let token = CancellationToken::new();
            let mut work = AuthorityWork::new(BUDGET, &token);
            plain
                .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
                .await?;
            let winners = super::append::group_tests::requests(authority, 2)?;
            let winner = winners.last().ok_or("missing winning retry")?;
            let operation = winner.commit.operation_id;
            let mut requests = competing_requests(authority, grouped)?;
            let first = requests.first_mut().ok_or("missing losing proposal")?;
            first.commit = winner.commit.clone();
            // Distinct attempt keys force CAS conflict recovery rather than a
            // replay of the winner's successful Streams transaction.
            first.commit.fingerprint = Digest::from_bytes([97; 32]);
            if grouped {
                let first = requests.first().ok_or("missing first proposal")?;
                let next = Head {
                    epoch: first.epoch,
                    sequence: Sequence::new(1),
                    digest: authority_commit_digest(
                        authority,
                        first.epoch,
                        Sequence::new(1),
                        first.commit.operation_id,
                        first.commit.fingerprint,
                        first.expected.digest,
                        &first.commit.payload,
                    ),
                };
                requests
                    .get_mut(1)
                    .ok_or("missing second proposal")?
                    .expected = next;
            }
            let path = work.operation_path(&AuthorityPlacement::default(), authority, operation)?;
            let mut spy = ReleaseSpy::new(stream);
            spy.before_commit = winners;
            spy.deny_probes_after_commit = true;
            spy.corrupt_recovered_locator = Some((path, corruption));
            let store = StreamAuthorityStore::new(Arc::new(spy));
            let error = if grouped {
                store
                    .compare_and_append_group(&requests, &[], &mut work, None::<&mut ()>)
                    .await
                    .err()
            } else {
                store
                    .compare_and_append(requests.remove(0), &mut work, None::<&mut ()>)
                    .await
                    .err()
            };
            assert!(
                matches!(
                    error,
                    Some(AuthorityError::Authority(
                        AuthorityStoreError::Corrupt(_) | AuthorityStoreError::CorruptStatic(_)
                    ))
                ),
                "recovery accepted malformed locator {corruption}, grouped={grouped}: {error:?}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn rejected_append_attempt_does_not_poison_operation_retry_or_lost_ack() -> TestResult {
    let stream = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(stream.clone());
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &token);
    let id = AuthorityId::from_bytes([41; 16]);
    store
        .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await?;
    let expected = store.head(id, &mut work).await?;
    let request = crate::GuardedAppend {
        authority_id: id,
        epoch: expected.epoch,
        expected,
        commit: proposed(42),
        permit: PublicationPermit::Unrestricted,
    };
    let mut winner = request.clone();
    winner.commit = proposed(43);
    let mut spy = ReleaseSpy::new(stream.clone());
    spy.before_commit = vec![winner];
    let racing = StreamAuthorityStore::new(Arc::new(spy));
    let FsAppendOutcome::Conflict { actual } = racing
        .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
        .await?
    else {
        return Err("first append must lose the injected head race".into());
    };
    let mut retry = request;
    retry.expected = actual;
    let FsAppendOutcome::Committed(committed) = racing
        .compare_and_append(retry.clone(), &mut work, None::<&mut ()>)
        .await?
    else {
        return Err("rejected attempt poisoned the same operation retry".into());
    };
    assert_eq!(committed.operation_id, retry.commit.operation_id);
    assert!(
        matches!(store.compare_and_append(retry.clone(), &mut work, None::<&mut ()>).await?,
        FsAppendOutcome::AlreadyCommitted(existing) if existing == committed)
    );
    let mut wrong = retry;
    wrong.commit.fingerprint = Digest::from_bytes([44; 32]);
    assert!(matches!(
        store
            .compare_and_append(wrong, &mut work, None::<&mut ()>)
            .await?,
        FsAppendOutcome::IdempotencyConflict { .. }
    ));

    let lost = crate::GuardedAppend {
        expected: store.head(id, &mut work).await?,
        commit: proposed(45),
        authority_id: id,
        epoch: expected.epoch,
        permit: PublicationPermit::Unrestricted,
    };
    let mut spy = ReleaseSpy::new(stream);
    spy.lose_ack = true;
    let lossy = StreamAuthorityStore::new(Arc::new(spy));
    assert!(
        lossy
            .compare_and_append(lost.clone(), &mut work, None::<&mut ()>)
            .await
            .is_err()
    );
    assert!(
        matches!(lossy.compare_and_append(lost.clone(), &mut work, None::<&mut ()>).await?,
        FsAppendOutcome::AlreadyCommitted(existing) if existing.operation_id == lost.commit.operation_id)
    );
    Ok(())
}

#[tokio::test]
async fn rejected_gate_attempt_retries_without_changing_authority_head() -> TestResult {
    let (stream, reservation) = reserved().await?;
    let mut spy = ReleaseSpy::new(stream);
    spy.before_gate_release = Some(reservation);
    let store = StreamAuthorityStore::new(Arc::new(spy));
    let token = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &token);
    let request = append(reservation, permit(reservation));
    assert!(
        matches!(store.compare_and_append(request.clone(), &mut work, None::<&mut ()>).await?,
        FsAppendOutcome::Fenced { actual_epoch } if actual_epoch == reservation.expected.epoch)
    );
    assert_eq!(
        store.head(reservation.authority_id, &mut work).await?,
        reservation.expected
    );
    let mut retry = request;
    retry.permit = PublicationPermit::Unrestricted;
    assert!(matches!(
        store
            .compare_and_append(retry, &mut work, None::<&mut ()>)
            .await?,
        FsAppendOutcome::Committed(_)
    ));
    Ok(())
}
