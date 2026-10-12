use super::*;
use crate::OperationReceipt;
use crate::kernel::{
    CanonicalDecodeError, VolumeCreated, VolumeCreatedError, encode_publication_payload,
    encode_volume_created,
};
use crate::model::{Lifecycle, VolumeConfig};
use crate::storage::ObjectKind;
#[tokio::test]
pub(super) async fn settlement_times_and_retired_accounting_use_original_stream_records() {
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let authority = StreamAuthorityStore::new(Arc::clone(&stream));
    let authority_id = AuthorityId::from_bytes([0x71; 16]);
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    authority
        .create_authority(authority_id, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await
        .expect("create authority");
    let head = authority.head(authority_id, &mut work).await.expect("head");
    let proposed = ProposedCommit {
        operation_id: OperationId::from_bytes([0x72; 16]),
        fingerprint: Digest::from_bytes([0x73; 32]),
        payload: Bytes::from_static(b"accounting settlement"),
    };
    let request = crate::GuardedAppend {
        authority_id,
        epoch: head.epoch,
        expected: head,
        commit: proposed.clone(),
        permit: PublicationPermit::Unrestricted,
    };
    let outcome = authority
        .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
        .await
        .expect("append");
    let FsAppendOutcome::Committed(durable) = outcome else {
        panic!("expected committed record");
    };
    let record = read_one(
        stream.as_ref(),
        stream_path(authority_id, "/records").expect("path"),
        1,
    )
    .await
    .expect("exact authoritative record");
    assert_eq!(
        durable.settled_at_micros,
        (record.committed_at_micros != 0).then_some(record.committed_at_micros)
    );
    let limit = ReplayLimit {
        records: 8,
        payload_bytes: 1_024,
    };
    assert_eq!(
        authority
            .replay(authority_id, Sequence::GENESIS, limit, &mut work)
            .await
            .expect("replay")
            .0,
        vec![durable.clone()]
    );
    assert_eq!(
        authority
            .find_operation(authority_id, proposed.operation_id, &mut work)
            .await
            .expect("find"),
        Some(durable.clone())
    );
    assert_eq!(
        authority
            .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
            .await
            .expect("retry"),
        FsAppendOutcome::AlreadyCommitted(durable.clone())
    );
    let mut legacy = record.clone();
    legacy.committed_at_micros = 0;
    let borrowed = decode_record(&legacy).expect("legacy");
    validate_record(authority_id, &legacy, &borrowed).expect("legacy identity");
    let payload = Bytes::copy_from_slice(borrowed.payload);
    assert_eq!(
        durable_commit(borrowed, legacy.committed_at_micros, payload).settled_at_micros,
        None
    );
    authority
        .retire_authority(authority_id, &mut work)
        .await
        .expect("retire");
    for failure in [
        authority.head(authority_id, &mut work).await.err(),
        authority
            .replay(authority_id, Sequence::GENESIS, limit, &mut work)
            .await
            .err(),
        authority
            .find_operation(authority_id, proposed.operation_id, &mut work)
            .await
            .err(),
        authority
            .compare_and_append(request, &mut work, None::<&mut ()>)
            .await
            .map_err(|error| error.into_error(std::convert::identity))
            .err(),
    ] {
        assert!(matches!(failure, Some(AuthorityStoreError::Retired)));
    }
    let accounting = AccountingAuthority(&authority);
    assert_eq!(
        accounting
            .replay(authority_id, Sequence::GENESIS, limit, &mut work)
            .await
            .expect("accounting retains retired history")
            .0,
        vec![durable.clone()]
    );
    assert_eq!(
        accounting
            .head(authority_id, &mut work)
            .await
            .expect("historical head")
            .sequence,
        durable.sequence
    );
}

#[cfg(not(target_arch = "wasm32"))]
const NATIVE_AUTHORITY_BUDGET: WorkBudget = crate::performance::work_budget(1 << 28, 1 << 28);

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
pub(super) async fn original_append_owner_seals_after_planning_and_again_on_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let store = StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default()));
    let authority = AuthorityId::from_bytes([0x86; 16]);
    let mut work = AuthorityWork::new(NATIVE_AUTHORITY_BUDGET, &cancellation);
    store
        .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let original = crate::NativeMaterializationWork::new(NATIVE_AUTHORITY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let expected = Head::genesis(Epoch::GENESIS);
    let request = crate::GuardedAppend {
        authority_id: authority,
        epoch: expected.epoch,
        expected,
        commit: ProposedCommit {
            operation_id: OperationId::from_bytes([0x87; 16]),
            fingerprint: Digest::from_bytes([0x88; 32]),
            payload: Bytes::from_static(b"owner-sealed"),
        },
        permit: PublicationPermit::Unrestricted,
    };
    let expected_root = original.clone();
    let reject = move |work: &crate::NativeMaterializationWork| {
        assert!(work.shares_ledger(&expected_root));
        assert!(work.receipt().bytes_hashed > 0);
        std::future::ready(Err(AuthorityStoreError::Retired))
    };
    let mut work = AuthorityWork::native(&original, &cancellation);
    let failure = store
        .compare_and_append(
            request.clone(),
            &mut work,
            Some(&mut crate::NativeAuthorityGuard(reject.clone())),
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity));
    assert!(matches!(failure, Err(AuthorityStoreError::Retired)));
    assert_eq!(original.receipt().backend_write_operations, 0);
    let mut work = AuthorityWork::new(NATIVE_AUTHORITY_BUDGET, &cancellation);
    assert_eq!(store.head(authority, &mut work).await?, expected);
    let mut work = AuthorityWork::native(&original, &cancellation);
    let outcome = store
        .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    assert!(matches!(outcome, FsAppendOutcome::Committed(_)));
    drop(outcome);
    let before = original.receipt();
    let mut work = AuthorityWork::native(&original, &cancellation);
    let failure = store
        .compare_and_append(
            request,
            &mut work,
            Some(&mut crate::NativeAuthorityGuard(reject)),
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity));
    assert!(matches!(failure, Err(AuthorityStoreError::Retired)));
    assert_eq!(
        original.receipt().backend_write_operations,
        before.backend_write_operations
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
pub(super) async fn original_creation_denial_and_cancellation_publish_nothing()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let store = StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default()));
    let authority = AuthorityId::from_bytes([0x89; 16]);
    let mut budget = NATIVE_AUTHORITY_BUDGET;
    budget.allocation_operations = 1;
    let original =
        crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    let before = original.receipt();
    let first = ProposedCommit {
        operation_id: OperationId::from_bytes([0x8a; 16]),
        fingerprint: Digest::from_bytes([0x8b; 32]),
        payload: Bytes::from_static(b"denied"),
    };
    let mut work = AuthorityWork::native(&original, &cancellation);
    let failure = store
        .create_first_record(authority, first.clone(), &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity));
    assert!(matches!(
        failure,
        Err(AuthorityStoreError::NativeOriginal(
            crate::NativeTreeMaterializationError::Work(_)
        ))
    ));
    assert_eq!(
        original.receipt().allocation_operations,
        before.allocation_operations
    );
    assert_eq!(original.receipt().backend_write_operations, 0);
    assert_eq!(original.receipt().authority_records_appended, 0);
    cancellation.cancel();
    let spent = original.receipt();
    let mut work = AuthorityWork::native(&original, &cancellation);
    let failure = store
        .create_first_record(authority, first, &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity));
    assert!(matches!(
        failure,
        Err(AuthorityStoreError::Cancelled)
            | Err(AuthorityStoreError::NativeOriginal(
                crate::NativeTreeMaterializationError::Cancelled
            ))
    ));
    assert_eq!(original.receipt(), spent);
    let fresh = CancellationToken::new();
    let mut work = AuthorityWork::new(NATIVE_AUTHORITY_BUDGET, &fresh);
    assert!(matches!(
        store.head(authority, &mut work).await,
        Err(AuthorityStoreError::Missing)
    ));
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
pub(super) async fn original_authority_custom_defaults_never_fall_back_to_ordinary_io()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    let cancellation = CancellationToken::new();
    let provider = Arc::new(ReleaseSpy::new(Arc::new(
        acyclic_stream::MemoryStream::default(),
    )));
    let store = StreamAuthorityStore::new(Arc::clone(&provider));
    let original = crate::NativeMaterializationWork::new(NATIVE_AUTHORITY_BUDGET, &cancellation)
        .map_err(|(error, _)| error)?;
    let authority = AuthorityId::from_bytes([0x7a; 16]);
    let mut work = AuthorityWork::native(&original, &cancellation);
    for result in [
        store.head(authority, &mut work).await.map(|_| ()),
        store
            .replay(
                authority,
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
            .fence(authority, Head::genesis(Epoch::GENESIS), &mut work)
            .await
            .map(|_| ()),
        store
            .find_operation(authority, OperationId::from_bytes([0x7b; 16]), &mut work)
            .await
            .map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(AuthorityStoreError::NativeStream(
                acyclic_stream::StreamError::Unsupported
            ))
        ));
    }
    assert!(!provider.writes.load(Ordering::SeqCst));
    assert!(!provider.reads.load(Ordering::SeqCst));
    assert_eq!(original.receipt().backend_read_operations, 0);
    assert_eq!(original.receipt().backend_write_operations, 0);
    assert_eq!(original.receipt().authority_records_read, 0);
    original.receipt().verify(NATIVE_AUTHORITY_BUDGET)?;
    Ok(())
}

#[tokio::test]
pub(super) async fn authority_without_publication_gate_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let authority_id = AuthorityId::from_bytes([0x31; 16]);
    let epoch = Epoch::GENESIS.get().to_le_bytes();
    let mut conditions = Vec::new();
    let mut mutations = Vec::new();
    for (suffix, domain) in [
        ("", AUTHORITY_MARKER),
        ("/records", GENESIS_DOMAIN),
        ("/epochs", EPOCH_DOMAIN),
    ] {
        let path = stream_path(authority_id, suffix)?;
        let value = if suffix.is_empty() {
            acyclic_stream::OwnedBytes::from_static(domain)
        } else {
            [domain, epoch.as_slice()].concat().into()
        };
        conditions.push(acyclic_stream::CommitCondition::Absent { path: path.clone() });
        mutations.push(acyclic_stream::CommitMutation::Append {
            path,
            records: vec![value],
        });
    }
    acyclic_stream::StreamProvider::commit(
        stream.as_ref(),
        acyclic_stream::CommitRequest {
            conditions,
            mutations,
            idempotency_key: stream_key(b"gateless-authority", &authority_id.into_bytes())?,
        },
    )
    .await?;
    let authority = StreamAuthorityStore::new(Arc::clone(&stream));
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let refused = (authority
        .create_authority(authority_id, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity)))
    .expect_err("an authority without its publication gate is refused");
    assert!(matches!(refused, AuthorityStoreError::Corrupt(_)));
    assert!(matches!(
        acyclic_stream::StreamProvider::tail(
            stream.as_ref(),
            stream_path(authority_id, "/publication-gate")?
        )
        .await,
        Err(acyclic_stream::StreamError::NotFound)
    ));
    Ok(())
}

#[tokio::test]
pub(super) async fn existing_authority_rejects_a_corrupt_publication_gate() {
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let authority = StreamAuthorityStore::new(Arc::clone(&stream));
    let authority_id = AuthorityId::from_bytes([0x35; 16]);
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    (authority
        .create_authority(authority_id, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity)))
    .expect("create authority");
    let gate = stream_path(authority_id, "/publication-gate").expect("gate path");
    acyclic_stream::StreamProvider::commit(
        stream.as_ref(),
        acyclic_stream::CommitRequest {
            conditions: vec![acyclic_stream::CommitCondition::Tail {
                path: gate.clone(),
                expected: 1,
            }],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: gate,
                records: vec![acyclic_stream::OwnedBytes::from_static(
                    b"invalid-publication-gate",
                )],
            }],
            idempotency_key: stream_key(b"corrupt-publication-gate", &authority_id.into_bytes())
                .expect("corruption key"),
        },
    )
    .await
    .expect("append invalid gate record");

    let error = (authority
        .create_authority(authority_id, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity)))
    .expect_err("corrupt gate must fail authority reopen");
    assert!(matches!(error, AuthorityStoreError::Corrupt(_)));
}

#[tokio::test]
pub(super) async fn publication_reservation_excludes_unreserved_writers()
-> Result<(), Box<dyn std::error::Error>> {
    let (stream, reservation) = release_fixture().await?;
    let authority = StreamAuthorityStore::new(Arc::clone(&stream));
    let authority_id = reservation.authority_id;
    let head = reservation.expected;
    let operation_id = reservation.operation_id;
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let retry = (authority
        .reserve_publication(authority_id, head, operation_id, &mut work)
        .await)?;
    assert!(matches!(retry, ReservationOutcome::AlreadyReserved(value) if value == reservation));
    let request = crate::GuardedAppend {
        authority_id,
        epoch: head.epoch,
        expected: head,
        commit: ProposedCommit {
            operation_id: OperationId::from_bytes([0x43; 16]),
            fingerprint: Digest::from_bytes([0x44; 32]),
            payload: Bytes::from_static(b"reserved write"),
        },
        permit: PublicationPermit::Unrestricted,
    };
    let ordinary = (authority
        .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity)))?;
    assert!(matches!(ordinary, FsAppendOutcome::Fenced { .. }));
    let reserved = (authority
        .compare_and_append(
            crate::GuardedAppend {
                permit: PublicationPermit::Reservation {
                    operation_id: operation_id.into_bytes(),
                    gate_tail: reservation.gate_tail,
                    expected: reservation.expected,
                },
                ..request
            },
            &mut work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity)))?;
    assert!(matches!(reserved, FsAppendOutcome::Committed(_)));
    let changed = (authority.head(authority_id, &mut work).await)?;
    let stale = (authority
        .reserve_publication(authority_id, head, operation_id, &mut work)
        .await)?;
    assert!(matches!(stale, ReservationOutcome::Conflict { actual } if actual == changed));
    (authority
        .reserve_publication(authority_id, changed, operation_id, &mut work)
        .await)
        .expect_err("an active operation must not adopt a different source head");
    let mut denied_work = AuthorityWork::new(WorkBudget::default(), &cancellation);
    let denied_release = (authority
        .release_publication(reservation, &mut denied_work)
        .await)
        .expect_err("release must charge its idempotency read before doing work");
    assert!(matches!(denied_release, AuthorityStoreError::Work(_)));
    (authority.release_publication(reservation, &mut work).await)?;
    (authority.release_publication(reservation, &mut work).await)?;
    let current = (authority.head(authority_id, &mut work).await)?;
    let reacquired = match (authority
        .reserve_publication(authority_id, current, operation_id, &mut work)
        .await)?
    {
        ReservationOutcome::Reserved(reservation) => reservation,
        outcome => panic!("unexpected reacquisition outcome: {outcome:?}"),
    };
    let forged = PublicationReservation {
        operation_id: OperationId::from_bytes([0x99; 16]),
        ..reacquired
    };
    assert!((authority.release_publication(forged, &mut work).await).is_err());
    let gate = stream_path(authority_id, "/publication-gate")?;
    let advanced = acyclic_stream::StreamProvider::commit(
        stream.as_ref(),
        acyclic_stream::CommitRequest {
            conditions: vec![acyclic_stream::CommitCondition::Tail {
                path: gate.clone(),
                expected: reacquired.gate_tail,
            }],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: gate.clone(),
                records: vec![acyclic_stream::OwnedBytes::from(
                    [
                        PUBLICATION_GATE_ACTIVE,
                        &reacquired.operation_id.into_bytes(),
                    ]
                    .concat(),
                )],
            }],
            idempotency_key: stream_key(b"advance-active-gate", &authority_id.into_bytes())?,
        },
    )
    .await?;
    assert!(matches!(
        advanced,
        acyclic_stream::CommitOutcome::Committed(_)
    ));
    let release_key = stream_key(
        b"release-publication",
        &[
            reacquired.authority_id.into_bytes().as_slice(),
            reacquired.operation_id.into_bytes().as_slice(),
            reacquired.gate_tail.to_le_bytes().as_slice(),
        ]
        .concat(),
    )?;
    let retained_conflict = acyclic_stream::StreamProvider::commit(
        stream.as_ref(),
        acyclic_stream::CommitRequest {
            conditions: vec![acyclic_stream::CommitCondition::Tail {
                path: gate.clone(),
                expected: reacquired.gate_tail,
            }],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: gate,
                records: vec![acyclic_stream::OwnedBytes::from_static(
                    PUBLICATION_GATE_FREE,
                )],
            }],
            idempotency_key: release_key,
        },
    )
    .await?;
    assert!(matches!(
        retained_conflict,
        acyclic_stream::CommitOutcome::Conflict(_)
    ));
    (authority.release_publication(reacquired, &mut work).await)
        .expect_err("retained release conflict must not become success");
    let gate = stream_path(authority_id, "/publication-gate")?;
    let tail = acyclic_stream::StreamProvider::tail(stream.as_ref(), gate.clone()).await?;
    let record = read_one(stream.as_ref(), gate, tail - 1).await?;
    assert_eq!(
        decode_publication_gate(&record.value)?,
        Some(reacquired.operation_id)
    );
    Ok(())
}

struct ReleaseSpy {
    inner: Arc<acyclic_stream::MemoryStream>,
    reads: std::sync::atomic::AtomicBool,
    writes: std::sync::atomic::AtomicBool,
    cancel_inspect: Option<CancellationToken>,
    lose_ack: bool,
    corrupt_observation: u8,
}

impl ReleaseSpy {
    fn new(inner: Arc<acyclic_stream::MemoryStream>) -> Self {
        Self {
            inner,
            reads: std::sync::atomic::AtomicBool::new(false),
            writes: std::sync::atomic::AtomicBool::new(false),
            cancel_inspect: None,
            lose_ack: false,
            corrupt_observation: 0,
        }
    }
}

#[async_trait::async_trait]
impl acyclic_stream::StreamProvider for ReleaseSpy {
    async fn inspect_idempotency(
        &self,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<Option<acyclic_stream::IdempotencyObservation>, acyclic_stream::StreamError> {
        self.reads.store(true, std::sync::atomic::Ordering::SeqCst);
        let mut result = self.inner.inspect_idempotency(key).await?;
        if let Some(observation) = result.as_mut() {
            if self.corrupt_observation == 1 {
                observation.idempotency_key = stream_key(b"another-release", b"identity")
                    .map_err(|_| acyclic_stream::StreamError::InvalidArgument)?;
            }
            if let acyclic_stream::IdempotencyOutcome::Commit(
                acyclic_stream::CommitOutcome::Committed(envelope),
            ) = &mut observation.outcome
            {
                if let Some(acyclic_stream::CommittedMutation::Append(append)) =
                    envelope.mutations.first_mut()
                {
                    match self.corrupt_observation {
                        2 => append.path = acyclic_stream::StreamPath::new("other/gate")?,
                        3 => append.start += 1,
                        4 => append.end += 1,
                        5 => append.tail += 1,
                        6 => append.records[0].sequence += 1,
                        7 => {
                            append.records[0].commit_id =
                                acyclic_stream::CommitId::from_bytes([0xff; 32])
                        }
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
    async fn tail(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<u64, acyclic_stream::StreamError> {
        self.reads.store(true, std::sync::atomic::Ordering::SeqCst);
        self.inner.tail(path).await
    }
    async fn read(
        &self,
        request: acyclic_stream::ReadRequest,
    ) -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.reads.store(true, std::sync::atomic::Ordering::SeqCst);
        self.inner.read(request).await
    }
    async fn commit(
        &self,
        request: acyclic_stream::CommitRequest,
    ) -> Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
        self.writes.store(true, std::sync::atomic::Ordering::SeqCst);
        let result = self.inner.commit(request).await?;
        if self.lose_ack {
            Err(acyclic_stream::StreamError::Unavailable)
        } else {
            Ok(result)
        }
    }
    async fn bounds(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> {
        self.inner.bounds(path).await
    }
    async fn append(
        &self,
        request: acyclic_stream::AppendRequest,
    ) -> Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> {
        self.inner.append(request).await
    }
    async fn fork(
        &self,
        request: acyclic_stream::ForkRequest,
    ) -> Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> {
        self.inner.fork(request).await
    }
    async fn follow(
        &self,
        path: acyclic_stream::StreamPath,
        from: u64,
    ) -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
        self.inner.follow(path, from).await
    }
    async fn children(
        &self,
        request: acyclic_stream::ChildrenRequest,
    ) -> Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> {
        self.inner.children(request).await
    }
    async fn read_commit(
        &self,
        id: acyclic_stream::CommitId,
    ) -> Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> {
        self.inner.read_commit(id).await
    }
}

pub(super) async fn release_fixture()
-> Result<(Arc<acyclic_stream::MemoryStream>, PublicationReservation), Box<dyn std::error::Error>> {
    let inner = Arc::new(acyclic_stream::MemoryStream::default());
    let authority = StreamAuthorityStore::new(Arc::clone(&inner));
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let id = AuthorityId::from_bytes([0xd1; 16]);
    authority
        .create_authority(id, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let head = authority.head(id, &mut work).await?;
    let outcome = authority
        .reserve_publication(id, head, OperationId::from_bytes([0xd2; 16]), &mut work)
        .await?;
    let ReservationOutcome::Reserved(reservation) = outcome else {
        return Err("reservation denied".into());
    };
    Ok((inner, reservation))
}

#[tokio::test]
pub(super) async fn release_lost_ack_recovers_original_envelope_after_gate_advance()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    let (inner, reservation) = release_fixture().await?;
    let cancellation = CancellationToken::new();
    let mut spy = ReleaseSpy::new(Arc::clone(&inner));
    spy.lose_ack = true;
    let spy = Arc::new(spy);
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    StreamAuthorityStore::new(Arc::clone(&spy))
        .release_publication(reservation, &mut work)
        .await
        .expect_err("provider loses release acknowledgement");
    assert!(spy.writes.load(Ordering::SeqCst));
    assert_eq!(work.spent().durability_operations, 0);
    assert_eq!(work.spent().authority_bytes_written, 0);
    let authority = StreamAuthorityStore::new(Arc::clone(&inner));
    let new_operation = OperationId::from_bytes([0xd3; 16]);
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let outcome = authority
        .reserve_publication(
            reservation.authority_id,
            reservation.expected,
            new_operation,
            &mut work,
        )
        .await?;
    assert!(matches!(outcome, ReservationOutcome::Reserved(_)));
    let spy = Arc::new(ReleaseSpy::new(Arc::clone(&inner)));
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    StreamAuthorityStore::new(Arc::clone(&spy))
        .release_publication(reservation, &mut work)
        .await?;
    assert!(!spy.writes.load(Ordering::SeqCst));
    assert_eq!(work.spent().authority_records_read, 1);
    assert_eq!(
        work.spent().authority_bytes_read,
        PUBLICATION_GATE_FREE.len() as u64
    );
    assert_eq!(work.spent().backend_write_operations, 0);
    let gate = stream_path(reservation.authority_id, "/publication-gate")?;
    let tail = acyclic_stream::StreamProvider::tail(inner.as_ref(), gate.clone()).await?;
    let record = read_one(inner.as_ref(), gate, tail - 1).await?;
    assert_eq!(decode_publication_gate(&record.value)?, Some(new_operation));
    for corrupt in 1..=10 {
        let mut spy = ReleaseSpy::new(Arc::clone(&inner));
        spy.corrupt_observation = corrupt;
        let spy = Arc::new(spy);
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let failure = StreamAuthorityStore::new(Arc::clone(&spy))
            .release_publication(reservation, &mut work)
            .await
            .expect_err("observation must bind the original release");
        assert!(matches!(failure, AuthorityStoreError::Corrupt(_)));
        assert!(!spy.writes.load(Ordering::SeqCst));
    }
    let cancellation = CancellationToken::new();
    let mut spy = ReleaseSpy::new(inner);
    spy.cancel_inspect = Some(cancellation.clone());
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let failure = StreamAuthorityStore::new(Arc::new(spy))
        .release_publication(reservation, &mut work)
        .await
        .expect_err("cancellation retains the returned observation prefix");
    assert!(matches!(failure, AuthorityStoreError::Cancelled));
    assert_eq!(work.spent().authority_records_read, 1);
    assert_eq!(
        work.spent().authority_bytes_read,
        PUBLICATION_GATE_FREE.len() as u64
    );
    Ok(())
}

#[tokio::test]
pub(super) async fn release_rejects_committed_wrong_request_and_mutation_family()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::atomic::Ordering;
    let cancellation = CancellationToken::new();
    for wrong_family in [false, true] {
        let (inner, reservation) = release_fixture().await?;
        let gate = stream_path(reservation.authority_id, "/publication-gate")?;
        let key = stream_key(
            b"release-publication",
            &[
                reservation.authority_id.into_bytes().as_slice(),
                reservation.operation_id.into_bytes().as_slice(),
                reservation.gate_tail.to_le_bytes().as_slice(),
            ]
            .concat(),
        )?;
        if wrong_family {
            let outcome = acyclic_stream::StreamProvider::append(
                inner.as_ref(),
                acyclic_stream::AppendRequest {
                    path: gate,
                    records: vec![acyclic_stream::OwnedBytes::from_static(
                        PUBLICATION_GATE_FREE,
                    )],
                    if_tail: Some(reservation.gate_tail),
                    idempotency_key: Some(key),
                },
            )
            .await?;
            assert!(matches!(
                outcome,
                acyclic_stream::AppendOutcome::Committed(_)
            ));
        } else {
            let outcome = acyclic_stream::StreamProvider::commit(
                inner.as_ref(),
                acyclic_stream::CommitRequest {
                    conditions: vec![acyclic_stream::CommitCondition::Tail {
                        path: gate.clone(),
                        expected: reservation.gate_tail,
                    }],
                    mutations: vec![acyclic_stream::CommitMutation::Append {
                        path: gate,
                        records: vec![acyclic_stream::OwnedBytes::from_static(
                            PUBLICATION_GATE_FREE,
                        )],
                    }],
                    idempotency_key: key,
                },
            )
            .await?;
            assert!(matches!(
                outcome,
                acyclic_stream::CommitOutcome::Committed(_)
            ));
        }
        let spy = Arc::new(ReleaseSpy::new(inner));
        let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let failure = (StreamAuthorityStore::new(Arc::clone(&spy))
            .release_publication(reservation, &mut work)
            .await)
            .expect_err("arbitrary retained success must not release this reservation");
        assert!(matches!(failure, AuthorityStoreError::Corrupt(_)));
        assert!(!spy.writes.load(Ordering::SeqCst));
    }
    let (inner, reservation) = release_fixture().await?;
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    (StreamAuthorityStore::new(inner)
        .release_publication(reservation, &mut work)
        .await)?;
    assert_eq!(
        work.spent().authority_bytes_written,
        PUBLICATION_GATE_FREE.len() as u64
    );
    Ok(())
}

#[test]
pub(super) fn generation_records_are_bound_to_their_authority_and_sequence()
-> Result<(), Box<dyn std::error::Error>> {
    let volume_id = crate::foundation::VolumeId::from_bytes([0x71; 16]);
    let authority_id = volume_authority_id(volume_id);
    let root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([0x72; 32]),
    };
    let creation = encode_volume_created(
        VolumeCreated {
            volume_id,
            config: VolumeConfig::portable(Lifecycle::Ephemeral),
            initial_generation_root: root,
        },
        |_| Ok::<_, VolumeCreatedError>(()),
        |_| VolumeCreatedError::Decode(CanonicalDecodeError::AllocationFailed),
    )?;
    let publication = encode_publication_payload(volume_id, root);
    let generation = GenerationId::new(root.digest);
    let cancellation = CancellationToken::new();
    let mut meter = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);

    let foreign = AuthorityId::from_bytes([0x73; 16]);
    for (authority, sequence, payload, expected) in [
        (authority_id, 1, creation.as_slice(), Some(generation)),
        (authority_id, 2, publication.as_slice(), Some(generation)),
        (authority_id, 2, creation.as_slice(), None),
        (authority_id, 1, publication.as_slice(), None),
        (foreign, 1, creation.as_slice(), None),
        (foreign, 2, publication.as_slice(), None),
    ] {
        let result =
            generation_from_payload(authority, Sequence::new(sequence), payload, &mut meter);
        if let Some(expected) = expected {
            assert_eq!(result?, Some(expected));
        } else {
            assert!(result.is_err());
        }
    }
    Ok(())
}

#[tokio::test]
pub(super) async fn generation_owner_guard_seals_after_planning_and_on_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let authority = StreamAuthorityStore::new(Arc::clone(&stream));
    let cancellation = CancellationToken::new();
    let volume = crate::foundation::VolumeId::from_bytes([0x91; 16]);
    let source_authority = volume_authority_id(volume);
    let root = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([0x92; 32]),
    };
    let source_payload = encode_volume_created(
        VolumeCreated {
            volume_id: volume,
            config: VolumeConfig::portable(Lifecycle::Ephemeral),
            initial_generation_root: root,
        },
        |_| Ok::<_, VolumeCreatedError>(()),
        |_| VolumeCreatedError::Decode(CanonicalDecodeError::AllocationFailed),
    )?;
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    authority
        .create_first_record(
            source_authority,
            ProposedCommit {
                operation_id: OperationId::from_bytes([0x93; 16]),
                fingerprint: Digest::from_bytes([0x94; 32]),
                payload: Bytes::from(source_payload),
            },
            &mut work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let source = crate::GenerationForkSource {
        authority: source_authority,
        generation: GenerationId::new(root.digest),
        lineage: crate::GenerationFork::PublishedPrefix,
    };
    let destination = AuthorityId::from_bytes([0x95; 16]);
    let operation = OperationId::from_bytes([0x96; 16]);
    let budget = WorkBudget {
        allocation_operations: 10_000,
        ..WorkBudget::UNBOUNDED
    };
    let mut guard = |remaining: WorkBudget| {
        assert!(remaining.allocation_operations < budget.allocation_operations);
        assert!(remaining.backend_read_operations < budget.backend_read_operations);
        std::future::ready(Err::<OperationReceipt<()>, _>(OperationFailure::new(
            crate::FsError::WorkspaceDeleted,
            WorkCounters {
                items_examined: 1,
                ..WorkCounters::default()
            },
        )))
    };
    let mut work = AuthorityWork::new(budget, &cancellation);
    let failed = authority
        .fork_authority(
            source,
            destination,
            operation,
            &mut work,
            Some(&mut (&mut guard)),
        )
        .await
        .expect_err("deleted owner seals after preparation");
    assert!(matches!(
        failed,
        AuthorityError::Owner(crate::FsError::WorkspaceDeleted)
    ));
    let failed_work = work.spent();
    assert!(failed_work.backend_read_operations > 0);
    assert!(failed_work.allocation_operations > 0);
    assert_eq!(failed_work.backend_write_operations, 0);
    assert!(matches!(
        acyclic_stream::StreamProvider::tail(stream.as_ref(), stream_path(destination, "")?).await,
        Err(acyclic_stream::StreamError::NotFound)
    ));
    let mut work = AuthorityWork::new(budget, &cancellation);
    authority
        .fork_authority(source, destination, operation, &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let mut work = AuthorityWork::new(budget, &cancellation);
    let retried = authority
        .fork_authority(
            source,
            destination,
            operation,
            &mut work,
            Some(&mut (&mut guard)),
        )
        .await
        .expect_err("existing destination requires a fresh seal");
    assert!(matches!(
        retried,
        AuthorityError::Owner(crate::FsError::WorkspaceDeleted)
    ));
    assert!(work.spent().backend_read_operations > failed_work.backend_read_operations);
    assert_eq!(work.spent().backend_write_operations, 0);
    Ok(())
}
#[tokio::test]
pub(super) async fn native_first_record_seals_prepared_request_and_ack_recovery()
-> Result<(), Box<dyn std::error::Error>> {
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let authority = StreamAuthorityStore::new(Arc::clone(&stream));
    let cancellation = CancellationToken::new();
    let destination = AuthorityId::from_bytes([0xa1; 16]);
    let proposed = || ProposedCommit {
        operation_id: OperationId::from_bytes([0xa2; 16]),
        fingerprint: Digest::from_bytes([0xa3; 32]),
        payload: Bytes::from_static(b"owned-canonical-first-record"),
    };
    let budget = WorkBudget {
        allocation_operations: 10_000,
        ..WorkBudget::UNBOUNDED
    };
    let mut reject_prepared = |remaining: WorkBudget| {
        assert!(remaining.allocation_operations < budget.allocation_operations);
        assert!(remaining.bytes_hashed < budget.bytes_hashed);
        std::future::ready(Err::<OperationReceipt<()>, _>(
            OperationFailure::before_work(crate::FsError::WorkspaceDeleted),
        ))
    };
    let mut work = AuthorityWork::new(budget, &cancellation);
    let rejected = authority
        .create_first_record(
            destination,
            proposed(),
            &mut work,
            Some(&mut (&mut reject_prepared)),
        )
        .await
        .expect_err("owner rejected the prepared request");
    assert!(matches!(
        rejected,
        AuthorityError::Owner(crate::FsError::WorkspaceDeleted)
    ));
    assert!(work.spent().bytes_encoded > 0);
    assert!(work.spent().bytes_hashed > 0);
    assert_eq!(work.spent().backend_write_operations, 0);
    assert!(matches!(
        acyclic_stream::StreamProvider::tail(stream.as_ref(), stream_path(destination, "")?).await,
        Err(acyclic_stream::StreamError::NotFound)
    ));
    let mut work = AuthorityWork::new(budget, &cancellation);
    assert!(
        authority
            .create_first_record(destination, proposed(), &mut work, None::<&mut ()>)
            .await
            .map_err(|error| error.into_error(std::convert::identity))?
    );
    let mut seals = 0;
    let mut reject_recovery = |_| {
        seals += 1;
        std::future::ready(if seals == 1 {
            Ok(OperationReceipt {
                value: (),
                work: WorkCounters::default(),
            })
        } else {
            Err(OperationFailure::before_work(
                crate::FsError::WorkspaceDeleted,
            ))
        })
    };
    let mut work = AuthorityWork::new(budget, &cancellation);
    let recovered = authority
        .create_first_record(
            destination,
            proposed(),
            &mut work,
            Some(&mut (&mut reject_recovery)),
        )
        .await
        .expect_err("retained ACK requires the current owner seal");
    assert!(matches!(
        recovered,
        AuthorityError::Owner(crate::FsError::WorkspaceDeleted)
    ));
    assert!(work.spent().authority_records_appended > 0);
    Ok(())
}
#[tokio::test]
pub(super) async fn readonly_reservation_recovers_original_head_after_authority_advance()
-> Result<(), Box<dyn std::error::Error>> {
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let authority = StreamAuthorityStore::new(Arc::clone(&stream));
    let cancellation = CancellationToken::new();
    let authority_id = AuthorityId::from_bytes([0xb1; 16]);
    let operation_id = OperationId::from_bytes([0xb2; 16]);
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    authority
        .create_authority(authority_id, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    let original_head = authority.head(authority_id, &mut work).await?;
    let ReservationOutcome::Reserved(reservation) = authority
        .reserve_publication(authority_id, original_head, operation_id, &mut work)
        .await?
    else {
        panic!("unexpected acquisition");
    };
    authority
        .compare_and_append(
            crate::GuardedAppend {
                authority_id,
                epoch: original_head.epoch,
                expected: original_head,
                commit: ProposedCommit {
                    operation_id: OperationId::from_bytes([0xb3; 16]),
                    fingerprint: Digest::from_bytes([0xb4; 32]),
                    payload: Bytes::from_static(b"original-reserved-operation-effect"),
                },
                permit: PublicationPermit::Reservation {
                    operation_id: operation_id.into_bytes(),
                    gate_tail: reservation.gate_tail,
                    expected: original_head,
                },
            },
            &mut work,
            None::<&mut ()>,
        )
        .await
        .map_err(|error| error.into_error(std::convert::identity))?;
    assert_ne!(
        authority.head(authority_id, &mut work).await?,
        original_head
    );
    let spy = Arc::new(ReleaseSpy::new(Arc::clone(&stream)));
    let queried = StreamAuthorityStore::new(Arc::clone(&spy));
    let mut work = AuthorityWork::new(
        WorkBudget {
            allocation_operations: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let denied = queried
        .publication_reservation(authority_id, operation_id, &mut work)
        .await
        .expect_err("no path allocation admitted");
    assert!(matches!(denied, AuthorityStoreError::Work(_)));
    assert!(!spy.reads.load(std::sync::atomic::Ordering::SeqCst));
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let recovered = queried
        .publication_reservation(authority_id, operation_id, &mut work)
        .await?;
    assert_eq!(recovered, Some(reservation));
    assert_eq!(
        recovered.expect("original evidence").expected,
        original_head
    );
    assert_eq!(work.spent().backend_write_operations, 0);
    assert!(work.spent().bytes_hashed > 0);
    assert!(!spy.writes.load(std::sync::atomic::Ordering::SeqCst));
    let mut work = AuthorityWork::new(
        WorkBudget {
            bytes_hashed: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let refused = queried
        .publication_reservation(authority_id, operation_id, &mut work)
        .await
        .expect_err("proof hashing refused after actual reads");
    assert!(matches!(refused, AuthorityStoreError::Work(_)));
    assert!(work.spent().backend_read_operations > 0);
    assert_eq!(work.spent().bytes_hashed, 0);
    let mut corrupted = ReleaseSpy::new(Arc::clone(&stream));
    corrupted.corrupt_observation = 10;
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let uncertain = StreamAuthorityStore::new(Arc::new(corrupted))
        .publication_reservation(authority_id, operation_id, &mut work)
        .await
        .expect_err("unqualified historical conditions cannot create a proof");
    assert!(
        matches!(uncertain, AuthorityStoreError::ReservationOriginalHeadUnavailable {
        authority_id: actual_authority, operation_id: actual_operation, gate_tail,
    } if actual_authority == authority_id && actual_operation == operation_id && gate_tail == reservation.gate_tail)
    );
    assert_eq!(work.spent().backend_write_operations, 0);
    assert!(work.spent().bytes_hashed > 0);
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    authority
        .release_publication(reservation, &mut work)
        .await?;
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    assert_eq!(
        queried
            .publication_reservation(authority_id, operation_id, &mut work)
            .await?,
        None
    );
    assert_eq!(work.spent().backend_write_operations, 0);
    assert!(!spy.writes.load(std::sync::atomic::Ordering::SeqCst));
    Ok(())
}
