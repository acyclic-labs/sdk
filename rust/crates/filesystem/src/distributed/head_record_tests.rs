use super::*;
use acyclic_stream::{MemoryStream, OwnedBytes, StreamProvider};

type TestResult = Result<(), Box<dyn std::error::Error>>;
const AUTHORITY: AuthorityId = AuthorityId::from_bytes([73; 16]);
const BUDGET: WorkBudget = crate::performance::BOUNDARY_BUDGET;

struct Fallback<'a>(&'a StreamAuthorityStore<MemoryStream>);
impl AsyncAuthorityStore for Fallback<'_> {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        self.0.operation_claims()
    }

    async fn head(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        self.0.head(authority, work).await
    }
    async fn replay(
        &self,
        authority: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError> {
        self.0.replay(authority, after, limit, work).await
    }
}

async fn created() -> Result<StreamAuthorityStore<MemoryStream>, Box<dyn std::error::Error>> {
    created_with(Some(Bytes::from_static(b"generation-payload"))).await
}

async fn created_with(
    payload: Option<Bytes>,
) -> Result<StreamAuthorityStore<MemoryStream>, Box<dyn std::error::Error>> {
    crate::initialize_test_native_io();
    let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(BUDGET, &cancellation);
    if let Some(payload) = payload {
        store
            .create_first_record(
                AUTHORITY,
                ProposedCommit {
                    operation_id: OperationId::from_bytes([8; 16]),
                    fingerprint: Digest::from_bytes([9; 32]),
                    payload,
                },
                &mut work,
                None::<&mut ()>,
            )
            .await?;
    } else {
        store
            .create_authority(AUTHORITY, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await?;
    }
    Ok(store)
}

async fn single_record(
    store: &StreamAuthorityStore<MemoryStream>,
) -> Result<DurableCommit, Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let (records, _allocation) = store
        .replay(
            AUTHORITY,
            Sequence::GENESIS,
            ReplayLimit {
                records: 1,
                payload_bytes: STREAM_RECORD_LIMIT,
            },
            &mut AuthorityWork::new(BUDGET, &cancellation),
        )
        .await?;
    records
        .into_iter()
        .next()
        .ok_or_else(|| "missing source record".into())
}

struct FundedReplay<'a>(&'a [u8], acyclic_stream::OperationClaims);
impl AsyncAuthorityStore for FundedReplay<'_> {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        &self.1
    }

    async fn head(
        &self,
        _: AuthorityId,
        _: &mut AuthorityWork<'_>,
    ) -> Result<Head, AuthorityStoreError> {
        Ok(Head {
            epoch: Epoch::GENESIS,
            sequence: Sequence::new(1),
            digest: Digest::ZERO,
        })
    }

    async fn replay(
        &self,
        _: AuthorityId,
        _: Sequence,
        _: ReplayLimit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<DurableCommit>, AuthorityAllocation), AuthorityStoreError> {
        let body = self.0;
        let bytes = (body.len() + std::mem::size_of::<DurableCommit>()) as u64;
        let original = work
            .original
            .ok_or(AuthorityStoreError::NativeOriginalUnsupported)?;
        let allocation = original.claim(bytes, 2, WorkCounters::UNCHARGED)?;
        Ok((
            vec![DurableCommit {
                epoch: Epoch::GENESIS,
                sequence: Sequence::new(1),
                digest: Digest::ZERO,
                previous_digest: Digest::ZERO,
                operation_id: OperationId::from_bytes([0; 16]),
                fingerprint: Digest::ZERO,
                payload: Bytes::copy_from_slice(body),
                settled_at_micros: None,
            }],
            Some(allocation),
        ))
    }
}

#[tokio::test]
async fn default_head_payload_keeps_external_replay_funding_until_payload_drops() -> TestResult {
    crate::initialize_test_native_io();
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|(e, _)| e)?;
    let baseline = original.live_allocation_bytes();
    // Bind funding first, then destructure the payload: this is also the
    // facade's drop order on all normal, error and unwinding paths.
    let (value, allocation) = FundedReplay(b"funded-head-value!", Default::default())
        .head_payload(
            AUTHORITY,
            4096,
            &mut AuthorityWork::native(&original, &cancellation),
        )
        .await?;
    let (_, payload) = value;
    let payload = payload.ok_or("missing funded payload")?;
    assert_eq!(payload.as_ref(), b"funded-head-value!");
    assert!(original.live_allocation_bytes() >= baseline + payload.len() as u64);
    assert!(allocation.is_some());
    drop(payload);
    drop(allocation);
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn exact_head_record_is_authenticated_once_with_default_fallback_equivalence() -> TestResult {
    let store = created().await?;
    let fixture = single_record(&store).await?;
    let encoded =
        crate::streams_record::StreamsDurableRecord::encode(&fixture, STREAM_RECORD_LIMIT)?;
    let cancel = CancellationToken::new();
    let mut old = AuthorityWork::new(BUDGET, &cancel);
    let (expected, _expected_allocation) = Fallback(&store)
        .head_payload(AUTHORITY, 4096, &mut old)
        .await?;
    let mut work = AuthorityWork::new(BUDGET, &cancel);
    let (actual, _actual_allocation) = Arc::new(store)
        .head_payload(AUTHORITY, 4096, &mut work)
        .await?;
    assert_eq!(actual, expected);
    let record = actual.1.ok_or("missing head payload")?;
    assert_eq!(old.spent().authority_records_read, 3);
    assert_eq!(work.spent().authority_records_read, 2);
    assert_eq!(
        old.spent().authority_bytes_read - work.spent().authority_bytes_read,
        encoded.len() as u64
    );
    assert_eq!(old.spent().bytes_hashed, work.spent().bytes_hashed * 2);
    assert!(work.spent().bytes_copied < old.spent().bytes_copied);
    assert!(work.spent().allocation_operations < old.spent().allocation_operations);
    assert_eq!(record.as_ref(), b"generation-payload");
    Ok(())
}

#[tokio::test]
async fn bounded_head_record_preserves_genesis_payload_limits_and_cancellation() -> TestResult {
    let store = created().await?;
    let cancel = CancellationToken::new();
    for maximum in [0, 1, 17, 18, 4096] {
        let mut old = AuthorityWork::new(BUDGET, &cancel);
        let mut work = AuthorityWork::new(BUDGET, &cancel);
        let expected = Fallback(&store)
            .head_payload(AUTHORITY, maximum, &mut old)
            .await;
        let actual = store.head_payload(AUTHORITY, maximum, &mut work).await;
        match (actual, expected) {
            (Ok((actual, _)), Ok((expected, _))) => assert_eq!(actual, expected),
            (Err(actual), Err(expected)) => assert_eq!(actual.to_string(), expected.to_string()),
            _ => return Err("payload limit behavior changed".into()),
        }
    }
    let genesis = AuthorityId::from_bytes([74; 16]);
    let mut work = AuthorityWork::new(BUDGET, &cancel);
    store
        .create_authority(genesis, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await?;
    assert_eq!(
        store.head_payload(genesis, 0, &mut work).await?.0,
        (Head::genesis(Epoch::GENESIS), None)
    );
    for native in [false, true] {
        let original =
            crate::NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(e, _)| e)?;
        let denied = original.native_volume_work(WorkBudget {
            authority_records_read: 1,
            ..BUDGET
        })?;
        let mut work = if native {
            AuthorityWork::native(&denied, &cancel)
        } else {
            AuthorityWork::new(
                WorkBudget {
                    authority_records_read: 1,
                    ..BUDGET
                },
                &cancel,
            )
        };
        assert!(
            store
                .head_payload(AUTHORITY, 4096, &mut work)
                .await
                .is_err()
        );
        assert!(work.spent().authority_records_read <= 1);
    }
    cancel.cancel();
    let mut work = AuthorityWork::new(BUDGET, &cancel);
    assert!(
        store
            .head_payload(AUTHORITY, 4096, &mut work)
            .await
            .is_err()
    );
    assert_eq!(work.spent(), WorkCounters::UNCHARGED);
    Ok(())
}

#[tokio::test]
async fn native_head_record_retains_exact_payload_custody_and_resource_charges() -> TestResult {
    let store = created().await?;
    let cancel = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(e, _)| e)?;
    let resource = original.native_volume_work(BUDGET)?;
    let baseline = original.live_allocation_bytes();
    let mut work = AuthorityWork::native(&resource, &cancel);
    let (value, _allocation) = store.head_payload(AUTHORITY, 4096, &mut work).await?;
    let (_, record) = value;
    let record = record.ok_or("missing native record")?;
    assert_eq!(record.as_ref(), b"generation-payload");
    assert_eq!(original.receipt().authority_records_read, 2);
    drop(work);
    assert!(original.live_allocation_bytes() > baseline);
    let spent = original.receipt();
    drop(record);
    assert_eq!(original.live_allocation_bytes(), baseline);
    assert_eq!(original.receipt(), spent);
    let denied = resource.native_volume_work(WorkBudget {
        bytes_copied: 0,
        ..BUDGET
    })?;
    let mut work = AuthorityWork::native(&denied, &cancel);
    assert!(
        store
            .head_payload(AUTHORITY, 4096, &mut work)
            .await
            .is_err()
    );
    assert_eq!(denied.receipt().bytes_copied, spent.bytes_copied);
    Ok(())
}

#[tokio::test]
async fn retained_head_record_still_rejects_corrupt_digest_and_stream_sequence() -> TestResult {
    let good = created().await?;
    let cancel = CancellationToken::new();
    let record = single_record(&good).await?;
    for corrupt_sequence in [false, true] {
        let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
        store
            .create_authority(
                AUTHORITY,
                Epoch::GENESIS,
                &mut AuthorityWork::new(BUDGET, &cancel),
                None::<&mut ()>,
            )
            .await?;
        let mut record = record.clone();
        if corrupt_sequence {
            record.sequence = Sequence::new(2);
            record.digest = authority_commit_digest(
                AUTHORITY,
                record.epoch,
                record.sequence,
                record.operation_id,
                record.fingerprint,
                record.previous_digest,
                &record.payload,
            );
        } else {
            record.digest = Digest::from_bytes([0xff; 32]);
        }
        let encoded =
            crate::streams_record::StreamsDurableRecord::encode(&record, STREAM_RECORD_LIMIT)?;
        let path = stream_path(AUTHORITY, "/records")?;
        store
            .provider
            .commit(CommitRequest {
                conditions: vec![CommitCondition::Tail {
                    path: path.clone(),
                    expected: 1,
                }],
                mutations: vec![CommitMutation::Append {
                    path,
                    records: vec![OwnedBytes::from(encoded).into()],
                }],
                idempotency_key: stream_key(b"corrupt-head", &[u8::from(corrupt_sequence)])?,
            })
            .await?;
        for native in [false, true] {
            let original =
                crate::NativeMaterializationWork::new(BUDGET, &cancel).map_err(|(e, _)| e)?;
            let mut work = if native {
                AuthorityWork::native(&original, &cancel)
            } else {
                AuthorityWork::new(BUDGET, &cancel)
            };
            assert!(matches!(
                store.head_payload(AUTHORITY, 4096, &mut work).await,
                Err(AuthorityStoreError::Corrupt(_) | AuthorityStoreError::CorruptStatic(_))
            ));
        }
    }
    Ok(())
}

proptest::proptest! {
    // External replay funding survives every payload length, including empty
    // payloads, and releases completely after the value and guard are dropped.
    #[test]
    fn default_payload_keeps_external_funding_for_arbitrary_bodies(
        body in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..2049),
    ) {
        crate::initialize_test_native_io();
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(async {
            let cancellation = CancellationToken::new();
            let original = crate::NativeMaterializationWork::new(BUDGET, &cancellation)
                .map_err(|(e, _)| e).unwrap();
            let baseline = original.live_allocation_bytes();
            let (value, allocation) = FundedReplay(&body, Default::default()).head_payload(
                AUTHORITY, 4096, &mut AuthorityWork::native(&original, &cancellation)
            ).await.unwrap();
            let (_, payload) = value;
            let payload = payload.unwrap();
            proptest::prop_assert_eq!(payload.as_ref(), body.as_slice());
            proptest::prop_assert!(allocation.is_some());
            proptest::prop_assert!(original.live_allocation_bytes() >= baseline + body.len() as u64);
            let receipt = original.receipt();
            drop(payload);
            drop(allocation);
            proptest::prop_assert_eq!(original.live_allocation_bytes(), baseline);
            proptest::prop_assert_eq!(original.receipt(), receipt);
            Ok(())
        })?;
    }

    // Joint authentication preserves the complete fallback value/error for
    // arbitrary payloads and bounds, including genesis and empty payloads.
    #[test]
    fn joint_head_matches_fallback_for_arbitrary_payload_limits(
        payload in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..1025),
        maximum in 0u16..1030,
        genesis in proptest::prelude::any::<bool>(),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(async move {
            let store = created_with((!genesis).then(|| Bytes::from(payload))).await.unwrap();
            let cancellation = CancellationToken::new();
            let mut old = AuthorityWork::new(BUDGET, &cancellation);
            let expected = Fallback(&store).head_payload(AUTHORITY, u64::from(maximum), &mut old).await;
            let mut work = AuthorityWork::new(BUDGET, &cancellation);
            let actual = store.head_payload(AUTHORITY, u64::from(maximum), &mut work).await;
            match (actual, expected) {
                (Ok((actual, _)), Ok((expected, _))) => proptest::prop_assert_eq!(actual, expected),
                (Err(actual), Err(expected)) => proptest::prop_assert_eq!(actual.to_string(), expected.to_string()),
                (actual, expected) => proptest::prop_assert!(false, "actual={actual:?}, expected={expected:?}"),
            }
            proptest::prop_assert!(old.spent().verify(BUDGET).is_ok());
            proptest::prop_assert!(work.spent().verify(BUDGET).is_ok());
            Ok(())
        })?;
    }
}
