use super::*;


    use super::*;
    use crate::kernel::{VolumeCreated, encode_publication_payload, encode_volume_created};
    use crate::model::{Lifecycle, VolumeConfig};
    use crate::storage::ObjectKind;
    #[tokio::test]
    pub(super) async fn settlement_times_and_retired_accounting_use_original_stream_records() {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let authority_id = AuthorityId::from_bytes([0x71; 16]);
        let cancellation = CancellationToken::new();({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).expect("create authority");
        let head =({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect("head").value;
        let proposed = ProposedCommit {
            operation_id: OperationId::from_bytes([0x72; 16]),
            fingerprint: Digest::from_bytes([0x73; 32]),
            payload: Bytes::from_static(b"accounting settlement"),
        };
        let outcome =({ let __authority_args = (authority_id, head.epoch, head, proposed.clone(), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = authority.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).expect("append").value;
        let FsAppendOutcome::Committed(durable) = outcome else {
            panic!("expected committed record");
        };
        let record = read_one(stream.as_ref(), stream_path(authority_id, "/records").expect("path"), 1)
            .await.expect("exact authoritative record");
        assert_eq!(durable.settled_at_micros,
            (record.committed_at_micros != 0).then_some(record.committed_at_micros));
        let limit = ReplayLimit { records: 8, payload_bytes: 1_024 };
        assert_eq!(({ let __authority_args = (authority_id, Sequence::new(0), limit, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).expect("replay").value, vec![durable.clone()]);
        assert_eq!(({ let __authority_args = (authority_id, proposed.operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.find_operation(__authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).expect("find").value, Some(durable.clone()));
        assert_eq!(({ let __authority_args = (authority_id, head.epoch, head, proposed.clone(), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = authority.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).expect("retry").value, FsAppendOutcome::AlreadyCommitted(durable.clone()));

        let mut legacy = record.clone();
        legacy.committed_at_micros = 0;
        let borrowed = decode_record(&legacy).expect("legacy");
        validate_record(authority_id, &legacy, &borrowed).expect("legacy identity");
        let payload = Bytes::copy_from_slice(borrowed.payload);
        assert_eq!(durable_commit(borrowed, legacy.committed_at_micros, payload).settled_at_micros, None);({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.retire_authority(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect("retire");
        assert!(matches!(({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect_err("ordinary head stays retired").error, AuthorityStoreError::Retired));
        assert!(matches!(({ let __authority_args = (authority_id, Sequence::new(0), limit, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).expect_err("ordinary replay stays retired").error, AuthorityStoreError::Retired));
        assert!(matches!(({ let __authority_args = (authority_id, proposed.operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.find_operation(__authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).expect_err("ordinary operation lookup stays retired").error, AuthorityStoreError::Retired));
        assert!(matches!(({ let __authority_args = (authority_id, head.epoch, head, proposed, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.4, __authority_args.5); let __authority_result = authority.compare_and_append(crate::GuardedAppend { authority_id: __authority_args.0, epoch: __authority_args.1, expected: __authority_args.2, commit: __authority_args.3, permit: crate::PublicationPermit::Unrestricted }, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).expect_err("ordinary retry stays retired").error, AuthorityStoreError::Retired));
        let accounting = AccountingAuthority(&authority);
        assert_eq!(({ let __authority_args = (authority_id, Sequence::new(0), limit, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = accounting.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).expect("accounting retains retired history").value, vec![durable.clone()]);
        assert_eq!(({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = accounting.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect("historical head").value.sequence, durable.sequence);
    }

    #[cfg(not(target_arch = "wasm32"))]
    const NATIVE_AUTHORITY_BUDGET: WorkBudget = crate::performance::work_budget(1 << 28, 1 << 28);

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    pub(super) async fn original_append_owner_seals_after_planning_and_again_on_recovery()
        -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let store = StreamAuthorityStore::new(Arc::new(acyclic_stream::MemoryStream::default()));
        let authority = AuthorityId::from_bytes([0x86; 16]);({ let __authority_args = (authority, Epoch::GENESIS, NATIVE_AUTHORITY_BUDGET, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = store.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
        let original = crate::NativeMaterializationWork::new(NATIVE_AUTHORITY_BUDGET, &cancellation)
            .map_err(|(error, _)| error)?;
        let expected = Head::genesis(Epoch::GENESIS);
        let request = crate::GuardedAppend { authority_id: authority, epoch: expected.epoch, expected,
            commit: ProposedCommit { operation_id: OperationId::from_bytes([0x87; 16]),
                fingerprint: Digest::from_bytes([0x88; 32]), payload: Bytes::from_static(b"owner-sealed") },
            permit: PublicationPermit::Unrestricted };
        let expected_root = original.clone();
        let mut reject = move |work: &crate::NativeMaterializationWork| {
            assert!(work.shares_ledger(&expected_root));
            assert!(work.receipt().bytes_hashed > 0);
            work.admit(WorkCounters::UNCHARGED).expect("unlocked original root");
            std::future::ready(Err(AuthorityStoreError::Retired))
        };
        let failure =(async { let __authority_args = (request.clone(), &original, &cancellation, &mut reject,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = store.compare_and_append(__authority_args.0, &mut __authority_work, Some(&mut crate::NativeAuthorityGuard((__authority_args.3).clone()))).await.map_err(|error| error.into_error(std::convert::Into::into)).map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result }.await);
        assert!(matches!(failure, Err(AuthorityStoreError::Retired)));
        assert_eq!(original.receipt().backend_write_operations, 0);
        assert_eq!(({ let __authority_args = (authority, NATIVE_AUTHORITY_BUDGET, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = store.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value, expected);
        let (outcome, allocation) =(async { let __authority_args = (request.clone(), &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = store.compare_and_append(__authority_args.0, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result }.await)?;
        assert!(matches!(outcome, FsAppendOutcome::Committed(_)));
        drop(outcome);
        drop(allocation);
        let before = original.receipt();
        let failure =(async { let __authority_args = (request, &original, &cancellation, &mut reject,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = store.compare_and_append(__authority_args.0, &mut __authority_work, Some(&mut crate::NativeAuthorityGuard((__authority_args.3).clone()))).await.map_err(|error| error.into_error(std::convert::Into::into)).map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result }.await);
        assert!(matches!(failure, Err(AuthorityStoreError::Retired)));
        assert_eq!(original.receipt().backend_write_operations, before.backend_write_operations);
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
        let original = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let before = original.receipt();
        let first = ProposedCommit { operation_id: OperationId::from_bytes([0x8a; 16]),
            fingerprint: Digest::from_bytes([0x8b; 32]), payload: Bytes::from_static(b"denied") };
        let failure =(async { let __authority_args = (authority, first.clone(), &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = store.create_first_record(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_result }.await);
        assert!(matches!(failure, Err(AuthorityStoreError::NativeOriginal(
            crate::NativeTreeMaterializationError::Work(_)))));
        assert_eq!(original.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(original.receipt().backend_write_operations, 0);
        assert_eq!(original.receipt().authority_records_appended, 0);
        cancellation.cancel();
        let spent = original.receipt();
        let failure =(async { let __authority_args = (authority, first, &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = store.create_first_record(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_result }.await);
        assert!(matches!(failure, Err(AuthorityStoreError::Cancelled)
            | Err(AuthorityStoreError::NativeOriginal(crate::NativeTreeMaterializationError::Cancelled))));
        assert_eq!(original.receipt(), spent);
        let fresh = CancellationToken::new();
        assert!(matches!(({ let __authority_args = (authority, NATIVE_AUTHORITY_BUDGET, &fresh,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = store.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }),
            Err(failure) if matches!(failure.error, AuthorityStoreError::Missing)));
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    pub(super) async fn original_authority_custom_defaults_never_fall_back_to_ordinary_io()
        -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering;
        let cancellation = CancellationToken::new();
        let provider = Arc::new(ReleaseSpy::new(Arc::new(acyclic_stream::MemoryStream::default())));
        let store = StreamAuthorityStore::new(Arc::clone(&provider));
        let original = crate::NativeMaterializationWork::new(NATIVE_AUTHORITY_BUDGET, &cancellation)
            .map_err(|(error, _)| error)?;
        let authority = AuthorityId::from_bytes([0x7a; 16]);
        assert!(matches!(&(async { let __authority_args = (authority, &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.1, __authority_args.2); let __authority_result = store.head(__authority_args.0, &mut __authority_work).await; __authority_result }.await),
            Err(AuthorityStoreError::NativeStream(error))
                if matches!(error, acyclic_stream::StreamError::Unsupported)));
        assert!(matches!(&(async { let __authority_args = (authority, Sequence::GENESIS, ReplayLimit { records: 1, payload_bytes: 8 }, &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.3, __authority_args.4); let __authority_result = store.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result }.await),
            Err(AuthorityStoreError::NativeStream(error))
                if matches!(error, acyclic_stream::StreamError::Unsupported)));
        assert!(matches!(&(async { let __authority_args = (authority, Head::genesis(Epoch::GENESIS), &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = store.fence(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_result }.await), Err(AuthorityStoreError::NativeStream(error))
                if matches!(error, acyclic_stream::StreamError::Unsupported)));
        assert!(matches!(&(async { let __authority_args = (authority, OperationId::from_bytes([0x7b; 16]), &original, &cancellation,); let mut __authority_work = crate::AuthorityWork::native(__authority_args.2, __authority_args.3); let __authority_result = store.find_operation(__authority_args.0, __authority_args.1, &mut __authority_work).await.map(|(value, allocation)| (value, allocation.expect("native authority allocation"))); __authority_result }.await), Err(AuthorityStoreError::NativeStream(error))
                if matches!(error, acyclic_stream::StreamError::Unsupported)));
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
        for (suffix, domain) in [("", AUTHORITY_MARKER), ("/records", GENESIS_DOMAIN), ("/epochs", EPOCH_DOMAIN)] {
            let path = stream_path(authority_id, suffix)?;
            let value = if suffix.is_empty() { acyclic_stream::OwnedBytes::from_static(domain) }
                else { [domain, epoch.as_slice()].concat().into() };
            conditions.push(acyclic_stream::CommitCondition::Absent { path: path.clone() });
            mutations.push(acyclic_stream::CommitMutation::Append { path, records: vec![value] });
        }
        acyclic_stream::StreamProvider::commit(stream.as_ref(), acyclic_stream::CommitRequest {
            conditions, mutations, idempotency_key: stream_key(b"gateless-authority", &authority_id.into_bytes())?,
        }).await?;
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let refused =({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) }).expect_err("an authority without its publication gate is refused");
        assert!(matches!(refused.error, AuthorityStoreError::Corrupt(_)));
        assert!(matches!(acyclic_stream::StreamProvider::tail(stream.as_ref(), stream_path(authority_id, "/publication-gate")?).await,
            Err(acyclic_stream::StreamError::NotFound)));
        Ok(())
    }

    #[tokio::test]
    pub(super) async fn existing_authority_rejects_a_corrupt_publication_gate() {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let authority_id = AuthorityId::from_bytes([0x35; 16]);
        let cancellation = CancellationToken::new();({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })
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
                    records: vec![acyclic_stream::OwnedBytes::from_static(b"invalid-publication-gate")],
                }],
                idempotency_key: stream_key(
                    b"corrupt-publication-gate",
                    &authority_id.into_bytes(),
                )
                .expect("corruption key"),
            },
        )
        .await
        .expect("append invalid gate record");

        let error =({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })
            .expect_err("corrupt gate must fail authority reopen");
        assert!(matches!(error.error, AuthorityStoreError::Corrupt(_)));
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
        let retry =({ let __authority_args = (authority_id, head, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.reserve_publication(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
        assert!(matches!(retry, ReservationOutcome::AlreadyReserved(value) if value == reservation));
        let request = crate::GuardedAppend {
            authority_id, epoch: head.epoch, expected: head,
            commit: ProposedCommit {
                operation_id: OperationId::from_bytes([0x43; 16]),
                fingerprint: Digest::from_bytes([0x44; 32]),
                payload: Bytes::from_static(b"reserved write"),
            },
            permit: PublicationPermit::Unrestricted,
        };
        let ordinary =({ let __authority_args = (request.clone(), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.compare_and_append(__authority_args.0, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?.value;
        assert!(matches!(ordinary, FsAppendOutcome::Fenced { .. }));
        let reserved =({ let __authority_args = (crate::GuardedAppend {
            permit: PublicationPermit::Reservation {
                operation_id: operation_id.into_bytes(), gate_tail: reservation.gate_tail,
                expected: reservation.expected,
            },
            ..request
        }, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.compare_and_append(__authority_args.0, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?.value;
        assert!(matches!(reserved, FsAppendOutcome::Committed(_)));
        let changed =({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
        let stale =({ let __authority_args = (authority_id, head, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.reserve_publication(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
        assert!(matches!(stale, ReservationOutcome::Conflict { actual } if actual == changed));({ let __authority_args = (authority_id, changed, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.reserve_publication(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect_err("an active operation must not adopt a different source head");
        let denied_release =({ let __authority_args = (reservation, WorkBudget::default(), &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect_err("release must charge its idempotency read before doing work");
        assert!(matches!(denied_release.error, AuthorityStoreError::Work(_)));({ let __authority_args = (reservation, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?;({ let __authority_args = (reservation, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?;
        let current =({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
        let reacquired =match ({ let __authority_args = (authority_id, current, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.reserve_publication(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value {
            ReservationOutcome::Reserved(reservation) => reservation,
            outcome => panic!("unexpected reacquisition outcome: {outcome:?}"),
        };
        let forged = PublicationReservation {
            operation_id: OperationId::from_bytes([0x99; 16]), ..reacquired
        };
        assert!(({ let __authority_args = (forged, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).is_err());
        let gate = stream_path(authority_id, "/publication-gate")?;
        let advanced = acyclic_stream::StreamProvider::commit(stream.as_ref(), acyclic_stream::CommitRequest {
            conditions: vec![acyclic_stream::CommitCondition::Tail {
                path: gate.clone(), expected: reacquired.gate_tail,
            }],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: gate.clone(), records: vec![acyclic_stream::OwnedBytes::from([PUBLICATION_GATE_ACTIVE, &reacquired.operation_id.into_bytes()].concat())],
            }],
            idempotency_key: stream_key(b"advance-active-gate", &authority_id.into_bytes())?,
        }).await?;
        assert!(matches!(advanced, acyclic_stream::CommitOutcome::Committed(_)));
        let release_key = stream_key(
            b"release-publication", &[reacquired.authority_id.into_bytes().as_slice(),
                reacquired.operation_id.into_bytes().as_slice(), reacquired.gate_tail.to_le_bytes().as_slice()].concat())?;
        let retained_conflict = acyclic_stream::StreamProvider::commit(stream.as_ref(), acyclic_stream::CommitRequest {
            conditions: vec![acyclic_stream::CommitCondition::Tail {
                path: gate.clone(), expected: reacquired.gate_tail,
            }],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: gate, records: vec![acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE)],
            }],
            idempotency_key: release_key,
        }).await?;
        assert!(matches!(retained_conflict, acyclic_stream::CommitOutcome::Conflict(_)));({ let __authority_args = (reacquired, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect_err("retained release conflict must not become success");
        let gate = stream_path(authority_id, "/publication-gate")?;
        let tail = acyclic_stream::StreamProvider::tail(stream.as_ref(), gate.clone()).await?;
        let record = read_one(stream.as_ref(), gate, tail - 1).await?;
        assert_eq!(decode_publication_gate(&record.value)?, Some(reacquired.operation_id));
        Ok(())
    }

    struct ReleaseSpy {
        inner: Arc<acyclic_stream::MemoryStream>,
        reads: std::sync::atomic::AtomicBool,
        writes: std::sync::atomic::AtomicBool,
        fail_inspect: bool,
        cancel_inspect: Option<CancellationToken>,
        lose_ack: bool,
        corrupt_observation: u8,
    }

    impl ReleaseSpy {
        fn new(inner: Arc<acyclic_stream::MemoryStream>) -> Self {
            Self {
                inner, reads: std::sync::atomic::AtomicBool::new(false),
                writes: std::sync::atomic::AtomicBool::new(false),
                fail_inspect: false, cancel_inspect: None, lose_ack: false,
                corrupt_observation: 0,
            }
        }
    }

    #[async_trait::async_trait]
    impl acyclic_stream::StreamProvider for ReleaseSpy {
        async fn inspect_idempotency(&self, key: acyclic_stream::IdempotencyKey)
            -> Result<Option<acyclic_stream::IdempotencyObservation>, acyclic_stream::StreamError> {
            self.reads.store(true, std::sync::atomic::Ordering::SeqCst);
            if self.fail_inspect { return Err(acyclic_stream::StreamError::Unavailable); }
            let mut result = self.inner.inspect_idempotency(key).await?;
            if let Some(observation) = result.as_mut() {
                if self.corrupt_observation == 1 {
                    observation.idempotency_key = stream_key(b"another-release", b"identity")
                        .map_err(|_| acyclic_stream::StreamError::InvalidArgument)?;
                }
                if let acyclic_stream::IdempotencyOutcome::Commit(
                    acyclic_stream::CommitOutcome::Committed(envelope),
                ) = &mut observation.outcome {
                    if let Some(acyclic_stream::CommittedMutation::Append(append)) = envelope.mutations.first_mut() {
                        match self.corrupt_observation {
                            2 => append.path = acyclic_stream::StreamPath::new("other/gate")?,
                            3 => append.start += 1,
                            4 => append.end += 1,
                            5 => append.tail += 1,
                            6 => append.records[0].sequence += 1,
                            7 => append.records[0].commit_id = acyclic_stream::CommitId::from_bytes([0xff; 32]),
                            8 => append.records[0].value = acyclic_stream::OwnedBytes::from_static(b"not-free"),
                            9 => append.records.push(append.records[0].clone()),
                            10 => observation.request_digest = [0; 32],
                            _ => {}
                        }
                    }
                }
            }
            if let Some(cancellation) = &self.cancel_inspect { cancellation.cancel(); }
            Ok(result)
        }
        async fn tail(&self, path: acyclic_stream::StreamPath) -> Result<u64, acyclic_stream::StreamError> {
            self.reads.store(true, std::sync::atomic::Ordering::SeqCst);
            self.inner.tail(path).await
        }
        async fn read(&self, request: acyclic_stream::ReadRequest)
            -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> {
            self.reads.store(true, std::sync::atomic::Ordering::SeqCst);
            self.inner.read(request).await
        }
        async fn commit(&self, request: acyclic_stream::CommitRequest)
            -> Result<acyclic_stream::CommitOutcome, acyclic_stream::StreamError> {
            self.writes.store(true, std::sync::atomic::Ordering::SeqCst);
            let result = self.inner.commit(request).await?;
            if self.lose_ack { Err(acyclic_stream::StreamError::Unavailable) } else { Ok(result) }
        }
        async fn bounds(&self, path: acyclic_stream::StreamPath)
            -> Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError> { self.inner.bounds(path).await }
        async fn append(&self, request: acyclic_stream::AppendRequest)
            -> Result<acyclic_stream::AppendOutcome, acyclic_stream::StreamError> { self.inner.append(request).await }
        async fn fork(&self, request: acyclic_stream::ForkRequest)
            -> Result<acyclic_stream::ForkReceipt, acyclic_stream::StreamError> { self.inner.fork(request).await }
        async fn follow(&self, path: acyclic_stream::StreamPath, from: u64)
            -> Result<acyclic_stream::RecordStream, acyclic_stream::StreamError> { self.inner.follow(path, from).await }
        async fn children(&self, request: acyclic_stream::ChildrenRequest)
            -> Result<acyclic_stream::ChildStream, acyclic_stream::StreamError> { self.inner.children(request).await }
        async fn read_commit(&self, id: acyclic_stream::CommitId)
            -> Result<acyclic_stream::CommittedEnvelope, acyclic_stream::StreamError> { self.inner.read_commit(id).await }
    }

    pub(super) async fn release_fixture()
        -> Result<(Arc<acyclic_stream::MemoryStream>, PublicationReservation), Box<dyn std::error::Error>> {
        let inner = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&inner));
        let cancellation = CancellationToken::new();
        let id = AuthorityId::from_bytes([0xd1; 16]);({ let __authority_args = (id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
        let head =({ let __authority_args = (id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
        let outcome =({ let __authority_args = (id, head, OperationId::from_bytes([0xd2; 16]), WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.reserve_publication(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
        let ReservationOutcome::Reserved(reservation) = outcome else { return Err("reservation denied".into()); };
        Ok((inner, reservation))
    }

    #[tokio::test]
    pub(super) async fn release_lost_ack_recovers_original_envelope_after_gate_advance()
        -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering;
        let (inner, reservation) = release_fixture().await?;
        let mut spy = ReleaseSpy::new(Arc::clone(&inner));
        spy.lose_ack = true;
        let spy = Arc::new(spy);
        let failure =({ let __authority_args = (reservation, WorkBudget::UNBOUNDED, &CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = StreamAuthorityStore::new(Arc::clone(&spy)).release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
            .expect_err("provider loses release acknowledgement");
        assert!(spy.writes.load(Ordering::SeqCst));
        assert_eq!(failure.work.durability_operations, 0);
        assert_eq!(failure.work.authority_bytes_written, 0);
        let authority = StreamAuthorityStore::new(Arc::clone(&inner));
        let new_operation = OperationId::from_bytes([0xd3; 16]);
        let outcome =({ let __authority_args = (reservation.authority_id, reservation.expected, new_operation, WorkBudget::UNBOUNDED, &CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.reserve_publication(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
        assert!(matches!(outcome, ReservationOutcome::Reserved(_)));
        let spy = Arc::new(ReleaseSpy::new(Arc::clone(&inner)));
        let receipt =({ let __authority_args = (reservation, WorkBudget::UNBOUNDED, &CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = StreamAuthorityStore::new(Arc::clone(&spy)).release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?;
        assert!(!spy.writes.load(Ordering::SeqCst));
        assert_eq!(receipt.work.authority_records_read, 1);
        assert_eq!(receipt.work.authority_bytes_read, PUBLICATION_GATE_FREE.len() as u64);
        assert_eq!(receipt.work.backend_write_operations, 0);
        let gate = stream_path(reservation.authority_id, "/publication-gate")?;
        let tail = acyclic_stream::StreamProvider::tail(inner.as_ref(), gate.clone()).await?;
        let record = read_one(inner.as_ref(), gate, tail - 1).await?;
        assert_eq!(decode_publication_gate(&record.value)?, Some(new_operation));
        for corrupt in 1..=10 {
            let mut spy = ReleaseSpy::new(Arc::clone(&inner));
            spy.corrupt_observation = corrupt;
            let spy = Arc::new(spy);
            let failure =({ let __authority_args = (reservation, WorkBudget::UNBOUNDED, &CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = StreamAuthorityStore::new(Arc::clone(&spy)).release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
                .expect_err("retained observation must bind exact original release");
            assert!(matches!(failure.error, AuthorityStoreError::Corrupt(_)));
            assert!(!spy.writes.load(Ordering::SeqCst));
        }
        let cancellation = CancellationToken::new();
        let mut spy = ReleaseSpy::new(inner);
        spy.cancel_inspect = Some(cancellation.clone());
        let failure =({ let __authority_args = (reservation, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = StreamAuthorityStore::new(Arc::new(spy)).release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
            .expect_err("cancellation retains the actual returned observation prefix");
        assert!(matches!(failure.error, AuthorityStoreError::Cancelled));
        assert_eq!(failure.work.authority_records_read, 1);
        assert_eq!(failure.work.authority_bytes_read, PUBLICATION_GATE_FREE.len() as u64);
        Ok(())
    }

    #[tokio::test]
    pub(super) async fn release_rejects_committed_wrong_request_and_mutation_family()
        -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering;
        for wrong_family in [false, true] {
            let (inner, reservation) = release_fixture().await?;
            let gate = stream_path(reservation.authority_id, "/publication-gate")?;
            let key = stream_key(b"release-publication", &[reservation.authority_id.into_bytes().as_slice(), reservation.operation_id.into_bytes().as_slice(), reservation.gate_tail.to_le_bytes().as_slice()].concat())?;
            if wrong_family {
                let outcome = acyclic_stream::StreamProvider::append(inner.as_ref(), acyclic_stream::AppendRequest {
                    path: gate, records: vec![acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE)],
                    if_tail: Some(reservation.gate_tail), idempotency_key: Some(key),
                }).await?;
                assert!(matches!(outcome, acyclic_stream::AppendOutcome::Committed(_)));
            } else {
                let outcome = acyclic_stream::StreamProvider::commit(inner.as_ref(), acyclic_stream::CommitRequest {
                    conditions: vec![acyclic_stream::CommitCondition::Tail {
                        path: gate.clone(), expected: reservation.gate_tail,
                    }],
                    mutations: vec![acyclic_stream::CommitMutation::Append {
                        path: gate, records: vec![acyclic_stream::OwnedBytes::from_static(PUBLICATION_GATE_FREE)],
                    }],
                    idempotency_key: key,
                }).await?;
                assert!(matches!(outcome, acyclic_stream::CommitOutcome::Committed(_)));
            }
            let spy = Arc::new(ReleaseSpy::new(inner));
            let failure =({ let __authority_args = (reservation, WorkBudget::UNBOUNDED, &CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = StreamAuthorityStore::new(Arc::clone(&spy)).release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
                .expect_err("arbitrary retained success must not release this reservation");
            assert!(matches!(failure.error, AuthorityStoreError::Corrupt(_)));
            assert!(!spy.writes.load(Ordering::SeqCst));
        }
        let (inner, reservation) = release_fixture().await?;
        let receipt =({ let __authority_args = (reservation, WorkBudget::UNBOUNDED, &CancellationToken::new(),); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = StreamAuthorityStore::new(inner).release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?;
        assert_eq!(receipt.work.authority_bytes_written, PUBLICATION_GATE_FREE.len() as u64);
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
        let creation = encode_volume_created(VolumeCreated {
            volume_id,
            config: VolumeConfig::portable(Lifecycle::Ephemeral),
            initial_generation_root: root,
        }, |_| Ok::<_, crate::kernel::VolumeCreatedError>(()), |_| crate::kernel::VolumeCreatedError::Decode(crate::kernel::CanonicalDecodeError::AllocationFailed))?;
        let publication = encode_publication_payload(volume_id, root);
        let generation = GenerationId::new(root.digest);
        let cancellation = CancellationToken::new();
        let mut meter = ForkWork::new(WorkBudget::UNBOUNDED, &cancellation);

        let foreign = AuthorityId::from_bytes([0x73; 16]);
        for (authority, sequence, payload, expected) in [
            (authority_id, 1, creation.as_slice(), Some(generation)),
            (authority_id, 2, publication.as_slice(), Some(generation)),
            (authority_id, 2, creation.as_slice(), None),
            (authority_id, 1, publication.as_slice(), None),
            (foreign, 1, creation.as_slice(), None),
            (foreign, 2, publication.as_slice(), None),
        ] {
            let result = generation_from_payload(authority, Sequence::new(sequence), payload, &mut meter);
            if let Some(expected) = expected {
                assert_eq!(result?, Some(expected));
            } else { assert!(result.is_err()); }
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
        let root = ObjectId { kind: ObjectKind::GenerationRoot, digest: Digest::from_bytes([0x92; 32]) };
        let source_payload = encode_volume_created(VolumeCreated {
            volume_id: volume, config: VolumeConfig::portable(Lifecycle::Ephemeral), initial_generation_root: root,
        }, |_| Ok::<_, crate::kernel::VolumeCreatedError>(()), |_| crate::kernel::VolumeCreatedError::Decode(crate::kernel::CanonicalDecodeError::AllocationFailed))?;({ let __authority_args = (source_authority, ProposedCommit {
            operation_id: OperationId::from_bytes([0x93; 16]),
            fingerprint: Digest::from_bytes([0x94; 32]), payload: Bytes::from(source_payload),
        }, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_first_record(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
        let source = crate::GenerationForkSource {
            authority: source_authority, generation: GenerationId::new(root.digest),
            lineage: crate::GenerationFork::PublishedPrefix,
        };
        let destination = AuthorityId::from_bytes([0x95; 16]);
        let operation = OperationId::from_bytes([0x96; 16]);
        let budget = WorkBudget { allocation_operations: 10_000, ..WorkBudget::UNBOUNDED };
        let mut guard = |remaining: WorkBudget| {
            assert!(remaining.allocation_operations < budget.allocation_operations);
            assert!(remaining.backend_read_operations < budget.backend_read_operations);
            std::future::ready(Err::<crate::FsReceipt<()>, _>(OperationFailure::new(
                crate::FsError::WorkspaceDeleted,
                WorkCounters { items_examined: 1, ..WorkCounters::default() },
            )))
        };
        let failed =({ let __authority_args = (source, destination, operation, budget, &cancellation, &mut guard,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.fork_authority(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work, Some(&mut (&mut *__authority_args.5))).await.map_err(|error| error.into_error(crate::FsError::from)); __authority_work.receipt(__authority_result) }).expect_err("deleted destination owner seals after preparation");
        assert!(matches!(failed.error, crate::FsError::WorkspaceDeleted));
        assert!(failed.work.backend_read_operations > 0);
        assert!(failed.work.allocation_operations > 0);
        assert_eq!(failed.work.backend_write_operations, 0);
        assert!(matches!(acyclic_stream::StreamProvider::tail(
            stream.as_ref(), stream_path(destination, "")?,
        ).await, Err(acyclic_stream::StreamError::NotFound)));({ let __authority_args = (source, destination, operation, budget, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.fork_authority(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
        let retried =({ let __authority_args = (source, destination, operation, budget, &cancellation, &mut guard,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.fork_authority(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work, Some(&mut (&mut *__authority_args.5))).await.map_err(|error| error.into_error(crate::FsError::from)); __authority_work.receipt(__authority_result) }).expect_err("existing destination requires a fresh recovery seal");
        assert!(matches!(retried.error, crate::FsError::WorkspaceDeleted));
        assert!(retried.work.backend_read_operations > failed.work.backend_read_operations);
        assert_eq!(retried.work.backend_write_operations, 0);
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
        let budget = WorkBudget { allocation_operations: 10_000, ..WorkBudget::UNBOUNDED };
        let mut reject_prepared = |remaining: WorkBudget| {
            assert!(remaining.allocation_operations < budget.allocation_operations);
            assert!(remaining.bytes_hashed < budget.bytes_hashed);
            std::future::ready(Err::<crate::FsReceipt<()>, _>(OperationFailure::before_work(crate::FsError::WorkspaceDeleted)))
        };
        let rejected =({ let __authority_args = (destination, proposed(), budget, &cancellation, &mut reject_prepared,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_first_record(__authority_args.0, __authority_args.1, &mut __authority_work, Some(&mut (&mut *__authority_args.4))).await.map_err(|error| error.into_error(crate::FsError::from)); __authority_work.receipt(__authority_result) }).expect_err("owner rejected the actual prepared canonical request");
        assert!(matches!(rejected.error, crate::FsError::WorkspaceDeleted));
        assert!(rejected.work.bytes_encoded > 0);
        assert!(rejected.work.bytes_hashed > 0);
        assert_eq!(rejected.work.backend_write_operations, 0);
        assert!(matches!(acyclic_stream::StreamProvider::tail(
            stream.as_ref(), stream_path(destination, "")?,
        ).await, Err(acyclic_stream::StreamError::NotFound)));
        assert!(({ let __authority_args = (destination, proposed(), budget, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_first_record(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?.value);
        let mut seals = 0;
        let mut reject_recovery = |_| {
            seals += 1;
            std::future::ready(if seals == 1 {
                Ok(crate::OperationReceipt { value: (), work: WorkCounters::default() })
            } else {
                Err(OperationFailure::before_work(crate::FsError::WorkspaceDeleted))
            })
        };
        let recovered =({ let __authority_args = (destination, proposed(), budget, &cancellation, &mut reject_recovery,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_first_record(__authority_args.0, __authority_args.1, &mut __authority_work, Some(&mut (&mut *__authority_args.4))).await.map_err(|error| error.into_error(crate::FsError::from)); __authority_work.receipt(__authority_result) }).expect_err("retained ACK also requires the current owner seal");
        assert!(matches!(recovered.error, crate::FsError::WorkspaceDeleted));
        assert!(recovered.work.authority_records_appended > 0);
        Ok(())
    }
    #[tokio::test]
    pub(super) async fn readonly_reservation_recovers_original_head_after_authority_advance()
    -> Result<(), Box<dyn std::error::Error>> {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let authority = StreamAuthorityStore::new(Arc::clone(&stream));
        let cancellation = CancellationToken::new();
        let authority_id = AuthorityId::from_bytes([0xb1; 16]);
        let operation_id = OperationId::from_bytes([0xb2; 16]);({ let __authority_args = (authority_id, Epoch::GENESIS, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = authority.create_authority(__authority_args.0, __authority_args.1, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)); __authority_work.receipt(__authority_result) })?;
        let original_head =({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
        let reservation =match ({ let __authority_args = (authority_id, original_head, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.reserve_publication(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value {
            ReservationOutcome::Reserved(reservation) => reservation,
            other => panic!("unexpected acquisition: {other:?}"),
        };({ let __authority_args = (crate::GuardedAppend {
            authority_id, epoch: original_head.epoch, expected: original_head,
            commit: ProposedCommit {
                operation_id: OperationId::from_bytes([0xb3; 16]),
                fingerprint: Digest::from_bytes([0xb4; 32]),
                payload: Bytes::from_static(b"original-reserved-operation-effect"),
            },
            permit: PublicationPermit::Reservation {
                operation_id: operation_id.into_bytes(), gate_tail: reservation.gate_tail, expected: original_head,
            },
        }, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.compare_and_append(__authority_args.0, &mut __authority_work, None::<&mut ()>).await.map_err(|error| error.into_error(std::convert::identity)).map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) })?;
        let advanced_head =({ let __authority_args = (authority_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?.value;
        assert_ne!(advanced_head, original_head);
        let spy = Arc::new(ReleaseSpy::new(Arc::clone(&stream)));
        let queried = StreamAuthorityStore::new(Arc::clone(&spy));
        let denied =({ let __authority_args = (authority_id, operation_id, WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED }, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = queried.publication_reservation(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect_err("no path allocation admitted");
        assert!(matches!(denied.error, AuthorityStoreError::Work(_)));
        assert!(!spy.reads.load(std::sync::atomic::Ordering::SeqCst));
        let recovered =({ let __authority_args = (authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = queried.publication_reservation(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?;
        assert_eq!(recovered.value, Some(reservation));
        assert_eq!(recovered.value.expect("original evidence").expected, original_head);
        assert_eq!(recovered.work.backend_write_operations, 0);
        assert!(recovered.work.bytes_hashed > 0);
        assert!(!spy.writes.load(std::sync::atomic::Ordering::SeqCst));
        let refused =({ let __authority_args = (authority_id, operation_id, WorkBudget { bytes_hashed: 0, ..WorkBudget::UNBOUNDED }, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = queried.publication_reservation(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect_err("query proof hashing refused after actual reads");
        assert!(matches!(refused.error, AuthorityStoreError::Work(_)));
        assert!(refused.work.backend_read_operations > 0);
        assert_eq!(refused.work.bytes_hashed, 0);
        let mut corrupted = ReleaseSpy::new(Arc::clone(&stream));
        corrupted.corrupt_observation = 10;
        let uncertain =({ let __authority_args = (authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = StreamAuthorityStore::new(Arc::new(corrupted)).publication_reservation(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) }).expect_err("unqualified historical conditions cannot create a proof");
        assert!(matches!(uncertain.error, AuthorityStoreError::ReservationOriginalHeadUnavailable {
            authority_id: actual_authority, operation_id: actual_operation, gate_tail,
        } if actual_authority == authority_id && actual_operation == operation_id && gate_tail == reservation.gate_tail));
        assert_eq!(uncertain.work.backend_write_operations, 0);
        assert!(uncertain.work.bytes_hashed > 0);({ let __authority_args = (reservation, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.release_publication(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?;
        let inactive =({ let __authority_args = (authority_id, operation_id, WorkBudget::UNBOUNDED, &cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.2, __authority_args.3); let __authority_result = queried.publication_reservation(__authority_args.0, __authority_args.1, &mut __authority_work).await; __authority_work.receipt(__authority_result) })?;
        assert_eq!(inactive.value, None);
        assert_eq!(inactive.work.backend_write_operations, 0);
        assert!(!spy.writes.load(std::sync::atomic::Ordering::SeqCst));
        Ok(())
    }
