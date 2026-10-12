use super::*;
use crate::{AsyncObjectStore, Digest, ObjectKind, WorkError};
use wasm_bindgen::JsValue;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn obsolete_database_version_is_rejected_without_deletion() -> Result<(), JsValue> {
    const NAME: &str = "acyclic-fs-obsolete-schema-rejection";
    const SENTINELS: &str = "legacy-sentinel-records";
    const KEY: &str = "original-record";
    const VALUE: &[u8] = b"obsolete-version-4-persisted-record";
    delete_database(NAME).await?;
    let obsolete_version = DATABASE_VERSION + 3;
    let old = Database::open(NAME)
        .with_version(obsolete_version)
        .with_on_upgrade_needed(|_, database| {
            database.create_object_store(SENTINELS).build()?;
            Ok(())
        })
        .await
        .map_err(js_error)?;
    let write = old
        .transaction(SENTINELS)
        .with_mode(TransactionMode::Readwrite)
        .build()
        .map_err(js_error)?;
    write
        .object_store(SENTINELS)
        .map_err(js_error)?
        .put(JsValue::from(Uint8Array::from(VALUE)))
        .with_key(KEY)
        .primitive()
        .map_err(js_error)?
        .await
        .map_err(js_error)?;
    write.commit().await.map_err(js_error)?;
    old.close();
    let object_failure = IndexedDbObjectStore::open(NAME, 1_024)
        .await
        .err()
        .ok_or_else(|| js_error("obsolete object database was accepted"))?;
    let authority_failure = IndexedDbAuthorityStore::open(NAME, 1_024)
        .await
        .err()
        .ok_or_else(|| js_error("obsolete authority database was accepted"))?;
    for failure in [object_failure, authority_failure] {
        let exception = JsValue::from(failure)
            .dyn_into::<web_sys::DomException>()
            .map_err(|_| js_error("version rejection lost its original DOM exception"))?;
        assert_eq!(exception.name(), "VersionError");
    }
    // The rejected version-4 database must remain intact.
    let retained = Database::open(NAME)
        .with_version(obsolete_version)
        .with_on_upgrade_needed(|_, _| {
            Err(indexed_db_futures::error::Error::from(js_sys::Error::new(
                "rejected database must not be recreated",
            )))
        })
        .await
        .map_err(js_error)?;
    assert_eq!(retained.version(), f64::from(obsolete_version));
    let read = retained.transaction(SENTINELS).build().map_err(js_error)?;
    let unchanged = read
        .object_store(SENTINELS)
        .map_err(js_error)?
        .get::<JsValue, _, _>(KEY)
        .primitive()
        .map_err(js_error)?
        .await
        .map_err(js_error)?;
    let unchanged = unchanged.ok_or_else(|| js_error("obsolete sentinel was deleted"))?;
    assert_eq!(Uint8Array::new(&unchanged).to_vec().as_slice(), VALUE);
    drop(read);
    retained.close();
    delete_database(NAME).await?;
    Ok(())
}

#[wasm_bindgen_test]
async fn indexed_db_objects_are_bounded_idempotent_and_authenticated() -> Result<(), JsValue> {
    const DATABASE_NAME: &str = "acyclic-fs-object-conformance-v2";
    delete_database(DATABASE_NAME).await?;
    let store = IndexedDbObjectStore::open(DATABASE_NAME, 1_024)
        .await
        .map_err(js_error)?;
    let cancellation = CancellationToken::new();
    let input = b"browser-object-vector".to_vec();
    let bytes = OwnedBytes::from(input);
    let object_id = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, &bytes),
    };
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    store
        .put(object_id, bytes.clone(), &mut work)
        .await
        .map_err(js_error)?;
    assert_eq!(work.spent().object_bytes_written, bytes.len() as u64);
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    store
        .put(object_id, bytes.clone(), &mut work)
        .await
        .map_err(js_error)?;
    assert_eq!(work.spent().object_bytes_written, 0);
    assert_eq!(work.spent().object_bytes_read, bytes.len() as u64);
    let below_retry_peak = WorkBudget {
        peak_allocation_bytes: work.spent().peak_allocation_bytes - 1,
        ..WorkBudget::UNBOUNDED
    };
    let mut work = crate::AuthorityWork::new(below_retry_peak, &cancellation);
    let denied = store
        .put(object_id, bytes.clone(), &mut work)
        .await
        .err()
        .ok_or_else(|| js_error("retained existing Blob handle escaped admission"))?;
    assert!(matches!(
        denied,
        ObjectStoreError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            ..
        })
    ));
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let read = store
        .read(object_id, 1_024, &mut work)
        .await
        .map_err(js_error)?;
    assert_eq!(read.bytes, bytes);
    assert_eq!(
        read.retention,
        ObjectReadRetention::Owned {
            logical_bytes: bytes.len() as u64
        }
    );
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    assert!(
        store
            .contains(object_id, &mut work)
            .await
            .map_err(js_error)?
    );
    let one_byte_short = WorkBudget {
        peak_allocation_bytes: OBJECT_KEY_BYTES + 2 * bytes.len() as u64 - 1,
        ..WorkBudget::UNBOUNDED
    };
    let mut work = crate::AuthorityWork::new(one_byte_short, &cancellation);
    let bounded = store
        .read(object_id, 1_024, &mut work)
        .await
        .err()
        .ok_or_else(|| {
            js_error("one-byte-short browser allocation budget unexpectedly succeeded")
        })?;
    assert!(matches!(
        bounded,
        ObjectStoreError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            ..
        })
    ));
    let forged = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: Digest::from_bytes([9; 32]),
    };
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let mismatch = store
        .put(forged, OwnedBytes::from_static(b"forged"), &mut work)
        .await
        .err()
        .ok_or_else(|| js_error("forged browser object unexpectedly succeeded"))?;
    assert!(matches!(mismatch, ObjectStoreError::DigestMismatch));
    Ok(())
}

fn blob_write(bytes: &'static [u8]) -> ObjectWrite {
    let bytes = OwnedBytes::from_static(bytes);
    ObjectWrite {
        object_id: ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &bytes),
        },
        bytes,
    }
}

#[wasm_bindgen_test]
async fn indexed_db_object_batches_commit_all_or_nothing() -> Result<(), JsValue> {
    const DATABASE_NAME: &str = "acyclic-fs-object-batch-v1";
    delete_database(DATABASE_NAME).await?;
    let store = IndexedDbObjectStore::open(DATABASE_NAME, 1_024)
        .await
        .map_err(js_error)?;
    let cancellation = CancellationToken::new();
    let first = blob_write(b"first batch object");
    let second = blob_write(b"second batch object");
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    store
        .put_many(&[first.clone(), second.clone()], &mut work)
        .await
        .map_err(js_error)?;
    assert_eq!(
        work.spent().object_bytes_written,
        (first.bytes.len() + second.bytes.len()) as u64
    );
    let third = blob_write(b"third batch object");
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    store
        .put_many(&[first.clone(), third.clone()], &mut work)
        .await
        .map_err(js_error)?;
    assert_eq!(work.spent().object_bytes_written, third.bytes.len() as u64);
    assert_eq!(work.spent().object_bytes_read, first.bytes.len() as u64);
    let requests =
        [first.clone(), second.clone(), third.clone()].map(|write| crate::ObjectReadRequest {
            object_id: write.object_id,
            maximum_bytes: 1_024,
        });
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let (batch, _) = store
        .read_many(&requests, &mut work)
        .await
        .map_err(js_error)?;
    assert_eq!(
        batch
            .iter()
            .map(|read| read.bytes.clone())
            .collect::<Vec<_>>(),
        vec![
            first.bytes.clone(),
            second.bytes.clone(),
            third.bytes.clone()
        ]
    );
    let below_peak = WorkBudget {
        peak_allocation_bytes: work.spent().peak_allocation_bytes - 1,
        ..WorkBudget::UNBOUNDED
    };
    let mut work = crate::AuthorityWork::new(below_peak, &cancellation);
    let rejected = store
        .read_many(&requests, &mut work)
        .await
        .err()
        .ok_or_else(|| js_error("batch retained peak escaped admission"))?;
    assert!(matches!(
        rejected,
        ObjectStoreError::Work(WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            ..
        })
    ));
    let reopened = IndexedDbObjectStore::open(DATABASE_NAME, 1_024)
        .await
        .map_err(js_error)?;
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let (persisted, _) = reopened
        .read_many(&requests, &mut work)
        .await
        .map_err(js_error)?;
    assert_eq!(persisted, batch);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancelled);
    let rejected = reopened
        .read_many(&requests, &mut work)
        .await
        .err()
        .ok_or_else(|| js_error("cancelled batch unexpectedly materialized"))?;
    assert!(matches!(rejected, ObjectStoreError::Cancelled));
    assert_eq!(work.spent(), WorkCounters::default());
    let fourth = blob_write(b"fourth batch object");
    let mut forged = blob_write(b"forged batch object");
    forged.object_id.digest = Digest::from_bytes([9; 32]);
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let rejected = store
        .put_many(&[fourth.clone(), forged], &mut work)
        .await
        .err()
        .ok_or_else(|| js_error("forged batch unexpectedly succeeded"))?;
    assert!(matches!(rejected, ObjectStoreError::DigestMismatch));
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    assert!(
        !store
            .contains(fourth.object_id, &mut work)
            .await
            .map_err(js_error)?
    );
    for write in [first, second, third] {
        let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let read = store
            .read(write.object_id, 1_024, &mut work)
            .await
            .map_err(js_error)?;
        assert_eq!(read.bytes, write.bytes);
    }
    Ok(())
}

#[wasm_bindgen_test]
async fn indexed_db_authority_is_atomic_fenced_idempotent_and_replayable() -> Result<(), JsValue> {
    let store = open_clean_authority("acyclic-fs-authority-conformance-v2").await?;
    let authority = AuthorityId::from_bytes([7; 16]);
    let cancellation = CancellationToken::new();
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let genesis = Head::genesis(Epoch::GENESIS);
    assert_eq!(
        store
            .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
            .await
            .map_err(js_error)?,
        CreateAuthorityOutcome::Created(genesis)
    );
    assert_eq!(
        store
            .create_authority(
                authority,
                Epoch::new(9).map_err(js_error)?,
                &mut work,
                None::<&mut ()>
            )
            .await
            .map_err(js_error)?,
        CreateAuthorityOutcome::Existing(genesis)
    );
    let mut denied = crate::AuthorityWork::new(
        WorkBudget {
            backend_read_operations: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    assert!(
        store
            .create_authority(authority, Epoch::GENESIS, &mut denied, None::<&mut ()>)
            .await
            .is_err()
    );

    let first = proposal(8, 9, Bytes::from_static(b"first"));
    let request = append_request(
        authority,
        genesis,
        first.clone(),
        PublicationPermit::Unrestricted,
    );
    let (AppendOutcome::Committed(first_commit), _) = store
        .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
        .await
        .map_err(js_error)?
    else {
        panic!("first append was not committed")
    };
    assert_eq!(
        store
            .compare_and_append(request, &mut work, None::<&mut ()>)
            .await
            .map_err(js_error)?
            .0,
        AppendOutcome::AlreadyCommitted(first_commit.clone())
    );
    let conflict = ProposedCommit {
        fingerprint: Digest::from_bytes([10; 32]),
        ..first
    };
    assert_eq!(
        store
            .compare_and_append(
                append_request(
                    authority,
                    genesis,
                    conflict,
                    PublicationPermit::Unrestricted
                ),
                &mut work,
                None::<&mut ()>
            )
            .await
            .map_err(js_error)?
            .0,
        AppendOutcome::IdempotencyConflict {
            committed_fingerprint: first_commit.fingerprint
        }
    );

    let second = append_request(
        authority,
        commit_head(&first_commit),
        proposal(11, 12, Bytes::from_static(b"second")),
        PublicationPermit::Unrestricted,
    );
    let (AppendOutcome::Committed(second_commit), _) = store
        .compare_and_append(second, &mut work, None::<&mut ()>)
        .await
        .map_err(js_error)?
    else {
        panic!("second append was not committed")
    };
    let replay_limit = ReplayLimit {
        records: 8,
        payload_bytes: 64,
    };
    let mut replay_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    assert_eq!(
        store
            .replay(authority, Sequence::GENESIS, replay_limit, &mut replay_work)
            .await
            .map_err(js_error)?
            .0,
        vec![first_commit.clone(), second_commit.clone()]
    );
    let retained_peak = 2 * size_of::<DurableCommit>() as u64
        + COMMIT_PREFIX_BYTES as u64
        + first_commit.payload.len() as u64
        + 2 * (COMMIT_PREFIX_BYTES as u64 + second_commit.payload.len() as u64);
    assert!(replay_work.spent().peak_allocation_bytes >= retained_peak);
    let mut short = crate::AuthorityWork::new(
        WorkBudget {
            peak_allocation_bytes: retained_peak - 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    assert!(
        store
            .replay(authority, Sequence::GENESIS, replay_limit, &mut short)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .replay(
                authority,
                first_commit.sequence,
                ReplayLimit {
                    records: 1,
                    payload_bytes: 16
                },
                &mut work
            )
            .await
            .map_err(js_error)?
            .0,
        vec![second_commit.clone()]
    );
    assert_eq!(
        store
            .find_operation(authority, first_commit.operation_id, &mut work)
            .await
            .map_err(js_error)?
            .0,
        Some(first_commit)
    );

    let FenceOutcome::Advanced(fenced) = store
        .fence(authority, commit_head(&second_commit), &mut work)
        .await
        .map_err(js_error)?
    else {
        panic!("fresh fence conflicted")
    };
    assert_eq!(fenced.epoch, Epoch::new(2).map_err(js_error)?);
    assert_eq!(fenced.sequence, second_commit.sequence);
    assert_eq!(fenced.digest, second_commit.digest);
    let stale = GuardedAppend {
        epoch: Epoch::GENESIS,
        ..append_request(
            authority,
            fenced,
            proposal(13, 14, Bytes::from_static(b"stale")),
            PublicationPermit::Unrestricted,
        )
    };
    assert_eq!(
        store
            .compare_and_append(stale, &mut work, None::<&mut ()>)
            .await
            .map_err(js_error)?
            .0,
        AppendOutcome::Fenced {
            actual_epoch: fenced.epoch
        }
    );
    let mut bounded = crate::AuthorityWork::new(
        WorkBudget {
            durability_operations: 1,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let head = store
        .head(authority, &mut bounded)
        .await
        .map_err(js_error)?;
    let FenceOutcome::Advanced(next) = store
        .fence(authority, head, &mut bounded)
        .await
        .map_err(js_error)?
    else {
        panic!("first fence must advance")
    };
    assert!(matches!(
        store.fence(authority, next, &mut bounded).await,
        Err(AuthorityStoreError::Work(_))
    ));
    assert_eq!(bounded.spent().durability_operations, 1);
    assert_eq!(
        store.head(authority, &mut work).await.map_err(js_error)?,
        next
    );
    Ok(())
}

#[wasm_bindgen_test]
async fn indexed_db_publication_reservations_are_atomic_and_idempotent() -> Result<(), JsValue> {
    let store = open_clean_authority("acyclic-fs-authority-reservation-v3").await?;
    let authority = AuthorityId::from_bytes([31; 16]);
    let cancellation = CancellationToken::new();
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let genesis = Head::genesis(Epoch::GENESIS);
    store
        .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await
        .map_err(js_error)?;
    let operation = OperationId::from_bytes([32; 16]);
    let ReservationOutcome::Reserved(reservation) = store
        .reserve_publication(authority, genesis, operation, &mut work)
        .await
        .map_err(js_error)?
    else {
        panic!("publication gate was not reserved")
    };
    assert_eq!(
        store
            .reserve_publication(authority, genesis, operation, &mut work)
            .await
            .map_err(js_error)?,
        ReservationOutcome::AlreadyReserved(reservation)
    );
    let proposal = proposal(33, 34, Bytes::from_static(b"reserved"));
    let request = append_request(
        authority,
        genesis,
        proposal,
        PublicationPermit::Unrestricted,
    );
    assert!(matches!(
        store
            .compare_and_append(request.clone(), &mut work, None::<&mut ()>)
            .await
            .map_err(js_error)?
            .0,
        AppendOutcome::Fenced { .. }
    ));
    let request = GuardedAppend {
        permit: PublicationPermit::Reservation {
            operation_id: operation.into_bytes(),
            gate_tail: reservation.gate_tail,
            expected: genesis,
        },
        ..request
    };
    assert!(matches!(
        store
            .compare_and_append(request, &mut work, None::<&mut ()>)
            .await
            .map_err(js_error)?
            .0,
        AppendOutcome::Committed(_)
    ));
    store
        .release_publication(reservation, &mut work)
        .await
        .map_err(js_error)?;
    store
        .release_publication(reservation, &mut work)
        .await
        .map_err(js_error)?;
    assert!(
        store
            .release_publication(
                PublicationReservation {
                    operation_id: OperationId::from_bytes([99; 16]),
                    ..reservation
                },
                &mut work
            )
            .await
            .is_err()
    );
    Ok(())
}

#[wasm_bindgen_test]
async fn indexed_db_authority_serializes_competing_connections() -> Result<(), JsValue> {
    const NAME: &str = "acyclic-fs-authority-race-v2";
    let creator = open_clean_authority(NAME).await?;
    let authority = AuthorityId::from_bytes([21; 16]);
    let cancellation = CancellationToken::new();
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    creator
        .create_authority(authority, Epoch::GENESIS, &mut work, None::<&mut ()>)
        .await
        .map_err(js_error)?;
    let left = IndexedDbAuthorityStore::open(NAME, 1_024)
        .await
        .map_err(js_error)?;
    let right = IndexedDbAuthorityStore::open(NAME, 1_024)
        .await
        .map_err(js_error)?;
    let append = |store: IndexedDbAuthorityStore, seed| async move {
        let cancellation = CancellationToken::new();
        let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let request = append_request(
            authority,
            Head::genesis(Epoch::GENESIS),
            proposal(seed, seed, Bytes::from_static(b"race")),
            PublicationPermit::Unrestricted,
        );
        store
            .compare_and_append(request, &mut work, None::<&mut ()>)
            .await
            .map(|(outcome, _)| outcome)
    };
    let (left, right) = futures::join!(append(left, 22), append(right, 23));
    let outcomes = [left.map_err(js_error)?, right.map_err(js_error)?];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AppendOutcome::Committed(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, AppendOutcome::Conflict { .. }))
            .count(),
        1
    );
    Ok(())
}

#[wasm_bindgen_test]
async fn indexed_db_forks_and_creates_in_one_transaction_and_resolves_retries()
-> Result<(), JsValue> {
    let store = open_clean_authority("acyclic-fs-authority-fork-v1").await?;
    let cancellation = CancellationToken::new();
    let mut work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let proposal = |seed| proposal(seed, seed, Bytes::from(vec![seed; 8]));
    let fork = WorkspaceForkCommit {
        lineage: None,
        destination: AuthorityId::from_bytes([31; 16]),
        creation: proposal(32),
        retention: AuthorityId::from_bytes([33; 16]),
        retained: proposal(34),
    };
    assert_eq!(
        store
            .fork_workspace(
                fork.clone(),
                PublicationPermit::Unrestricted,
                &mut work,
                None::<&mut ()>
            )
            .await
            .map_err(js_error)?,
        WorkspaceForkOutcome::Committed
    );
    for (authority, commit) in [
        (fork.destination, &fork.creation),
        (fork.retention, &fork.retained),
    ] {
        assert_eq!(
            store
                .head(authority, &mut work)
                .await
                .map_err(js_error)?
                .sequence,
            Sequence::new(1)
        );
        let found = store
            .find_operation(authority, commit.operation_id, &mut work)
            .await
            .map_err(js_error)?
            .0
            .expect("first record is found by its operation");
        assert_eq!(found.fingerprint, commit.fingerprint);
    }
    assert_eq!(
        store
            .fork_workspace(
                fork.clone(),
                PublicationPermit::Unrestricted,
                &mut work,
                None::<&mut ()>
            )
            .await
            .map_err(js_error)?,
        WorkspaceForkOutcome::Committed
    );
    assert_eq!(
        store
            .fork_workspace(
                WorkspaceForkCommit {
                    creation: proposal(35),
                    ..fork
                },
                PublicationPermit::Unrestricted,
                &mut work,
                None::<&mut ()>
            )
            .await
            .map_err(js_error)?,
        WorkspaceForkOutcome::CreationRejected
    );
    let volume = AuthorityId::from_bytes([36; 16]);
    for (commit, expected) in [
        (proposal(37), true),
        (proposal(37), true),
        (proposal(38), false),
    ] {
        assert_eq!(
            store
                .create_first_record(volume, commit, &mut work, None::<&mut ()>)
                .await
                .map_err(js_error)?,
            expected
        );
    }
    Ok(())
}

fn proposal(operation: u8, fingerprint: u8, payload: Bytes) -> ProposedCommit {
    ProposedCommit {
        operation_id: OperationId::from_bytes([operation; 16]),
        fingerprint: Digest::from_bytes([fingerprint; 32]),
        payload,
    }
}

fn append_request(
    authority_id: AuthorityId,
    expected: Head,
    commit: ProposedCommit,
    permit: PublicationPermit,
) -> GuardedAppend {
    GuardedAppend {
        authority_id,
        epoch: expected.epoch,
        expected,
        commit,
        permit,
    }
}

fn commit_head(commit: &DurableCommit) -> Head {
    Head {
        epoch: commit.epoch,
        sequence: commit.sequence,
        digest: commit.digest,
    }
}

async fn delete_database(name: &str) -> Result<(), JsValue> {
    Database::delete_by_name(name)
        .map_err(js_error)?
        .await
        .map_err(js_error)
}

async fn open_clean_authority(name: &str) -> Result<IndexedDbAuthorityStore, JsValue> {
    delete_database(name).await?;
    IndexedDbAuthorityStore::open(name, 1_024)
        .await
        .map_err(js_error)
}

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}
