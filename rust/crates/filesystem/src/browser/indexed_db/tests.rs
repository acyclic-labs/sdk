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
    const DATABASE_NAME: &str = "acyclic-fs-authority-conformance-v2";
    let store = open_clean_authority(DATABASE_NAME).await?;
    let authority_id = AuthorityId::from_bytes([7; 16]);
    let cancellation = CancellationToken::new();
    let created = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::create_authority(
            &store,
            authority_id,
            Epoch::GENESIS,
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
    }
    .map_err(js_error)?;
    let genesis = Head::genesis(Epoch::GENESIS);
    assert_eq!(created, CreateAuthorityOutcome::Created(genesis));
    let existing = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::create_authority(
            &store,
            authority_id,
            Epoch::new(9).map_err(js_error)?,
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
    }
    .map_err(js_error)?;
    assert_eq!(existing, CreateAuthorityOutcome::Existing(genesis));
    let denied_gate = {
        let mut authority_work = crate::AuthorityWork::new(
            WorkBudget {
                backend_read_operations: 1,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        );
        AsyncAuthorityStore::create_authority(
            &store,
            authority_id,
            Epoch::GENESIS,
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
    };
    assert!(
        denied_gate.is_err(),
        "existing authority gate read escaped admission"
    );

    let first_proposal = ProposedCommit {
        operation_id: OperationId::from_bytes([8; 16]),
        fingerprint: Digest::from_bytes([9; 32]),
        payload: Bytes::from_static(b"first"),
    };
    let first = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::compare_and_append(
            &store,
            GuardedAppend {
                authority_id: authority_id,
                epoch: Epoch::GENESIS,
                expected: genesis,
                commit: first_proposal.clone(),
                permit: PublicationPermit::Unrestricted,
            },
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
        .map(|(value, _)| value)
    }
    .map_err(js_error)?;
    let AppendOutcome::Committed(first_commit) = first else {
        return Err(JsValue::from_str(
            "first authority append was not committed",
        ));
    };
    let retry = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::compare_and_append(
            &store,
            GuardedAppend {
                authority_id: authority_id,
                epoch: Epoch::GENESIS,
                expected: genesis,
                commit: first_proposal.clone(),
                permit: PublicationPermit::Unrestricted,
            },
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
        .map(|(value, _)| value)
    }
    .map_err(js_error)?;
    assert_eq!(retry, AppendOutcome::AlreadyCommitted(first_commit.clone()));
    let conflicting_identity = ProposedCommit {
        fingerprint: Digest::from_bytes([10; 32]),
        ..first_proposal
    };
    let identity_conflict = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::compare_and_append(
            &store,
            GuardedAppend {
                authority_id: authority_id,
                epoch: Epoch::GENESIS,
                expected: genesis,
                commit: conflicting_identity,
                permit: PublicationPermit::Unrestricted,
            },
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
        .map(|(value, _)| value)
    }
    .map_err(js_error)?;
    assert_eq!(
        identity_conflict,
        AppendOutcome::IdempotencyConflict {
            committed_fingerprint: first_commit.fingerprint
        }
    );

    let second_commit = append_second(&store, authority_id, &cancellation, &first_commit).await?;
    verify_replay_and_lookup(
        &store,
        authority_id,
        &cancellation,
        first_commit,
        second_commit.clone(),
    )
    .await?;
    verify_fencing(&store, authority_id, &cancellation, second_commit).await?;
    let budget = WorkBudget {
        durability_operations: 1,
        ..WorkBudget::UNBOUNDED
    };
    let mut work = crate::AuthorityWork::new(budget, &cancellation);
    let head = AsyncAuthorityStore::head(&store, authority_id, &mut work)
        .await
        .map_err(js_error)?;
    let FenceOutcome::Advanced(next) =
        AsyncAuthorityStore::fence(&store, authority_id, head, &mut work)
            .await
            .map_err(js_error)?
    else {
        panic!("first fence must advance")
    };
    assert!(matches!(
        AsyncAuthorityStore::fence(&store, authority_id, next, &mut work).await,
        Err(AuthorityStoreError::Work(_))
    ));
    assert_eq!(work.spent().durability_operations, 1);
    let mut verify = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    assert_eq!(
        AsyncAuthorityStore::head(&store, authority_id, &mut verify)
            .await
            .map_err(js_error)?,
        next
    );
    Ok(())
}

#[wasm_bindgen_test]
async fn indexed_db_publication_reservations_are_atomic_and_idempotent() -> Result<(), JsValue> {
    const DATABASE_NAME: &str = "acyclic-fs-authority-reservation-v3";
    let store = open_clean_authority(DATABASE_NAME).await?;
    let authority_id = AuthorityId::from_bytes([31; 16]);
    let cancellation = CancellationToken::new();
    let genesis = Head::genesis(Epoch::GENESIS);
    {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::create_authority(
            &store,
            authority_id,
            Epoch::GENESIS,
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
    }
    .map_err(js_error)?;
    let operation_id = OperationId::from_bytes([32; 16]);
    let reserved = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::reserve_publication(
            &store,
            authority_id,
            genesis,
            operation_id,
            &mut authority_work,
        )
        .await
    }
    .map_err(js_error)?;
    let ReservationOutcome::Reserved(reservation) = reserved else {
        return Err(JsValue::from_str("publication gate was not reserved"));
    };
    let retry = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::reserve_publication(
            &store,
            authority_id,
            genesis,
            operation_id,
            &mut authority_work,
        )
        .await
    }
    .map_err(js_error)?;
    assert_eq!(retry, ReservationOutcome::AlreadyReserved(reservation));
    let proposal = ProposedCommit {
        operation_id: OperationId::from_bytes([33; 16]),
        fingerprint: Digest::from_bytes([34; 32]),
        payload: Bytes::from_static(b"reserved"),
    };
    let blocked = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::compare_and_append(
            &store,
            GuardedAppend {
                authority_id: authority_id,
                epoch: Epoch::GENESIS,
                expected: genesis,
                commit: proposal.clone(),
                permit: PublicationPermit::Unrestricted,
            },
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
        .map(|(value, _)| value)
    }
    .map_err(js_error)?;
    assert!(matches!(blocked, AppendOutcome::Fenced { .. }));
    let committed = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::compare_and_append(
            &store,
            GuardedAppend {
                authority_id,
                epoch: Epoch::GENESIS,
                expected: genesis,
                commit: proposal,
                permit: PublicationPermit::Reservation {
                    operation_id: operation_id.into_bytes(),
                    gate_tail: reservation.gate_tail,
                    expected: genesis,
                },
            },
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
        .map(|(value, _)| value)
    }
    .map_err(js_error)?;
    assert!(matches!(committed, AppendOutcome::Committed(_)));
    release(&store, reservation, &cancellation)
        .await
        .map_err(js_error)?;
    release(&store, reservation, &cancellation)
        .await
        .map_err(js_error)?;
    let mut forged = reservation;
    forged.operation_id = OperationId::from_bytes([99; 16]);
    assert!(release(&store, forged, &cancellation).await.is_err());
    Ok(())
}

async fn release(
    store: &IndexedDbAuthorityStore,
    reservation: PublicationReservation,
    cancellation: &CancellationToken,
) -> AuthorityResult<()> {
    {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation);
        AsyncAuthorityStore::release_publication(store, reservation, &mut authority_work).await
    }
}

#[wasm_bindgen_test]
async fn indexed_db_authority_serializes_competing_connections() -> Result<(), JsValue> {
    const DATABASE_NAME: &str = "acyclic-fs-authority-race-v2";
    let creator = open_clean_authority(DATABASE_NAME).await?;
    let authority_id = AuthorityId::from_bytes([21; 16]);
    let cancellation = CancellationToken::new();
    {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::create_authority(
            &creator,
            authority_id,
            Epoch::GENESIS,
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
    }
    .map_err(js_error)?;
    let left = IndexedDbAuthorityStore::open(DATABASE_NAME, 1_024)
        .await
        .map_err(js_error)?;
    let right = IndexedDbAuthorityStore::open(DATABASE_NAME, 1_024)
        .await
        .map_err(js_error)?;
    let left_promise = competing_append(left, authority_id, 22);
    let right_promise = competing_append(right, authority_id, 23);
    let promises = Array::new();
    promises.push(&left_promise);
    promises.push(&right_promise);
    let joined = JsFuture::from(Promise::all(&promises)).await?;
    let results = Array::from(&joined);
    let mut codes = [results.get(0).as_string(), results.get(1).as_string()];
    codes.sort();
    assert_eq!(
        codes,
        [Some("committed".to_owned()), Some("conflict".to_owned())]
    );
    Ok(())
}

fn competing_append(
    store: IndexedDbAuthorityStore,
    authority_id: AuthorityId,
    identity_byte: u8,
) -> Promise {
    wasm_bindgen_futures::future_to_promise(async move {
        let cancellation = CancellationToken::new();
        let outcome = {
            let mut authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            AsyncAuthorityStore::compare_and_append(
                &store,
                GuardedAppend {
                    authority_id: authority_id,
                    epoch: Epoch::GENESIS,
                    expected: Head::genesis(Epoch::GENESIS),
                    commit: ProposedCommit {
                        operation_id: OperationId::from_bytes([identity_byte; 16]),
                        fingerprint: Digest::from_bytes([identity_byte; 32]),
                        payload: Bytes::from_static(b"race"),
                    },
                    permit: PublicationPermit::Unrestricted,
                },
                &mut authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|e| e.into_error(std::convert::identity))
            .map(|(value, _)| value)
        }
        .map_err(js_error)?;
        let code = match outcome {
            AppendOutcome::Committed(_) => "committed",
            AppendOutcome::Conflict { .. } => "conflict",
            _ => return Err(JsValue::from_str("unexpected competing append outcome")),
        };
        Ok(JsValue::from_str(code))
    })
}

async fn append_second(
    store: &IndexedDbAuthorityStore,
    authority_id: AuthorityId,
    cancellation: &CancellationToken,
    first: &DurableCommit,
) -> Result<DurableCommit, JsValue> {
    let first_head = Head {
        epoch: first.epoch,
        sequence: first.sequence,
        digest: first.digest,
    };
    let second = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation);
        AsyncAuthorityStore::compare_and_append(
            store,
            GuardedAppend {
                authority_id: authority_id,
                epoch: Epoch::GENESIS,
                expected: first_head,
                commit: ProposedCommit {
                    operation_id: OperationId::from_bytes([11; 16]),
                    fingerprint: Digest::from_bytes([12; 32]),
                    payload: Bytes::from_static(b"second"),
                },
                permit: PublicationPermit::Unrestricted,
            },
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
        .map(|(value, _)| value)
    }
    .map_err(js_error)?;
    match second {
        AppendOutcome::Committed(commit) => Ok(commit),
        _ => Err(JsValue::from_str(
            "second authority append was not committed",
        )),
    }
}

#[wasm_bindgen_test]
async fn indexed_db_forks_and_creates_in_one_transaction_and_resolves_retries()
-> Result<(), JsValue> {
    const DATABASE_NAME: &str = "acyclic-fs-authority-fork-v1";
    let store = open_clean_authority(DATABASE_NAME).await?;
    let cancellation = CancellationToken::new();
    let proposal = |seed: u8| ProposedCommit {
        operation_id: OperationId::from_bytes([seed; 16]),
        fingerprint: Digest::from_bytes([seed; 32]),
        payload: Bytes::from(vec![seed; 8]),
    };
    let fork = WorkspaceForkCommit {
        lineage: None,
        destination: AuthorityId::from_bytes([31; 16]),
        creation: proposal(32),
        retention: AuthorityId::from_bytes([33; 16]),
        retained: proposal(34),
    };
    // Both authorities and both first records land in one transaction.
    let committed = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::fork_workspace(
            &store,
            fork.clone(),
            PublicationPermit::Unrestricted,
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
    }
    .map_err(js_error)?;
    assert_eq!(committed, WorkspaceForkOutcome::Committed);
    for (authority, commit) in [
        (fork.destination, &fork.creation),
        (fork.retention, &fork.retained),
    ] {
        let head = {
            let mut authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            AsyncAuthorityStore::head(&store, authority, &mut authority_work).await
        }
        .map_err(js_error)?;
        assert_eq!(head.sequence, Sequence::new(1));
        let found = {
            let mut authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            AsyncAuthorityStore::find_operation(
                &store,
                authority,
                commit.operation_id,
                &mut authority_work,
            )
            .await
            .map(|(value, _)| value)
        }
        .map_err(js_error)?
        .ok_or_else(|| JsValue::from_str("first record is not found by its operation"))?;
        assert_eq!(found.fingerprint, commit.fingerprint);
    }
    // A retry finds both first records and changes nothing.
    let retried = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::fork_workspace(
            &store,
            fork.clone(),
            PublicationPermit::Unrestricted,
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
    }
    .map_err(js_error)?;
    assert_eq!(retried, WorkspaceForkOutcome::Committed);
    // Another creation at the same destination is rejected.
    let rejected = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
        AsyncAuthorityStore::fork_workspace(
            &store,
            WorkspaceForkCommit {
                creation: proposal(35),
                ..fork.clone()
            },
            PublicationPermit::Unrestricted,
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
    }
    .map_err(js_error)?;
    assert_eq!(rejected, WorkspaceForkOutcome::CreationRejected);

    // One authority with its first record, then its retry and a
    // conflicting creation.
    let volume = AuthorityId::from_bytes([36; 16]);
    for (commit, expected) in [
        (proposal(37), true),
        (proposal(37), true),
        (proposal(38), false),
    ] {
        let created = {
            let mut authority_work =
                crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
            AsyncAuthorityStore::create_first_record(
                &store,
                volume,
                commit,
                &mut authority_work,
                None::<&mut ()>,
            )
            .await
            .map_err(|e| e.into_error(std::convert::identity))
        }
        .map_err(js_error)?;
        assert_eq!(created, expected);
    }
    Ok(())
}

async fn delete_database(name: &str) -> Result<(), JsValue> {
    Database::delete_by_name(name)
        .map_err(js_error)?
        .await
        .map_err(js_error)?;
    Ok(())
}

async fn open_clean_authority(database_name: &str) -> Result<IndexedDbAuthorityStore, JsValue> {
    delete_database(database_name).await?;
    IndexedDbAuthorityStore::open(database_name, 1_024)
        .await
        .map_err(js_error)
}

async fn verify_replay_and_lookup(
    store: &IndexedDbAuthorityStore,
    authority_id: AuthorityId,
    cancellation: &CancellationToken,
    first: DurableCommit,
    second: DurableCommit,
) -> Result<(), JsValue> {
    let mut replay_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation);
    let replay = {
        AsyncAuthorityStore::replay(
            store,
            authority_id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 8,
                payload_bytes: 64,
            },
            &mut replay_work,
        )
        .await
        .map(|(value, _)| value)
    }
    .map_err(js_error)?;
    assert_eq!(replay, vec![first.clone(), second.clone()]);
    let retained_peak = 2 * size_of::<DurableCommit>() as u64
        + COMMIT_PREFIX_BYTES as u64
        + first.payload.len() as u64
        + 2 * (COMMIT_PREFIX_BYTES as u64 + second.payload.len() as u64);
    assert!(replay_work.spent().peak_allocation_bytes >= retained_peak);
    assert!(
        {
            let mut authority_work = crate::AuthorityWork::new(
                WorkBudget {
                    peak_allocation_bytes: retained_peak - 1,
                    ..WorkBudget::UNBOUNDED
                },
                cancellation,
            );
            AsyncAuthorityStore::replay(
                store,
                authority_id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 8,
                    payload_bytes: 64,
                },
                &mut authority_work,
            )
            .await
            .map(|(value, _)| value)
        }
        .is_err(),
        "replay accepted a budget below its simultaneously retained buffers"
    );
    let resumed = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation);
        AsyncAuthorityStore::replay(
            store,
            authority_id,
            first.sequence,
            ReplayLimit {
                records: 1,
                payload_bytes: 16,
            },
            &mut authority_work,
        )
        .await
        .map(|(value, _)| value)
    }
    .map_err(js_error)?;
    assert_eq!(resumed, vec![second]);
    let found = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation);
        AsyncAuthorityStore::find_operation(
            store,
            authority_id,
            first.operation_id,
            &mut authority_work,
        )
        .await
        .map(|(value, _)| value)
    }
    .map_err(js_error)?;
    assert_eq!(found, Some(first));
    Ok(())
}

async fn verify_fencing(
    store: &IndexedDbAuthorityStore,
    authority_id: AuthorityId,
    cancellation: &CancellationToken,
    second: DurableCommit,
) -> Result<(), JsValue> {
    let fenced = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation);
        AsyncAuthorityStore::fence(
            store,
            authority_id,
            Head {
                epoch: Epoch::GENESIS,
                sequence: second.sequence,
                digest: second.digest,
            },
            &mut authority_work,
        )
        .await
    }
    .map_err(js_error)?;
    let FenceOutcome::Advanced(fenced_head) = fenced else {
        return Err(js_error("fresh IndexedDB fence conflicted"));
    };
    assert_eq!(fenced_head.epoch, Epoch::new(2).map_err(js_error)?);
    assert_eq!(fenced_head.sequence, second.sequence);
    assert_eq!(fenced_head.digest, second.digest);
    let stale = {
        let mut authority_work = crate::AuthorityWork::new(WorkBudget::UNBOUNDED, cancellation);
        AsyncAuthorityStore::compare_and_append(
            store,
            GuardedAppend {
                authority_id: authority_id,
                epoch: Epoch::GENESIS,
                expected: fenced_head,
                commit: ProposedCommit {
                    operation_id: OperationId::from_bytes([13; 16]),
                    fingerprint: Digest::from_bytes([14; 32]),
                    payload: Bytes::from_static(b"stale"),
                },
                permit: PublicationPermit::Unrestricted,
            },
            &mut authority_work,
            None::<&mut ()>,
        )
        .await
        .map_err(|e| e.into_error(std::convert::identity))
        .map(|(value, _)| value)
    }
    .map_err(js_error)?;
    assert_eq!(
        stale,
        AppendOutcome::Fenced {
            actual_epoch: fenced_head.epoch
        }
    );
    Ok(())
}

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}
