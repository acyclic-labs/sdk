use super::super::history::HistoryIo;
use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
use acyclic_stream::MemoryStream;
use std::sync::Arc;

fn budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 32,
        authority_bytes_read: 1_048_576,
        backend_read_operations: 32,
        source_bytes_read: 1_048_576,
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        bytes_hashed: 1_048_576,
        authority_records_appended: 32,
        authority_bytes_written: 1_048_576,
        backend_write_operations: 32,
        items_examined: 1_048_576,
        allocation_operations: 1_048_576,
        peak_allocation_bytes: 1_048_576,
        ..WorkBudget::default()
    }
}

#[tokio::test]
async fn native_message_lookup_provider_corruption_bound_and_cancellation_release_records()
-> Result<()> {
    let authority = Authority {
        kind: crate::core::AggregateKind::Conversation,
        id: "native-message-lookup".into(),
    };
    let issuer = crate::core::AuthorityIssuer::new("message-lookup", [9; 32], authority.clone());
    let message_id = uuid::Uuid::from_bytes([11; 16]);
    let path = publication_path(
        &authority,
        "harness/v2/conversation-messages/",
        Some((&message_id, 36)),
        &mut None,
    )?
    .0;
    let client = Arc::new(MemoryStream::default());
    let outcome = client
        .commit(CommitRequest {
            conditions: vec![CommitCondition::Absent { path: path.clone() }],
            mutations: vec![CommitMutation::Append {
                path,
                records: vec![OwnedBytes::from_static(b"{{")],
            }],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(
                b"native-corrupt-message",
            ))?,
        })
        .await?;
    assert!(matches!(
        outcome,
        acyclic_stream::CommitOutcome::Committed(_)
    ));
    drop(outcome);
    for mode in 0..4 {
        let cancellation = CancellationToken::new();
        let mut allowance = budget();
        if mode == 3 {
            allowance.source_bytes_read = 0;
        }
        let native = NativeMaterializationWork::new(allowance, &cancellation)
            .expect("original message query ledger");
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&native)
                .expect("original query custody"),
        );
        if mode == 2 {
            cancellation.cancel();
        }
        let error = find_message_with_work(
            &client,
            &authority,
            &issuer.verifier(&mut None).expect("verifier copy"),
            message_id,
            1,
            if mode == 1 { 1 } else { 1_048_576 },
            &mut Some(&mut io),
        )
        .await
        .expect_err("original provider record is corrupt or denied");
        match mode {
            0 => assert!(matches!(&error, Error::NativeDecode { .. })),
            1 => assert!(matches!(&error, Error::NativeBoundary { .. })),
            2 => assert!(matches!(
                &error,
                Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                    ..
                }
            )),
            _ => assert!(matches!(&error, Error::NativeWork { .. })),
        }
        drop(error);
        assert_eq!(io.into_custody()?.bytes(), 0);
    }
    let missing_id = uuid::Uuid::from_bytes([12; 16]);
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("original missing query ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)
            .expect("original query custody"),
    );
    let absent = find_message_with_work(
        &client,
        &authority,
        &issuer.verifier(&mut None).expect("verifier copy"),
        missing_id,
        1,
        1_048_576,
        &mut Some(&mut io),
    )
    .await?;
    assert_eq!(absent, (None, 0));
    assert_eq!(io.into_custody()?.bytes(), 0);

    let location = MessageLocation {
        location: OperationLocation {
            authority: authority.clone(),
            operation_id: OperationId::from_bytes([13; 16]),
            revision: 1,
            intent_digest: [0; 32],
            effect_position: None,
        },
        message_id: missing_id,
        sequence: 1,
    };
    let path = publication_path(
        &authority,
        "harness/v2/conversation-messages/",
        Some((&missing_id, 36)),
        &mut None,
    )?
    .0;
    client
        .commit(CommitRequest {
            conditions: vec![CommitCondition::Absent { path: path.clone() }],
            mutations: vec![CommitMutation::Append {
                path,
                records: vec![OwnedBytes::from(crate::contract::canonical_json_bytes(
                    &location,
                )?)],
            }],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(
                b"native-missing-canonical",
            ))?,
        })
        .await?;
    let native = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("original missing canonical ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)
            .expect("original query custody"),
    );
    let error = find_message_with_work(
        &client,
        &authority,
        &issuer.verifier(&mut None).expect("verifier copy"),
        missing_id,
        1,
        1_048_576,
        &mut Some(&mut io),
    )
    .await
    .expect_err("original canonical stream is absent");
    assert!(matches!(
        &error,
        Error::NativeBoundary {
            reason: "indexed canonical event is missing",
            ..
        }
    ));
    drop(error);
    assert_eq!(io.into_custody()?.bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn foreign_cursor_record_metadata_is_rejected_without_byte_rebinding() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let path = StreamPath::new("harness/v3/foreign-record-metadata")?;
    acyclic_stream::Stream::new(std::sync::Arc::clone(&client), path.as_str())?
        .append_at(OwnedBytes::from_static(b"record"), 0)
        .await?;
    let cancellation = CancellationToken::new();
    let producer = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("genuine native record producer");
    let receiver =
        NativeMaterializationWork::new(budget(), &cancellation).expect("different receiver ledger");
    let mut records = read_records(
        &client,
        &path,
        0,
        1,
        &mut Some(&mut crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&producer)?,
        )),
    )
    .await?
    .map_err(|error| sdk_error(error, &None))?;
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&receiver).expect("receiver custody"),
    );
    let error = records
        .next(&mut Some(&mut io))
        .await
        .expect_err("foreign SDK output metadata is not this call's custody");
    assert!(matches!(
        error,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            custody: None,
        }
    ));
    drop(records);
    assert_eq!(io.into_custody()?.bytes(), 0);
    assert_eq!(producer.live_allocation_bytes(), 0);
    assert_eq!(receiver.live_allocation_bytes(), 0);
    Ok(())
}

#[tokio::test]
async fn original_provider_publication_releases_exact_source_after_receipt_drop() -> Result<()> {
    use crate::core::{
        Action, AggregateKind, ApplyResult, AuthorityIssuer, Command, LifecycleState, Reducer,
        SchemaRegistry,
    };
    let authority = Authority {
        kind: AggregateKind::Agent,
        id: "publication-custody".into(),
    };
    let issuer = AuthorityIssuer::new("publication-test", [7; 32], authority.clone());
    let reducer = Reducer::new(
        authority.clone(),
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
    );
    let command = Command {
        operation_id: OperationId::from_bytes([91; 16]),
        idempotency_key: crate::IdempotencyKey("publication-custody".into()),
        expected_revision: 0,
        scope: issuer
            .issue(
                "root",
                &crate::Capabilities::new(["lifecycle:manage"]),
                None,
                None,
                &mut None,
            )
            .expect("scope proof"),
        causal_parent: None,
        action: Action::TransitionLifecycle {
            to: LifecycleState::Active,
            reason: None,
        },
    };
    let ApplyResult::Applied { event } = reducer.plan(&command)? else {
        panic!("fresh original lifecycle command");
    };
    let cancellation = CancellationToken::new();
    let native = NativeMaterializationWork::new(budget(), &cancellation)
        .expect("original finite publication ledger");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)
            .expect("original source custody"),
    );
    let mut work: Option<&mut dyn HistoryIo> = Some(&mut io);
    let encoded = crate::wire_codec::encode_event(&authority, &event, &mut work)?;
    let original_capacity = encoded.capacity() as u64;
    let mut publication = IndexedPublication::new(&authority, &event, encoded, &mut work)?;
    assert_eq!(
        publication.bytes.retained_capacity(),
        Some(original_capacity as usize)
    );
    assert!(publication.bytes.matches_meter(&native.runtime_meter()));
    assert!(
        publication
            .indexes
            .iter()
            .all(|(_, _, bytes)| bytes.matches_meter(&native.runtime_meter()))
    );
    let old_snapshot = publication.indexes[0].2.clone();
    let old_snapshot_capacity = old_snapshot
        .retained_capacity()
        .expect("guarded original index");
    publication.set_effect_position_with_work(3, &mut work)?;
    let retained = publication.retained_owned();
    let client = Arc::new(MemoryStream::default());
    let request = publication
        .request(
            &client,
            IdempotencyKey::new(OwnedBytes::from_static(b"publication-custody"))?,
            &mut None,
        )
        .await?
        .0;
    let acyclic_stream::CommitOutcome::Committed(mut envelope) = client.commit(request).await?
    else {
        panic!("original absent/Tail atomic publication");
    };
    let retry = publication
        .request(
            &client,
            IdempotencyKey::new(OwnedBytes::from_static(b"publication-custody"))?,
            &mut None,
        )
        .await?
        .0;
    let acyclic_stream::CommitOutcome::Committed(retried) = client.commit(retry).await? else {
        panic!("same original request is an idempotent publication");
    };
    assert_eq!(retried.commit_id, envelope.commit_id);
    publication.verify(&retried, &mut None)?;
    drop(retried);

    let query_cancellation = CancellationToken::new();
    let query = NativeMaterializationWork::new(budget(), &query_cancellation)
        .expect("distinct native cache reader");
    let mut query_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&query).expect("query metadata custody"),
    );
    let cached = one_record(&client, &publication.path, 0, &mut Some(&mut query_io))
        .await?
        .expect("real provider cached event");
    assert_eq!(cached.value, publication.bytes);
    assert!(cached.value.matches_meter(&native.runtime_meter()));
    assert!(!cached.value.matches_meter(&query.runtime_meter()));
    drop(cached);
    assert_eq!(query_io.into_custody()?.bytes(), 0);
    assert_eq!(query.live_allocation_bytes(), 0);
    publication.verify(&envelope, &mut work)?;
    let mut replay_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)
            .expect("same original replay decoder"),
    );
    let (replayed, consumed) = bounded_operation_with_retained_quota(
        &client,
        &authority,
        &issuer.verifier(&mut None).expect("verifier copy"),
        event.operation_id,
        2 * acyclic_stream::MAX_RECORD_BYTES as u64,
        false,
        &mut Some(&mut replay_io),
    )
    .await?;
    let (replayed, replay_paid) =
        replayed.expect("actual immutable locator resolves original event");
    assert_eq!(replayed, event);
    assert!(consumed > publication.bytes.len() as u64);
    let mut replay_custody = replay_io.into_custody()?;
    assert_eq!(replay_custody.bytes(), replay_paid);
    drop(replayed);
    replay_custody
        .release(replay_paid)
        .expect("original decoded event physically dropped");
    assert_eq!(replay_custody.bytes(), 0);
    if let CommittedMutation::Append(append) = &mut envelope.mutations[0] {
        append.start += 1;
    }
    assert!(matches!(
        publication.verify(&envelope, &mut work),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "indexed publication append is invalid"
        })
    ));
    if let CommittedMutation::Append(append) = &mut envelope.mutations[0] {
        append.start -= 1;
    }
    let key = super::super::stream_idempotency_key_with_work(
        publication.path.as_str(),
        &command.idempotency_key,
        &mut work,
    )?;
    cancellation.cancel();
    let denied = publication
        .request(&client, key, &mut work)
        .await
        .err()
        .expect("cancellation before original SDK bounds");
    assert!(matches!(
        denied,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            custody: None,
        }
    ));
    assert!(matches!(
        publication.verify(&envelope, &mut work),
        Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
            custody: None,
        })
    ));
    drop(work);
    let mut custody = io.into_custody()?;
    assert_eq!(custody.bytes(), retained);
    drop(envelope);
    drop(publication);
    custody
        .release(retained)
        .expect("physically dropped source publication");
    assert_eq!(custody.bytes(), 0);
    assert!(native.live_allocation_bytes() >= original_capacity);
    drop(client);
    assert!(native.live_allocation_bytes() >= old_snapshot_capacity as u64);
    drop(old_snapshot);
    assert_eq!(native.live_allocation_bytes(), 0);
    Ok(())
}
