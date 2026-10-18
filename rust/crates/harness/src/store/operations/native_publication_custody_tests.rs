use super::*;
use crate::store::checkpoints::native_staging_custody_tests::budget;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use acyclic_stream::MemoryStream;
use std::sync::Arc;

#[tokio::test]
async fn native_message_lookup_provider_corruption_bound_and_cancellation_release_records()
-> Result<()> {
    let authority = Authority {
        kind: crate::core::AggregateKind::Conversation,
        id: "native-message-lookup".into(),
    };
    let issuer = crate::core::AuthorityIssuer::new("message-lookup", [9; 32], authority.clone());
    let id = uuid::Uuid::from_bytes([11; 16]);
    let path = publication_path(
        &authority,
        "harness/v2/conversation-messages/",
        Some(&id),
        &mut None,
    )?
    .0;
    let client = Arc::new(MemoryStream::default());
    acyclic_stream::Stream::new(client.clone(), path.as_str())?
        .append(b"{{".as_slice())
        .await?;
    let cancellation = CancellationToken::new();
    let native =
        NativeMaterializationWork::new(budget(), &cancellation).expect("finite decoder budget");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let error = find_message_with_work(
        &client,
        &authority,
        &issuer.verifier(&mut None)?,
        id,
        1,
        1_048_576,
        &mut Some(&mut io),
    )
    .await
    .expect_err("corrupt index");
    // The typed JSON admission (#684) rejects the truncated index before any boundary
    // diagnostic is built, so no reason string or custody survives the rejection.
    assert!(matches!(
        error,
        Error::NativeJsonAdmission {
            code: crate::wire::ErrorCode::Invalid,
            kind: crate::executor::json::JsonInputError::Incomplete,
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
    .map_err(|error| crate::store::operations::stream_error_with_work(error, &None))?;
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
    assert_eq!(
        producer.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    assert_eq!(
        receiver.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
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
        issuer.verifier(&mut None)?,
        SchemaRegistry::new(),
    );
    let command = Command {
        operation_id: OperationId::from_bytes([91; 16]),
        idempotency_key: crate::IdempotencyKey("publication-custody".into()),
        expected_revision: 0,
        scope: issuer.issue(
            "root",
            &crate::Capabilities::new(["lifecycle:manage"]),
            None,
            None,
            &mut None,
        )?,
        causal_parent: None,
        action: Action::TransitionLifecycle {
            to: LifecycleState::Active,
            reason: None,
        },
    };
    let ApplyResult::Applied { event } = reducer.plan(&command)? else {
        panic!("fresh lifecycle");
    };
    let cancellation = CancellationToken::new();
    let native =
        NativeMaterializationWork::new(budget(), &cancellation).expect("finite publication budget");
    let mut io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let encoded = crate::wire_codec::encode_event(&authority, &event, &mut Some(&mut io))?;
    let original_capacity = encoded.capacity() as u64;
    let publication =
        IndexedPublication::new(&authority, &event, encoded, None, &mut Some(&mut io))?;
    let retained = publication.retained_owned();
    let client = Arc::new(MemoryStream::default());
    let mut commit_id = None;
    let request = publication
        .request(
            &client,
            IdempotencyKey::new("publication-custody")?,
            &mut None,
        )
        .await?
        .0;
    for _ in 0..2 {
        let acyclic_stream::CommitOutcome::Committed(envelope) =
            client.commit(request.clone()).await?
        else {
            panic!("atomic publication");
        };
        publication.verify(&envelope, &mut Some(&mut io))?;
        if let Some(prior) = &commit_id {
            assert_eq!(prior, &envelope.commit_id);
        }
        commit_id = Some(envelope.commit_id);
    }
    drop(request);
    let query =
        NativeMaterializationWork::new(budget(), &cancellation).expect("finite query budget");
    let mut query_io =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&query)?);
    let cached = one_record(&client, &publication.path, 0, &mut Some(&mut query_io))
        .await?
        .unwrap();
    assert_eq!(cached.value, publication.bytes);
    assert!(cached.value.matches_meter(&native.runtime_meter()));
    assert!(!cached.value.matches_meter(&query.runtime_meter()));
    drop(cached);
    assert_eq!(query_io.into_custody()?.bytes(), 0);
    assert_eq!(
        query.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    let mut replay_io = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&native)?,
    );
    let (replayed, _) = bounded_operation_with_retained_quota(
        &client,
        &authority,
        &issuer.verifier(&mut None)?,
        event.operation_id,
        2 * acyclic_stream::MAX_RECORD_BYTES as u64,
        false,
        &mut Some(&mut replay_io),
    )
    .await?;
    let (replayed, paid) = replayed.unwrap();
    assert_eq!(replayed, event);
    let mut replay_custody = replay_io.into_custody()?;
    drop(replayed);
    replay_custody.release(paid)?;
    assert_eq!(replay_custody.bytes(), 0);
    let mut custody = io.into_custody()?;
    drop(publication);
    custody.release(retained)?;
    assert_eq!(custody.bytes(), 0);
    assert!(native.live_allocation_bytes() >= original_capacity);
    drop(client);
    assert_eq!(
        native.live_allocation_bytes(),
        NativeMaterializationWork::accounting_state_bytes()
    );
    Ok(())
}
