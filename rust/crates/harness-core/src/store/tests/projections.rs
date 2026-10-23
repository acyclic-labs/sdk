use super::*;
use futures::TryStreamExt as _;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn stale_writer_does_not_advance_its_projection() -> Result<()> {
    let provider = Arc::new(MemoryStream::default());
    let client = provider;
    let mut first = with_content(&client).await?;
    let mut stale = with_content(&client).await?;
    first
        .execute(
            command(1)?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let rejected = command(2)?;
    assert!(matches!(
        stale
            .execute(
                rejected.clone(),
                #[cfg(feature = "filesystem")]
                None,
                &mut None
            )
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(stale.reducer().revision(), 0);
    let mut reopened = with_content(&client).await?;
    assert!(matches!(
        reopened.reconcile(&rejected).await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}

pub(super) fn projection_command(identity: u128, revision: u64) -> Result<Command> {
    let mut next = command(1)?;
    next.operation_id = OperationId::from_bytes(identity.to_le_bytes());
    next.idempotency_key = IdempotencyKey::new(format!("projection-{identity}"))?;
    next.expected_revision = revision;
    Ok(next)
}

pub(super) async fn projection_fixture(
    events: u64,
) -> Result<Arc<crate::test_stream::LostSessionAck<MemoryStream>>> {
    let client = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let mut writer = with_content(&client).await?;
    for revision in 0..events {
        writer
            .execute(
                projection_command(u128::from(revision + 1), revision)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    client.forbid_writes.store(true, Ordering::SeqCst);
    client.reset_io();
    Ok(client)
}

async fn open_projection<P: StreamProvider>(
    client: &Arc<P>,
    snapshot: Option<Snapshot>,
    limits: HistoryReadLimits,
) -> Result<StreamAggregate<P>> {
    StreamAggregate::open(
        client,
        (authority(), issuer().verifier(&mut None)?, schemas()),
        snapshot,
        limits,
        DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await
}

#[tokio::test]
async fn default_cold_projection_enforces_bounds_and_recovers_failed_observations() -> Result<()> {
    let client = projection_fixture(69).await?;
    let defaults = default_projection_read_limits();
    let loaded = checkpoints::load_with_work(
        &client,
        &authority(),
        &issuer().verifier(&mut None)?,
        defaults,
        None,
        &mut None,
    )
    .await?;
    let suffix = HistoryReader::new(
        &client,
        &authority(),
        &issuer().verifier(&mut None)?,
        &mut None,
    )?
    .read_page(
        &HistoryCursor {
            authority: authority(),
            after_revision: 64,
            through_revision: 69,
        },
        defaults,
        None,
    )
    .await?;
    let suffix_bytes = suffix.events.iter().try_fold(0_u64, |sum, event| {
        Ok::<_, Error>(sum + encode_event(&authority(), event, &mut None)?.len() as u64)
    })?;
    let exact = HistoryReadLimits {
        maximum_events: 6,
        maximum_bytes: loaded.consumed_bytes + suffix_bytes,
    };
    assert_eq!(
        open_projection(&client, None, exact)
            .await?
            .reducer()
            .revision(),
        69
    );
    for limits in [
        HistoryReadLimits {
            maximum_events: 5,
            ..exact
        },
        HistoryReadLimits {
            maximum_bytes: exact.maximum_bytes - 1,
            ..exact
        },
    ] {
        assert!(open_projection(&client, None, limits).await.is_err());
    }
    client.history_read_fault.store(1, Ordering::SeqCst);
    assert!(open_projection(&client, None, defaults).await.is_err());
    assert_eq!(
        open_projection(&client, None, defaults)
            .await?
            .reducer()
            .revision(),
        69
    );
    Ok(())
}

#[tokio::test]
async fn projection_checkpoint_commit_loss_preserves_original_admission() -> Result<()> {
    let client = projection_fixture(63).await?;
    client.forbid_writes.store(false, Ordering::SeqCst);
    let mut writer = with_content(&client).await?;
    let original = projection_command(64, 63)?;
    client.aggregate_commit_fault.store(1, Ordering::SeqCst);
    let _uncertain = writer
        .execute(
            original.clone(),
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await;
    drop(writer);
    let mut cold = with_content(&client).await?;
    cold.execute(
        original.clone(),
        #[cfg(feature = "filesystem")]
        None,
        &mut None,
    )
    .await?;
    assert_eq!(cold.reducer().revision(), 64);
    assert!(matches!(
        cold.execute(
            original,
            #[cfg(feature = "filesystem")]
            None,
            &mut None
        )
        .await?,
        ApplyResult::Replayed { .. }
    ));
    assert_eq!(
        acyclic_stream::Stream::new(
            client,
            "harness/v3/projection-checkpoints/conversations/conversation-1"
        )?
        .tail()
        .await?,
        1
    );
    Ok(())
}

#[tokio::test]
async fn cold_projection_rejects_forged_state_and_non_atomic_heads() -> Result<()> {
    use acyclic_stream::{CommitCondition, CommitMutation, CommitRequest};
    let source = projection_fixture(64).await?;
    let head_path =
        StreamPath::new("harness/v3/projection-checkpoints/conversations/conversation-1")?;
    let heads = acyclic_stream::Stream::new(source.clone(), head_path.as_str())?
        .read(0, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let location: Value = crate::contract::json_from_slice(&heads[0].value)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let digest: [u8; 32] = serde_json::from_value(location["chunks"][0]["digest"].clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let chunk_path = format!(
        "harness/v3/projection-chunks/conversations/conversation-1/{}",
        blake3::Hash::from_bytes(digest).to_hex()
    );
    let chunks = acyclic_stream::Stream::new(source.clone(), &chunk_path)?
        .read(0, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let canonical =
        acyclic_stream::Stream::new(source.clone(), authority().stream_path(&mut None)?)?
            .read(0, 64)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
    for forged in [true, false] {
        let client = Arc::new(MemoryStream::default());
        let mut location = location.clone();
        let mut payload = chunks[0].value.to_vec();
        let mut path = chunk_path.clone();
        if forged {
            let mut snapshot: Snapshot = crate::contract::json_from_slice(&payload)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            snapshot.attestation[0] ^= 1;
            payload = crate::contract::canonical_json_bytes(&snapshot)?;
            let digest = blake3::hash(&payload);
            location["bytes"] = json!(payload.len());
            location["chunks"] = json!([{"digest": *digest.as_bytes(), "bytes": payload.len()}]);
            path = format!("harness/v3/projection-chunks/conversations/conversation-1/{digest}");
        }
        acyclic_stream::Stream::new(client.clone(), path)?
            .append(payload)
            .await?;
        let stream =
            acyclic_stream::Stream::new(client.clone(), authority().stream_path(&mut None)?)?;
        stream
            .append_batch(
                canonical[..63]
                    .iter()
                    .map(|record| acyclic_stream::AppendRecord {
                        value: record.value.clone(),
                        headers: record.headers.clone(),
                    })
                    .collect(),
                Some(0),
                None,
            )
            .await?;
        let head_bytes = OwnedBytes::from(crate::contract::canonical_json_bytes(&location)?);
        if forged {
            client
                .commit(CommitRequest {
                    conditions: vec![
                        CommitCondition::Tail {
                            path: stream.path().clone(),
                            expected: 63,
                        },
                        CommitCondition::Absent {
                            path: head_path.clone(),
                        },
                    ],
                    mutations: vec![
                        CommitMutation::Append {
                            path: stream.path().clone(),
                            records: vec![canonical[63].value.clone().into()],
                        },
                        CommitMutation::Append {
                            path: head_path.clone(),
                            records: vec![head_bytes.into()],
                        },
                    ],
                    idempotency_key: StreamIdempotencyKey::new("copied-projection")?,
                })
                .await?;
        } else {
            stream.append(canonical[63].value.clone()).await?;
            acyclic_stream::Stream::new(client.clone(), head_path.as_str())?
                .append(head_bytes)
                .await?;
        }
        assert!(
            open_projection(&client, None, default_projection_read_limits())
                .await
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn supplied_projection_charges_snapshot_and_suffix_before_io() -> Result<()> {
    let client = projection_fixture(64).await?;
    let snapshot = with_content(&client)
        .await?
        .reducer()
        .snapshot(1, &mut None)?
        .0;
    let snapshot_bytes = crate::contract::canonical_json_bytes(&snapshot)?.len() as u64;
    client.forbid_writes.store(false, Ordering::SeqCst);
    let mut writer = with_content(&client).await?;
    for revision in 64..69 {
        writer
            .execute(
                projection_command(u128::from(revision + 1), revision)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    client.forbid_writes.store(true, Ordering::SeqCst);
    let suffix = acyclic_stream::Stream::new(client.clone(), authority().stream_path(&mut None)?)?
        .read(64, 5)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let exact = HistoryReadLimits {
        maximum_events: 6,
        maximum_bytes: snapshot_bytes
            + suffix
                .iter()
                .map(|record| record.value.len() as u64)
                .sum::<u64>(),
    };
    assert_eq!(
        open_projection(&client, Some(snapshot.copy(&mut None)?), exact)
            .await?
            .reducer()
            .revision(),
        69
    );
    for limits in [
        HistoryReadLimits {
            maximum_events: 5,
            ..exact
        },
        HistoryReadLimits {
            maximum_bytes: exact.maximum_bytes - 1,
            ..exact
        },
    ] {
        assert!(
            open_projection(&client, Some(snapshot.copy(&mut None)?), limits)
                .await
                .is_err()
        );
    }
    client.reset_io();
    assert!(
        open_projection(
            &client,
            Some(snapshot),
            HistoryReadLimits {
                maximum_bytes: snapshot_bytes - 1,
                ..exact
            }
        )
        .await
        .is_err()
    );
    assert_eq!(client.io_snapshot().reads, 0);
    Ok(())
}
