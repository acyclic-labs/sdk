use super::*;
use futures::TryStreamExt as _;

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

fn projection_command(identity: u128, revision: u64) -> Result<Command> {
    let mut command = command(1)?;
    command.operation_id = OperationId::from_bytes(identity.to_le_bytes());
    command.idempotency_key = IdempotencyKey::new(format!("projection-{identity}"))?;
    command.expected_revision = revision;
    Ok(command)
}

#[tokio::test]
async fn checkpoint_open_pins_canonical_cut_before_selecting_checkpoint() -> Result<()> {
    use std::sync::atomic::Ordering;
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let client = provider.clone();
    let mut writer = with_content(&client).await?;
    for cut in [64_u64, 128] {
        while writer.reducer().revision() < cut {
            let revision = writer.reducer().revision();
            writer
                .execute(
                    projection_command(u128::from(revision + 1), revision)?,
                    #[cfg(feature = "filesystem")]
                    None,
                    &mut None,
                )
                .await?;
        }
        provider
            .stale_projection_head_tail
            .store(cut / 64, Ordering::SeqCst);
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let cold = StreamAggregate::open(
            &client,
            authority(),
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas(),
            None,
            crate::store::default_projection_read_limits(),
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None,
        )
        .await?;
        assert_eq!(cold.reducer().revision(), cut);
        assert_eq!(
            cold.reducer()
                .snapshot(usize::MAX, &mut None)?
                .0
                .events
                .len(),
            1
        );
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        provider
            .stale_projection_head_tail
            .store(0, Ordering::SeqCst);
        provider.forbid_writes.store(false, Ordering::SeqCst);
    }
    Ok(())
}

#[tokio::test]
async fn default_cold_projection_enforces_bounds_and_recovers_failed_observations() -> Result<()> {
    use std::sync::atomic::Ordering;
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let client = provider.clone();
    let mut writer = with_content(&client).await?;
    for revision in 0..69 {
        writer
            .execute(
                projection_command(u128::from(revision + 1), revision)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    drop(writer);
    provider.forbid_writes.store(true, Ordering::SeqCst);
    let defaults = default_projection_read_limits();
    let loaded = checkpoints::load_with_work(
        &client,
        &authority(),
        &issuer().verifier(&mut None).expect("verifier copy"),
        defaults,
        None,
        &mut None,
    )
    .await?;
    let suffix = HistoryReader::new(
        &client,
        &authority(),
        &(issuer().verifier(&mut None).expect("verifier copy")),
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
        StreamAggregate::open(
            &client,
            authority(),
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas(),
            None,
            exact,
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None
        )
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
        HistoryReadLimits {
            maximum_events: 0,
            ..exact
        },
    ] {
        assert!(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                schemas(),
                None,
                limits,
                crate::store::DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut None
            )
            .await
            .is_err()
        );
    }
    for fault in [1, 2, 3, 4] {
        provider.history_read_fault.store(fault, Ordering::SeqCst);
        assert!(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                schemas(),
                None,
                crate::store::default_projection_read_limits(),
                crate::store::DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut None
            )
            .await
            .is_err()
        );
        assert_eq!(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                schemas(),
                None,
                crate::store::default_projection_read_limits(),
                crate::store::DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut None
            )
            .await?
            .reducer()
            .revision(),
            69
        );
    }
    assert!(
        StreamAggregate::open(
            &client,
            authority(),
            AuthorityIssuer::new("test", [8; 32], authority())
                .verifier(&mut None)
                .expect("verifier copy"),
            schemas(),
            None,
            crate::store::default_projection_read_limits(),
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None
        )
        .await
        .is_err()
    );
    assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn projection_checkpoint_commit_loss_preserves_original_admission() -> Result<()> {
    use std::sync::atomic::Ordering;
    for fault in [1, 2, 3, 4, 5] {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = provider.clone();
        let mut writer = with_content(&client).await?;
        for revision in 0..63 {
            writer
                .execute(
                    projection_command(u128::from(revision + 1), revision)?,
                    #[cfg(feature = "filesystem")]
                    None,
                    &mut None,
                )
                .await?;
        }
        let original = projection_command(64, 63)?;
        provider
            .aggregate_commit_fault
            .store(fault, Ordering::SeqCst);
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
                std::sync::Arc::clone(&client),
                "harness/v3/projection-checkpoints/conversations/conversation-1"
            )?
            .tail()
            .await?,
            1
        );
    }
    Ok(())
}

fn forge_projection_payload(location: &mut Value, payload: &mut Vec<u8>, mode: u8) -> Result<()> {
    if mode == 1 {
        let mut forged: Value = crate::contract::json_from_slice(payload)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        forged["projection"]["lifecycle"] = json!("completed");
        let digest = crate::contract::canonical_json_digest(&(
            &forged["format_version"],
            &forged["authority"],
            &forged["revision"],
            &forged["events"],
            &forged["projection"],
        ))?;
        forged["state_digest"] = json!(digest);
        *payload = crate::contract::canonical_json_bytes(&forged)?;
        location["digest"] = json!(*blake3::hash(payload).as_bytes());
        location["bytes"] = json!(payload.len());
        location["chunks"] = json!([{
            "digest": *blake3::hash(payload).as_bytes(), "bytes": payload.len()
        }]);
    } else if mode == 4 {
        location["bytes"] = json!(u64::MAX);
    } else if mode == 5 {
        payload[0] ^= 1;
    } else if mode == 6 {
        location["chunks"] = json!([]);
    } else if mode == 7 {
        location["chunks"][0]["bytes"] = json!(0);
    } else if mode == 8 {
        location["chunks"][0]["bytes"] = json!(1);
    }
    Ok(())
}

async fn publish_copied_projection(
    client: &std::sync::Arc<MemoryStream>,
    canonical: &[acyclic_stream::Record],
    head_path: acyclic_stream::StreamPath,
    head_bytes: OwnedBytes,
    non_atomic: bool,
) -> Result<()> {
    use acyclic_stream::{CommitCondition, CommitMutation, CommitRequest};
    let stream = acyclic_stream::Stream::new(
        std::sync::Arc::clone(&client),
        authority().stream_path(&mut None)?,
    )?;
    stream
        .append_batch(
            canonical[..63]
                .iter()
                .map(|record| record.value.clone())
                .collect(),
            Some(0),
            None,
        )
        .await?;
    if non_atomic {
        stream.append(canonical[63].value.clone()).await?;
        acyclic_stream::Stream::new(std::sync::Arc::clone(&client), head_path.as_str())?
            .append(head_bytes)
            .await?;
    } else {
        let outcome = client
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
                        records: vec![canonical[63].value.clone()],
                    },
                    CommitMutation::Append {
                        path: head_path,
                        records: vec![head_bytes],
                    },
                ],
                idempotency_key: StreamIdempotencyKey::new("copied-projection")?,
            })
            .await?;
        assert!(matches!(outcome, CommitOutcome::Committed(_)));
    }
    Ok(())
}

async fn copied_projection_client(
    source: &std::sync::Arc<crate::test_stream::LostSessionAck<MemoryStream>>,
    mode: u8,
) -> Result<std::sync::Arc<MemoryStream>> {
    use acyclic_stream::StreamPath;
    let head_path =
        StreamPath::new("harness/v3/projection-checkpoints/conversations/conversation-1")?;
    let index = acyclic_stream::Stream::new(std::sync::Arc::clone(&source), head_path.as_str())?
        .read(0, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let mut location: Value = crate::contract::json_from_slice(&index[0].value)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let digest: [u8; 32] = serde_json::from_value(location["chunks"][0]["digest"].clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let chunk_path = format!(
        "harness/v3/projection-chunks/conversations/conversation-1/{}",
        blake3::Hash::from_bytes(digest).to_hex()
    );
    let chunk = acyclic_stream::Stream::new(std::sync::Arc::clone(&source), &chunk_path)?
        .read(0, 1)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let mut payload = chunk[0].value.to_vec();
    forge_projection_payload(&mut location, &mut payload, mode)?;
    let canonical = acyclic_stream::Stream::new(
        std::sync::Arc::clone(&source),
        authority().stream_path(&mut None)?,
    )?
    .read(0, 64)
    .await?
    .try_collect::<Vec<_>>()
    .await?;
    let client = Arc::new(MemoryStream::default());
    if mode != 0 {
        let digest: [u8; 32] = serde_json::from_value(if mode == 6 {
            json!(*blake3::hash(&payload).as_bytes())
        } else {
            location["chunks"][0]["digest"].clone()
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
        acyclic_stream::Stream::new(
            std::sync::Arc::clone(&client),
            format!(
                "harness/v3/projection-chunks/conversations/conversation-1/{}",
                blake3::Hash::from_bytes(digest).to_hex()
            ),
        )?
        .append(OwnedBytes::from(payload))
        .await?;
    }
    publish_copied_projection(
        &client,
        &canonical,
        head_path,
        OwnedBytes::from(crate::contract::canonical_json_bytes(&location)?),
        mode == 3,
    )
    .await?;
    Ok(client)
}

#[tokio::test]
async fn cold_projection_rejects_forged_state_and_non_atomic_heads() -> Result<()> {
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let source = provider;
    let mut writer = with_content(&source).await?;
    for revision in 0..64 {
        writer
            .execute(
                projection_command(u128::from(revision + 1), revision)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    for mode in 0..=8 {
        let client = copied_projection_client(&source, mode).await?;
        let result = StreamAggregate::open(
            &client,
            authority(),
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas(),
            None,
            crate::store::default_projection_read_limits(),
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None,
        )
        .await;
        match mode {
            1 => assert!(matches!(result, Err(Error::Unauthorized(_)))),
            2 => assert_eq!(result?.reducer().revision(), 64),
            _ => assert!(result.is_err()),
        }
    }
    Ok(())
}

#[tokio::test]
async fn supplied_projection_charges_snapshot_and_suffix_before_io() -> Result<()> {
    use std::sync::atomic::Ordering;
    let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
    let client = provider.clone();
    let mut writer = with_content(&client).await?;
    for revision in 0..64 {
        writer
            .execute(
                projection_command(u128::from(revision + 1), revision)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    let snapshot = writer
        .reducer()
        .snapshot(1, &mut None)
        .map(|(snapshot, _)| snapshot)?;
    let snapshot_bytes = crate::contract::canonical_json_bytes(&snapshot)?.len() as u64;
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
    let suffix = acyclic_stream::Stream::new(
        std::sync::Arc::clone(&client),
        authority().stream_path(&mut None)?,
    )?
    .read(64, 5)
    .await?
    .try_collect::<Vec<_>>()
    .await?;
    let bytes = snapshot_bytes
        + suffix
            .iter()
            .map(|record| record.value.len() as u64)
            .sum::<u64>();
    provider.forbid_writes.store(true, Ordering::SeqCst);
    let restored = StreamAggregate::open(
        &client,
        authority(),
        issuer().verifier(&mut None).expect("verifier copy"),
        schemas(),
        Some(snapshot.copy(&mut None)?),
        HistoryReadLimits {
            maximum_events: 6,
            maximum_bytes: bytes,
        },
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    assert_eq!(restored.reducer().revision(), 69);
    for limits in [
        HistoryReadLimits {
            maximum_events: 5,
            maximum_bytes: bytes,
        },
        HistoryReadLimits {
            maximum_events: 6,
            maximum_bytes: bytes - 1,
        },
    ] {
        assert!(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(&mut None).expect("verifier copy"),
                schemas(),
                Some(snapshot.copy(&mut None)?),
                limits,
                crate::store::DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut None
            )
            .await
            .is_err()
        );
    }
    provider.observation_reads.store(0, Ordering::SeqCst);
    assert!(
        StreamAggregate::open(
            &client,
            authority(),
            issuer().verifier(&mut None).expect("verifier copy"),
            schemas(),
            Some(snapshot),
            HistoryReadLimits {
                maximum_events: 6,
                maximum_bytes: snapshot_bytes - 1
            },
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None
        )
        .await
        .is_err()
    );
    assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
    Ok(())
}
