use super::*;
use crate::core::LifecycleState;
use futures::{FutureExt, StreamExt};

fn limits() -> HistoryReadLimits {
    HistoryReadLimits {
        maximum_events: 2,
        maximum_bytes: 65_536,
    }
}

#[tokio::test]
async fn follows_later_owner_waiting_without_scheduler_changes() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut writer = with_content(&client).await?;
    for (revision, state) in [(0, LifecycleState::Active), (1, LifecycleState::Waiting)] {
        let mut next = command(revision as u8 + 1)?;
        next.expected_revision = revision;
        next.action = Action::TransitionLifecycle {
            to: state,
            reason: None,
        };
        next.scope = issuer().issue(
            "root",
            &Capabilities::new(["lifecycle:manage"]),
            None,
            None,
            &mut None,
        )?;
        if revision == 0 {
            writer
                .execute(
                    next,
                    #[cfg(feature = "filesystem")]
                    None,
                    &mut None,
                )
                .await?;
            continue;
        }
        let mut history = writer.history_reader()?.follow(revision, limits()).await?;
        assert!(history.next().now_or_never().is_none());
        // No coordinator/scheduler event is published by this owner transition.
        writer
            .execute(
                next.clone(),
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
        let batch = history
            .next()
            .now_or_never()
            .expect("append must wake live replay")
            .expect("live replay cannot end at the old snapshot")?;
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].revision, 2);
        assert_eq!(batch[0].operation_id, next.operation_id);
        assert!(matches!(
            batch[0].payload,
            EventPayload::LifecycleTransitioned {
                to: LifecycleState::Waiting,
                ..
            }
        ));
        assert!(history.next().now_or_never().is_none());
    }
    Ok(())
}

#[tokio::test]
async fn follow_batches_ready_records_with_count_and_byte_bounds() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut writer = with_content(&client).await?;
    for revision in 0..5 {
        writer
            .execute(
                projection_command(u128::from(revision) + 1, revision)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    let event = writer.read_event_at(1, &mut None).await?.0;
    let bytes = encode_event(&authority(), &event, &mut None)?.len() as u64;
    for (bounds, lengths) in [
        (limits(), vec![2, 2, 1]),
        (
            HistoryReadLimits {
                maximum_bytes: bytes,
                ..limits()
            },
            vec![1; 5],
        ),
    ] {
        let mut history = writer.history_reader()?.follow(0, bounds).await?;
        let mut revision = 0;
        for length in lengths {
            let events = history
                .next()
                .now_or_never()
                .expect("ready records cannot wait")
                .expect("records available")?;
            assert_eq!(events.len(), length);
            for event in events {
                revision += 1;
                assert_eq!(event.revision, revision);
            }
        }
        assert_eq!(revision, 5);
        assert!(history.next().now_or_never().is_none());
    }
    for bounds in [
        HistoryReadLimits {
            maximum_events: 0,
            ..limits()
        },
        HistoryReadLimits {
            maximum_bytes: 0,
            ..limits()
        },
    ] {
        assert!(writer.history_reader()?.follow(0, bounds).await.is_err());
    }
    let mut too_small = writer
        .history_reader()?
        .follow(
            0,
            HistoryReadLimits {
                maximum_bytes: 1,
                ..limits()
            },
        )
        .await?;
    assert!(too_small.next().await.expect("oversized record").is_err());
    assert!(writer.history_reader()?.follow(6, limits()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn follow_verifies_authority_and_attestation() -> Result<()> {
    for wrong_authority in [false, true] {
        let client = Arc::new(MemoryStream::default());
        let mut reducer = Reducer::new(authority(), issuer().verifier(&mut None)?, schemas());
        let ApplyResult::Applied { mut event } = reducer.apply(&command(1)?)? else {
            panic!("fresh event");
        };
        let mut encoded_owner = authority();
        if wrong_authority {
            encoded_owner.id = "other".into();
        } else {
            event.intent_digest = [0; 32];
        }
        acyclic_stream::Stream::new(client.clone(), authority().stream_path(&mut None)?)?
            .append(encode_event(&encoded_owner, &event, &mut None)?)
            .await?;
        let reader = HistoryReader::new(
            &client,
            &authority(),
            &issuer().verifier(&mut None)?,
            &mut None,
        )?;
        let mut history = reader.follow(0, limits()).await?;
        assert!(history.next().await.expect("invalid record").is_err());
    }
    Ok(())
}
