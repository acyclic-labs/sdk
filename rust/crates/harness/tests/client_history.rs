//! Verified per-record push through the public history API.
use acyclic_harness::{
    Capabilities, IdempotencyKey, OperationId, Result,
    core::{
        Action, AggregateKind, ApplyResult, Authority, AuthorityIssuer, Command, EventPayload,
        LifecycleState, Reducer, SchemaRegistry,
    },
    store::{
        DEFAULT_TERMINAL_EFFECTS, HistoryReader, StreamAggregate, default_projection_read_limits,
    },
    wire_codec::encode_event_envelope,
};
use acyclic_stream::MemoryStream;
use futures::{FutureExt, StreamExt};
use prost::Message;
use std::sync::Arc;

fn authority() -> Authority {
    Authority {
        kind: AggregateKind::Task,
        id: "live-history".into(),
    }
}
fn issuer() -> AuthorityIssuer {
    AuthorityIssuer::new("test", [7; 32], authority())
}
fn transition(revision: u64, to: LifecycleState) -> Result<Command> {
    let operation_id = OperationId::from_bytes([revision as u8 + 1; 16]);
    Ok(Command {
        operation_id,
        idempotency_key: IdempotencyKey::new(operation_id.to_string())?,
        expected_revision: revision,
        scope: issuer().issue(
            "root",
            &Capabilities::new(["lifecycle:manage"]),
            None,
            None,
            &mut None,
        )?,
        causal_parent: None,
        action: Action::TransitionLifecycle { to, reason: None },
    })
}
async fn writer(client: &Arc<MemoryStream>) -> Result<StreamAggregate<MemoryStream>> {
    StreamAggregate::open(
        client,
        (
            authority(),
            issuer().verifier(&mut None)?,
            SchemaRegistry::new(),
        ),
        None,
        default_projection_read_limits(),
        DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await
}

#[tokio::test]
async fn follows_later_owner_waiting_without_scheduler_changes() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut writer = writer(&client).await?;
    for (revision, state) in [(0, LifecycleState::Active), (1, LifecycleState::Waiting)] {
        let next = transition(revision, state)?;
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
        let mut history = writer.history_reader()?.follow(revision, 65_536).await?;
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
        let event = history
            .next()
            .now_or_never()
            .expect("append must wake live replay")
            .expect("live replay cannot end at the old snapshot")?;
        assert_eq!(event.revision, 2);
        assert_eq!(event.operation_id, next.operation_id);
        assert!(matches!(
            event.payload,
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
async fn follow_pushes_individual_ready_records_and_checks_byte_bounds() -> Result<()> {
    let client = Arc::new(MemoryStream::default());
    let mut writer = writer(&client).await?;
    for (revision, state) in [
        LifecycleState::Active,
        LifecycleState::Waiting,
        LifecycleState::Active,
        LifecycleState::Waiting,
        LifecycleState::Active,
    ]
    .into_iter()
    .enumerate()
    {
        writer
            .execute(
                transition(revision as u64, state)?,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
    }
    let mut bytes = 0;
    for revision in 1..=5 {
        let event = writer.read_event_at(revision, &mut None).await?.0;
        bytes =
            bytes.max(encode_event_envelope(&authority(), &event, &mut None)?.encoded_len() as u64);
    }
    for maximum in [bytes, 65_536] {
        let mut history = writer.history_reader()?.follow(0, maximum).await?;
        for revision in 1..=5 {
            let event = history
                .next()
                .now_or_never()
                .expect("ready append cannot wait")
                .expect("record available")?;
            assert_eq!(event.revision, revision);
        }
        assert!(history.next().now_or_never().is_none());
    }
    assert!(writer.history_reader()?.follow(0, 0).await.is_err());
    let mut too_small = writer.history_reader()?.follow(0, 1).await?;
    assert!(too_small.next().await.expect("oversized record").is_err());
    assert!(writer.history_reader()?.follow(6, 65_536).await.is_err());
    Ok(())
}

#[tokio::test]
async fn follow_verifies_authority_and_attestation() -> Result<()> {
    for wrong_authority in [false, true] {
        let client = Arc::new(MemoryStream::default());
        let mut reducer = Reducer::new(
            authority(),
            issuer().verifier(&mut None)?,
            SchemaRegistry::new(),
        );
        let ApplyResult::Applied { mut event } =
            reducer.apply(&transition(0, LifecycleState::Active)?)?
        else {
            panic!("fresh event");
        };
        let mut encoded_owner = authority();
        if wrong_authority {
            encoded_owner.id = "other".into();
        } else {
            event.intent_digest = [0; 32];
        }
        acyclic_stream::Stream::new(client.clone(), authority().stream_path(&mut None)?)?
            .append(encode_event_envelope(&encoded_owner, &event, &mut None)?.encode_to_vec())
            .await?;
        let reader = HistoryReader::new(
            &client,
            &authority(),
            &issuer().verifier(&mut None)?,
            &mut None,
        )?;
        let mut history = reader.follow(0, 65_536).await?;
        assert!(history.next().await.expect("invalid record").is_err());
    }
    Ok(())
}
