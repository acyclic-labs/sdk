//! Canonical usage is the sum of stored model steps, including steps with tools.
use super::*;
use acyclic_harness::{
    AgentId, OperationId,
    context::{CompactionPolicy, ModelTokenCount},
    conversation::{BoundedContentFuture, ContentResidencyVerifier, FileRef, Limits},
    filesystem::{MemoryHarnessStorage, PreparationIo},
    model::{Model, ModelEvent},
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
    wire_api::{ReplayMode, replay_history},
    wire_codec::{decode_event_payload, encode_authority},
};
use acyclic_stream::OwnedBytes;
use futures::StreamExt;
use serde_json::{Value, json};

struct Files(Arc<MemoryHarnessStorage>);
impl ContentResidencyVerifier for Files {
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, OwnedBytes>> {
        self.0.content_reader().read(file, maximum, io)
    }
}

async fn whole_turn_usage(first: u16, second: Option<u16>, final_step_only: bool) -> Result<()> {
    let limits = Limits {
        file_bytes: 65_536,
        ..Limits::default()
    };
    let storage = Arc::new(
        MemoryHarnessStorage::new(AgentId::from_bytes([31; 16]), limits.file_bytes).await?,
    );
    let turn = OperationId::from_bytes([32; 16]);
    let file = storage
        .stage(turn, "input.txt", b"read me", "text/plain", "input.txt")
        .await?;
    let first_metadata = json!({"tokens": first, "cost": {"micro_units":3}, "finish":"tool"});
    let final_metadata = second.map_or(
        Value::Null,
        |second| json!({"tokens": second, "cost": {"micro_units":4}, "finish":"stop"}),
    );
    let model = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(acyclic_harness::model::ToolCall::new(
                    "read".into(),
                    "read".into(),
                    json!({"path":file.path()}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: first_metadata.clone(),
                }),
            ]),
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::Content {
                    delta: "done".into(),
                }),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: final_metadata.clone(),
                }),
            ]),
        ])
        .with_token_counter(|request| {
            Ok(ModelTokenCount::from_upper_bounds(
                request.request_digest(),
                512,
                vec![512; request.request().messages.len()],
            ))
        }),
    );
    let bundle = storage
        .builder()
        .model(
            Model::new("test".into(), "usage".into(), "1".into(), Value::Null)?,
            model.clone(),
        )
        .grant("model:generate")
        .compaction(CompactionPolicy::Disabled)
        .limits(limits)
        .build()?;
    let input = storage
        .stage(turn, "user.txt", b"read the file", "text/plain", "user.txt")
        .await?;
    let output = storage
        .run_conversation(&bundle, turn, input.clone(), Vec::new(), 2)
        .await?;
    assert_eq!(output.steps, 2);
    assert_eq!(
        output.metadata, final_metadata,
        "TurnOutput keeps the provider's final-step metadata contract"
    );
    assert_eq!(
        storage
            .run_conversation(&bundle, turn, input, Vec::new(), 2)
            .await?,
        output
    );
    assert_eq!(
        model.requests().len(),
        2,
        "writer retry must reuse stored steps"
    );

    let authority = encode_authority(storage.conversation());
    let cursor = wire::ReplayCursor {
        authority: Some(authority.clone()),
        generation: "generation".into(),
        revision: 0,
    };
    let mut history = replay_history(
        storage.stream(),
        &storage.verifier(),
        cursor.clone(),
        1_000_000,
        ReplayMode::Snapshot,
    )
    .await?;
    let mut deliveries = Vec::new();
    while let Some(delivery) = history.next().await {
        let mut delivery = delivery?;
        if final_step_only {
            // Negative control: substitute the old writer's final-only file,
            // then run precisely the same production replay and accounting fold.
            for event in &mut delivery.events {
                let (mut payload, _) = decode_event_payload(
                    &event.event_type,
                    &event.canonical_payload_json,
                    &mut None,
                )?;
                if let EventPayload::ConversationMessageAppended { message } = &mut payload
                    && message.kind == acyclic_harness::conversation::MessageKind::Assistant
                {
                    let bytes = serde_json::to_vec(&final_metadata).unwrap();
                    let old = storage
                        .stage(
                            OperationId::from_bytes([33; 16]),
                            "old-metadata.json",
                            &bytes,
                            "application/json",
                            "old-metadata.json",
                        )
                        .await?;
                    message
                        .extensions
                        .insert("acyclic.model.metadata".into(), old);
                    event.canonical_payload_json = serde_json::to_vec(&payload).unwrap();
                }
            }
        }
        deliveries.push(delivery);
    }
    let expected = json!({"tokens":u32::from(first) + u32::from(second.unwrap_or(0)), "cost":{"micro_units":if second.is_some() {7} else {3}}, "finish":if second.is_some() {"stop"} else {"tool"}});
    let mut committed = 0;
    for delivery in deliveries {
        for event in delivery.events {
            let (payload, _) =
                decode_event_payload(&event.event_type, &event.canonical_payload_json, &mut None)?;
            if let EventPayload::ConversationMessageAppended { message } = payload
                && message.kind == acyclic_harness::conversation::MessageKind::Assistant
            {
                let reference = &message.extensions["acyclic.model.metadata"];
                let metadata = Files(storage.clone())
                    .read(reference, 65_536, None)?
                    .await?;
                assert_eq!(
                    serde_json::from_slice::<Value>(&metadata).expect("canonical writer metadata"),
                    expected,
                    "finished turn must retain all step usage"
                );
                committed += 1;
            }
        }
    }
    assert_eq!(committed, 1);
    Ok(())
}

#[tokio::test]
async fn canonical_writer_preserves_tool_step_usage_through_commit_retry_and_reopen() -> Result<()>
{
    for (first, second) in [(2, Some(5)), (9, Some(0)), (1, Some(1)), (7, None)] {
        whole_turn_usage(first, second, false).await?;
    }
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "finished turn must retain all step usage")]
async fn negative_control_final_step_only_metadata_loses_tool_usage() {
    whole_turn_usage(2, Some(5), true).await.unwrap();
}

#[path = "../src/model/usage.rs"]
mod production_usage;

proptest::proptest! {
    // Property: the exact shared live/writer accumulator retains the sum of every
    // completed step in provider units, independently of tool/answer boundaries.
    #[test]
    fn canonical_step_usage_equals_the_sum_of_live_completions(
        steps in proptest::collection::vec(proptest::option::of((0u16..1000, 0u16..1000)), 0..64),
    ) {
        let mut metadata = Value::Null;
        let mut tokens = 0u64;
        let mut cost = 0u64;
        let mut present = false;
        for step in steps {
            let value = match step {
                Some((step_tokens, step_cost)) => {
                    present = true;
                    tokens += u64::from(step_tokens);
                    cost += u64::from(step_cost);
                    json!({"tokens":step_tokens, "cost":{"micro_units":step_cost}})
                }
                None => Value::Null,
            };
            production_usage::accumulate_usage(&mut metadata, value).unwrap();
            let expected = if present {json!({"tokens":tokens, "cost":{"micro_units":cost}})} else {Value::Null};
            proptest::prop_assert_eq!(&metadata, &expected);
            let decoded: Value = serde_json::from_slice(&serde_json::to_vec(&metadata).unwrap()).unwrap();
            proptest::prop_assert_eq!(&decoded, &metadata);
        }
    }
}
