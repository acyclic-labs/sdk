//! A source run's [`Recording`], read through each agent's original journal.
use super::*;
use crate::{
    executor::{ExecutionEvent, ExecutionPayloadField, ExecutionRecord},
    model::{ModelEvent, RecordedCall, RecordedToolOutcome, Recording, replay_key},
    tool::ToolResult,
};

/// Only the current call can still receive settlements. The next admission
/// releases its completed predecessor to the recording in journal order.
#[derive(Default)]
enum Input {
    #[default]
    Empty,
    Started([u8; 32], u32, RecordedCall),
    Completed([u8; 32], u32, RecordedCall),
}

impl Input {
    fn start(&mut self, key: [u8; 32], step: u32, recording: &mut Recording) {
        self.flush(recording);
        *self = Self::Started(key, step, RecordedCall::default());
    }

    fn event(&mut self, bytes: Vec<u8>) -> Result<()> {
        let terminal = matches!(
            acyclic_harness_core::contract::json_from_slice(&bytes).map_err(failed)?,
            ModelEvent::Completed { .. }
        );
        let Self::Started(_, _, call) = self else {
            return Err(Error::Storage("model event precedes its admission".into()));
        };
        call.events.push(bytes);
        if terminal && let Self::Started(key, step, call) = std::mem::take(self) {
            *self = Self::Completed(key, step, call);
        }
        Ok(())
    }

    /// Attaches a settlement to the answer at `step` that made the call; a
    /// summary never makes one.
    fn tool(&mut self, step: u32, call_id: &str, outcome: RecordedToolOutcome) {
        if let Self::Completed(_, recorded, call) = self
            && *recorded == step
        {
            call.tools.push((call_id.to_owned(), outcome));
        }
    }

    fn flush(&mut self, recording: &mut Recording) {
        // An interrupted call has no answer to replay.
        if let Self::Completed(digest, _, call) = std::mem::take(self) {
            recording.insert(digest, call);
        }
    }
}

async fn record_agent(
    connection: &Connection,
    agent: &AgentPath,
    recording: &mut Recording,
) -> Result<()> {
    let (mut input, mut after) = (0, 0);
    let mut calls = Input::default();
    loop {
        let AgentReply::Events {
            input: admitted,
            page,
            ..
        } = connection
            .execute(&AgentCommand::Events {
                agent: agent.clone(),
                input,
                after,
                wait: false,
            })
            .await?
        else {
            return Err(unexpected());
        };
        if admitted != input {
            calls.flush(recording);
        }
        input = admitted;
        let Some(page) = page else {
            break;
        };
        for record in &page.items {
            observe(connection, agent, input, record, &mut calls, recording).await?;
        }
        if let Some(next) = page.next {
            after = next;
        } else {
            calls.flush(recording);
            input = input
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("recording input continuation overflows".into()))?;
            after = 0;
        }
    }
    calls.flush(recording);
    Ok(())
}

async fn observe(
    connection: &Connection,
    agent: &AgentPath,
    input: u64,
    record: &ExecutionRecord,
    calls: &mut Input,
    recording: &mut Recording,
) -> Result<()> {
    let payload = |field| {
        Conversation::read(
            connection,
            agent,
            ReadTarget::Execution {
                input,
                sequence: record.sequence,
                field,
            },
        )
    };
    match &record.event {
        ExecutionEvent::ModelStarted { step, .. } => {
            let key = replay_key(&payload(ExecutionPayloadField::AdmittedRequest).await?)?;
            calls.start(key, *step, recording);
        }
        ExecutionEvent::ModelInline { event, .. } => calls.event(event.as_bytes().to_vec())?,
        ExecutionEvent::Model { .. } => {
            calls.event(payload(ExecutionPayloadField::Event).await?)?
        }
        ExecutionEvent::ToolCompleted { step, call_id, .. } => {
            let result: ToolResult = acyclic_harness_core::contract::json_from_slice(
                &payload(ExecutionPayloadField::Result).await?,
            )
            .map_err(failed)?;
            calls.tool(*step, call_id, RecordedToolOutcome::Completed(result));
        }
        // A refusal settles before dispatch and is decided again on replay.
        ExecutionEvent::ToolFailed {
            step,
            call_id,
            reason,
            cause,
        } if !reason.precedes_dispatch() => calls.tool(
            *step,
            call_id,
            RecordedToolOutcome::Failed {
                reason: *reason,
                cause: cause.clone(),
            },
        ),
        _ => {}
    }
    Ok(())
}

impl Conversation {
    /// Every completed model call of this tree's settled turns, as a
    /// [`Recording`] for [`crate::model::RecordedModel`]. Each agent's journal
    /// is paged once, from its own fork cut; payloads are read for each
    /// request, model events stored outside their frames and tool results.
    /// Call after the source run settles.
    pub async fn recording(&self) -> Result<Recording> {
        let mut recording = Recording::default();
        let mut tree = super::tree::TreeWalk::new(self.agent());
        while let Some(agent) = tree.next(&self.connection).await? {
            record_agent(&self.connection, &agent, &mut recording).await?;
        }
        Ok(recording)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::{Value, json};

    proptest! {
        #[test]
        fn export_releases_completed_prefixes_and_preserves_settlements(
            admissions in prop::collection::vec((any::<u8>(), any::<bool>(), 0u8..4), 0..32),
        ) {
            let mut input = Input::default();
            let mut actual = Recording::default();
            let mut expected = Recording::default();
            for (index, (key, complete, tools)) in admissions.into_iter().enumerate() {
                // Admissions can share a step; repeated keys retain source order.
                let step = index as u32 / 2;
                let key = [key; 32];
                input.start(key, step, &mut actual);
                prop_assert_eq!(&actual, &expected, "the previous completed prefix is released");
                let content = acyclic_harness_core::contract::canonical_json_bytes(
                    &ModelEvent::Content { delta: format!("generated {index}\0 text") },
                ).unwrap();
                input.event(content.clone()).unwrap();
                let mut call = RecordedCall { events: vec![content], tools: Vec::new() };
                if complete {
                    let terminal = acyclic_harness_core::contract::canonical_json_bytes(
                        &ModelEvent::Completed { metadata: json!({"generated":index}) },
                    ).unwrap();
                    input.event(terminal.clone()).unwrap();
                    call.events.push(terminal);
                }
                for tool in 0..tools {
                    let id = format!("call-{tool}");
                    let outcome = RecordedToolOutcome::Completed(ToolResult { value: json!([index, tool]) });
                    input.tool(step, &id, outcome.clone());
                    if complete { call.tools.push((id, outcome)); }
                }
                // A settlement for another step cannot attach to this answer.
                input.tool(step + 1, "other-step", RecordedToolOutcome::Completed(ToolResult { value: Value::Null }));
                if complete { expected.insert(key, call); }
            }
            input.flush(&mut actual);
            input.flush(&mut actual);
            prop_assert_eq!(actual, expected, "partial calls are discarded; completed bytes and order survive");
        }
    }

    #[test]
    fn completion_uses_no_history_buffer_allocation_and_keeps_admitted_nesting() -> Result<()> {
        let mut metadata = Value::Null;
        for _ in 0..128 {
            metadata = Value::Array(vec![metadata]);
        }
        let rejected =
            acyclic_harness_core::contract::canonical_json_bytes(&ModelEvent::Completed {
                metadata: Value::Array(vec![metadata.clone()]),
            })?;
        assert!(acyclic_harness_core::contract::json_from_slice::<ModelEvent>(&rejected).is_err());
        // Generated typed operands, not external provider-output fixtures.
        let bytes = acyclic_harness_core::contract::canonical_json_bytes(&ModelEvent::Completed {
            metadata,
        })?;
        assert!(
            serde_json::from_slice::<ModelEvent>(&bytes).is_err(),
            "the default JSON decoder rejects an otherwise admitted event"
        );
        let decoded = acyclic_test_support::measure(|| {
            acyclic_harness_core::contract::json_from_slice::<ModelEvent>(&bytes).unwrap();
        });
        for count in [1, 64, 1024] {
            let mut input = Input::default();
            let mut recording = Recording::default();
            for step in 0..count {
                input.start([0; 32], step, &mut recording);
                let Input::Started(_, _, call) = &mut input else {
                    unreachable!()
                };
                call.events.reserve_exact(1);
                let event = bytes.clone();
                let completed = acyclic_test_support::measure(|| input.event(event).unwrap());
                assert_eq!(
                    completed.count_total, decoded.count_total,
                    "completion adds no allocation beyond the original event decoder"
                );
                assert_eq!(completed.bytes_total, decoded.bytes_total);
            }
            // Restoring the old completed-call Vec adds storage without
            // changing the original event or its retained payload bytes.
            let buffered = acyclic_test_support::measure(|| {
                let mut old = Vec::new();
                let Input::Completed(key, step, call) = std::mem::take(&mut input) else {
                    unreachable!()
                };
                old.push((key, step, call));
                std::hint::black_box(&old);
                let (key, step, call) = old.pop().unwrap();
                input = Input::Completed(key, step, call);
            });
            assert!(buffered.count_total > 0);
            input.flush(&mut recording);
            assert_eq!(recording.len(), count as usize);
        }
        Ok(())
    }
}
