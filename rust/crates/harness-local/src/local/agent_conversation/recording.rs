//! A source run's [`Recording`], read through each agent's original journal.
use super::*;
use crate::{
    executor::{ExecutionEvent, ExecutionPayloadField, ExecutionRecord},
    model::{ModelDispatch, ModelEvent, RecordedCall, RecordedToolOutcome, Recording, replay_key},
    tool::ToolResult,
};

/// Completed calls of one input, by step, collecting their tool results.
#[derive(Default)]
struct Input {
    started: Option<([u8; 32], u32, RecordedCall)>,
    completed: Vec<([u8; 32], u32, RecordedCall)>,
}

impl Input {
    fn event(&mut self, event: ModelEvent) -> Result<()> {
        let terminal = matches!(event, ModelEvent::Completed { .. });
        let Some((_, _, call)) = self.started.as_mut() else {
            return Err(Error::Storage("model event precedes its admission".into()));
        };
        call.events
            .push(crate::contract::canonical_json_bytes(&event)?);
        if terminal {
            self.completed.extend(self.started.take());
        }
        Ok(())
    }

    /// Attaches a settlement to the answer at `step` that made the call; a
    /// summary never makes one.
    fn tool(&mut self, step: u32, call_id: &str, outcome: RecordedToolOutcome) {
        if let Some((_, _, call)) = self
            .completed
            .iter_mut()
            .rev()
            .find(|(_, recorded, _)| *recorded == step)
        {
            call.tools.push((call_id.to_owned(), outcome));
        }
    }

    fn flush(&mut self, recording: &mut Recording) {
        // An interrupted call has no answer to replay.
        self.started = None;
        for (digest, _, call) in self.completed.drain(..) {
            recording.insert(digest, call);
        }
    }
}

/// Every committed execution record of `agent`, in journal order, with the
/// input that owns it. Pages from the agent's own fork cut; a short page
/// advances to the next input.
async fn execution_records(
    connection: &Connection,
    agent: &AgentPath,
) -> Result<Vec<(u64, ExecutionRecord)>> {
    let (mut input, mut after) = (0, 0);
    let mut records = Vec::new();
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
        input = admitted;
        let Some(page) = page else {
            break;
        };
        records.extend(page.items.into_iter().map(|record| (input, record)));
        if let Some(next) = page.next {
            after = next;
        } else {
            input = input
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("recording input continuation overflows".into()))?;
            after = 0;
        }
    }
    Ok(records)
}

async fn record_agent(
    connection: &Connection,
    agent: &AgentPath,
    recording: &mut Recording,
) -> Result<()> {
    let mut calls = Input::default();
    let mut current = None;
    for (input, record) in execution_records(connection, agent).await? {
        if current != Some(input) {
            calls.flush(recording);
            current = Some(input);
        }
        observe(connection, agent, input, &record, &mut calls).await?;
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
        ExecutionEvent::ModelStarted {
            step,
            purpose,
            request_digest,
            ..
        } => {
            let key = replay_key(&payload(ExecutionPayloadField::AdmittedRequest).await?)?;
            calls.started = Some((
                key,
                *step,
                RecordedCall {
                    source: Some(ModelDispatch {
                        operation_id: purpose.attempt_operation(record.operation_id, None)?,
                        step: *step,
                        request_digest: *request_digest,
                    }),
                    ..Default::default()
                },
            ));
        }
        ExecutionEvent::ModelInline { event, .. } => {
            calls.event(serde_json::from_str(event).map_err(failed)?)?
        }
        ExecutionEvent::ModelWait {
            purpose: crate::executor::ModelPurpose::Count,
            ..
        } => {}
        ExecutionEvent::ModelWait { observation, .. } => {
            calls.event(ModelEvent::Wait(*observation))?
        }
        ExecutionEvent::Model { event, .. } => {
            let bytes = payload(ExecutionPayloadField::Event).await?;
            event.descriptor().verify(&bytes, &mut None)?;
            calls.event(crate::executor::decode_model_payload(
                event.descriptor().media_type(),
                &bytes,
                None,
            )?)?
        }
        ExecutionEvent::ToolCompleted { step, call_id, .. } => {
            let result: ToolResult =
                serde_json::from_slice(&payload(ExecutionPayloadField::Result).await?)
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
    /// Every committed execution record of this conversation's agent, in
    /// journal order. Call after the source run settles.
    ///
    /// # Errors
    /// Original session, journal or encoding failure.
    pub async fn execution_records(&self) -> Result<Vec<ExecutionRecord>> {
        Ok(execution_records(&self.connection, self.agent())
            .await?
            .into_iter()
            .map(|(_, record)| record)
            .collect())
    }

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
