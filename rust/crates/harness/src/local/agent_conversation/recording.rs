//! A source run's [`Recording`], read through each agent's original journal.
use super::*;
use crate::{
    executor::{ExecutionEvent, ExecutionPayloadField, ExecutionRecord},
    model::{ModelEvent, RecordedCall, Recording, replay_key},
    tool::ToolResult,
};

/// Completed calls of one input, by step, collecting their tool results.
#[derive(Default)]
struct Input {
    started: Option<([u8; 32], u32, RecordedCall)>,
    completed: Vec<([u8; 32], u32, RecordedCall)>,
}

impl Input {
    fn event(&mut self, bytes: Vec<u8>) -> Result<()> {
        let terminal = matches!(
            serde_json::from_slice(&bytes).map_err(failed)?,
            ModelEvent::Completed { .. }
        );
        let Some((_, _, call)) = self.started.as_mut() else {
            return Err(Error::Storage("model event precedes its admission".into()));
        };
        call.events.push(bytes);
        if terminal {
            self.completed.extend(self.started.take());
        }
        Ok(())
    }

    fn flush(&mut self, recording: &mut Recording) {
        // An interrupted call has no answer to replay.
        self.started = None;
        for (digest, _, call) in self.completed.drain(..) {
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
            observe(connection, agent, input, record, &mut calls).await?;
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
            calls.started = Some((key, *step, RecordedCall::default()));
        }
        ExecutionEvent::ModelInline { event, .. } => calls.event(event.as_bytes().to_vec())?,
        ExecutionEvent::Model { .. } => calls.event(payload(ExecutionPayloadField::Event).await?)?,
        ExecutionEvent::ToolCompleted { step, call_id, .. } => {
            let result: ToolResult =
                serde_json::from_slice(&payload(ExecutionPayloadField::Result).await?)
                    .map_err(failed)?;
            // The answer at this step made the call; a summary never does.
            if let Some((_, _, call)) = calls
                .completed
                .iter_mut()
                .rev()
                .find(|(_, recorded, _)| recorded == step)
            {
                call.tools.push((call_id.clone(), result));
            }
        }
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
