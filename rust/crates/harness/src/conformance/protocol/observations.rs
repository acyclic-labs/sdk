//! Complete original tool results through the public execution query and reader.
use super::{Endpoint, Result, TOOL, call};
use acyclic_harness_client::{AgentCommand, ExecutionPosition, ExecutionQuery, ReadTarget};
use acyclic_harness_core::{
    agent::AgentPath,
    executor::{ExecutionEvent, ExecutionPayloadField},
    tool::{RecordedInvocation, ToolInvocation, ToolResult},
};

pub(super) async fn verify(endpoint: &dyn Endpoint, agent: &AgentPath, input: u64) -> Result {
    let mut after = 0;
    let mut operation = None;
    let mut pending = None;
    let mut completed = 0;
    loop {
        let command = AgentCommand::Events {
            agent: agent.clone(),
            input,
            after,
            wait: true,
        };
        let original = call(endpoint, &command).await??;
        // Reopening the exact public cursor preserves the source positions and
        // immutable references. It neither sends another input nor rediscovers a tail.
        if serde_json::to_value(&original)?
            != serde_json::to_value(call(endpoint, &command).await??)?
        {
            return Err("execution replay changed the original page".into());
        }
        let mut query = ExecutionQuery::try_from(original)?;
        let page = query
            .page
            .as_ref()
            .ok_or("settled input omitted its execution page")?;
        if query.input != input
            || page.operation.is_none()
            || operation.is_some_and(|original| page.operation != Some(original))
        {
            return Err("execution page selected another original input or operation".into());
        }
        operation = page.operation;
        for record in &page.items {
            after = after.checked_add(1).ok_or("execution cursor overflow")?;
            if record.sequence != after || Some(record.operation_id) != operation {
                return Err("execution page skipped or substituted an original record".into());
            }
            match &record.event {
                ExecutionEvent::ToolStarted {
                    step,
                    call_id,
                    invocation: original,
                    ..
                } => {
                    let bytes = read(
                        endpoint,
                        agent,
                        input,
                        record.sequence,
                        ExecutionPayloadField::Invocation,
                    )
                    .await?;
                    if bytes != endpoint.read_file(agent, original).await? {
                        return Err(
                            "execution reader changed the original invocation envelope".into()
                        );
                    }
                    let recorded: RecordedInvocation = serde_json::from_slice(&bytes)?;
                    if recorded.metadata != super::invocation_metadata()
                        || recorded.target.is_some()
                        || recorded.filesystem.is_some()
                    {
                        return Err(
                            "execution invocation changed original envelope authority".into()
                        );
                    }
                    // Mutate the actual producer's envelope, not a parallel
                    // fixture. Readers must reject an unknown outer field even
                    // when its original invocation remains valid.
                    let mut foreign: serde_json::Value = serde_json::from_slice(&bytes)?;
                    foreign["foreign_invocation_authority"] = true.into();
                    if serde_json::from_value::<RecordedInvocation>(foreign).is_ok() {
                        return Err(
                            "execution invocation accepted foreign envelope authority".into()
                        );
                    }
                    let mut invocation = recorded.invocation;
                    admission(&invocation, record.operation_id, *step, call_id)?;
                    // Change only the actual saved operation. Its model call ID,
                    // name, arguments and envelope still belong to the original call.
                    let original = invocation.operation_id;
                    let mut changed = original.into_bytes();
                    changed[0] ^= 1;
                    invocation.operation_id =
                        acyclic_harness_core::OperationId::from_bytes(changed);
                    if admission(&invocation, record.operation_id, *step, call_id).is_ok() {
                        return Err("execution invocation accepted another tool operation".into());
                    }
                    invocation.operation_id = original;
                    if pending.is_some() {
                        return Err("tool admission overlaps its original invocation".into());
                    }
                    pending = Some((*step, call_id.clone(), invocation.arguments));
                    // A valid observation cannot disclose an unrelated payload field.
                    refuse(
                        endpoint,
                        agent,
                        input,
                        record.sequence,
                        ExecutionPayloadField::Result,
                    )
                    .await?;
                }
                ExecutionEvent::ToolCompleted { step, call_id, .. } => {
                    let (started, identity, arguments) =
                        pending.take().ok_or("tool has no admission")?;
                    let result: ToolResult = serde_json::from_slice(
                        &read(
                            endpoint,
                            agent,
                            input,
                            record.sequence,
                            ExecutionPayloadField::Result,
                        )
                        .await?,
                    )?;
                    if started != *step || identity != *call_id || result.value != arguments {
                        return Err("tool completion differs from its original admission".into());
                    }
                    completed += 1;
                }
                ExecutionEvent::ToolFailed { .. } => {
                    return Err("the shared echo tool failed".into());
                }
                _ => {}
            }
        }
        if page.end != after || page.next.is_some_and(|next| next != after) {
            return Err("execution continuation differs from its original page".into());
        }
        if page.next.is_none() && query.settled {
            // Refine the shared cursor against the real decoded source page.
            // Neither the same short page without its actor fence nor a full
            // page with a continuation may advance into the next input.
            if query.continuation()?
                != (ExecutionPosition {
                    input: input.checked_add(1).ok_or("execution input overflow")?,
                    after: 0,
                })
            {
                return Err("settled execution did not retire its original input".into());
            }
            let expected = ExecutionPosition { input, after };
            query.settled = false;
            if query.continuation()? != expected {
                return Err("execution cursor retired an input without its actor fence".into());
            }
            query.settled = true;
            query
                .page
                .as_mut()
                .ok_or("execution page disappeared")?
                .next = Some(after);
            if query.continuation()? != expected {
                return Err("execution cursor retired a page with an original continuation".into());
            }
            break;
        }
        // This input was already settled before the vector began. A short,
        // unfinished page is a missing producer fence, never a reason to poll.
        if page.next.is_none() || page.items.is_empty() {
            return Err("settled execution omitted its final input fence".into());
        }
    }
    if pending.is_some() || completed != endpoint.effects().await? {
        return Err("complete execution selection lost an original tool result".into());
    }
    let absent = after.checked_add(1).ok_or("execution cursor overflow")?;
    refuse(
        endpoint,
        agent,
        input,
        absent,
        ExecutionPayloadField::Result,
    )
    .await
}

fn admission(
    invocation: &ToolInvocation,
    parent: acyclic_harness_core::OperationId,
    step: u32,
    call_id: &str,
) -> Result {
    if invocation.operation_id != ToolInvocation::model_call_operation(parent, step, call_id, None)?
        || invocation.call_id != call_id
        || invocation.name != TOOL
    {
        return Err("tool admission differs from its original invocation".into());
    }
    Ok(())
}

async fn read(
    endpoint: &dyn Endpoint,
    agent: &AgentPath,
    input: u64,
    sequence: u64,
    field: ExecutionPayloadField,
) -> Result<Vec<u8>> {
    let reply = call(endpoint, &command(agent, input, sequence, field)).await??;
    let (bytes, length) = reply.content()?;
    // The shared echo fixture's payloads fit one ordinary range. A truncated
    // result fails instead of being certified from a matching prefix.
    if bytes.len() as u64 != length {
        return Err("the shared tool payload was truncated".into());
    }
    Ok(bytes)
}

async fn refuse(
    endpoint: &dyn Endpoint,
    agent: &AgentPath,
    input: u64,
    sequence: u64,
    field: ExecutionPayloadField,
) -> Result {
    if !matches!(call(endpoint, &command(agent, input, sequence, field)).await?,
        Err(acyclic_harness_client::AgentControlFailure::Refused { code, .. })
            if code == acyclic_harness_core::agent::Error::Record("absent payload").code())
    {
        return Err("absent original execution payload was not a typed refusal".into());
    }
    Ok(())
}

fn command(
    agent: &AgentPath,
    input: u64,
    sequence: u64,
    field: ExecutionPayloadField,
) -> AgentCommand {
    AgentCommand::Read {
        agent: agent.clone(),
        target: ReadTarget::Execution {
            input,
            sequence,
            field,
        },
        offset: 0,
    }
}
