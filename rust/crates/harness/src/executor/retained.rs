use super::*;

pub(crate) async fn completed_tool_projection(
    journal: &dyn ExecutionJournal,
    records: &[ExecutionRecord],
    step: u32,
    expected: &ToolInvocation,
    tools: &[crate::tool::ToolDefinition],
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<crate::model::ToolResultContent> {
    let invocation = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ToolStarted {
                step: recorded,
                call_id,
                invocation,
            } if *recorded == step && call_id == &expected.call_id => Some(invocation),
            _ => None,
        })
        .ok_or(Error::Indeterminate(expected.operation_id))?;
    if retain_preparation(load_json::<ToolInvocation>(journal, invocation, io).await?) != *expected
    {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Conflict,
            "completed tool prefix invocation differs",
        ));
    }
    let (result, projection) = records
        .iter()
        .find_map(|record| match &record.event {
            ExecutionEvent::ToolCompleted {
                step: recorded,
                call_id,
                result,
                projection,
            } if *recorded == step && call_id == &expected.call_id => Some((result, projection)),
            _ => None,
        })
        .ok_or(Error::Indeterminate(expected.operation_id))?;
    let definition = tools
        .iter()
        .find(|tool| tool.name == expected.name)
        .ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "completed tool prefix has no pinned schema",
            )
        })?;
    let result = retain_preparation(load_json::<ToolResult>(journal, result, io).await?);
    let projection = retain_preparation(load_json::<Value>(journal, projection, io).await?);
    crate::executor::with_history_io(io, |work| {
        crate::executor::validate_json_schema_value(
            &definition.output_schema,
            &result.value,
            "tool output",
            work,
        )
    })?;
    definition.validate_projection(&projection, limits, io)
}

/// Reads only durable artifacts; never evaluates context, policy or providers.
pub(crate) async fn retained_model_step(
    journal: &dyn ExecutionJournal,
    records: &[ExecutionRecord],
    step: u32,
    purpose: ModelPurpose,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<RetainedModelStep> {
    let mut events = Vec::new();
    let mut admission = ModelEventAdmission::default();
    let mut started = None;
    for record in records {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
        }
        if started.is_none()
            && let ExecutionEvent::ModelStarted {
                step: event_step,
                purpose: event_purpose,
                request_digest,
                request,
            } = &record.event
            && *event_step == step
            && *event_purpose == purpose
        {
            started = Some((*request_digest, request));
        }
        if let ExecutionEvent::Model {
            step: event_step,
            purpose: event_purpose,
            event,
        } = &record.event
            && *event_step == step
            && *event_purpose == purpose
        {
            let event = retain_preparation(load_json::<ModelEvent>(journal, event, io).await?);
            let index = events.len();
            stock_vec_push(&mut events, event, io)?;
            admission.observe(&events[index], limits, io)?;
        }
    }
    if started.is_none() && !events.is_empty() {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "model observations exist without an admitted attempt",
        ));
    }
    let request = if let Some((digest, reference)) = started {
        let recorded = load_model_request(journal, reference, limits, io).await?;
        if recorded.request_digest() != digest {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "recorded model request digest differs from admission",
            ));
        }
        Some(recorded)
    } else {
        None
    };
    Ok(RetainedModelStep {
        request,
        admission,
        events,
    })
}
