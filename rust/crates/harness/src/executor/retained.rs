use super::*;

pub(crate) async fn completed_tool_projection(
    journal: &dyn ExecutionJournal,
    records: &[ExecutionRecord],
    step: u32,
    expected: crate::tool::RecordedInvocation<&ToolInvocation, &Value>,
    tools: &[crate::tool::ToolDefinition],
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<crate::model::ToolResultContent> {
    validate_journal(journal, io)?;
    let crate::tool::RecordedInvocation {
        invocation: expected,
        metadata,
    } = expected;
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
    let recorded = load_json::<crate::tool::RecordedInvocation>(journal, invocation, io).await?;
    if recorded.0.invocation != *expected || recorded.0.metadata != *metadata {
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
    let result = load_json::<ToolResult>(journal, result, io).await?;
    let projection = load_json::<Value>(journal, projection, io).await?;
    crate::executor::with_history_io(io, |work| {
        crate::executor::validate_json_schema_value(
            &definition.output_schema,
            &result.0.value,
            "tool output",
            work,
        )
    })?;
    definition.validate_projection(&projection.0, limits, io)
}

/// Reads only durable artifacts; never evaluates context, policy or providers.
pub(crate) async fn retained_model_step<'a>(
    journal: &dyn ExecutionJournal,
    records: &[ExecutionRecord],
    step: u32,
    purpose: ModelPurpose,
    limits: Limits,
    io: Option<&'a PreparationIo>,
) -> Result<(
    RetainedModelStep,
    Option<crate::conversation::ContentHold<'a>>,
)> {
    validate_journal(journal, io)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
        io.retain_owned(std::mem::size_of::<RetainedModelStep>() as u64)?;
    }
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
            let event = retain_preparation(load_json::<ModelEvent>(journal, event, io).await?)?;
            stock_vec_push(&mut events, event, io)?;
            let event = events.last().ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "model observation was not retained",
                )
            })?;
            admission.observe(event, limits, io)?;
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
    let retained = RetainedModelStep {
        request,
        admission,
        events,
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let hold = if stock_is_native(io) {
        super::payload::preparation_output(io, before, retained.logical_owned_bytes()?)?
    } else {
        None
    };
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let hold = None;
    Ok((retained, hold))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl RetainedModelStep {
    fn logical_owned_bytes(&self) -> Result<u64> {
        use std::mem::size_of;
        let mut bytes = native_size_add(
            size_of::<Self>() as u64,
            native_size_slots(self.admission.calls.capacity(), size_of::<String>())?,
        )?;
        if let Some(request) = &self.request {
            bytes = native_size_add(
                bytes,
                request.logical_owned_bytes()?
                    - size_of::<crate::model::PreparedModelRequest>() as u64,
            )?;
        }
        bytes = native_size_add(
            bytes,
            native_events_owned_bytes(&self.events)? - size_of::<Vec<ModelEvent>>() as u64,
        )?;
        for call in &self.admission.calls {
            bytes = native_size_add(bytes, call.capacity() as u64)?;
        }
        Ok(bytes)
    }
}
