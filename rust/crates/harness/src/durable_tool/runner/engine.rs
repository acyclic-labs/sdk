use super::*;

impl DurableToolRunner {
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "one canonical ordered durable tool state machine"
    )]
    pub(super) async fn run_engine(
        &self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: &ToolDefinition,
        invocation: (ToolInvocation, crate::NativeCustody),
        context: &ToolContext,
        tool: &crate::tool::Tool,
        io: Option<&crate::executor::PreparationIo>,
        reconcile_only: bool,
    ) -> Result<(Outcome<Value>, crate::NativeCustody)> {
        let invocation = &invocation.0;
        if tool.definition != *definition || invocation.name != definition.name {
            return Err(tool_error(
                io,
                crate::wire::ErrorCode::Conflict,
                "durable tool definition changed after admission",
            ));
        }
        crate::executor::with_history_io(io, |work| {
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work)
        })?;
        if context.operation_id() != operation_id
            || context.call_id() != invocation.call_id
            || context.task().durable_task_id() != Some(task_id)
        {
            return Err(tool_error(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "tool context is not bound to this admitted effect",
            ));
        }
        let scope = context.effective_scope();
        let content_limit = self.limits.file_bytes.min(scope.limits().file_bytes);
        let projection_limit = self.limits.render_bytes.min(scope.limits().render_bytes);
        let projection_limits = Limits {
            file_bytes: content_limit,
            render_bytes: projection_limit,
            path_bytes: self.limits.path_bytes.min(scope.limits().path_bytes),
            attachments: self.limits.attachments.min(scope.limits().attachments),
            ..self.limits
        };
        tool_json_bound(invocation, content_limit, io)?;
        let digest = crate::executor::preparation_digest(
            &(&task_id, definition, invocation, projection_limits),
            io,
        )?;
        loop {
            // Authentication is repeated BEFORE reading or publishing replay.
            self.authorize(tool, definition, invocation, context, io)
                .await?;
            let records = self.replay(operation_id, io).await?;
            for (index, record) in records.0.iter().enumerate() {
                match (index, &record.event) {
                    (0, ExecutionEvent::Started { request_digest })
                        if *request_digest == digest => {}
                    (
                        1,
                        ExecutionEvent::ToolStarted {
                            step: 0,
                            call_id,
                            invocation: reference,
                        },
                    ) if *call_id == invocation.call_id => {
                        if reference.descriptor().byte_length() > content_limit {
                            return Err(tool_error(
                                io,
                                crate::wire::ErrorCode::Conflict,
                                "durable tool invocation exceeds admitted descriptor limit",
                            ));
                        }
                        let pinned = self.load::<ToolInvocation>(reference, io).await?;
                        if pinned.0 != *invocation {
                            return Err(tool_error(
                                io,
                                crate::wire::ErrorCode::Conflict,
                                "durable tool call identity changed",
                            ));
                        }
                    }
                    (
                        2,
                        ExecutionEvent::ToolCompleted {
                            step: 0, call_id, ..
                        }
                        | ExecutionEvent::ToolFailed {
                            step: 0, call_id, ..
                        },
                    ) if *call_id == invocation.call_id => {}
                    _ => {
                        return Err(tool_error(
                            io,
                            crate::wire::ErrorCode::Conflict,
                            "durable tool journal has an incompatible history",
                        ));
                    }
                }
            }
            if let Some(ExecutionEvent::ToolCompleted {
                result, projection, ..
            }) = records.0.get(2).map(|record| &record.event)
            {
                if result.descriptor().byte_length() > content_limit
                    || projection.descriptor().byte_length() > projection_limit
                {
                    return Err(tool_error(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "durable tool history exceeds admitted descriptor limits",
                    ));
                }
                let result = self.load::<ToolResult>(result, io).await?;
                let projection = self.load::<Value>(projection, io).await?;
                if let Err(error) = tool_json_bound(&result.0, content_limit, io)
                    .and_then(|()| tool_json_bound(&projection.0, projection_limit, io))
                {
                    if tool_propagate(&error, io) {
                        return Err(error);
                    }
                    return Err(Error::Conflict(
                        "durable tool history exceeds admitted limits".into(),
                    ));
                }
                crate::executor::with_history_io(io, |work| {
                    crate::executor::validate_json_schema_value(
                        &definition.output_schema,
                        &result.0.value,
                        "tool output",
                        work,
                    )
                })?;
                tool_validate_projection(definition, &projection.0, projection_limits, io)?;
                return tool_success(result, io);
            }
            if let Some(ExecutionEvent::ToolFailed { reason, .. }) =
                records.0.get(2).map(|record| &record.event)
            {
                return tool_outcome(
                    Outcome::Failed {
                        message: String::new(),
                    },
                    Some(*reason),
                    io,
                );
            }
            let started = !records.0.is_empty();
            let dispatched = records.0.len() >= 2;
            if reconcile_only && !dispatched {
                return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
            }
            drop(records);
            if !started {
                let _event_metadata = tool_metadata::<ExecutionEvent>(io)?;
                let event = ExecutionEvent::Started {
                    request_digest: digest,
                };
                self.append(operation_id, "tool:started", &event, io)
                    .await?;
            }
            let mut claimed = false;
            if !dispatched {
                let reference = self
                    .stage(operation_id, "tool:invocation", invocation, io)
                    .await?;
                let current = self.replay(operation_id, io).await?;
                if matches!(current.0.as_slice(), [record]
                    if matches!(&record.event, ExecutionEvent::Started { request_digest }
                        if *request_digest == digest))
                {
                    let key = tool_label("tool:claim", io)?;
                    let call = tool_string(&invocation.call_id, io)?;
                    let _event_metadata = tool_metadata::<ExecutionEvent>(io)?;
                    let event = ExecutionEvent::ToolStarted {
                        step: 0,
                        call_id: call.0,
                        invocation: reference.0,
                    };
                    match self.append_tail(operation_id, 1, &key.0, &event, io).await {
                        Ok(true) => claimed = true,
                        Ok(false) => continue,
                        Err(error) if tool_propagate(&error, io) => return Err(error),
                        Err(Error::Indeterminate(_)) => {
                            return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                        }
                        Err(error) => return Err(error),
                    }
                } else if !matches!(current.0.get(1).map(|record| &record.event),
                    Some(ExecutionEvent::ToolStarted { step: 0, call_id, .. })
                        if *call_id == invocation.call_id)
                {
                    return Err(tool_error(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "durable tool journal changed before dispatch",
                    ));
                } else {
                    continue;
                }
            }
            macro_rules! fail {
                ($reason:expr) => {
                    if let Some(outcome) = self.fail(operation_id, invocation, $reason, io).await? {
                        return Ok(outcome);
                    }
                    continue;
                };
            }
            let result = if claimed {
                match self.invoke(tool, invocation, context, io).await {
                    Ok(result) => result,
                    Err(error) if tool_propagate(&error, io) => return Err(error),
                    Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                        return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                    }
                    Err(_) => {
                        fail!(ToolFailureKind::ExecutorRejected);
                    }
                }
            } else {
                match self.reconcile(tool, invocation, context, io).await {
                    Ok(Some(result)) => result,
                    Ok(None) => {
                        return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                    }
                    Err(error) if tool_propagate(&error, io) => return Err(error),
                    Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                        return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                    }
                    Err(error) => return Err(error),
                }
            };
            if let Err(error) = crate::executor::with_history_io(io, |work| {
                crate::executor::validate_json_schema_value(
                    &definition.output_schema,
                    &result.0.value,
                    "tool output",
                    work,
                )
            }) {
                if tool_propagate(&error, io) {
                    return Err(error);
                }
                fail!(ToolFailureKind::InvalidOutput);
            }
            if let Err(error) = tool_json_bound(&result.0, content_limit, io) {
                if tool_propagate(&error, io) {
                    return Err(error);
                }
                fail!(ToolFailureKind::PublicationRejected);
            }
            let projection = match tool_project(tool, invocation, &result.0, io) {
                Ok(projection) => projection,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(_) => {
                    fail!(ToolFailureKind::ProjectionRejected);
                }
            };
            if let Err(error) =
                tool_validate_projection(definition, &projection.0, projection_limits, io)
                    .and_then(|()| tool_json_bound(&projection.0, projection_limit, io))
            {
                if tool_propagate(&error, io) {
                    return Err(error);
                }
                fail!(ToolFailureKind::ProjectionRejected);
            }
            let result_ref = match self.stage(operation_id, "tool:result", &result.0, io).await {
                Ok(reference) => reference,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                }
                Err(_) => {
                    fail!(ToolFailureKind::PublicationRejected);
                }
            };
            let projection_ref = match self
                .stage(operation_id, "tool:projection", &projection.0, io)
                .await
            {
                Ok(reference) => reference,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                }
                Err(_) => {
                    fail!(ToolFailureKind::PublicationRejected);
                }
            };
            let key = tool_label("tool:completed", io)?;
            let call = tool_string(&invocation.call_id, io)?;
            let _event_metadata = tool_metadata::<ExecutionEvent>(io)?;
            let event = ExecutionEvent::ToolCompleted {
                step: 0,
                call_id: call.0,
                result: result_ref.0,
                projection: projection_ref.0,
            };
            match self.append_tail(operation_id, 2, &key.0, &event, io).await {
                Ok(true) => return tool_success(result, io),
                Ok(false) => continue,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                    return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
                }
                Err(error) => return Err(error),
            }
        }
    }
}
