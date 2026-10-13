use super::*;

impl DurableToolRunner {
    pub(super) async fn authorize(
        &self,
        tool: &crate::tool::Tool,
        definition: &ToolDefinition,
        invocation: &ToolInvocation,
        context: &ToolContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let scope = context.effective_scope();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let grant_hold;
            let (grant, hold) = work.display(&format_args!("tool:call:{}", definition.name))?;
            grant_hold = hold;
            if !scope.grants().contains(&grant) {
                return Err(tool_error(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "scope lacks tool call grant",
                ));
            }
            crate::executor::with_history_io(io, |work| {
                crate::executor::validate_json_schema_value(
                    &definition.input_schema,
                    &invocation.arguments,
                    "tool input",
                    work,
                )
            })?;
            tool.executor
                .authorize_with_native_context(context, invocation, work)?;
            drop(grant);
            drop(grant_hold);
            return match context.authorize_tool(definition, invocation, work).await? {
                Some(operation) => Err(Error::Indeterminate(operation)),
                None => Ok(()),
            };
        }
        let required = capability::tool_call(&definition.name);
        if !scope.grants().contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        crate::executor::validate_json_schema_value(
            &definition.input_schema,
            &invocation.arguments,
            "tool input",
            &mut None,
        )?;
        tool.executor.authorize_with_context(context, invocation)?;
        match context
            .task()
            .authorize_tool(definition, invocation)
            .await?
        {
            Some(operation) => Err(Error::Indeterminate(operation)),
            None => Ok(()),
        }
    }

    pub(super) async fn replay(
        &self,
        operation_id: OperationId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Vec<crate::executor::ExecutionRecord>, crate::NativeCustody)> {
        let (_, records, hold) = replay_execution(self.journal.as_ref(), operation_id, 4, |_| true, io).await?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(hold) = hold {
            let (records, custody) = crate::filesystem::native_decoded((records, hold))?;
            return Ok((records, Some(custody)));
        }
        Ok((records, None))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) async fn load<
        T: crate::executor::PreparationRecord + crate::filesystem::UploadRecord + Send,
    >(
        &self,
        reference: &crate::conversation::FileRef,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(T, crate::NativeCustody)> {
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            let (value, hold) = load_json::<T>(self.journal.as_ref(), reference, Some(io)).await?;
            let mut hold = hold.expect("funded decoding retains its output");
            let retained = (std::mem::size_of::<T>() as u64)
                .checked_add(crate::filesystem::UploadRecord::logical_owned_bytes(&value)?)
                .ok_or_else(|| tool_error(Some(io), crate::wire::ErrorCode::Storage,
                    "native stock logical payload size overflow"))?;
            hold.substitute(retained)?;
            let (value, custody) = crate::filesystem::native_decoded((value, hold))?;
            return Ok((value, Some(custody)));
        }
        Ok((
            load_json::<T>(self.journal.as_ref(), reference, None)
                .await?
                .0,
            None,
        ))
    }

    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    pub(super) async fn load<T: crate::executor::PreparationRecord + Send>(
        &self,
        reference: &crate::conversation::FileRef,
        _io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(T, crate::NativeCustody)> {
        Ok((
            load_json::<T>(self.journal.as_ref(), reference, None)
                .await?
                .0,
            None,
        ))
    }

    pub(super) async fn stage<T: serde::Serialize + Sync>(
        &self,
        operation_id: OperationId,
        key: &str,
        value: &T,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(crate::conversation::FileRef, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            let reference = stage_json(self.journal.as_ref(), operation_id, key, value, Some(io)).await?;
            let custody = io.take_native_custody(reference.logical_owned_bytes()?)?;
            return Ok((reference, Some(custody)));
        }
        Ok((
            stage_json(self.journal.as_ref(), operation_id, key, value, None).await?,
            None,
        ))
    }

    pub(super) async fn append(
        &self,
        operation_id: OperationId,
        key: &str,
        event: &ExecutionEvent,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        self.journal.append(operation_id, key, event, io)?.await
    }

    pub(super) async fn append_tail(
        &self,
        operation_id: OperationId,
        tail: u64,
        key: &str,
        event: &ExecutionEvent,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<bool> {
        self.journal
            .append_if_tail(operation_id, tail, key, event, io)?
            .await
    }

    pub(super) async fn invoke(
        &self,
        tool: &crate::tool::Tool,
        invocation: &ToolInvocation,
        context: &ToolContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (result, custody) = tool
                .executor
                .execute_with_native_context(context, invocation, work)?
                .await?;
            return Ok((result, Some(custody)));
        }
        Ok((
            tool.executor
                .execute(invocation.clone(), Some(context.clone()))
                .await?,
            None,
        ))
    }

    pub(super) async fn reconcile(
        &self,
        tool: &crate::tool::Tool,
        invocation: &ToolInvocation,
        context: &ToolContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<(ToolResult, crate::NativeCustody)>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (result, custody) = tool
                .executor
                .reconcile_with_native_context(context, invocation, work)?
                .await?;
            return Ok(result.map(|result| (result, Some(custody))));
        }
        Ok(tool
            .executor
            .reconcile(invocation.clone(), Some(context.clone()))
            .await?
            .map(|result| (result, None)))
    }

    pub(super) async fn fail(
        &self,
        operation_id: OperationId,
        invocation: &ToolInvocation,
        reason: ToolFailureKind,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<(Outcome<Value>, crate::NativeCustody)>> {
        let key = tool_label("tool:failed", io)?;
        let call = tool_string(&invocation.call_id, io)?;
        let event = ExecutionEvent::ToolFailed {
            step: 0,
            call_id: call.0,
            reason,
        };
        match self.append_tail(operation_id, 2, &key.0, &event, io).await {
            Ok(true) => tool_outcome(
                Outcome::Failed {
                    message: String::new(),
                },
                Some(reason),
                io,
            )
            .map(Some),
            Ok(false) => Ok(None),
            Err(error) if tool_propagate(&error, io) => Err(error),
            Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => {
                tool_outcome(Outcome::Indeterminate { operation_id }, None, io).map(Some)
            }
            Err(error) => Err(error),
        }
    }
}
