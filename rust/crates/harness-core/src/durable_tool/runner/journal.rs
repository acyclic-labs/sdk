use super::*;
pub(super) async fn authorize(
    tool: &Tool,
    invocation: &ToolInvocation,
    context: &ToolContext,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let grant = tool_text(format_args!("tool:call:{}", tool.definition.name), io)?;
    if !context.effective_scope().grants().contains(&grant.0) {
        return Err(boundary(io, Unauthorized, "scope lacks tool call grant"));
    }
    with_history_io(io, |work| {
        validate_json_schema_value(
            &tool.definition.input_schema,
            &invocation.arguments,
            "tool input",
            work,
        )
    })?;
    tool.executor
        .authorize(None, Some(context), invocation, native_work(io))?;
    drop(grant);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = native_work(io) {
        return match context
            .authorize_tool(&tool.definition, invocation, work)
            .await?
        {
            Some(operation) => Err(Error::Indeterminate(operation)),
            None => Ok(()),
        };
    }
    match context
        .task()
        .authorize_tool(&tool.definition, invocation)
        .await?
    {
        Some(operation) => Err(Error::Indeterminate(operation)),
        None => Ok(()),
    }
}
impl DurableToolRunner<'_> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) async fn load<
        T: crate::executor::PreparationRecord + crate::filesystem::UploadRecord + Send,
    >(
        &self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<(T, NativeCustody)> {
        let mut decoded = load_json::<T>(self.journal.as_ref(), reference, io).await?;
        if native_work(io).is_some() {
            let retained = (std::mem::size_of::<T>() as u64)
                .checked_add(decoded.0.logical_owned_bytes()?)
                .ok_or_else(|| boundary(io, Storage, "payload size overflow"))?;
            decoded
                .1
                .as_mut()
                .ok_or_else(|| boundary(io, Storage, "funded decode lacks output custody"))?
                .substitute(retained)?;
            let custody = decoded
                .1
                .take()
                .ok_or_else(|| boundary(io, Storage, "funded decode lacks output custody"))?;
            let (value, custody) = crate::filesystem::native_decoded((decoded.0, custody))?;
            return Ok((value, Some(custody)));
        }
        Ok((decoded.0, None))
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    pub(super) async fn load<T: crate::executor::PreparationRecord + Send>(
        &self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<(T, NativeCustody)> {
        Ok((
            load_json::<T>(self.journal.as_ref(), reference, io)
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
        io: Option<&PreparationIo>,
    ) -> Result<(FileRef, NativeCustody)> {
        let reference = stage_json(self.journal.as_ref(), operation_id, key, value, io).await?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io.filter(|io| native_work(Some(io)).is_some()) {
            let custody = io.take_native_custody(reference.logical_owned_bytes()?)?;
            return Ok((reference, Some(custody)));
        }
        Ok((reference, None))
    }
    pub(super) async fn invoke(
        &self,
        tool: &Tool,
        invocation: &ToolInvocation,
        context: &ToolContext,
        io: Option<&PreparationIo>,
    ) -> Result<(ToolResult, NativeCustody)> {
        let _ = io;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = native_work(io) {
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
        tool: &Tool,
        invocation: &ToolInvocation,
        context: &ToolContext,
        io: Option<&PreparationIo>,
    ) -> Result<Option<(ToolResult, NativeCustody)>> {
        let _ = io;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = native_work(io) {
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
        error: &Error,
        io: Option<&PreparationIo>,
    ) -> Result<Option<(Outcome<Value>, NativeCustody)>> {
        tracing::warn!(
            operation_id = %operation_id,
            call_id = %invocation.call_id,
            ?reason,
            cause = ?error,
            "durable tool call failed"
        );
        let key = tool_text(format_args!("tool:failed:{}", OperationId::new()), io)?;
        let call = tool_text(format_args!("{}", invocation.call_id), io)?;
        let message = tool_text(format_args!("{}", crate::executor::CauseText(error)), io)?;
        let cause = crate::executor::ToolFailureCause {
            class: crate::executor::ToolErrorClass::of(error),
            message: message.0,
        };
        let failed = tool_outcome(operation_id, Some((reason, &cause)), io)?;
        let event = ExecutionEvent::ToolFailed {
            step: 0,
            call_id: call.0,
            reason,
            cause,
        };
        match match self
            .journal
            .append_if_tail(operation_id, 2, &key.0, &event, io)
        {
            Ok(future) => future.await,
            Err(error) => Err(error),
        } {
            Ok(true) => Ok(Some(failed)),
            Ok(false) => Ok(None),
            Err(error) if tool_propagate(&error, io) => Err(error),
            Err(error) if error.leaves_effect_uncertain() => {
                tool_outcome(operation_id, None, io).map(Some)
            }
            Err(error) => Err(error),
        }
    }
}
