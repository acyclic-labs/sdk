use super::*;

impl<P: StreamProvider> TaskJournalOwner<P> {
    pub(crate) async fn for_tool_workflow(
        mut self,
        context: &crate::runtime::ToolContext,
        tool: &crate::durable_tool::ResumableTool,
        invocation: &crate::tool::ToolInvocation,
        cancellation: &acyclic_fs::CancellationToken,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        self.invocation_stop = Some(cancellation.clone());
        self.require_task_context_binding(context.task(), io)?;
        if invocation.operation_id == self.operation_id() {
            return Err(crate::conversation::content_error(io, crate::wire::ErrorCode::Unauthorized,
                "tool workflow differs from original task owner"));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            context.task().require_original_native_dispatch(work)?;
        }
        let mut history = io.map(crate::executor::PreparationIo::history_io);
        let mut work = history.as_mut().map(|work| work as &mut dyn crate::store::HistoryIo);
        self.require_mail_scope(context.task().scope(), &mut work)?;
        self.verify(false, &mut work).await?;
        let before = io.map(crate::executor::PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        self.workflow = tool.workflow_admission(context, invocation, io)?;
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        self.workflow_output = Some(crate::core::copy_value(&tool.definition.output_schema, &mut quota)?);
        quota.transfer();
        drop(quota);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            let mut custody = io.take_native_custody(io.owned_bytes()?.checked_sub(before)
                .ok_or_else(|| io.invalid("tool workflow custody underflow"))?)?;
            self.native_custody.as_mut().ok_or_else(|| io.invalid("tool workflow has no original owner custody"))?
                .absorb(&mut custody)?;
        }
        Ok(self)
    }
}
