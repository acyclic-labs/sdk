use super::*;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn effect_command(
        &self,
        CommandInvocation {
            context,
            fence,
            command,
            reconciliation_only,
            io,
            ..
        }: CommandInvocation<'_>,
        payload: Value,
    ) -> Result<TaskCommandProgress> {
        let command = command.operation_id;
        let plan: crate::effect_host::TaskEffectPlan = mail::decode_command(payload, io)?;
        let effects = self.effects.as_ref().ok_or_else(|| {
            crate::filesystem::task_runtime::observed::boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "effect command provider registry is not selected",
            )
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        let work = io
            .map(|io| {
                io.original_native_work()
                    .ok_or_else(|| io.invalid("dispatch requires original native work"))
            })
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let work = None;
        let observed;
        #[cfg(not(target_arch = "wasm32"))]
        let original = work
            .map(|work| context.journal_owner::<P>(fence, work))
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let original: Option<Arc<crate::durable_host::TaskJournalOwner<P>>> = None;
        let owner = if let Some(owner) = &original {
            owner.as_ref()
        } else {
            let task = context.durable_task_id().ok_or(Error::Unauthorized(
                "effect context has no original task".into(),
            ))?;
            observed = self
                .runtime
                .task_host()
                .journal_owner(task, fence, io)
                .await?;
            &observed
        };
        let (status, custody) = effects
            .run_task_effect(owner, command, &plan, work, reconciliation_only)
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        match io {
            Some(io) => io.absorb_native_custody(custody.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native effect result has no original custody",
            })?)?,
            None => drop(custody),
        }
        #[cfg(target_arch = "wasm32")]
        let _ = custody;
        match status {
            crate::core::EffectStatus::Succeeded { result } => {
                ready(Outcome::Succeeded(result), io)
            }
            crate::core::EffectStatus::Failed { message } => {
                ready(Outcome::<Value>::Failed { message }, io)
            }
            crate::core::EffectStatus::Planned if reconciliation_only => mail::reconciled(io),
            crate::core::EffectStatus::Planned
            | crate::core::EffectStatus::Dispatched
            | crate::core::EffectStatus::Indeterminate => Ok(TaskCommandProgress::Indeterminate),
        }
    }
}
