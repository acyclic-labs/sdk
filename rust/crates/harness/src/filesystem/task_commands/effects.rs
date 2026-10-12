use super::*;

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn effect_command(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        command: OperationId,
        payload: Value,
        reconciliation_only: bool,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<TaskCommandProgress> {
        let plan: crate::effect_host::TaskEffectPlan = mail::decode_command(payload, io)?;
        let effects = self.effects.as_ref().ok_or_else(|| {
            crate::filesystem::task_runtime::observed::boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "effect command provider registry is not selected",
            )
        })?;
        let status = 'status: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                let work = io.original_native_work().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "effect command requires the original native work",
                })?;
                let owner = self
                    .runtime
                    .task_host()
                    .journal_owner_with_native_work(task, fence, work)
                    .await?;
                let (status, custody) = if reconciliation_only {
                    effects
                        .reconcile_task_effect(&owner, command, &plan, Some(work))
                        .await?
                } else {
                    effects
                        .run_task_effect(&owner, command, &plan, Some(work))
                        .await?
                };
                io.absorb_native_custody(custody.ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native effect result has no original custody",
                })?)?;
                break 'status status;
            }
            let owner = self
                .runtime
                .task_host()
                .journal_owner(task, fence.clone())
                .await?;
            let status = if reconciliation_only {
                effects
                    .reconcile_task_effect(&owner, command, &plan, None)
                    .await.map(|result| result.0)?
            } else {
                effects.run_task_effect(&owner, command, &plan, None).await.map(|result| result.0)?
            };
            break 'status status;
        };
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
