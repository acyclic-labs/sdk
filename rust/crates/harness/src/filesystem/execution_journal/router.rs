use super::*;
use crate::runtime::InteractionRouter;

impl<P, A, O> InteractionRouter for FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn route<'a>(
        &'a self,
        operation_id: OperationId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<InteractionOutcome>> {
        Box::pin(async move {
            let task = self
                .owner
                .as_ref()
                .ok_or_else(|| {
                    canonical_boundary(
                        None,
                        crate::wire::ErrorCode::Unsupported,
                        "interaction journal has no task owner",
                    )
                })?
                .0
                .task_binding()
                .0;
            let id = crate::durable_host::task_interaction_id(task, operation_id);
            self.open_interaction(id, &interaction, None)?.await?;
            Ok(self
                .interaction_outcome(id, None)?
                .await?
                .unwrap_or(InteractionOutcome::Indeterminate { operation_id }))
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_interactions(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        if self.owner.is_some() && self.original_native_execution_work_matches(work) {
            Ok(())
        } else {
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "interaction journal has no original native owner",
            })
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn route_with_native_work<'a>(
        &'a self,
        operation_id: OperationId,
        interaction: Interaction,
        request_custody: acyclic_fs::NativeAllocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (InteractionOutcome, acyclic_fs::NativeAllocation)>>
    {
        let paid = (interaction, request_custody);
        self.supports_native_interactions(work)?;
        if !paid.1.work().shares_allocation_context(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "interaction request differs from original work",
            });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let paid = paid;
            let task = self
                .owner
                .as_ref()
                .expect("native interaction owner was checked")
                .0
                .task_binding()
                .0;
            let id = crate::durable_host::task_interaction_id_with_native_work(
                task,
                operation_id,
                work,
            )?;
            let io = PreparationIo::from_native_work(work)?;
            self.open_interaction(id, &paid.0, Some(&io))?.await?;
            let outcome = self.interaction_outcome(id, Some(&io))?.await?;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((
                outcome.unwrap_or(InteractionOutcome::Indeterminate { operation_id }),
                custody,
            ))
        })
    }
}
