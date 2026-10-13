use super::*;
use acyclic_harness::filesystem::PreparationIo;

impl crate::pi_host::PiControlHost<'_> {
    pub(crate) async fn observe_source(
        &self,
        shared: Arc<TaskContext>,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        work: &NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        let supplied: (PiObserveSourceCommand, acyclic_fs::NativeAllocation) =
            self.decode(&payload, OBSERVE_COMMAND_DIAGNOSTIC_LABEL_BYTES, work)?;
        let input = supplied.0;
        let actual_context = self.context(shared, &fence, work).await?;
        let (shared, owner, caller, _) = &actual_context;
        let context = shared.as_ref();
        let original = original_operation(
            self.protected.policy().root_task,
            input.caller,
            &input.sdk_operation,
            work,
        )?;
        if owner.task_id() != self.protected.policy().root_task
            || input.sdk_operation.is_empty()
            || command.operation_id != derived(original, "observe-root-source", work)?
        {
            return Err(denied(
                "Pi Root observer changed its actual owner or original SDK operation",
            ));
        }
        crate::pi_inspection::authorize_selected_source(
            &self.runtime.runtime,
            self.protected.policy().root_task,
            context,
            owner,
            input.caller,
            self.protected.policy().session_limits.depth,
            work,
        )
        .await?;
        let actual = self
            .roots
            .get(&input.root_key)
            .ok_or_else(|| invalid("Pi observed root has no actual lazy producer"))?;
        let observed = if let Some(decoded) =
            crate::pi_root_provider::read_pi_input::<ObservedSource>(
                &self.protected,
                &self.filesystem,
                command.operation_id,
                "root-source-observation",
                self.protected.policy().maximum_payload_bytes,
                OBSERVED_SOURCE_DIAGNOSTIC_LABEL_BYTES,
                context,
                work,
            )
            .await?
        {
            let observed = &decoded.0;
            if observed.caller != input.caller
                || observed.sdk_operation != input.sdk_operation
                || observed.root_key != input.root_key
            {
                return Err(denied(
                    "Pi original Root observation retry changed its source request",
                ));
            }
            actual
                .workspace
                .validate_snapshot(
                    observed.snapshot,
                    &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                        work,
                        cancellation: work.cancellation(),
                    },
                )
                .await
                .map_err(crate::pi_fork_host::native_lazy_error)?;
            decoded
        } else {
            let frozen = self
                .freeze_root_source(shared, owner, caller, &input.root_key, actual, work)
                .await?;
            let snapshot = frozen.0;
            owner
                .verify(
                    false,
                    &mut Some(&mut &PreparationIo::from_native_work(work)?),
                )
                .await?;
            let observed = ObservedSource {
                caller: input.caller,
                sdk_operation: input.sdk_operation,
                root_key: input.root_key,
                snapshot,
            };
            let _published = crate::pi_root_provider::stage_pi_input(
                &self.protected,
                &self.filesystem,
                command.operation_id,
                "root-source-observation",
                &observed,
                work,
            )
            .await?;
            owner
                .verify(
                    false,
                    &mut Some(&mut &PreparationIo::from_native_work(work)?),
                )
                .await?;
            (observed, frozen.1)
        };
        ready(
            &PiRootSource {
                snapshot: observed.0.snapshot,
                root_key: &observed.0.root_key,
            },
            work,
        )
    }
}
