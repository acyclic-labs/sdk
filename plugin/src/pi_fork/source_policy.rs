//! Same-session project source authorization.
use super::*;

impl RootProjectSourceProvider for PiProjectSourcePolicy<'_> {
    fn verify<'a>(
        &'a self,
        _context: RootProjectSourceContext<'a>,
        _snapshot: &'a ProjectSourceSnapshot,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
        Box::pin(async {
            Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unsupported,
                reason: "Pi source authorization requires its original native work",
            })
        })
    }

    fn supports_native_source(&self) -> bool {
        true
    }

    fn verify_with_native_work<'a>(
        &'a self,
        context: RootProjectSourceContext<'a>,
        snapshot: &'a ProjectSourceSnapshot,
        work: &'a NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, ()>> {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            let coordinator = self.coordinator;
            coordinator
                .runtime
                .verify_owner_with_native_work(&coordinator.owner, work)?;
            self.caller_owner
                .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
                .await?;
            if self.caller_context.durable_task_id() != Some(self.caller_owner.task_id())
                || context.graph_root != coordinator.owner.policy().root_task
                || context.caller.task_id() != self.caller_owner.task_id()
                || context.caller.task.name != "acyclic.pi"
                || context.source.task.name != "acyclic.pi"
                || snapshot.native_snapshot.as_ref() != Some(self.native_snapshot)
                || snapshot.generation.as_resource().key()
                    != self.frozen.authored_generation.digest().as_bytes()
                || context.maximum_work == 0
                || context.maximum_bytes == 0
            {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi selected source changed its original operation, graph or native cut",
                });
            }
            if context.owner != coordinator.runtime.authority.verifier.audience() {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi selected source changed its original protected audience",
                });
            }
            let maximum = coordinator.owner.policy().maximum_payload_bytes;
            let bytes =
                crate::pi_root_provider::canonical_pi_value(&context.source.input, maximum, work)?;
            let original = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(
                &bytes.0,
                maximum,
                maximum,
                acyclic_harness::wire::ErrorCode::Invalid,
                acyclic_harness::JsonValueSchema::None,
                crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
                work,
            )?;
            if !original
                .0
                .binding
                .project_volumes
                .values()
                .any(|volume| volume == &snapshot.project)
                || snapshot
                    .project
                    .storage_name(&mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))?
                    != self.source.workspace().name().as_str()
            {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi selected source changed its originally admitted project",
                });
            }
            self.caller_owner
                .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
                .await?;
            self.source
                .validate_snapshot(
                    self.frozen,
                    &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                        work,
                        cancellation: work.cancellation(),
                    },
                )
                .await
                .map_err(crate::pi_fork_host::native_lazy_error)?;
            self.caller_owner
                .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
                .await?;
            if !context.created_project_authenticated {
                if context.source.task_id() != context.graph_root {
                    return Err(Error::NativeBoundary {
                        code: acyclic_harness::wire::ErrorCode::Unauthorized,
                        reason: "Pi selected child lacks its actual project creator proof",
                    });
                }
                let PiTaskExecution::Host { root, .. } = &original.0.execution else {
                    return Err(Error::NativeBoundary {
                        code: acyclic_harness::wire::ErrorCode::Unauthorized,
                        reason: "Pi original Root was not its admitted bootstrap task",
                    });
                };
                self.caller_owner
                    .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
                    .await?;
                let actual_source = coordinator
                    .runtime
                    .runtime
                    .readonly_task_context_with_native_work(context.source.task_id(), work)
                    .await?;
                self.caller_owner
                    .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
                    .await?;
                crate::pi_root_provider::prove_root_project_source_with_native_work(
                    coordinator.owner.clone(),
                    &coordinator.resources,
                    coordinator.filesystem.clone(),
                    coordinator.stream.clone(),
                    &original.0.binding,
                    root,
                    &snapshot.project,
                    self.frozen,
                    actual_source.1.clone(),
                    &actual_source.0,
                    work,
                )
                .await?;
                self.caller_owner
                    .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
                    .await?;
            }
            Ok(())
        })
    }
}
