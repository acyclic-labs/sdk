//! Durable Pi spawn through the existing SDK fork host.
use super::*;
use crate::pi_selected_host::invalid;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use acyclic_harness::{
    Error, OperationId, Result,
    filesystem::{PreparationIo, TaskCommandProgress},
    runtime::TaskStateProvider,
    store::HistoryWork,
    workflow::WorkflowCommand,
};
use pi_fork::{PiForkOperations, PreparedPiFork, RetainedSpawnCommit};
use pi_native_host::{PiNativeCommandHost, PiNativeLiveSource};
use serde::de::DeserializeOwned;

mod catalog;
mod input;
mod observation;

impl PiSession {
    pub(super) async fn spawn_commit(
        &mut self,
        caller: TaskId,
        arguments: &Value,
        owner: &pi_owner::PiOwnerConfig,
        original_work: &NativeMaterializationWork,
    ) -> Result<(Value, NativeAllocation)> {
        self.runtime
            .verify_owner_with_native_work(owner, original_work)?;
        let native = self.native_host_with_native_work(original_work)?;
        let work = if caller == owner.policy().root_task {
            original_work.clone()
        } else {
            native.invocation_work(caller).await?
        };
        let committed = self.spawn_native_commit(caller, arguments, owner, &work)?;
        let commit = &committed.0;
        let original = pi_fork::original_operation(
            owner.policy().root_task,
            caller,
            &commit.sdk_operation,
            &work,
        )?;
        let operations = PiForkOperations::new(original, &work)?;
        let task = commit.child.task_id;
        let coordinator = pi_fork::PiForkCoordinator::new(
            self.runtime.clone(),
            self.filesystem.clone(),
            self.stream.clone(),
            &self.resources,
            native.protected.clone(),
            &work,
        )?;
        let plan = coordinator.load_original_plan(original).await?;
        if plan
            .as_ref()
            .is_some_and(|plan| plan.original != original || plan.commit != *commit)
        {
            return Err(invalid("Pi retry changed its original spawn intent"));
        }
        if let Some(route) = self.state.routes.get(&commit.alias) {
            self.spawn_validate_route(route, commit, operations, &work)?;
            let plan = plan
                .as_ref()
                .ok_or_else(|| invalid("Pi created route lost its immutable fork plan"))?;
            self.spawn_validate_admitted(plan, owner.policy().maximum_payload_bytes, &work)
                .await?;
            let launch = self
                .spawn_input::<pi_native::PiNativeLaunchIntent>(
                    owner,
                    pi_fork::derived(original, "native-input", &work)?,
                    "native-input",
                    pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES,
                    &work,
                )
                .await?
                .ok_or_else(|| invalid("Pi created child lost its immutable native intent"))?;
            if launch.0 != commit.launch {
                return Err(invalid("Pi duplicate changed its sealed native intent"));
            }
            return self
                .spawn_observation(
                    commit,
                    operations,
                    true,
                    &work,
                    owner.policy().maximum_payload_bytes,
                )
                .await;
        }
        let pending = self
            .state
            .pending
            .iter()
            .find(|entry| entry.fork_key == operations.fork.into_bytes());
        if let Some(pending) = pending {
            self.spawn_validate_pending(pending, commit, operations, &work)?;
            if plan.is_none() {
                return Err(invalid(
                    "Pi admitted preparation lost its immutable fork plan",
                ));
            }
        }
        if self.state.routes.len() >= MAXIMUM_ADAPTER_ROUTES
            || (pending.is_none() && self.state.pending.len() >= MAXIMUM_ADAPTER_PENDING)
            || self.native_jobs.contains_key(&task)
        {
            return Err(invalid(
                "Pi child catalog is exhausted or already owns this worker",
            ));
        }
        let live = if caller == owner.policy().root_task {
            None
        } else {
            let live = native
                .live_source(caller, &work)
                .await?
                .ok_or_else(|| invalid("Pi recursive spawn requires its running caller"))?;
            if live.prepared.binding != *self.binding(caller)?
                || live.context.durable_task_id() != Some(caller)
                || !live.view.manifest().volumes.iter().any(|mapping| {
                    Path::new(&commit.launch.source.parent_cwd).starts_with(&mapping.path)
                })
            {
                return Err(invalid(
                    "Pi recursive spawn changed its caller binding or view",
                ));
            }
            live.owner
                .verify(
                    false,
                    &mut Some(&mut HistoryWork::new(PreparationIo::from_native_work(
                        &work,
                    )?)),
                )
                .await?;
            Some(live)
        };
        let key = commit
            .child
            .project_volumes
            .keys()
            .next()
            .ok_or_else(|| invalid("Pi child has no selected project"))?;
        let root_source = if commit.source_task != owner.policy().root_task {
            None
        } else if let Some(plan) = &plan {
            Some(plan.frozen)
        } else {
            let operation = pi_fork::derived(original, "observe-root-source", &work)?;
            if let Some(observed) = self
                .spawn_input::<pi_fork_host::ObservedSource>(
                    owner,
                    operation,
                    "root-source-observation",
                    pi_fork_host::OBSERVED_SOURCE_DIAGNOSTIC_LABEL_BYTES,
                    &work,
                )
                .await?
            {
                if observed.0.caller != caller
                    || observed.0.sdk_operation != commit.sdk_operation
                    || observed.0.root_key != *key
                {
                    return Err(invalid("Pi retry changed its Root source observation"));
                }
                Some(observed.0.snapshot)
            } else {
                let command = self
                    .spawn_command(
                        owner,
                        operation,
                        pi_fork_host::PI_OBSERVE_SOURCE_COMMAND_KIND,
                        &pi_fork_host::PiObserveSourceCommand {
                            caller,
                            sdk_operation: commit.sdk_operation.clone(),
                            root_key: key.clone(),
                        },
                        &work,
                    )
                    .await?;
                let host = self.command_host(&work, Some(native.clone()))?;
                let acknowledgement = host.control_step_with_native_work(command.0, false).await?;
                if host.pending_to_save()?.is_some() {
                    return Err(invalid(
                        "Pi Root source observer unexpectedly admitted a child",
                    ));
                }
                let Some(acknowledgement) = acknowledgement else {
                    return self
                        .spawn_observation(
                            commit,
                            operations,
                            false,
                            &work,
                            owner.policy().maximum_payload_bytes,
                        )
                        .await;
                };
                let observed: pi_fork_host::PiRootSource =
                    serde_json::from_value(acknowledgement.0)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                if observed.root_key != *key {
                    return Err(invalid(
                        "Pi Root source acknowledgement changed its project",
                    ));
                }
                Some(observed.snapshot)
            }
        };
        let payload = pi_fork_host::PiForkCommand {
            commit: committed.0,
            root_source,
        };
        let commit = &payload.commit;
        let key = commit
            .child
            .project_volumes
            .keys()
            .next()
            .ok_or_else(|| invalid("Pi child has no project"))?;
        let command = self
            .spawn_command(
                owner,
                original,
                pi_fork_host::PI_FORK_COMMAND_KIND,
                &payload,
                &work,
            )
            .await?;
        if !self
            .state
            .pending
            .iter()
            .any(|entry| entry.fork_key == operations.fork.into_bytes())
            && let Some(plan) = &plan
        {
            let waiting = if live.is_some() {
                true
            } else {
                self.runtime
                    .runtime
                    .task_host()
                    .workflow_suspension_with_native_work(caller, &work)
                    .await?
                    .0
                    .as_ref()
                    .is_some_and(|state| state.waiting_command == Some(original))
            };
            if waiting {
                let admission = self
                    .runtime
                    .runtime
                    .task_host()
                    .observe_admission_with_native_work(plan.preparation.task_id(), &work)
                    .await?;
                if admission.0 != plan.preparation {
                    return Err(invalid("Pi original preparation admission was substituted"));
                }
                let mut reservation = self
                    .command_host(&work, Some(native.clone()))?
                    .reservation_with_native_work(plan, &work)?;
                let mut batch = pi_native::pi_native_vec(1, &work)?;
                batch.1.absorb(&mut reservation.1)?;
                batch.0.push(reservation.0);
                self.spawn_save_pending(Some(batch), commit, operations, &work)?;
            }
        }
        let saved_pending = self
            .state
            .pending
            .iter()
            .any(|entry| entry.fork_key == operations.fork.into_bytes());
        let (mut acknowledgement, pending, mut prepared) = self
            .spawn_step(
                &native,
                live.as_ref(),
                &command.0,
                &payload,
                live.is_some() && plan.is_some(),
                &work,
            )
            .await?;
        let newly_saved = pending.is_some();
        self.spawn_save_pending(pending, commit, operations, &work)?;
        if prepared.is_none() && (newly_saved || (saved_pending && acknowledgement.is_none())) {
            drop(acknowledgement.take());
            let result = self
                .spawn_step(&native, live.as_ref(), &command.0, &payload, true, &work)
                .await?;
            acknowledgement = result.0;
            self.spawn_save_pending(result.1, commit, operations, &work)?;
            prepared = result.2;
        }
        if prepared.is_none() && acknowledgement.is_some() {
            let plan = coordinator
                .load_original_plan(original)
                .await?
                .ok_or_else(|| invalid("Pi prepared acknowledgement lost its fork plan"))?;
            let source = if plan.commit.source_task == owner.policy().root_task {
                self.roots
                    .get(key)
                    .ok_or_else(|| invalid("Pi Root source handle is unavailable"))?
                    .workspace
                    .clone()
            } else {
                native
                    .live_workspace(plan.commit.source_task)
                    .ok_or_else(|| invalid("Pi selected child source handle is unavailable"))?
            };
            prepared = coordinator
                .observe_prepared(&plan, source, work.cancellation().clone())
                .await?;
        }
        let Some(prepared) = prepared else {
            return self
                .spawn_observation(
                    commit,
                    operations,
                    false,
                    &work,
                    owner.policy().maximum_payload_bytes,
                )
                .await;
        };
        if let Some(acknowledgement) = acknowledgement
            && (prepared.binding.task_id != task
                || acknowledgement.0.get("task").and_then(Value::as_str)
                    != Some(prepared.report.request.child.id.as_str()))
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Storage,
                reason: "Pi fork acknowledgement changed its admitted child",
            });
        }
        if let Some(live) = live {
            live.owner
                .verify(
                    false,
                    &mut Some(&mut HistoryWork::new(PreparationIo::from_native_work(
                        &work,
                    )?)),
                )
                .await?;
        }
        self.spawn_save_created(commit, operations, &prepared, &work)?;
        native.register_prepared(prepared).await?;
        let job = super::pi_lifecycle_session::spawn_native_job(&work, || async move {
            native.run_child(task).await
        })?;
        self.native_jobs.insert(task, job);
        self.spawn_observation(
            commit,
            operations,
            true,
            &work,
            owner.policy().maximum_payload_bytes,
        )
        .await
    }

    async fn spawn_step(
        &self,
        native: &Arc<PiNativeCommandHost>,
        live: Option<&PiNativeLiveSource>,
        command: &WorkflowCommand,
        input: &pi_fork_host::PiForkCommand,
        retry: bool,
        work: &NativeMaterializationWork,
    ) -> Result<(
        Option<(Value, NativeAllocation)>,
        Option<(Vec<PendingSpawn>, NativeAllocation)>,
        Option<PreparedPiFork>,
    )> {
        let host = self.command_host(work, Some(native.clone()))?;
        let reply = if let Some(live) = live {
            live.owner
                .verify(
                    false,
                    &mut Some(&mut HistoryWork::new(PreparationIo::from_native_work(
                        work,
                    )?)),
                )
                .await?;
            let mut payload = pi_root_provider::guarded_pi_value(
                input,
                host.protected.policy().maximum_payload_bytes,
                work,
            )?;
            match host
                .run(
                    live.context.clone(),
                    live.context.owned_task_fence()?,
                    command.clone(),
                    payload.0,
                    retry,
                    work,
                )
                .await?
            {
                TaskCommandProgress::ReadyWithNativeCustody { value, mut custody } => {
                    if let Err(cause) = custody.absorb(&mut payload.1) {
                        drop(value);
                        return Err(cause.into());
                    }
                    Some((value, custody))
                }
                TaskCommandProgress::Ready(_) => {
                    return Err(invalid("Pi fork acknowledgement lost its SDK funding"));
                }
                TaskCommandProgress::Pending | TaskCommandProgress::Indeterminate => None,
            }
        } else {
            host.control_step_with_native_work(command.clone(), retry)
                .await?
        };
        Ok((
            reply,
            host.pending_to_save()?,
            host.take_prepared(input.commit.child.task_id)?,
        ))
    }

    async fn spawn_command<T: Serialize>(
        &self,
        owner: &pi_owner::PiOwnerConfig,
        operation: OperationId,
        kind: &str,
        input: &T,
        work: &NativeMaterializationWork,
    ) -> Result<(WorkflowCommand, NativeAllocation)> {
        let payload = pi_root_provider::stage_pi_input(
            owner,
            &self.filesystem,
            operation,
            "command",
            input,
            work,
        )
        .await?;
        Ok((
            WorkflowCommand {
                operation_id: operation,
                kind: kind.into(),
                payload: payload.0,
            },
            payload.1,
        ))
    }

    async fn spawn_input<T: DeserializeOwned>(
        &self,
        owner: &pi_owner::PiOwnerConfig,
        operation: OperationId,
        label: &str,
        schema_bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<Option<(T, NativeAllocation)>> {
        let Some(reference) =
            pi_root_provider::observe_pi_input(owner, &self.filesystem, operation, label, work)
                .await?
        else {
            return Ok(None);
        };
        let authority = owner.authority(work)?;
        let maximum = owner
            .policy()
            .maximum_payload_bytes
            .min(MAXIMUM_ADAPTER_STATE_BYTES);
        let io = PreparationIo::from_native_work(work)?;
        let reader = acyclic_harness::filesystem::FilesystemContentVerifier::new(
            self.filesystem.clone(),
            &authority.verifier,
            &authority.signed,
            maximum,
            Some(&io),
        )?;
        let bytes = reader
            .read_with_native_work(&reference.0, maximum, work)
            .await?;
        Ok(Some(pi_root_provider::guarded_pi_decode(
            &bytes.0,
            maximum,
            std::mem::size_of::<T>() as u64,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            schema_bytes,
            work,
        )?))
    }
}
