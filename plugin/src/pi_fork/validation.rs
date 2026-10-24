//! Validation of the original Pi fork.
use super::*;
use acyclic_harness::filesystem::PreparationIo;

impl<'a> PiForkCoordinator<'a> {
    /// Validates a plan once per publication, caller and owner: a plan read
    /// back from its publication is checked the first time and looked up after.
    pub(super) fn validate_plan(&self, plan: &PiForkPlan, caller: TaskId) -> Result<()> {
        let key = plan
            .published
            .map(|published| self.owner.validated_plan_key(&published, caller));
        self.resources
            .validated_fork_plans
            .validate_once(key, || self.validate_plan_contents(plan, caller))
    }

    fn validate_plan_contents(&self, plan: &PiForkPlan, caller: TaskId) -> Result<()> {
        let original = original_operation(
            self.owner.policy().root_task,
            caller,
            &plan.commit.sdk_operation,
            &self.work,
        )?;
        let operations = PiForkOperations::new(original, &self.work)?;
        {
            let work = &self.work;
            plan.request
                .validate(&mut Some(&mut &PreparationIo::from_native_work(work)?))?;
            plan.preparation
                .validate(&mut Some(&mut &PreparationIo::from_native_work(work)?))?;
        }
        let child_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(
            &plan.request.child.input,
            acyclic_harness::JsonValueSchema::None,
            crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
        )?;
        let child_input = &child_input.0;
        let decoded_request: (ForkRequest, acyclic_fs::NativeAllocation) = self
            .decode_input_value(
                &plan.request.payload,
                acyclic_harness::JsonValueSchema::ForkRequest,
                FORK_PLAN_DIAGNOSTIC_LABEL_BYTES,
            )?;
        let request = &decoded_request.0;
        request.validate(&mut Some(&mut &PreparationIo::from_native_work(
            &self.work,
        )?))?;
        let child_launch = match &child_input.execution {
            PiTaskExecution::Child { launch } => launch,
            PiTaskExecution::Host { .. } => {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi fork admission substituted a root task",
                });
            }
        };
        {
            let work = &self.work;
            plan.commit.launch.validate(work)?;
        }
        let mut projects =
            request
                .selections
                .iter()
                .filter_map(|selection| match &selection.revision {
                    acyclic_harness::fork::ResourceRevision::Project { generation, .. }
                        if selection.required =>
                    {
                        Some(generation)
                    }
                    _ => None,
                });
        let project_selected = projects.next().is_some_and(|generation| {
            generation.as_resource().key() == plan.frozen.authored_generation.digest().as_bytes()
        }) && projects.next().is_none();
        let scratch_selected = request.selections.iter().any(|selection| matches!(
            &selection.revision, acyclic_harness::fork::ResourceRevision::PrivateVolume { paths, .. }
                if selection.required && paths.is_empty()));
        let native_snapshot = match request
            .authorized_source
            .as_ref()
            .and_then(|source| source.native_snapshot.as_ref())
        {
            Some(snapshot) => self.same_payload(snapshot, &plan.frozen)?,
            None => plan.commit.source_task == caller,
        };
        let source_authorized = match (
            &request.authorized_source,
            plan.commit.source_task == caller,
        ) {
            (None, true) => true,
            (Some(source), false) => {
                source.caller_task == caller
                    && source.source_task == plan.commit.source_task
                    && source.operation_id == operations.fork
                    && source.graph_root == self.owner.policy().root_task
                    && native_snapshot
            }
            _ => false,
        };
        if plan.original != original
            || plan.commit.caller != caller
            || plan.commit.alias != plan.commit.launch.agent_alias
            || plan.preparation.operation_id != operations.preparation
            || plan.preparation.parent != Some(caller)
            || !self.same_payload(&plan.preparation.input, &plan.request)?
            || plan.request.child.operation_id != operations.child
            || plan.request.child.parent != Some(caller)
            || !creation_proofs_bind(plan, request, operations.preparation)
            || plan.request.source_checkpoint.is_none()
            || !task_id_text_matches(caller, &request.parent.id)
            || plan
                .request
                .command
                .as_ref()
                .is_none_or(|command| command.operation_id != operations.fork)
            || request.operation_id != operations.fork
            || request.child_agent != operations.agent
            || !task_id_text_matches(plan.commit.child.task_id, &request.child.id)
            || child_input.binding != plan.commit.child
            || child_launch.operation_id != operations.native
            || child_launch.kind != NATIVE_LAUNCH_TASK_COMMAND_KIND
            || !project_selected
            || !scratch_selected
            || !source_authorized
            || request.preparation.child_result_volume.as_ref()
                != plan
                    .commit
                    .child
                    .results
                    .as_ref()
                    .map(|result| &result.volume)
            || plan
                .commit
                .child
                .results
                .as_ref()
                .is_none_or(|result| !matches!(result.allocation, PiScratchAllocation::Preparing))
            || plan
                .commit
                .launch
                .invocation
                .environment
                .keys()
                .any(|key| crate::pi_caller::is_environment_key(key))
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi fork plan changed the original admission, operation, or caller cut",
            });
        }
        Ok(())
    }
}

/// The child's creation proofs are exactly one per volume this fork creates, each
/// retained by this fork's own preparation task under the request's provider.
fn creation_proofs_bind(
    plan: &PiForkPlan,
    request: &ForkRequest,
    preparation: OperationId,
) -> bool {
    let created = [
        Some(&request.preparation.child_private_volume),
        Some(&request.preparation.child_project_volume),
        request.preparation.child_result_volume.as_ref(),
    ]
    .into_iter()
    .flatten()
    .count();
    let proofs = &plan.request.child.resource_proofs;
    proofs.len() == created
        && proofs.iter().all(|proof| {
            proof.preparation == task_id_for_admission(preparation)
                && proof.provider == plan.request.provider
        })
}
