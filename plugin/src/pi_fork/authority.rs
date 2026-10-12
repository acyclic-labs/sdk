//! Authority of the original Pi fork.
use super::*;

impl<'a> PiForkCoordinator<'a> {
    pub(super) fn remaining(&self, fallback: WorkBudget) -> Result<WorkBudget> {
        self.work
            .remaining()
            .map(|remaining| remaining.intersect_budget(fallback))
            .map_err(acyclic_harness::Error::from)
    }

    pub(super) async fn authenticate(
        &self,
        context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>,
        fence: &LeaseFence,
    ) -> Result<(TaskAdmissionRecord, acyclic_fs::NativeAllocation)> {
        let host = self.runtime.runtime.task_host();
        let admission = {
            let work = &self.work;
            self.runtime
                .verify_owner_with_native_work(&self.owner, work)?;
            owner
                .verify(false)
                .await?;
            let (value, allocation) = host
                .observe_admission_with_native_work(owner.task_id(), work)
                .await?;
            (value, allocation)
        };
        if context.durable_task_id() != Some(owner.task_id())
            || context.owned_task_fence()? != *fence
            || admission.0.grants != *context.scope().grants()
            || admission.0.limits != context.scope().limits()
            || admission.0.run_limits != context.scope().run_limits()
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi fork context differs from the original caller admission or fence",
            });
        }
        Ok(admission)
    }

    /// The same original-root policy seals inspect/import/fork reads.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact original caller, source selection and bounds"
    )]
    pub(crate) async fn authorize_selected_project_with_native_work(
        &self,
        context: &acyclic_harness::runtime::TaskContext,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        source_task: TaskId,
        source: Arc<LocalLazyWorkspace>,
        frozen: LazySnapshotRef,
        operation: OperationId,
        maximum_work: u64,
        maximum_bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<(
        acyclic_harness::source_authorization::ProjectSourceReadHandle,
        acyclic_fs::NativeAllocation,
    )> {
        self.runtime
            .verify_owner_with_native_work(&self.owner, work)?;
        crate::pi_inspection::authorize_selected_source(
            &self.runtime.runtime,
            self.owner.policy().root_task,
            context,
            owner,
            source_task,
            self.owner.policy().session_limits.depth,
            work,
        )
        .await?;
        let admitted = self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(source_task, work)
            .await?;
        let maximum = self.owner.policy().maximum_payload_bytes;
        let bytes = crate::pi_root_provider::canonical_pi_value(&admitted.0.input, maximum, work)?;
        let original = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(
            &bytes.0,
            maximum,
            maximum,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?;
        let mut project = None;
        for volume in original.0.binding.project_volumes.values() {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            if volume.storage_name(&mut Some(&mut io))? == source.workspace().name().as_str() {
                project = Some(volume);
                break;
            }
        }
        let project = project.ok_or_else(|| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi selected project has no actual original admitted source binding",
        })?;
        let selected = project_snapshot(project, frozen, maximum, work)?;
        let native_snapshot =
            selected
                .0
                .native_snapshot
                .as_ref()
                .ok_or_else(|| Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi selected native cut is absent",
                })?;
        let policy = PiProjectSourcePolicy {
            coordinator: self,
            source,
            frozen,
            caller_context: context,
            caller_owner: owner,
            native_snapshot,
        };
        owner
            .authorize_project_source_with_native_work(
                context,
                &self.runtime.authority.issuer,
                &policy,
                source_task,
                operation,
                &selected.0,
                maximum_work,
                maximum_bytes,
                work,
            )?
            .await
    }

    pub(super) async fn selected_graph(
        &self,
        context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>,
        source: TaskId,
    ) -> Result<()> {
        let work = &self.work;
        return crate::pi_inspection::authorize_selected_source(
            &self.runtime.runtime,
            self.owner.policy().root_task,
            context,
            owner,
            source,
            self.owner.policy().session_limits.depth,
            work,
        )
        .await;
    }
    pub(super) async fn authenticate_launch(
        &self,
        context: &acyclic_harness::runtime::TaskContext,
        plan: &PiForkPlan,
    ) -> Result<()> {
        let child_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(
            &plan.request.child.input,
            acyclic_harness::JsonValueSchema::None,
            crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
        )?;
        let PiTaskExecution::Child { launch } = &child_input.0.execution else {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi retained child is not its native launch task",
            });
        };
        let reference = crate::pi_root_provider::observe_pi_input(
            &self.owner,
            &self.filesystem,
            derived(plan.original, "native-input", &self.work)?,
            "native-input",
            &self.work,
        )
        .await?
        .ok_or_else(|| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi launch input has no original immutable publication",
        })?;
        if launch.payload != reference.0 {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native launch changed its original immutable input locator",
            });
        }
        self.require_input_read(context)?;
        let bytes = self.read_original_input(&reference.0).await?;
        let actual: (PiNativeLaunchIntent, acyclic_fs::NativeAllocation) = self
            .decode_original_input(
                bytes.as_ref(),
                acyclic_harness::JsonValueSchema::None,
                crate::pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES,
            )?;
        if actual.0 != plan.commit.launch {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native SDK source cut, session bytes, or invocation changed",
            });
        }
        Ok(())
    }

    pub(super) fn native_copy<T: serde::de::DeserializeOwned, S: Serialize>(
        &self,
        value: &S,
        schema: acyclic_harness::JsonValueSchema,
        work: &NativeMaterializationWork,
    ) -> Result<(T, acyclic_fs::NativeAllocation)> {
        let maximum = self.owner.policy().maximum_payload_bytes;
        let bytes = crate::pi_root_provider::canonical_pi_value(value, maximum, work)?;
        crate::pi_root_provider::guarded_pi_decode(
            &bytes.0,
            maximum,
            maximum,
            acyclic_harness::wire::ErrorCode::Invalid,
            schema,
            FORK_PLAN_DIAGNOSTIC_LABEL_BYTES,
            work,
        )
    }

    pub(super) async fn native_generation(
        &self,
        context: &acyclic_harness::runtime::TaskContext,
        owner: &TaskJournalOwner<LocalStream>,
        volume: &VolumeRef,
        work: &NativeMaterializationWork,
    ) -> Result<(
        acyclic_harness::resources::GenerationRef,
        acyclic_fs::NativeAllocation,
    )> {
        context
            .verify_original_volume_read_with_native_work(owner, volume, work)
            .await?;
        let name = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let value = volume.storage_name(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
        let workspace = self
            .resources
            .fs
            .open_workspace_with_native_work(&name.0, work, work.cancellation())
            .await
            .map_err(native_workspace_error)?;
        (owner)
            .verify(false)
            .await?;
        let head = workspace
            .head(work, work.cancellation())
            .await
            .map_err(|error| native_workspace_error((error, None)))?;
        (owner)
            .verify(false)
            .await?;
        acyclic_harness::resources::GenerationRef::new_with_native_work(
            volume.provider(),
            head.id().digest().as_bytes(),
            None,
            work,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact caller snapshot descriptors remain borrowed"
    )]
    pub(super) fn native_source_snapshot(
        &self,
        task: TaskId,
        project: &VolumeRef,
        project_generation: &acyclic_harness::resources::GenerationRef,
        private_volume: &VolumeRef,
        private_generation: &acyclic_harness::resources::GenerationRef,
        work: &NativeMaterializationWork,
    ) -> Result<(RecursiveSourceSnapshot, acyclic_fs::NativeAllocation)> {
        #[derive(Serialize)]
        struct Source<'a> {
            task: TaskId,
            project: &'a VolumeRef,
            project_generation: &'a acyclic_harness::resources::GenerationRef,
            private_volume: &'a VolumeRef,
            private_generation: &'a acyclic_harness::resources::GenerationRef,
        }
        self.native_copy(
            &Source {
                task,
                project,
                project_generation,
                private_volume,
                private_generation,
            },
            acyclic_harness::JsonValueSchema::None,
            work,
        )
    }
}
