//! Exact user-approved publication to the original Root.
use super::*;

impl PiControlHost<'_> {
    pub(super) async fn writeback(
        &self,
        input: &PiSelectedCommand,
        source: &PiNativeSelectedSource,
        imported: LazySnapshotRef,
        operation: OperationId,
        reconcile: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskCommandProgress> {
        let context = &source.context;
        let owner = &source.owner;
        let key = source.key.as_str();
        let binding = self.root_binding()?;
        let volume = binding
            .project_volumes
            .get(key)
            .ok_or_else(|| denied("Root lacks this project"))?;
        let root_task = self.protected.policy().root_task;
        if input.caller != root_task
            || owner.task_id() != root_task
            || self.native()?.selected_source_task(input.source) != Some(root_task)
        {
            return Err(denied("writeback is not the child's original Root"));
        }
        let actual = self
            .roots
            .get(key)
            .ok_or_else(|| denied("Root workspace unavailable"))?;
        let retained = self
            .retained
            .roots
            .get(key)
            .ok_or_else(|| denied("Root identity unavailable"))?;
        let creation = retained.created().map_err(Error::Unauthorized)?;
        let identity = actual.physical.source.inner().root_identity();
        if creation.native_root_identity != identity.to_bytes()
            || creation.source_identity != actual.physical.source.reference().identity
            || imported.workspace_id != actual.workspace.workspace().id()
            || creation.repository_workspace_id != actual.workspace.workspace().id().into_bytes()
        {
            return Err(denied("writeback changed its protected creator allocation"));
        }
        let original = self
            .native()?
            .frozen_source(input.source)
            .ok_or_else(|| denied("writeback lost the creator's frozen Root facts"))?;
        if original.workspace_id != actual.workspace.workspace().id() {
            return Err(denied("writeback baseline names another Root workspace"));
        }
        let fs_operation = acyclic_fs::OperationId::from_bytes(operation.into_bytes());
        let candidate = actual
            .workspace
            .workspace()
            .generation(imported.authored_generation, work, work.cancellation())
            .await
            .map_err(|error| native_workspace_error((error, None)))?;
        let input_cut = self.admission_input(root_task, work).await?;
        let PiTaskExecution::Host { root: intent, .. } = input_cut.0.execution else {
            return Err(denied("writeback replaced its Root admission with a child"));
        };
        let root = PiWritebackRoot::new(
            Arc::clone(&self.protected),
            self.resources,
            Arc::clone(&self.filesystem),
            self.stream.clone(),
            Arc::clone(context),
            Arc::clone(owner),
            (
                crate::pi_root_inputs::PiRootRequest {
                    binding: input_cut.0.binding,
                    root: intent,
                },
                input_cut.1,
            ),
            key,
            original,
            &retained.path,
            operation,
            identity,
            work,
        )
        .await?;
        let options_path = work.copy_path(&root.operation_directory)?;
        let options = acyclic_fs::MaterializeOptions::native(options_path.0);
        let generations = build_writeback_generations(
            &root,
            actual,
            original,
            &candidate,
            &input.paths,
            operation,
            work,
        )
        .await?;
        if reconcile {
            let store = self.resources.store.materialization(work.clone());
            let journal = acyclic_fs::MaterializationJournalStore::load(&store, fs_operation)
                .await
                .map_err(|error| match error {
                    acyclic_fs::LocalCoreStateStoreError::MaterializationWork(cause) => {
                        Error::NativeWork {
                            cause,
                            custody: None,
                        }
                    }
                    source => Error::NativeWorkspacePublication {
                        source: acyclic_fs::NativeWorkspacePublicationError::State(source),
                        custody: None,
                    },
                })?;
            let Some(journal) = journal else {
                return Ok(TaskCommandProgress::Pending);
            };
            if !journal.is_valid_for(fs_operation) {
                return Err(denied("writeback journal is invalid"));
            }
            if journal.phase != acyclic_fs::MaterializationPhase::Applied {
                return Ok(TaskCommandProgress::Pending);
            }
            let digest = input
                .approval_digest
                .as_deref()
                .ok_or_else(|| denied("applied writeback has no exact user approval"))?;
            validate_applied_writeback(&root, &generations, &journal, digest, work)?;
            let approved = crate::pi_operator_host::read_prepared_approval(
                &self.runtime,
                &self.filesystem,
                &self.protected,
                &input.sdk_operation,
                work,
            )
            .await?
            .ok_or_else(|| denied("applied writeback lost its prepared approval"))?;
            if approved.0.operation != operation
                || approved.0.command.source != input.source
                || approved.0.command.root.as_deref() != Some(key)
                || approved.0.command.paths != input.paths
                || approved.0.approval_digest != digest
                || approved.0.imported != imported.authored_generation
            {
                return Err(denied("applied writeback changed its original approval"));
            }
            crate::pi_operator_host::require_resolved_approval(
                &self.filesystem,
                &self.stream,
                &self.protected,
                &approved.0,
                work,
            )
            .await?;
            let mut io = HistoryWork::new(PreparationIo::from_native_work(work)?);
            owner.verify(false, &mut Some(&mut io)).await?;
            return ready(
                &PiWritebackResponse {
                    status: "published",
                    source: input.source,
                    root: key,
                    paths: &input.paths,
                    approval_digest: digest,
                    operation: &input.sdk_operation,
                    original_operation: operation,
                    source_snapshot: None,
                    selected_snapshot: None,
                    imported: None,
                    receipt: Some(&journal),
                },
                work,
            );
        }
        let journal =
            prepare_writeback(&root, &generations, &self.resources.store, &options, work).await?;
        let digest_value = writeback_digest_value(&root, &generations, &journal);
        let mut digest = crate::pi_root_provider::digest_pi_value(
            &digest_value,
            work.budget().bytes_encoded,
            work,
        )?;
        let mut encoded = crate::pi_root_provider::canonical_pi_value(
            &digest_value,
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
            work,
        )?;
        digest.1.absorb(&mut encoded.1)?;
        let canonical = String::from_utf8(encoded.0).map_err(|error| {
            drop(error);
            invalid("approval codec returned non-UTF8")
        })?;
        let approval_data = (digest.0, canonical, digest.1);
        let root_display = work.display(&root.path.display())?;
        let source_display = self
            .native()?
            .with_selected_project_path(input.source, &source.volume, |path| {
                work.display(&path.display()).map_err(Error::from)
            })
            .transpose()?;
        let descriptor = crate::pi_operator::PiPreparedApproval {
            version: 1,
            session: &binding.native_session_id,
            command: PiSelectedCommand::normalized(input, key),
            operation,
            approval_digest: &approval_data.0,
            private_volume: &binding.scratch.volume,
            source_snapshot: generations.original_snapshot,
            selected_snapshot: source.snapshot,
            imported: generations.imported,
            journal: &journal,
            canonical_digest_input: &approval_data.1,
            root_path: &root_display.0,
            source_path: source_display.as_ref().map_or("", |(value, _)| value),
        };
        crate::pi_operator_host::open_prepared_approval(
            &self.runtime,
            &self.filesystem,
            &self.protected,
            context,
            owner,
            binding,
            &descriptor,
            work,
        )
        .await?;
        let mut response = PiWritebackResponse {
            status: "prepared",
            source: input.source,
            root: key,
            paths: &input.paths,
            approval_digest: &approval_data.0,
            operation: &input.sdk_operation,
            original_operation: operation,
            source_snapshot: Some(descriptor.source_snapshot),
            selected_snapshot: Some(descriptor.selected_snapshot),
            imported: Some(descriptor.imported),
            receipt: None,
        };
        let Some(approval) = input.approval_digest.as_deref() else {
            return ready(&response, work);
        };
        if approval != descriptor.approval_digest {
            return Err(denied("approval differs from the exact original-root plan"));
        }
        crate::pi_operator_host::require_resolved_approval(
            &self.filesystem,
            &self.stream,
            &self.protected,
            descriptor,
            work,
        )
        .await?;
        let published = publish_writeback(
            &root,
            &generations,
            &journal,
            &options,
            &self.resources.store,
            approval,
            work,
        )
        .await?;
        if published.phase != acyclic_fs::MaterializationPhase::Applied {
            return Ok(TaskCommandProgress::Pending);
        }
        let mut io = HistoryWork::new(PreparationIo::from_native_work(work)?);
        owner.verify(false, &mut Some(&mut io)).await?;
        response.status = "published";
        response.approval_digest = approval;
        response.source_snapshot = None;
        response.selected_snapshot = None;
        response.imported = None;
        response.receipt = Some(&published);
        ready(&response, work)
    }
}
