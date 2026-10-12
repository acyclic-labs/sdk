//! One selected import and durable retry path.
use super::*;

impl PiControlHost<'_> {
    pub(super) async fn import_selection(
        &self,
        binding: &PiTaskBinding,
        input: &PiSelectedCommand,
        source: &PiNativeSelectedSource,
        import_operation: OperationId,
        prior: Option<&SelectedImportIntent>,
        reconcile: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<LazySnapshotRef>> {
        let context = &source.context;
        let owner = &source.owner;
        let key = source.key.as_str();
        let root = self.protected.policy().root_task;
        let destination = input.destination.unwrap_or(input.caller);
        if destination == root && destination != input.caller {
            return Err(denied(
                "selected publication changed its original Root or import operation",
            ));
        }
        let live = if destination == root {
            None
        } else {
            Some(
                self.native()?
                    .live_source(destination, work)
                    .await?
                    .ok_or_else(|| denied("destination has no original live owner"))?,
            )
        };
        let destination_binding = if destination == input.caller {
            binding
        } else {
            &live
                .as_ref()
                .ok_or_else(|| denied("destination is not live"))?
                .prepared
                .binding
        };
        let volume = destination_binding
            .project_volumes
            .get(key)
            .ok_or_else(|| denied("destination lacks this project"))?;
        let mut io = HistoryWork::new(PreparationIo::from_native_work(work)?);
        let workspace = if let Some(live) = &live {
            &live.prepared.workspace
        } else {
            &self
                .roots
                .get(key)
                .ok_or_else(|| denied("Root workspace unavailable"))?
                .workspace
        };
        let (mount, publication_work, permit) = if let Some(live) = &live {
            let (mount, work) = self
                .native()?
                .project_mount(destination, volume)
                .ok_or_else(|| denied("destination project is not mounted"))?;
            let permit = live
                .view
                .project_publication(volume)
                .ok_or_else(|| denied("destination lost its project publication permit"))?;
            (Some(mount), work, permit)
        } else {
            (
                None,
                work.clone(),
                acyclic_fs::PublicationPermit::Unrestricted,
            )
        };
        if !publication_work.shares_ledger(work) {
            return Err(denied("publication crossed the caller's work ledger"));
        }
        let (mount_guard, destination_guard) = if let Some(live) = &live {
            let guards = live
                .context
                .volume_callback_guard_with_native_work(&live.owner, &live.original_work)
                .await?
                .for_volume(&live.owner, volume, true, &publication_work)?;
            (Some(guards.0), Some(guards.1))
        } else {
            (None, None)
        };
        let caller_guard = context
            .volume_callback_guard_with_native_work(owner, source._custody.0.work())
            .await?
            .for_volume(owner, volume, true, &publication_work)?
            .1;
        caller_guard.check(&publication_work)?.await?;
        if let Some(guard) = &destination_guard {
            guard.check(&publication_work)?.await?;
        }
        let resuming = prior.is_some();
        let intent = if let Some(intent) = prior {
            if intent.caller != input.caller
                || intent.source != input.source
                || intent.root != key
                || &intent.destination != volume
                || intent.paths != input.paths
                || intent.operation != import_operation
            {
                return Err(denied("selected retry changed its sealed request"));
            }
            (SelectedImportIntent::from(intent), None)
        } else {
            if reconcile {
                return Ok(None);
            }
            let frozen = if let Some(live) = &live {
                crate::pi_fork_host::freeze_child_source(
                    &live.context,
                    &live.owner,
                    &live.view,
                    workspace,
                    &live.original_work,
                )
                .await?
            } else {
                let input = self.admission_input(input.caller, work).await?;
                crate::pi_fork_host::freeze_created_root_source(
                    Arc::clone(&self.protected),
                    self.resources,
                    Arc::clone(&self.filesystem),
                    self.stream.clone(),
                    context,
                    owner,
                    &input.0,
                    key,
                    &self.roots[key],
                    work,
                )
                .await?
            };
            owner.verify(false, &mut Some(&mut io)).await?;
            let intent = SelectedImportIntent {
                caller: input.caller,
                source: input.source,
                root: key,
                destination: volume,
                source_snapshot: source.snapshot,
                destination_snapshot: frozen.0,
                paths: input.paths.as_slice(),
                operation: import_operation,
            };
            let staged = crate::pi_root_provider::stage_pi_input(
                &self.protected,
                &self.filesystem,
                import_operation,
                "selected-import-intent",
                &intent,
                work,
            )
            .await?;
            drop(staged);
            (intent, Some(frozen))
        };
        if source.snapshot != intent.0.source_snapshot {
            return Err(denied("import changed its pinned source snapshot"));
        }
        owner.verify(false, &mut Some(&mut io)).await?;
        let fs_operation = acyclic_fs::OperationId::from_bytes(import_operation.into_bytes());
        let completed = if let Some(mount) = &mount {
            match mount
                .selected_import_snapshot(fs_operation, &publication_work, work.cancellation())
                .await
            {
                Err(acyclic_fs::NativeMountError::Source(error))
                    if matches!(
                        error.semantic_error(),
                        acyclic_fs::MountSourceError::OriginalSnapshotUnavailable
                            | acyclic_fs::MountSourceError::Indeterminate
                    ) =>
                {
                    return Ok(None);
                }
                result => result?,
            }
        } else {
            let accounting = acyclic_fs::lazy_workspace::LazyTreapWork::native_with_work(
                work,
                work.cancellation(),
            )?;
            match workspace
                .selected_import_snapshot(fs_operation, &accounting)
                .await
            {
                Err(error)
                    if matches!(
                        error.semantic_error(),
                        acyclic_fs::LazyWorkspaceError::OriginalSnapshotUnavailable
                    ) || matches!(error.semantic_error(), acyclic_fs::LazyWorkspaceError::SelectedImportIndeterminate { operation } if *operation == fs_operation) =>
                {
                    return Ok(None);
                }
                result => result.map_err(crate::pi_fork_host::native_lazy_error)?,
            }
        };
        let imported = if let Some(imported) = completed {
            imported
        } else {
            if reconcile || resuming {
                return Ok(None);
            }
            match import_native_project(
                source,
                workspace,
                mount.as_deref(),
                volume,
                intent.0.destination_snapshot,
                intent.0.paths,
                import_operation,
                permit,
                acyclic_harness::conversation::MAX_PRIVATE_DIRECTORY_PAGE,
                self.protected.policy().limits.file_bytes,
                &publication_work,
                {
                    let caller_guard = Arc::clone(&caller_guard);
                    let destination_guard = destination_guard.clone();
                    move |original: &acyclic_fs::NativeMaterializationWork| {
                        let original = original.clone();
                        let caller_guard = Arc::clone(&caller_guard);
                        let destination_guard = destination_guard.clone();
                        let mount_guard = mount_guard.clone();
                        async move {
                            if let Some(guard) = mount_guard {
                                guard.check(&original)?.await?;
                            }
                            caller_guard.check(&original)?.await?;
                            if let Some(guard) = destination_guard {
                                guard.check(&original)?.await?;
                            }
                            Ok(())
                        }
                    }
                },
            )
            .await
            {
                Err(Error::NativeSource { source, .. })
                    if matches!(source.semantic_error(),
                    acyclic_fs::LazyWorkspaceError::SelectedImportIndeterminate { operation } if *operation == fs_operation) =>
                {
                    return Ok(None);
                }
                result => result?,
            }
        };
        caller_guard.check(&publication_work)?.await?;
        if let Some(guard) = &destination_guard {
            guard.check(&publication_work)?.await?;
        }
        owner.verify(false, &mut Some(&mut io)).await?;
        Ok(Some(imported))
    }

    pub(super) async fn read_import_intent(
        &self,
        context: &TaskContext,
        operation: OperationId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<(SelectedImportIntent, acyclic_fs::NativeAllocation)>> {
        crate::pi_root_provider::read_pi_input_with_native_work(
            &self.protected,
            &self.filesystem,
            operation,
            "selected-import-intent",
            crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
            acyclic_harness::JsonValueSchema::None,
            SELECTED_DIAGNOSTIC_LABEL_BYTES,
            context,
            work,
        )
        .await
    }
}
