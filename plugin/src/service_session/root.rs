//! Root allocation and recovery through one authenticated provider.
use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_harness::{EffectAttemptId, runtime::TaskContext};

fn invalid(reason: &'static str) -> RootError {
    RootError::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason,
    }
}

impl PiSession {
    async fn root_observer(
        &self,
        context: Option<Arc<TaskContext>>,
        work: &NativeMaterializationWork,
    ) -> acyclic_harness::Result<Arc<pi_root_provider::PiRootProvider>> {
        let PluginTaskBinding::Pi(binding) = &self.state.task_binding else {
            return Err(invalid("Pi root has no original task binding"));
        };
        let owner = pi_owner::PiOwnerConfig::load(
            &self.resources.private_state_root,
            &self.relative_directory,
            work,
        )?
        .into_native_shared(work)?;
        let preparation = pi_root_inputs::PiRootPreparation::load(
            &self.resources.private_state_root,
            &self.relative_directory,
            &self.runtime,
            &self.filesystem,
            binding,
            pi_recovery::PiTaskOrigin::RootState(&self.state),
            context,
            work,
        )
        .await?;
        pi_root_provider::root_provider(
            owner,
            &self.resources,
            Arc::clone(&self.filesystem),
            Arc::clone(&self.stream),
            preparation,
            None,
            work.clone(),
        )
    }

    pub(super) async fn prepare_root(
        &mut self,
        work: &NativeMaterializationWork,
    ) -> acyclic_harness::Result<()> {
        let observer = self.root_observer(None, work).await?;
        let attempt =
            EffectAttemptId::from_bytes(observer.effect_id_with_native_work(work)?.into_bytes());
        let observed = observer
            .observe_allocation_with_native_work(attempt, work)
            .await?;
        let (allocation, handles) = if let Some(allocation) = observed {
            (allocation, BTreeMap::new())
        } else {
            let driver = self.command_host(work, None)?;
            let progress = driver.drive_root().await?;
            let unresolved = match progress.0 {
                Some(acyclic_harness::filesystem::TaskWorkerAttempt::Unresolved {
                    lease,
                    error,
                }) => {
                    drop(lease);
                    Some(error)
                }
                attempt => {
                    drop(attempt);
                    None
                }
            };
            drop(progress.1);
            if let Some(error) = unresolved {
                return Err(error);
            }
            let handles = driver.created_roots().await;
            let allocation = match driver.root_output().await? {
                Some(allocation) => allocation,
                None => {
                    let observer = self.root_observer(None, work).await?;
                    let attempt = EffectAttemptId::from_bytes(
                        observer.effect_id_with_native_work(work)?.into_bytes(),
                    );
                    observer
                        .observe_allocation_with_native_work(attempt, work)
                        .await?
                        .ok_or_else(|| {
                            invalid("Pi root has no authenticated successful allocation output")
                        })?
                }
            };
            (allocation, handles)
        };
        self.install_root_allocation(allocation, handles, work)
            .await
    }

    pub(super) async fn restore_root_sources(
        &mut self,
        work: &NativeMaterializationWork,
    ) -> acyclic_harness::Result<()> {
        let observer = self.root_observer(None, work).await?;
        let attempt =
            EffectAttemptId::from_bytes(observer.effect_id_with_native_work(work)?.into_bytes());
        let allocation = observer
            .observe_allocation_with_native_work(attempt, work)
            .await?
            .ok_or_else(|| {
                invalid("Pi Root source has no original successful allocation to restore")
            })?;
        self.install_root_allocation(allocation, BTreeMap::new(), work)
            .await
    }

    async fn install_root_allocation(
        &mut self,
        mut allocation: (
            (FileRef, pi_root_allocation::PiRootAllocation),
            NativeAllocation,
        ),
        mut handles: BTreeMap<String, pi_root_provider::PiRootHandle>,
        work: &NativeMaterializationWork,
    ) -> acyclic_harness::Result<()> {
        let PluginTaskBinding::Pi(binding) = &self.state.task_binding else {
            return Err(invalid("Pi root has no original task binding"));
        };
        allocation.0.1.validate(binding)?;
        if let PiScratchAllocation::Created(existing) = &binding.scratch.allocation
            && existing != &allocation.0.1.scratch
        {
            return Err(invalid("Pi original scratch creation was substituted"));
        }
        let read = self
            .runtime
            .runtime
            .readonly_task_context_with_native_work(binding.task_id, work)
            .await?;
        let observer = self.root_observer(Some(Arc::clone(&read.1)), work).await?;
        let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work,
            cancellation: work.cancellation(),
        };
        for (key, creation) in &allocation.0.1.roots {
            let root = self
                .state
                .roots
                .get(key)
                .ok_or_else(|| invalid("Pi allocation has an unselected root"))?;
            if let RootAllocation::Created(existing) = &root.allocation
                && (existing.repository_workspace_id != creation.repository_workspace_id
                    || existing.source_identity != creation.source_identity
                    || existing.native_root_identity != creation.native_root_identity
                    || existing.source_epoch < creation.source_epoch)
            {
                return Err(invalid("Pi original root creation was substituted"));
            }
            if handles.contains_key(key) || self.roots.contains_key(key) {
                continue;
            }
            let expected = acyclic_fs::demand::SourceReference {
                identity: creation.source_identity,
                epoch: creation.source_epoch,
            };
            let physical = self
                .resources
                .shared_roots
                .acquire_with_native_work(
                    &root.path,
                    SharedRootAdmission::Resume(expected),
                    work,
                    || observer.verify_created_source(&read.0, key, &allocation.0.1, work),
                )
                .await?;
            if physical.source.inner().root_identity().to_bytes() != creation.native_root_identity {
                return Err(invalid("Pi native root identity changed after allocation"));
            }
            let volume = binding
                .project_volumes
                .get(key)
                .ok_or_else(|| invalid("Pi root has no original project volume"))?;
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            let name = volume.storage_name(&mut Some(&mut io))?;
            let workspace = self
                .resources
                .fs
                .open_workspace(&name, work, work.cancellation())
                .await?;
            if workspace.id().into_bytes() != creation.repository_workspace_id {
                return Err(invalid("Pi selected project workspace was substituted"));
            }
            let resumed = LazyWorkspace::open(
                workspace,
                Arc::clone(&physical.source),
                self.resources.store.clone(),
                &mut accounting,
            )
            .await?;
            let (key, handle) = pi_root_provider::PiRootHandle::from_observed_with_native_work(
                resumed,
                physical,
                work.display(key)?,
                work,
            )?;
            handles.insert(key, handle);
        }
        self.roots.extend(handles);
        for (key, handle) in &self.roots {
            let rebound = handle
                .workspace
                .rebind_source(&mut accounting)
                .await
                .map_err(|source| RootError::NativeSource {
                    source,
                    custody: None,
                })?;
            let original =
                allocation.0.1.roots.get_mut(key).ok_or_else(|| {
                    invalid("Pi retained root handle is not selected by allocation")
                })?;
            if rebound.source.identity != original.source_identity
                || rebound.source.epoch < original.source_epoch
            {
                return Err(invalid(
                    "Pi root observation does not descend from original creation",
                ));
            }
            original.source_epoch = rebound.source.epoch;
        }
        let PluginTaskBinding::Pi(binding) = &mut self.state.task_binding else {
            return Err(invalid("Pi root binding changed during allocation"));
        };
        binding.scratch.allocation = PiScratchAllocation::Created(allocation.0.1.scratch);
        for (key, creation) in allocation.0.1.roots {
            self.state
                .roots
                .get_mut(&key)
                .ok_or_else(|| invalid("Pi allocated root disappeared"))?
                .allocation = RootAllocation::Created(creation);
        }
        self.slots.save_with_native_work(
            &self.resources.private_state_root,
            &self.relative_directory,
            &self.state,
            Survives::PowerLoss,
            work,
        )?;
        let PluginTaskBinding::Pi(binding) = &self.state.task_binding else {
            return Err(invalid("Pi root binding changed during allocation"));
        };
        let (roots, custody) = pi_native::pi_native_vec(self.roots.len(), work)?;
        let mut context = (roots, custody);
        for (key, handle) in &self.roots {
            let root = self
                .state
                .roots
                .get(key)
                .ok_or_else(|| invalid("Pi created root disappeared"))?;
            drop(
                WorkspaceGraph::new(self.resources.store.clone())
                    .register_root_with_native_work(
                        handle.workspace.workspace(),
                        work,
                        work.cancellation(),
                    )
                    .await
                    .map_err(|(source, custody)| RootError::NativeLineage { source, custody })?,
            );
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            );
            let name = binding
                .project_volumes
                .get(key)
                .ok_or_else(|| invalid("Pi root lost its project volume"))?
                .storage_name(&mut Some(&mut io))?;
            let mut name_custody = io.into_custody()?;
            let mut path = work.copy_path(&root.path)?;
            context.1.absorb(&mut name_custody)?;
            context.1.absorb(&mut path.1)?;
            context.0.push(WorkspaceContextRoot {
                root_id: WorkspaceRootId::from_bytes(root.root_id),
                source_path: path.0,
                workspace_id: handle.workspace.workspace().id(),
                workspace_name: name,
                parent_workspace_id: None,
                mount_path: None,
            });
        }
        drop(
            WorkspaceContextRegistry::new(self.resources.store.clone())
                .register_root_with_native_work(
                    WorkspaceContextId::from_bytes(self.state.root_context_id),
                    context,
                    work,
                    work.cancellation(),
                )
                .await
                .map_err(|(source, custody)| RootError::NativeContext { source, custody })?,
        );
        self.state.pending_root_registration = false;
        drop(allocation.0.0);
        drop(allocation.1);
        self.slots.save_with_native_work(
            &self.resources.private_state_root,
            &self.relative_directory,
            &self.state,
            Survives::PowerLoss,
            work,
        )
    }
}
