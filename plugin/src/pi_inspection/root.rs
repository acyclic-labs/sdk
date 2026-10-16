//! Original Root metadata and the SDK publication authority that owns it.
use super::*;
use std::{path::Path, sync::Arc};

pub(crate) struct PiWritebackRoot {
    pub(crate) task: TaskId,
    pub(crate) volume: VolumeRef,
    pub(crate) workspace: acyclic_fs::WorkspaceId,
    pub(crate) identity: acyclic_fs::NativeRootIdentity,
    pub(crate) publication: acyclic_harness::filesystem::CheckoutPublication,
    provider: Arc<crate::pi_root_provider::PiRootProvider>,
    allocation: crate::pi_root_allocation::PiRootAllocation,
    key: String,
    write_guard: Arc<dyn acyclic_fs::native_mount::NativeMountCallbackGuard>,
    _custody: NativeAllocation,
}
impl PiWritebackRoot {
    pub(crate) async fn new(
        protected_resources_filesystem_stream: (
            Arc<crate::pi_owner::PiOwnerConfig>,
            &crate::ServiceResources,
            Arc<crate::pi_native_host::PiNativeFilesystem>,
            Arc<LocalStream>,
        ),
        context_owner: (Arc<TaskContext>, Arc<TaskJournalOwner<LocalStream>>),
        request: (crate::pi_root_inputs::PiRootRequest, NativeAllocation),
        key_original_path_operation_identity: (
            &str,
            LazySnapshotRef,
            &Path,
            acyclic_harness::OperationId,
            acyclic_fs::NativeRootIdentity,
        ),
        work: &NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        let (protected, resources, filesystem, stream) = protected_resources_filesystem_stream;
        let (context, owner) = context_owner;
        let (key, original, path, operation, identity) = key_original_path_operation_identity;
        let task = owner.task_id();
        if !path.is_absolute()
            || request.0.binding.task_id != protected.policy().root_task
            || task != request.0.binding.task_id
            || context.durable_task_id() != Some(task)
        {
            return Err(selected_unauthorized(
                "writeback retained another Root creator",
            ));
        }
        let volume = request
            .0
            .binding
            .project_volumes
            .get(key)
            .ok_or_else(|| selected_unauthorized("writeback lost its project"))?;
        let write_guard = context.volume_callback(&owner, volume, true, work)?;
        let mut io = HistoryWork::new(PreparationIo::from_native_work(work)?);
        let volume = volume.copy(&mut Some(&mut io))?;
        let preparation = crate::pi_root_provider::load_original_preparation_with_native_work(
            &protected,
            &filesystem,
            &request.0.binding,
            &request.0.root,
            context,
            work,
        )
        .await?;
        let provider = crate::pi_root_provider::root_provider(
            protected,
            resources,
            filesystem,
            stream,
            preparation,
            None,
            work.clone(),
        )?;
        drop(request);
        let attempt = acyclic_harness::EffectAttemptId::from_bytes(
            provider.effect_id_with_native_work(work)?.into_bytes(),
        );
        let ((reference, allocation), mut custody) = provider
            .observe_allocation_with_native_work(attempt, work)
            .await?
            .ok_or_else(|| selected_unauthorized("writeback has no created Root"))?;
        drop(reference);
        let created = allocation
            .roots
            .get(key)
            .ok_or_else(|| selected_unauthorized("writeback lost its created project"))?;
        if created.repository_workspace_id != original.workspace_id.into_bytes()
            || created.native_root_identity != identity.to_bytes()
        {
            return Err(selected_unauthorized(
                "writeback changed its creator workspace or identity",
            ));
        }
        let key = work.display(key)?;
        let publication = acyclic_harness::filesystem::CheckoutPublication::new(
            path,
            identity,
            acyclic_fs::OperationId::from_bytes(operation.into_bytes()),
            work,
        )?;
        let mut io_custody = io.into_custody()?;
        custody.absorb(&mut io_custody)?;
        for mut incoming in [
            key.1,
            work.claim(
                std::mem::size_of::<Self>() as u64,
                1,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?,
        ] {
            custody.absorb(&mut incoming)?;
        }
        Ok(Arc::new(Self {
            task,
            volume,
            workspace: original.workspace_id,
            identity,
            publication,
            provider,
            allocation,
            key: key.0,
            write_guard,
            _custody: custody,
        }))
    }
    pub(crate) async fn verify(&self, work: &NativeMaterializationWork) -> Result<()> {
        self.write_guard.check_write(work)?.await?;
        let identity = self
            .provider
            .verify_created_source(&self.key, &self.allocation, work)
            .await?;
        if identity != self.identity {
            return Err(selected_unauthorized(
                "writeback changed its original native Root",
            ));
        }
        self.write_guard.check_write(work)?.await?;
        Ok(())
    }
}
