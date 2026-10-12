use super::*;

pub(super) struct MeasuredRootPolicy {
    pub(super) filesystem: Arc<MemoryFilesystem>,
    pub(super) root: TaskId,
    pub(super) bootstrap: ProjectSourceSnapshot,
}
impl RootProjectSourceProvider for MeasuredRootPolicy {
    fn verify<'a>(
        &'a self,
        context: RootProjectSourceContext<'a>,
        snapshot: &'a ProjectSourceSnapshot,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            if context.graph_root != self.root
                || context.maximum_work == 0
                || context.maximum_bytes == 0
                || context.cancellation.is_cancelled()
                || snapshot.native_snapshot.is_some()
                || (!context.created_project_authenticated
                    && (context.source.operation_id.into_bytes() != self.root.into_bytes()
                        || snapshot != &self.bootstrap))
            {
                return Err(Error::Unauthorized(
                    "explicit source policy or genuine bootstrap differs".into(),
                ));
            }
            let workspace = workspace_ref(
                snapshot.project.provider().clone(),
                &snapshot.project.storage_name(&mut None)?,
            )?;
            let actual = self.filesystem.resolve(&workspace).await?;
            if actual.generation != snapshot.generation {
                return Err(Error::Unauthorized(
                    "selected filesystem generation is not authentic".into(),
                ));
            }
            Ok(())
        })
    }
}

pub(in crate::durable_host::tests::created_resource_access) async fn runtime(
    fixture: &CreationFixture,
) -> Result<FilesystemTaskRuntime<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>> {
    let volume = VolumeRef::new(
        fixture.filesystem_provider.clone(),
        "scheduler".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
        &mut None,
    )?;
    FilesystemTaskRuntime::open(
        fixture.host.stream.clone(),
        fixture.filesystem.clone(),
        &volume,
        &fixture.host.verifier,
        &fixture.host.owner_scope,
        &fixture.root_scope,
        &fixture.host.tasks,
        &fixture.host.machines,
        &crate::tool::ToolRegistry::default(),
        crate::scheduler::SessionLimits {
            active_tasks: 16,
            total_tasks: 32,
            depth: 4,
            model_steps: 4,
        },
        16,
        65_536,
        Arc::new(SystemUnixMillisClock),
        None,
        &fixture.host.resource_creation,
        &crate::durable_tool::ResumableToolRegistry::default(),
        None,
    )
    .await
}
