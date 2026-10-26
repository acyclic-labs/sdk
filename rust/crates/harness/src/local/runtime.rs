use super::*;

pub(super) struct Configuration<'a> {
    pub passive: bool,
    pub files: Arc<LocalFilesystem>,
    #[cfg(any(test, feature = "native-execution"))]
    pub workspace: Option<Arc<workspace::LocalWorkspace>>,
    pub stream: Arc<LocalStream>,
    pub task_volume: &'a VolumeRef,
    pub issuer: &'a AuthorityRoot,
    pub approval_issuer: &'a AuthorityRoot,
    pub verifier: &'a AuthorityVerifier,
    pub agent: AgentId,
    pub project: &'a VolumeRef,
    pub scratch: &'a VolumeRef,
    pub journal: &'a VolumeRef,
    pub tools: &'a ToolRegistry,
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    pub capabilities: &'a Capabilities,
    pub limits: Limits,
    pub run_limits: TaskRunLimits,
    pub session_limits: SessionLimits,
    pub concurrency: usize,
    pub clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    pub approval_policy: &'a crate::approvals::ApprovalPolicy,
}

pub(super) async fn open(
    code: &crate::package::harness::HarnessCode,
    config: Configuration<'_>,
) -> Result<(
    Arc<LocalTaskRuntime>,
    Scope,
    Arc<crate::task_volume::TaskFiles<acyclic_stream::LocalStream>>,
)> {
    let task_grants = code
        .actors()
        .map(|identity| format!("task:spawn:{}@{}", identity.name, identity.version));
    let (scope, signed) = scopes(&config, task_grants)?;
    let projection_files = Arc::new(
        crate::task_volume::TaskFiles::open(
            config.stream.clone(),
            config.task_volume,
            config.verifier,
            &signed,
            config.limits.file_bytes,
        )
        .await?,
    );
    let task_files = Arc::new(projection_files.reopen_erased().await?);
    let approval_verifier = config.approval_issuer.verifier(&mut None)?;
    super::approvals::bind_owner(&config.stream, config.approval_issuer, config.agent).await?;
    let approval_scope = config.approval_issuer.bootstrap_scope(
        "approval-owner",
        &Capabilities::new([
            "interaction:open".to_owned(),
            config
                .journal
                .capability(VolumeOperation::Read, &mut None)?,
            config
                .journal
                .capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(config.agent),
        &mut None,
    )?;
    let creation = Arc::new(FilesystemResourceCreationProvider::new(
        config.files.clone(),
        config.issuer,
        None,
    )?);
    let machines = code.registry()?;
    let mut runtime = LocalTaskRuntime::open(
        config.stream,
        config.files.clone(),
        config.journal,
        Some(config.scratch),
        config.verifier,
        &signed,
        &scope,
        code.tasks(),
        &machines,
        config.tools,
        config.session_limits,
        config.concurrency,
        config.limits.file_bytes,
        config.clock.clone(),
        Some(Arc::new(config.approval_policy.bind()?)),
        &[creation],
        &crate::durable_tool::ResumableToolRegistry::default(),
        None,
    )
    .await?
    .with_authority_custodian(config.issuer)?
    .with_output_volume(config.project, None)?
    .with_task_files(task_files.clone())?
    .with_interaction_owner(
        approval_verifier,
        crate::core::SchemaRegistry::new(),
        approval_scope,
        config.journal.clone(),
    )?
    .with_approval_authority(config.approval_issuer)?;
    runtime.bind_task_files(Some(projection_files.clone()));
    #[cfg(any(test, feature = "native-execution"))]
    let runtime = match config.workspace {
        Some(workspace) => {
            let tools = Arc::new(
                checkout_tools::CheckoutTools::new(
                    workspace,
                    config.project,
                    config.clock,
                    &signed,
                    Arc::clone(&projection_files),
                )
                .await?,
            );
            let publisher = checkout_tools::publisher::CheckoutPublisher::new(
                tools.clone(),
                config.files,
                config.project,
                config.verifier,
                config.limits.file_bytes,
            );
            runtime
                .with_content_publisher(Arc::new(publisher))?
                .with_workspace_tools(tools)?
        }
        None => runtime,
    };
    Ok((Arc::new(runtime), signed, projection_files))
}

fn scopes(
    config: &Configuration<'_>,
    task_grants: impl IntoIterator<Item = String>,
) -> Result<(RuntimeScope, Scope)> {
    let mut grants = vec![
        "model:generate".to_owned(),
        crate::contract::capability::INTERACTION_ROUTE.to_owned(),
        "interaction:open".to_owned(),
        "operation:declare".to_owned(),
        "conversation:bind".to_owned(),
        "conversation:append".to_owned(),
        "conversation:select_context".to_owned(),
        "operation:observe".to_owned(),
        "operation:cancel".to_owned(),
        "operation:wake".to_owned(),
        "mail:read".to_owned(),
        "mail:send".to_owned(),
        "fork:publish".to_owned(),
        "project:merge".to_owned(),
    ];
    grants.extend(task_grants);
    if config.passive {
        grants.retain(|grant| {
            matches!(
                grant.as_str(),
                "fork:publish"
                    | "project:merge"
                    | "operation:declare"
                    | "operation:observe"
                    | "operation:cancel"
                    | "operation:wake"
            ) || grant.starts_with("task:spawn:")
        });
    }
    for volume in [config.project, config.scratch] {
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            grants.push(volume.capability(operation, &mut None)?);
        }
        grants.push(volume.directory_read_capability("", &mut None)?);
    }
    for definition in config.tools.definitions(None)? {
        grants.push(format!("tool:call:{}", definition.name));
    }
    #[cfg(any(feature = "mcp-stdio", feature = "mcp-http"))]
    grants.extend(
        config
            .capabilities
            .iter()
            // Registry definitions above already supply every tool-call grant.
            .filter(|grant| !grant.starts_with("tool:call:"))
            .map(str::to_owned),
    );
    grants.push(
        config
            .journal
            .directory_read_capability("inputs", &mut None)?,
    );
    let scope = RuntimeScope::new(
        Capabilities::new(grants.iter().map(String::as_str)),
        config.limits,
    )?
    .with_run_limits(config.run_limits, &mut None)?;
    for operation in [VolumeOperation::Read, VolumeOperation::Write] {
        grants.push(config.journal.capability(operation, &mut None)?);
        grants.push(config.task_volume.capability(operation, &mut None)?);
    }
    let signed = config.issuer.bootstrap_scope(
        "owner",
        &Capabilities::new(grants),
        Some(config.agent),
        &mut None,
    )?;
    Ok((scope, signed))
}
