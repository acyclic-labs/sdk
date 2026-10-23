use super::*;

pub(super) struct Configuration<'a> {
    pub files: Arc<LocalFilesystem>,
    #[cfg(feature = "native-execution")]
    pub workspace: Option<Arc<workspace::LocalWorkspace>>,
    pub stream: Arc<LocalStream>,
    pub task_volume: &'a VolumeRef,
    pub issuer: &'a AuthorityIssuer,
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
}

pub(super) async fn open(
    config: Configuration<'_>,
) -> Result<(
    Arc<LocalTaskRuntime>,
    Scope,
    Arc<crate::task_volume::TaskFiles<acyclic_stream::LocalStream>>,
)> {
    let stock = Arc::new(StockTurnMachine::new());
    let preparation = Arc::new(ForkPreparationMachine::new());
    let child = Arc::new(crate::filesystem::ChildAgentMachine::new());
    let definition = stock.definition()?;
    let child_definition = child.definition()?;
    let fork = preparation.definition::<crate::resource_preparation::ResourcePreparationRequest, crate::fork::ForkReport>()?;
    let mut grants = vec![
        "task:spawn:acyclic.direct_tool@1".to_owned(),
        "model:generate".to_owned(),
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
        format!(
            "task:spawn:{}@{}",
            child_definition.identity().name,
            child_definition.identity().version
        ),
        format!(
            "task:spawn:{}@{}",
            definition.identity().name,
            definition.identity().version
        ),
        format!(
            "task:spawn:{}@{}",
            fork.identity().name,
            fork.identity().version
        ),
    ];
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
    let signed = config.issuer.issue(
        "owner",
        &Capabilities::new(grants),
        Some(config.agent),
        None,
        &mut None,
    )?;
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
    let creation = Arc::new(FilesystemResourceCreationProvider::new(
        config.files.clone(),
        config.issuer,
        None,
    )?);
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    tasks.register(child_definition)?;
    tasks.register(fork)?;
    let mut machines = MachineRegistry::default();
    LocalHarness::register_commands(&mut tasks, &mut machines)?;
    machines.register(stock, &mut None)?;
    machines.register(child, &mut None)?;
    machines.register(preparation, &mut None)?;
    let mut runtime = LocalTaskRuntime::open(
        config.stream,
        config.files.clone(),
        config.journal,
        Some(config.scratch),
        config.verifier,
        &signed,
        &scope,
        &tasks,
        &machines,
        config.tools,
        config.session_limits,
        config.concurrency,
        config.limits.file_bytes,
        config.clock.clone(),
        None,
        &[creation],
        &crate::durable_tool::ResumableToolRegistry::default(),
        None,
    )
    .await?
    .with_authority_custodian(config.issuer)?
    .with_output_volume(config.project, None)?
    .with_task_files(task_files.clone())?;
    runtime.bind_task_files(Some(projection_files.clone()));
    #[cfg(feature = "native-execution")]
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
