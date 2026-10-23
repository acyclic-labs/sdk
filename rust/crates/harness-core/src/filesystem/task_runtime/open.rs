use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Opens the configured runtime using the optional original preparation observer.
    ///
    /// `private` is the signed agent's private volume for root compaction
    /// summaries. Every caller chooses it here; `None` suits runtimes whose
    /// tasks publish only into created private content.
    #[allow(
        clippy::too_many_arguments,
        reason = "explicit provider, policy, clock and authority boundaries"
    )]
    pub async fn open(
        stream: Arc<P>,
        filesystem: Arc<FilesystemHost<A, O>>,
        volume: &VolumeRef,
        private: Option<&VolumeRef>,
        verifier: &AuthorityVerifier,
        signed: &Scope,
        scope: &RuntimeScope,
        tasks: &TaskRegistry,
        machines: &MachineRegistry,
        tools: &ToolRegistry,
        session_limits: SessionLimits,
        concurrency: usize,
        maximum_payload_bytes: u64,
        clock: Arc<dyn UnixMillisClock>,
        policy: Option<Arc<dyn ToolPolicy>>,
        creation_providers: &[Arc<dyn crate::resource_preparation::ResourceCreationProvider>],
        resumable_tools: &ResumableToolRegistry,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<Self> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                let work = io.original_native_work().ok_or_else(|| {
                    io.invalid("native runtime configuration has no original work")
                })?;
                let io = super::super::PreparationIo::from_native_work(work)?;
                volume.validate(&mut Some(
                    &mut crate::filesystem::PreparationIo::from_native_work(work)
                        .map(crate::store::HistoryWork::new)?,
                ))?;
                if maximum_payload_bytes == 0 {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "stock runtime requires a positive payload limit",
                    });
                }
                if let Some(policy) = &policy {
                    policy.identity_with_native_work(work)?;
                }
                let payloads = FilesystemContentPublisher::new(
                    filesystem.clone(),
                    volume,
                    verifier,
                    signed,
                    maximum_payload_bytes,
                    Some(&io),
                )?;
                let reader = FilesystemContentVerifier::new(
                    filesystem.clone(),
                    verifier,
                    signed,
                    maximum_payload_bytes,
                    Some(&io),
                )?;
                let coordinator = DistributedCoordinator::open(
                    &stream,
                    reader.clone(),
                    Some(payloads.clone()),
                    &mut Some(
                        &mut crate::filesystem::PreparationIo::from_native_work(work)
                            .map(crate::store::HistoryWork::new)?,
                    ),
                )
                .await?;
                let copied_scope = scope.clone_with_native_work(work)?;
                io.absorb_native_custody(copied_scope.1)?;
                let copied_machines = machines.copy(work)?;
                io.absorb_native_custody(copied_machines.1)?;
                io.retain_owned(
                    (std::mem::size_of::<CoordinatorTaskHost<P>>()
                        + 2 * std::mem::size_of::<usize>()) as u64,
                )?;
                let mut host = CoordinatorTaskHost::new(
                    coordinator,
                    stream.clone(),
                    payloads,
                    reader.clone(),
                    io.history_io().clone_authority(verifier.audience())?,
                    signed.copy(&mut Some(&mut io.history_io()))?,
                    verifier.copy(&mut Some(&mut io.history_io()))?,
                    copied_scope.0,
                    tasks.clone_with_native_work(work)?,
                    copied_machines.0,
                    clock,
                )?
                .with_session_limits(session_limits)?
                .with_resource_creation_providers(creation_providers, Some(&io))?;
                if let Some(policy) = &policy {
                    host = host.with_policy(policy.clone())?;
                }
                let host = Arc::new(host);
                let copied_tools = tools.clone_with_native_work(work)?;
                io.absorb_native_custody(copied_tools.1)?;
                let copied_resumable = resumable_tools.clone_with_native_work(work)?;
                io.absorb_native_custody(copied_resumable.1)?;
                let copied_scope = scope.clone_with_native_work(work)?;
                io.absorb_native_custody(copied_scope.1)?;
                let harness = crate::runtime::Bindings {
                    tasks: tasks.clone_with_native_work(work)?,
                    tools: copied_tools.0,
                    resumable_tools: copied_resumable.0,
                    scope: copied_scope.0,
                    concurrency,
                    durable_host: Some(host.clone()),
                    content: Some(ContentBindings {
                        reader,
                        writer: None,
                    }),
                    policy: policy.clone(),
                    ..crate::runtime::Bindings::local()
                }
                .build(Some(&io))?;
                io.retain_owned(std::mem::size_of::<Self>() as u64)?;
                let volume = volume.copy(&mut Some(&mut io.history_io()))?;
                let verifier = verifier.copy(&mut Some(&mut io.history_io()))?;
                let signed = signed.copy(&mut Some(&mut io.history_io()))?;
                let tools = tools.clone_with_native_work(work)?;
                io.absorb_native_custody(tools.1)?;
                return Self::from_host(
                    stream,
                    harness,
                    host,
                    filesystem,
                    volume,
                    private,
                    verifier,
                    signed,
                    tools.0,
                    maximum_payload_bytes,
                    policy,
                    Some(&io),
                );
            }
            #[cfg(target_arch = "wasm32")]
            if io.is_some() {
                return Err(observed::boundary(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "native runtime configuration is unavailable on wasm",
                ));
            }
            let (volume, verifier, signed, scope, tasks, machines, tools) = (
                volume.clone(),
                verifier.clone(),
                signed.clone(),
                scope.clone(),
                tasks.clone(),
                machines.clone(),
                tools.clone(),
            );
            let resumable_tools = resumable_tools.clone();
            let payloads = FilesystemContentPublisher::new(
                filesystem.clone(),
                &volume,
                &verifier,
                &signed,
                maximum_payload_bytes,
                None,
            )?;
            let reader = FilesystemContentVerifier::new(
                filesystem.clone(),
                &verifier,
                &signed,
                maximum_payload_bytes,
                None,
            )?;
            let mut host = CoordinatorTaskHost::new(
                DistributedCoordinator::open(
                    &stream,
                    reader.clone(),
                    Some(payloads.clone()),
                    &mut None,
                )
                .await?,
                stream.clone(),
                payloads,
                reader.clone(),
                verifier.audience().clone(),
                signed.clone(),
                verifier.clone(),
                scope.clone(),
                tasks.clone(),
                machines,
                clock,
            )?
            .with_session_limits(session_limits)?
            .with_resource_creation_providers(creation_providers, None)?;
            if let Some(policy) = &policy {
                host = host.with_policy(policy.clone())?;
            }
            let host = Arc::new(host);
            let harness = crate::runtime::Bindings {
                tasks,
                tools: tools.clone(),
                resumable_tools,
                scope,
                concurrency,
                durable_host: Some(host.clone()),
                content: Some(ContentBindings {
                    reader,
                    writer: None,
                }),
                policy: policy.clone(),
                ..crate::runtime::Bindings::local()
            }
            .build(None)?;
            Self::from_host(
                stream,
                harness,
                host,
                filesystem,
                volume,
                private,
                verifier,
                signed,
                tools,
                maximum_payload_bytes,
                policy,
                None,
            )
        })?
        .await
    }

    /// Installs the same configured recursive source used by registered default tools.
    #[must_use]
    pub fn with_recursive_source(
        mut self,
        source: Arc<super::super::ConfiguredRecursiveToolSource<P, A, O>>,
    ) -> Self {
        self.recursive_source = Some(source);
        self
    }

    pub(in crate::filesystem) fn recursive_source(
        &self,
    ) -> Option<&Arc<super::super::ConfiguredRecursiveToolSource<P, A, O>>> {
        self.recursive_source.as_ref()
    }

    /// Attaches stock commands to the configured host and harness.
    #[allow(
        clippy::too_many_arguments,
        reason = "the original configured stock initializer and its optional observer"
    )]
    pub fn from_host(
        stream: Arc<P>,
        harness: Arc<AgentHarness>,
        host: Arc<CoordinatorTaskHost<P>>,
        filesystem: Arc<FilesystemHost<A, O>>,
        volume: VolumeRef,
        private: Option<&VolumeRef>,
        verifier: AuthorityVerifier,
        signed: Scope,
        tools: ToolRegistry,
        maximum_payload_bytes: u64,
        policy: Option<Arc<dyn ToolPolicy>>,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<Self> {
        if !harness.original_host_binding_matches(host.as_ref(), policy.as_deref()) {
            return Err(match io {
                Some(_) => observed::boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "stock runtime differs from the original host or policy binding",
                ),
                None => Error::Invalid(
                    "stock runtime differs from the harness's original host or policy binding"
                        .into(),
                ),
            });
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native runtime configuration has no original work",
            })?;
            volume.validate(&mut Some(
                &mut crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)?,
            ))?;
        } else {
            volume.validate(&mut None)?;
        }
        #[cfg(target_arch = "wasm32")]
        volume.validate(&mut None)?;
        if maximum_payload_bytes == 0 {
            return Err(match io {
                Some(_) => observed::boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "stock runtime requires a positive payload limit",
                ),
                None => Error::Invalid("stock runtime requires a positive payload limit".into()),
            });
        }
        #[cfg(not(target_arch = "wasm32"))]
        let _native_configuration_custody = io
            .map(|io| io.take_native_custody(io.owned_bytes()?))
            .transpose()?;
        let runtime = Self {
            stream,
            host,
            harness,
            filesystem,
            volume,
            verifier,
            signed,
            authority_custodian: None,
            tools,
            maximum_payload_bytes,
            policy,
            interaction_owner: None,
            recursive_source: None,
            task_files: None,
            #[cfg(not(target_arch = "wasm32"))]
            _native_configuration_custody,
        };
        match private {
            Some(private) => runtime.bind_private_volume(private, io),
            None => Ok(runtime),
        }
    }
}
