//! Configuration for the filesystem task runtime.

use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Opens the configured runtime using the optional original preparation observer.
    #[allow(
        clippy::too_many_arguments,
        reason = "explicit provider, policy, clock and authority boundaries"
    )]
    pub async fn open(
        stream: Arc<P>,
        filesystem: Arc<FilesystemHost<A, O>>,
        volume: &VolumeRef,
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
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io
                .original_native_work()
                .ok_or_else(|| io.invalid("native runtime configuration has no original work"))?;
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            volume.validate(&mut Some(&mut crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            )))?;
            if maximum_payload_bytes == 0 {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "stock runtime requires a positive payload limit",
                });
            }
            if let Some(policy) = &policy {
                policy.supports_native_policy(work)?;
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
                &mut Some(&mut crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(work)?,
                )),
            )
            .await?;
            let mut host = CoordinatorTaskHost::new_with_native_work(
                coordinator,
                stream.clone(),
                payloads,
                reader.clone(),
                verifier.audience(),
                signed,
                verifier,
                scope,
                tasks,
                machines,
                clock,
                work,
            )?
            .with_session_limits(session_limits, Some(work))?
            .with_resource_creation_providers(creation_providers, Some(work))?;
            if let Some(policy) = &policy {
                host = host.with_policy(policy.clone(), Some(work))?;
            }
            let host = host.into_native_shared(work)?;
            let harness_tasks = tasks.clone_with_native_work(work)?;
            let harness_tools = tools.clone_with_native_work(work)?;
            io.absorb_native_custody(harness_tools.1)?;
            let harness_resumable_tools = resumable_tools.clone_with_native_work(work)?;
            io.absorb_native_custody(harness_resumable_tools.1)?;
            let harness_scope = scope.clone_with_native_work(work)?;
            io.absorb_native_custody(harness_scope.1)?;
            let harness = crate::runtime::Bindings {
                tasks: harness_tasks,
                tools: harness_tools.0,
                resumable_tools: harness_resumable_tools.0,
                scope: harness_scope.0,
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
            &(verifier.audience()),
            signed.clone(),
            verifier.clone(),
            scope.clone(),
            tasks.clone(),
            machines,
            clock,
            None,
        )?
        .with_session_limits(session_limits, None)?
        .with_resource_creation_providers(creation_providers, None)?;
        if let Some(policy) = &policy {
            host = host.with_policy(policy.clone(), None)?;
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
            verifier,
            signed,
            tools,
            maximum_payload_bytes,
            policy,
            None,
        )
    }

    /// Installs the same configured recursive source used by registered default tools.
    #[must_use]
    pub fn with_recursive_source(
        mut self,
        source: Arc<crate::filesystem::ConfiguredRecursiveToolSource<P, A, O>>,
    ) -> Self {
        self.recursive_source = Some(source);
        self
    }

    pub(super) fn recursive_source(
        &self,
    ) -> Option<&Arc<crate::filesystem::ConfiguredRecursiveToolSource<P, A, O>>> {
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
        verifier: AuthorityVerifier,
        signed: Scope,
        tools: ToolRegistry,
        maximum_payload_bytes: u64,
        policy: Option<Arc<dyn ToolPolicy>>,
        io: Option<&crate::filesystem::PreparationIo>,
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
            volume.validate(&mut Some(&mut crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            )))?;
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
        let native_configuration_custody = io
            .map(|io| io.take_native_custody(io.owned_bytes()?))
            .transpose()?;
        Ok(Self {
            stream,
            host,
            harness,
            filesystem,
            volume,
            verifier,
            signed,
            tools,
            maximum_payload_bytes,
            policy,
            interaction_owner: None,
            recursive_source: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_configuration_custody,
        })
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn original_lease_fence_with_io(
        lease: &crate::distributed::WorkLease,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<LeaseFence> {
        io.retain_owned(std::mem::size_of::<LeaseFence>() as u64)?;
        observed::lease_fence(lease, Some(io))
    }

    /// Binds the existing conversation owner for task approval/question tickets.
    pub fn with_interaction_owner(
        mut self,
        verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        scope: Scope,
        private_volume: VolumeRef,
    ) -> Result<Self> {
        verifier.verify(&scope, &mut None)?;
        if !scope
            .capabilities()
            .contains(crate::contract::capability::INTERACTION_OPEN)
        {
            return Err(Error::Unauthorized(
                "interaction owner lacks interaction:open".into(),
            ));
        }
        self.interaction_owner = Some(TaskInteractionOwner {
            verifier,
            schemas,
            scope,
            volume: private_volume,
        });
        self.interaction_host(None)?;
        Ok(self)
    }

    pub(super) fn interaction_host(
        &self,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<FilesystemInteractionHost<P, A, O>> {
        let owner = self.interaction_owner.as_ref().ok_or_else(|| {
            observed::boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "task conversation owner is not bound",
            )
        })?;
        FilesystemInteractionHost::new(
            self.stream.clone(),
            Arc::clone(&self.filesystem),
            owner.verifier.audience(),
            &owner.verifier,
            &owner.schemas,
            &owner.scope,
            &owner.volume,
            self.maximum_payload_bytes,
            io,
        )
    }
}
