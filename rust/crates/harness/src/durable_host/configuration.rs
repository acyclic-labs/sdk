//! Configuration.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    #[allow(clippy::too_many_arguments)]
    /// Binds the signed owner and task registries on the optional observer.
    pub fn new(
        coordinator: DistributedCoordinator<P>,
        stream: std::sync::Arc<P>,
        payloads: Arc<dyn SchedulerPayloadStore>,
        reader: Arc<dyn ContentResidencyVerifier>,
        owner: &Authority,
        owner_scope: Scope,
        verifier: AuthorityVerifier,
        root_scope: RuntimeScope,
        tasks: TaskRegistry,
        machines: MachineRegistry,
        clock: Arc<dyn UnixMillisClock>,
        mut work: Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        verifier.verify_audience(owner, &mut work)?;
        verifier.verify(&owner_scope, &mut work)?;
        for capability in [
            capability::OPERATION_DECLARE,
            capability::OPERATION_OBSERVE,
            capability::OPERATION_CANCEL,
        ] {
            if !owner_scope.capabilities().contains(capability) {
                return Err(admission_boundary(
                    &work,
                    crate::wire::ErrorCode::Unauthorized,
                    "owner scope lacks a required operation capability",
                ));
            }
        }
        if !root_scope.grants().is_subset_of(owner_scope.capabilities()) {
            return Err(admission_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "runtime grants exceed the signed owner scope",
            ));
        }
        if let Some(observer) = work.as_deref_mut() {
            observer.retain_owned(std::mem::size_of::<Self>() as u64)?;
            observer.retain_owned(
                std::mem::size_of::<Mutex<DistributedCoordinator<P>>>()
                    .saturating_sub(std::mem::size_of::<DistributedCoordinator<P>>())
                    as u64,
            )?;
            observer.codec_charge(crate::executor::json::JsonCodecCharge::AllocationOperation {
                bytes: std::mem::size_of::<Mutex<DistributedCoordinator<P>>>() as u64,
            })?;
        }
        Ok(Self {
            coordinator: Arc::new(Mutex::new(coordinator)),
            stream,
            payloads,
            reader,
            owner_scope,
            verifier,
            root_scope,
            tasks,
            machines,
            clock,
            policy: None,
            policy_binding: None,
            effects: None,
            session_limits: None,
            resource_creation: Vec::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_configuration_custody: None,
        })
    }

    /// Builds the same configured owner from guarded copies on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_native_work(
        coordinator: DistributedCoordinator<P>,
        stream: std::sync::Arc<P>,
        payloads: Arc<dyn SchedulerPayloadStore>,
        reader: Arc<dyn ContentResidencyVerifier>,
        owner: &Authority,
        owner_scope: &Scope,
        verifier: &AuthorityVerifier,
        root_scope: &RuntimeScope,
        tasks: &TaskRegistry,
        machines: &MachineRegistry,
        clock: Arc<dyn UnixMillisClock>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let scope_copy = owner_scope.copy(&mut Some(&mut io.history_io()))?;
        let verifier_copy = verifier.copy(&mut Some(&mut io.history_io()))?;
        let root_copy = root_scope.clone_with_native_work(work)?;
        io.absorb_native_custody(root_copy.1)?;
        let tasks = tasks.clone_with_native_work(work)?;
        let machines = machines.copy(work)?;
        io.absorb_native_custody(machines.1)?;
        let mut host = Self::new(
            coordinator,
            stream,
            payloads,
            reader,
            owner,
            scope_copy,
            verifier_copy,
            root_copy.0,
            tasks,
            machines.0,
            clock,
            Some(&mut io.history_io()),
        )?;
        let retained = io.owned_bytes()?;
        host.native_configuration_custody = Some(io.into_native_custody(retained)?);
        Ok(host)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Shares the configuration on its original native ledger.
    pub fn into_native_shared(
        self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self
            .native_configuration_custody
            .as_ref()
            .is_some_and(|custody| custody.work().shares_ledger(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "host configuration differs from its original work",
            });
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        Ok(Arc::new(self))
    }

    /// Current trusted wall clock shared by admission, workers and effect gates.
    #[must_use]
    pub fn now_unix_millis(&self) -> u64 {
        self.clock.now_unix_millis()
    }

    pub(super) fn check_configuration_work(&self, work: crate::NativeWork<'_>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work
            && !self
                .native_configuration_custody
                .as_ref()
                .is_some_and(|custody| custody.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "configuration differs from original host work",
            });
        }
        let _ = work;
        Ok(())
    }

    /// Registers the explicitly trusted creation verifiers.
    pub fn with_resource_creation_providers(
        mut self,
        providers: &[Arc<dyn crate::resource_preparation::ResourceCreationProvider>],
        work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        self.check_configuration_work(work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut io = work
            .map(|work| {
                crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut observer = io
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = None;
        if !self.resource_creation.is_empty() {
            return Err(admission_boundary(
                &observer,
                crate::wire::ErrorCode::Conflict,
                "resource creation registry is already configured",
            ));
        }
        for (index, provider) in providers.iter().enumerate() {
            provider.provider().validate(&mut observer)?;
            if providers[..index]
                .iter()
                .any(|bound| bound.provider() == provider.provider())
            {
                return Err(admission_boundary(
                    &observer,
                    crate::wire::ErrorCode::Invalid,
                    "resource creation provider is registered twice",
                ));
            }
        }
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
        self.resource_creation = crate::core::copy_vector(providers, &mut quota, |provider, _| {
            Ok(Arc::clone(provider))
        })?;
        let retained = quota.retained();
        quota.transfer();
        drop(quota);
        drop(observer);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io.as_mut() {
            let mut custody = crate::store::history::HistoryIo::take_native_custody(io, retained)?;
            self.native_configuration_custody
                .as_mut()
                .expect("checked original host custody")
                .absorb(&mut custody)?;
        }
        let _ = retained;
        Ok(self)
    }

    /// Requires these immutable shared ceilings for every root admitted by this host.
    pub fn with_session_limits(
        mut self,
        limits: crate::scheduler::SessionLimits,
        work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        self.check_configuration_work(work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut io = work
            .map(|work| {
                crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut observer = io
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = None;
        limits.validate(&mut observer)?;
        self.session_limits = Some(limits);
        Ok(self)
    }

    /// Pins the policy enforced by this owner, independently of caller contexts.
    pub fn with_policy(
        mut self,
        policy: Arc<dyn ToolPolicy>,
        work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        self.check_configuration_work(work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            policy.supports_native_policy(work)?;
            let mut identity = policy.identity_with_native_work(work)?;
            crate::runtime::validate_policy_identity_with_native_work(
                &identity.0,
                &identity.1,
                work,
            )?;
            self.native_configuration_custody
                .as_mut()
                .expect("checked original host custody")
                .absorb(&mut identity.1)?;
            self.policy_binding = Some(identity.0);
            self.policy = Some(policy);
            return Ok(self);
        }
        let identity = policy.identity();
        crate::runtime::validate_policy_identity(&identity, None)?;
        self.policy_binding = Some(identity);
        self.policy = Some(policy);
        Ok(self)
    }

    /// Binds the conversation-owned provider effect reconciler.
    #[must_use]
    pub fn with_effects(mut self, effects: Arc<dyn DurableEffectObserver>) -> Self {
        self.effects = Some(effects);
        self
    }
}
