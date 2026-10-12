use super::*;

/// Task-local execution bounds pinned separately from content/render limits.

#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRunLimits {
    /// Maximum concurrent tasks in a child group, when bounded.
    pub concurrency: Option<usize>,
    /// Maximum model steps for this scoped task, when bounded.
    pub max_steps: Option<usize>,
    /// Absolute Unix deadline in milliseconds, when bounded.
    pub deadline_epoch_ms: Option<u64>,
}

impl TaskRunLimits {
    /// Rejects zero or nonportable runtime bounds.
    pub fn validate(&self, work: &mut Option<&mut dyn crate::store::HistoryIo>) -> Result<()> {
        if let Some(io) = work.as_deref() {
            io.check_cancellation()?;
        }
        if [
            self.concurrency.map(|value| value as u64),
            self.max_steps.map(|value| value as u64),
            self.deadline_epoch_ms,
        ]
        .into_iter()
        .flatten()
        .any(|value| value == 0 || value > crate::conversation::MAX_EXACT_JS_INTEGER)
        {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "task run limits are invalid",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_observed(&self, io: StartupIo<'_>) -> Result<()> {
        native_startup::item(io, 0)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            let mut observer = io.map(crate::filesystem::PreparationIo::history_io);
            self.validate(
                &mut observer
                    .as_mut()
                    .map(|io| io as &mut dyn crate::store::HistoryIo),
            )
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        self.validate(&mut None)
    }

    pub(super) fn check_deadline(&self) -> Result<()> {
        let Some(deadline) = self.deadline_epoch_ms else {
            return Ok(());
        };
        let now = acyclic_stream::UnixMillisClock::now_unix_millis(
            &acyclic_stream::SystemUnixMillisClock,
        );
        if deadline <= now {
            return Err(Error::Invalid("task deadline has expired".into()));
        }
        Ok(())
    }

    pub(super) fn allows(&self, child: &Self) -> bool {
        [
            (
                self.concurrency.map(|value| value as u64),
                child.concurrency.map(|value| value as u64),
            ),
            (
                self.max_steps.map(|value| value as u64),
                child.max_steps.map(|value| value as u64),
            ),
            (self.deadline_epoch_ms, child.deadline_epoch_ms),
        ]
        .into_iter()
        .all(|(ancestor, descendant)| {
            ancestor.is_none_or(|bound| descendant.is_some_and(|value| value <= bound))
        })
    }
}

/// Effective runtime authority and bounds; child scopes may only narrow them.

#[derive(Default)]
pub struct RuntimeScope {
    pub(super) grants: Capabilities,
    pub(super) limits: Limits,
    pub(super) run_limits: TaskRunLimits,
    pub(super) extensions: Option<ExtensionAdmission>,
    pub(super) extensions_sealed: bool,
    pub(super) extension_runtime: Option<Arc<ExtensionRuntime>>,
    pub(super) extension_schemas: Option<SchemaRegistry>,
    pub(super) resource_proofs: Vec<crate::resource_preparation::ResourceCreationProofRef>,
    pub(super) created_access: Option<Arc<crate::resource_preparation::CreatedResourceAccess>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) _native_grants_custody: Option<acyclic_fs::NativeAllocation>,
}

impl Clone for RuntimeScope {
    fn clone(&self) -> Self {
        Self {
            grants: self.grants.clone(),
            limits: self.limits,
            run_limits: self.run_limits,
            extensions: self.extensions.clone(),
            extensions_sealed: self.extensions_sealed,
            extension_runtime: self.extension_runtime.clone(),
            extension_schemas: self.extension_schemas.clone(),
            resource_proofs: self.resource_proofs.clone(),
            created_access: self.created_access.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_grants_custody: None,
        }
    }
}

impl RuntimeScope {
    /// Creates a checked root scope.
    pub fn new(grants: Capabilities, limits: Limits) -> Result<Self> {
        limits.validate(&mut None)?;
        Ok(Self {
            grants,
            limits,
            run_limits: TaskRunLimits::default(),
            extensions: None,
            extensions_sealed: false,
            extension_runtime: None,
            extension_schemas: None,
            resource_proofs: Vec::new(),
            created_access: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_grants_custody: None,
        })
    }

    /// Moves already-paid grants into a checked native root, without a copy.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        grants: (Capabilities, acyclic_fs::NativeAllocation),
        limits: Limits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        (&limits).validate(&mut Some(&mut crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        )))?;
        let custody = work
            .claim(
                std::mem::size_of::<Self>() as u64,
                0,
                acyclic_fs::WorkCounters::default(),
            )
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        let scope = Self {
            grants: grants.0,
            limits,
            run_limits: TaskRunLimits::default(),
            extensions: None,
            extensions_sealed: false,
            extension_runtime: None,
            extension_schemas: None,
            resource_proofs: Vec::new(),
            created_access: None,
            _native_grants_custody: Some(grants.1),
        };
        Ok((scope, custody))
    }

    /// Attenuates every grant and numeric bound.
    pub fn narrow(&self, grants: Capabilities, limits: Limits) -> Result<Self> {
        limits.validate(&mut None)?;
        if !self.allows_authority(&grants, &limits) {
            return Err(Error::Unauthorized(
                "runtime scope cannot widen ancestor authority".into(),
            ));
        }
        Ok(Self {
            grants,
            limits,
            run_limits: self.run_limits,
            extensions: self.extensions.clone(),
            extensions_sealed: true,
            extension_runtime: self.extension_runtime.clone(),
            extension_schemas: self.extension_schemas.clone(),
            resource_proofs: self.resource_proofs.clone(),
            created_access: self.created_access.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_grants_custody: None,
        })
    }

    pub(super) fn allows_authority(&self, grants: &Capabilities, limits: &Limits) -> bool {
        grants.is_subset_of(&self.grants)
            && limits.file_bytes <= self.limits.file_bytes
            && limits.path_bytes <= self.limits.path_bytes
            && limits.attachments <= self.limits.attachments
            && limits.render_bytes <= self.limits.render_bytes
            && limits.model_steps <= self.limits.model_steps
            && limits.model_events_per_step <= self.limits.model_events_per_step
            && limits.tool_calls_per_step <= self.limits.tool_calls_per_step
            && limits.context_messages <= self.limits.context_messages
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn validate_narrowing_with_native_work(
        &self,
        grants: &Capabilities,
        limits: &Limits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        (limits).validate(&mut Some(&mut crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        )))?;
        if !self.allows_authority(grants, limits) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "runtime scope cannot widen ancestor authority",
            });
        }
        Ok(())
    }

    /// Copies existing grants and concrete metadata on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let grants = self.grants.clone_with_native_work(work)?;
        self.clone_native_metadata(grants, self.limits, self.extensions_sealed, work)
    }

    /// Attenuates paid grants using the ordinary checks and native metadata copy.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn narrow_with_native_work(
        &self,
        grants: (Capabilities, acyclic_fs::NativeAllocation),
        limits: Limits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        self.validate_narrowing_with_native_work(&grants.0, &limits, work)?;
        self.clone_native_metadata(grants, limits, true, work)
    }

    /// Extends the caller's existing quota with the same physical proof copy.
    pub(crate) fn copy_resource_proofs(
        proofs: &[crate::resource_preparation::ResourceCreationProofRef],
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Vec<crate::resource_preparation::ResourceCreationProofRef>> {
        if quota.work().is_none() {
            return Ok(proofs.to_vec());
        }
        crate::core::copy_vector(proofs, quota, |proof, quota| {
            quota
                .work()
                .as_deref_mut()
                .expect("proof vector copy observer")
                .codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            let (copied, retained) =
                crate::core::resource_creation_proof_clone_with_custody(proof, quota.work())?;
            quota.adopt(retained)?;
            Ok(copied)
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn clone_native_metadata(
        &self,
        grants: (Capabilities, acyclic_fs::NativeAllocation),
        limits: Limits,
        sealed: bool,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        use crate::store::history::HistoryIo as _;
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let extension_schemas = match &self.extension_schemas {
            Some(schemas) => Some(schemas.copy(&mut Some(&mut io))?),
            None => None,
        };
        let extensions = match &self.extensions {
            Some(extensions) => Some(extensions.copy(&mut Some(&mut io))?),
            None => None,
        };
        let resource_proofs = {
            let mut observer = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
            let mut quota = crate::wire_codec::CodecQuota::from_retained(&mut observer, 0);
            let copied = Self::copy_resource_proofs(&self.resource_proofs, &mut quota)?;
            quota.transfer();
            copied
        };
        let scope = Self {
            grants: grants.0,
            limits,
            run_limits: self.run_limits,
            extensions,
            extensions_sealed: sealed,
            extension_runtime: self.extension_runtime.clone(),
            extension_schemas,
            resource_proofs,
            created_access: self.created_access.clone(),
            _native_grants_custody: Some(grants.1),
        };
        Ok((scope, io.into_custody()?))
    }

    /// Further bounds concurrency, steps, and deadline for descendants.
    pub fn with_run_limits(
        mut self,
        limits: TaskRunLimits,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Self> {
        self.validate_run_limit_attenuation(&limits, work)?;
        self.run_limits = limits;
        Ok(self)
    }

    pub(crate) fn validate_run_limit_attenuation(
        &self,
        limits: &TaskRunLimits,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        limits.validate(work)?;
        if !self.run_limits.allows(limits) {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task scope cannot widen run limits",
            ));
        }
        Ok(())
    }

    /// Exact task execution bounds pinned at admission.
    #[must_use]
    pub const fn run_limits(&self) -> TaskRunLimits {
        self.run_limits
    }

    pub(super) fn concurrency_bound(&self, configured: usize) -> usize {
        self.run_limits
            .concurrency
            .map_or(configured, |bound| configured.min(bound))
    }

    /// Pins one committed agent selection for this scope and all descendants.
    pub fn with_extensions_from(self, agent: &Reducer) -> Result<Self> {
        self.with_original_extensions(agent.original_extension_binding()?)
    }

    pub(crate) fn with_original_extensions(
        mut self,
        original: (Option<ExtensionAdmission>, SchemaRegistry),
    ) -> Result<Self> {
        if self.extensions_sealed {
            return Err(Error::Unauthorized(
                "descendant scope cannot change extension selection".into(),
            ));
        }
        let (selection, schemas) = original;
        if self.extensions.is_some() && self.extensions != selection {
            return Err(Error::Conflict(
                "runtime scope extension selection is already pinned".into(),
            ));
        }
        self.extensions = selection;
        self.extension_schemas = Some(schemas);
        if let Some(runtime) = &self.extension_runtime {
            self.validate_extension_runtime(runtime)?;
        }
        self.extensions_sealed = true;
        Ok(self)
    }

    /// Exact extension bindings selected when this scope was admitted.
    #[must_use]
    pub const fn extensions(&self) -> Option<&ExtensionAdmission> {
        self.extensions.as_ref()
    }

    /// Binds process-local executable implementations for this scope.
    pub fn with_extension_runtime(mut self, runtime: Arc<ExtensionRuntime>) -> Result<Self> {
        self.validate_extension_runtime(&runtime)?;
        if let Some(original) = &self.extension_runtime
            && !original.selected().eq(runtime.selected())
        {
            return Err(Error::Conflict(
                "runtime scope cannot retarget linked extension implementations".into(),
            ));
        }
        self.extension_runtime = Some(runtime);
        Ok(self)
    }

    pub(super) fn validate_extension_runtime(&self, runtime: &ExtensionRuntime) -> Result<()> {
        runtime.validate_admission(self.extensions.as_ref(), &mut None)?;
        if let Some(schemas) = &self.extension_schemas {
            for identity in runtime.selected() {
                if schemas.implementation_digest(&identity.name, identity.version, &mut None)?
                    != identity.digest
                {
                    return Err(Error::Conflict(
                        "linked implementation differs from original admission registry".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn extension_schema_registry(&self) -> Option<&SchemaRegistry> {
        self.extension_schemas.as_ref()
    }

    /// Returns the executable extension selection attached to this scope.
    #[must_use]
    pub fn extension_runtime(&self) -> Option<Arc<ExtensionRuntime>> {
        self.extension_runtime.clone()
    }

    /// Whether this scope was pinned by admission or derived from another.
    #[must_use]
    pub const fn is_sealed(&self) -> bool {
        self.extensions_sealed
    }

    pub(crate) fn with_replayed_extensions(
        mut self,
        extensions: Option<ExtensionAdmission>,
    ) -> Result<Self> {
        if let Some(extensions) = &extensions {
            extensions.validate(&mut None)?;
        }
        if self.extensions_sealed && self.extensions != extensions {
            return Err(Error::Conflict(
                "replay cannot retarget original extension admission".into(),
            ));
        }
        self.extensions = extensions;
        if let Some(runtime) = &self.extension_runtime {
            self.validate_extension_runtime(runtime)?;
        }
        self.extensions_sealed = true;
        Ok(self)
    }

    /// Exact durable proof references retained for descendant scope attenuation.
    #[must_use]
    pub fn resource_proofs(&self) -> &[crate::resource_preparation::ResourceCreationProofRef] {
        &self.resource_proofs
    }

    pub(crate) fn with_created_access_with_work(
        &self,
        admission: &TaskAdmissionRecord,
        access: Option<crate::resource_preparation::CreatedResourceAccess>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if let Some(access) = &access {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work
                .as_deref()
                .is_some_and(|io| io.original_native_work().is_some())
                && !access.has_native_custody()
            {
                return Err(scope_metadata_boundary(
                    work,
                    crate::wire::ErrorCode::Unauthorized,
                    "native created resource seal requires its original payload custody",
                ));
            }
            if access.operation != admission.operation_id
                || access.admission_digest != admission.canonical_digest_with_work(work)?
                || access.proofs != admission.resource_proofs
            {
                return Err(scope_metadata_boundary(
                    work,
                    crate::wire::ErrorCode::Unauthorized,
                    "created resource seal changed admission",
                ));
            }
        } else if !admission.resource_proofs.is_empty() {
            return Err(scope_metadata_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "created resource proof is not authenticated",
            ));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            self.validate_narrowing_with_native_work(&self.grants, &admission.limits, native)?;
        } else {
            admission.limits.validate(&mut None)?;
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        admission.limits.validate(&mut None)?;
        if !self.allows_authority(&self.grants, &admission.limits) {
            return Err(scope_metadata_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "runtime scope cannot widen ancestor authority",
            ));
        }
        for grant in admission.grants.iter() {
            if let Some(io) = work.as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            if !self.grants.contains(grant)
                && !access
                    .as_ref()
                    .is_some_and(|access| access.grants.contains(grant))
            {
                return Err(scope_metadata_boundary(
                    work,
                    crate::wire::ErrorCode::Unauthorized,
                    "runtime scope cannot widen ancestor authority",
                ));
            }
        }
        if self.extensions != admission.extensions {
            return Err(scope_metadata_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "replay cannot retarget original extension admission",
            ));
        }
        if let Some(extensions) = &admission.extensions {
            extensions.validate(work)?;
        }
        let mut quota =
            crate::wire_codec::CodecQuota::new(work, std::mem::size_of::<Self>() as u64)?;
        let grants = crate::core::copy_capabilities(&admission.grants, &mut quota)?;
        let proofs = access
            .as_ref()
            .map_or(self.resource_proofs.as_slice(), |access| {
                access.proofs.as_slice()
            });
        let resource_proofs = Self::copy_resource_proofs(proofs, &mut quota)?;
        let extensions = admission
            .extensions
            .as_ref()
            .map(|extensions| extensions.copy_with_quota(&mut quota))
            .transpose()?;
        let extension_schemas = self
            .extension_schemas
            .as_ref()
            .map(|schemas| schemas.copy(quota.work()))
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if access.is_some()
            && let Some(native) = quota
                .work()
                .as_deref()
                .and_then(|io| io.original_native_work())
        {
            native
                .charge(acyclic_fs::WorkCounters {
                    allocation_operations: 1,
                    ..Default::default()
                })
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
        }
        let created_access = access.map(Arc::new).or_else(|| self.created_access.clone());
        let scope = Self {
            grants,
            limits: admission.limits,
            run_limits: self.run_limits,
            extensions,
            extensions_sealed: true,
            extension_runtime: self.extension_runtime.clone(),
            extension_schemas,
            resource_proofs,
            created_access,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            _native_grants_custody: None,
        };
        let scope = scope.with_run_limits(admission.run_limits, quota.work())?;
        if let Some(runtime) = &scope.extension_runtime {
            runtime.validate_admission(scope.extensions.as_ref(), quota.work())?;
            if let Some(schemas) = &scope.extension_schemas {
                for identity in runtime.selected() {
                    if schemas.implementation_digest(
                        &identity.name,
                        identity.version,
                        quota.work(),
                    )? != identity.digest
                    {
                        return Err(scope_metadata_boundary(
                            quota.work(),
                            crate::wire::ErrorCode::Conflict,
                            "linked implementation differs from original admission registry",
                        ));
                    }
                }
            }
        }
        let held = if quota.work().is_some() {
            quota.retained()
        } else {
            0
        };
        quota.transfer();
        Ok((scope, held))
    }

    pub(super) fn resume_sealed(&self, admitted: Self, operation: OperationId) -> Result<Self> {
        let mut permitted = self.clone();
        if let Some(access) = &admitted.created_access {
            if access.operation != operation || access.proofs != admitted.resource_proofs {
                return Err(Error::Unauthorized(
                    "created resource seal belongs to another task".into(),
                ));
            }
            permitted.grants = Capabilities::new(self.grants.iter().chain(access.grants.iter()));
            permitted.resource_proofs = admitted.resource_proofs.clone();
            permitted.created_access = admitted.created_access.clone();
        } else if !admitted.resource_proofs.is_empty() {
            return Err(Error::Unauthorized(
                "unsealed created resource scope".into(),
            ));
        }
        permitted
            .narrow(admitted.grants, admitted.limits)?
            .with_run_limits(admitted.run_limits, &mut None)?
            .with_replayed_extensions(admitted.extensions)
    }

    pub(crate) async fn current_creation_parent_fence(
        &self,
        parent: TaskId,
    ) -> Result<crate::scheduler::LeaseFence> {
        let access = self.created_access.as_ref().ok_or_else(|| {
            Error::Unauthorized("created child has no inherited parent seal".into())
        })?;
        if access.operation.into_bytes() != parent.into_bytes() {
            return Err(Error::Unauthorized(
                "created child seal belongs to another parent".into(),
            ));
        }
        let lease = access
            .publication_lease
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("created child parent lease is not bound".into()))?;
        lease.verify().await?;
        lease.fence()
    }

    /// Returns the actor authenticated by the scratch allocation.
    pub(crate) fn current_created_agent(
        &self,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<crate::AgentId>> {
        self.current_created_content(work)?
            .map(|binding| match binding.volume.owner() {
                crate::conversation::VolumeOwner::Agent(agent) => Ok(*agent),
                _ => Err(scope_metadata_boundary(
                    work,
                    crate::wire::ErrorCode::Unauthorized,
                    "current created scratch is not agent-owned",
                )),
            })
            .transpose()
    }

    pub(crate) fn current_created_content(
        &self,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<&crate::resource_preparation::CreatedContentBinding>> {
        if let Some(io) = work.as_deref() {
            io.check_cancellation()?;
        }
        self.created_access
            .as_ref()
            .map(|access| access.current_content(work))
            .transpose()
            .map(Option::flatten)
    }

    pub(crate) fn created_grants(&self) -> Option<&Capabilities> {
        self.created_access.as_ref().map(|access| &access.grants)
    }

    pub(crate) fn with_created_publication_lease(
        mut self,
        lease: Arc<dyn crate::resource_preparation::CreatedTaskLease>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if let Some(access) = &mut self.created_access {
            if access.publication_lease.is_some() {
                return Ok(self);
            }
            let access = Arc::get_mut(access).ok_or_else(|| {
                crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "created publication seal is already shared",
                )
            })?;
            access.publication_lease = Some(lease);
        }
        Ok(self)
    }

    pub(super) async fn verify_created_write(
        &self,
        volume: &VolumeRef,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        if self.created_content(Some(volume)).is_some() {
            let lease = self
                .created_access
                .as_ref()
                .and_then(|access| access.publication_lease.as_ref())
                .ok_or_else(|| {
                    Error::Unauthorized("created resource publication lease is not bound".into())
                })?;
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                return io.verify_publication_lease(lease.as_ref()).await;
            }
            lease.verify().await?;
        }
        let _ = io;
        Ok(())
    }

    pub(crate) fn created_content(&self, volume: Option<&VolumeRef>) -> Option<&ContentBindings> {
        self.created_access
            .as_ref()?
            .content
            .iter()
            .find(|binding| {
                volume.map_or(binding.default_writer, |volume| &binding.volume == volume)
            })
            .map(|binding| &binding.binding)
    }

    pub(super) fn created_artifact_content(&self) -> Option<&ContentBindings> {
        self.created_access
            .as_ref()?
            .content
            .iter()
            .find(|binding| binding.artifact_writer)
            .map(|binding| &binding.binding)
    }

    /// Finds only an exact provider-authenticated, owner-consumed reference.
    pub(crate) fn created_reference(
        &self,
        file: &FileRef,
    ) -> Option<&Arc<dyn crate::conversation::ContentResidencyVerifier>> {
        self.created_access
            .as_ref()?
            .references
            .iter()
            .find(|binding| &binding.file == file)
            .map(|binding| &binding.reader)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn created_access(
        &self,
    ) -> Option<Arc<crate::resource_preparation::CreatedResourceAccess>> {
        self.created_access.clone()
    }

    /// Returns the exact effective grants.
    #[must_use]
    pub const fn grants(&self) -> &Capabilities {
        &self.grants
    }

    /// Returns the exact effective bounds.
    #[must_use]
    pub const fn limits(&self) -> Limits {
        self.limits
    }
}
