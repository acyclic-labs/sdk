use super::*;

/// Task-local execution bounds pinned separately from content/render limits.
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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
    pub(crate) grants: Capabilities,
    pub(super) limits: Limits,
    pub(super) run_limits: TaskRunLimits,
    pub(super) extensions: Option<ExtensionAdmission>,
    pub(super) extensions_sealed: bool,
    pub(super) extension_runtime: Option<Arc<ExtensionRuntime>>,
    pub(super) extension_schemas: Option<SchemaRegistry>,
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
            ..Self::default()
        })
    }

    /// Moves already-paid grants into a checked native root, without a copy.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        grants: (Capabilities, acyclic_fs::NativeAllocation),
        limits: Limits,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        limits.validate(&mut Some(&mut crate::store::HistoryWork::new(
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
            _native_grants_custody: Some(grants.1),
            ..Self::default()
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
        let grants = crate::Capabilities::copy(self.grants.iter(), work)?;
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

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn clone_native_metadata(
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
            Some(extensions) => Some(extensions.copy(&mut Some(&mut io))?.0),
            None => None,
        };
        let scope = Self {
            grants: grants.0,
            limits,
            run_limits: self.run_limits,
            extensions,
            extensions_sealed: sealed,
            extension_runtime: self.extension_runtime.clone(),
            extension_schemas,
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

    /// Pins one committed agent selection for this scope and all descendants.
    pub fn with_extensions_from(self, agent: &Reducer) -> Result<Self> {
        self.with_original_extensions((agent.extension_admission()?, agent.schemas().clone()))
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

crate::core::ownership::owned_struct!(TaskRunLimits {
    concurrency,
    max_steps,
    deadline_epoch_ms
});
