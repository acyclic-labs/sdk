//! Owner.
use super::*;

#[cfg(feature = "filesystem")]
impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Returns the original task identity without exposing its lease or signer.
    pub const fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub(crate) fn require_mail_scope(
        &self,
        scope: &RuntimeScope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
            observer.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
        }
        crate::core::inspect_metadata(&(scope.grants(), &self.input_grants), work)?;
        if scope.grants() != &self.input_grants || scope.limits() != self.input_limits {
            #[cfg(not(target_arch = "wasm32"))]
            if work
                .as_ref()
                .is_some_and(|observer| observer.original_native_work().is_some())
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "mailbox scope differs from retained admission",
                });
            }
            return Err(Error::Unauthorized(
                "mailbox scope differs from retained admission".into(),
            ));
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn require_mail_scope_with_native_work(
        &self,
        scope: &RuntimeScope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(acyclic_fs::WorkCounters::default())?;
        if !self
            ._native_custody
            .as_ref()
            .is_some_and(|custody| custody.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "mailbox scope differs from its original native admission",
            });
        }
        let mut observer = crate::filesystem::PreparationIo::from_native_work(work)
            .map(crate::store::HistoryWork::new)?;
        self.require_mail_scope(scope, &mut Some(&mut observer))
    }

    pub(crate) fn require_task_context_binding(
        &self,
        context: &crate::runtime::TaskContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            context.require_original_native_work(work)?;
            if !self
                ._native_custody
                .as_ref()
                .is_some_and(|custody| custody.work().shares_allocation_context(work))
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "read owner differs from its original admission ledger",
                });
            }
        }
        let proofs = self
            .input_created
            .as_ref()
            .map_or(&[][..], |access| access.proofs.as_slice());
        crate::executor::with_history_io(io, |work| {
            crate::core::inspect_metadata(
                &(
                    context.scope().grants(),
                    &self.input_grants,
                    context.scope().resource_proofs(),
                    proofs,
                    context.scope().extensions(),
                    self.extensions.as_ref(),
                ),
                work,
            )
        })?;
        if !context.original_task_binding_matches(self.task_id, &self.fence)
            || context.id() != self.operation_id()
            || !context
                .original_host_binding_matches(self.host.as_ref(), self.host.policy.as_deref())
            || context.scope().limits() != self.input_limits
            || context.scope().run_limits().deadline_epoch_ms != self.deadline_epoch_ms
            || context.scope().grants() != &self.input_grants
            || context.scope().resource_proofs() != proofs
            || context.scope().extensions() != self.extensions.as_ref()
        {
            return Err(crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "read context differs from its original SDK owner admission",
            ));
        }
        Ok(())
    }

    pub(crate) fn matches_task_binding(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
    ) -> bool {
        self.task_id == task && &self.fence == fence
    }

    pub(crate) fn original_host_binding_matches(&self, host: &CoordinatorTaskHost<P>) -> bool {
        Arc::ptr_eq(&self.host.coordinator, &host.coordinator)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn same_original_root_output_owner(&self, other: &Self) -> bool {
        self.root_binding.is_some()
            && other.root_binding.is_some()
            && self.readonly_original.is_none()
            && other.readonly_original.is_none()
            && Arc::ptr_eq(&self.host.coordinator, &other.host.coordinator)
            && self.task_id == other.task_id
            && self.workflow.request_digest == other.workflow.request_digest
            && self.fence == other.fence
            && self.input_agent == other.input_agent
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn native_owned_bytes(&self) -> Result<u64> {
        let mut bytes = std::mem::size_of::<Self>() as u64;
        let mut add = |owned: u64| -> Result<()> {
            bytes = bytes.checked_add(owned).ok_or(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::Overflow,
                ),
                custody: None,
            })?;
            Ok(())
        };
        for text in [
            &self.fence.reservation_id,
            &self.fence.placement,
            &self.workflow.initial.machine.name,
            &self.workflow.initial.machine.version,
        ] {
            add(text.capacity() as u64)?;
        }
        add(crate::contract::json_value_logical_owned_bytes(
            &self.workflow.initial.state,
        )?)?;
        add(crate::contract::json_value_logical_owned_bytes(
            &self.output_schema,
        )?)?;
        add(self.input_grants.logical_owned_bytes()?)?;
        if let Some(extensions) = &self.extensions {
            add(extensions.logical_owned_bytes()?)?;
        }
        if self.root_binding.is_some() {
            add(std::mem::size_of::<RootBootstrapBindingWitness>() as u64)?;
        }
        if let Some(original) = &self.readonly_original {
            add(original.logical_owned_bytes()?)?;
        }
        Ok(bytes)
    }

    pub(crate) const fn input_limits(&self) -> crate::conversation::Limits {
        self.input_limits
    }

    /// Verifies only a kernel-authenticated created-volume input.
    pub(crate) async fn verify_created_input_with_io(
        &self,
        file: &FileRef,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<bool> {
        self.validate_input_file(file, io)?;
        let mut reader = None;
        if let Some(access) = &self.input_created {
            for binding in &access.references {
                if let Some(io) = io {
                    let step = acyclic_fs::WorkCounters {
                        items_examined: 1,
                        ..Default::default()
                    };
                    io.preflight(step)?;
                    io.charge(step)?;
                }
                if &binding.0 == file {
                    reader = Some(&binding.1);
                    break;
                }
            }
            if reader.is_none() {
                for binding in &access.content {
                    if let Some(io) = io {
                        let step = acyclic_fs::WorkCounters {
                            items_examined: 1,
                            ..Default::default()
                        };
                        io.preflight(step)?;
                        io.charge(step)?;
                    }
                    if &binding.volume == file.volume() {
                        reader = Some(&binding.binding.reader);
                        break;
                    }
                }
            }
        }
        let Some(reader) = reader else {
            return Ok(false);
        };
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        self.verify(true, &mut work).await?;
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        let (bytes, retained) = match quota.work().as_deref_mut() {
            Some(observer) => {
                crate::store::history::read_content_with_work(observer, reader.as_ref(), file)
                    .await?
            }
            None => (
                reader
                    .read(file, file.descriptor().byte_length(), None)?
                    .await?,
                0,
            ),
        };
        quota.adopt(retained)?;
        file.descriptor().verify(&bytes, quota.work())?;
        self.verify(true, quota.work()).await?;
        drop(bytes);
        Ok(true)
    }

    pub(crate) fn validate_native_configuration(
        &self,
        binding: &crate::model::NativeConfigurationBinding,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<(&crate::core::SchemaRegistry, [u8; 32])> {
        let runtime = self.host.root_scope.extension_runtime();
        let (schemas, expected) = binding.verify_original(
            self.extensions.as_ref(),
            self.host.root_scope.extension_schema_registry(),
            runtime.as_deref(),
            io,
        )?;
        let file = &binding.configuration.content;
        self.validate_input_file(file, io)?;
        if file.descriptor().byte_length() > self.input_limits.render_bytes {
            return Err(Error::Invalid(
                "native options exceed render byte limit".into(),
            ));
        }
        Ok((schemas, expected))
    }

    pub(crate) async fn verify_native_configuration(
        &self,
        binding: &crate::model::NativeConfigurationBinding,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        let (schemas, expected) = self.validate_native_configuration(binding, io)?;
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        let (bytes, retained) = match quota.work().as_deref_mut() {
            Some(observer) => {
                crate::store::history::read_content_with_work(
                    observer,
                    self.host.reader.as_ref(),
                    &binding.configuration.content,
                )
                .await?
            }
            None => (
                self.host
                    .reader
                    .read(
                        &binding.configuration.content,
                        self.input_limits.render_bytes,
                        None,
                    )?
                    .await?,
                0,
            ),
        };
        quota.adopt(retained)?;
        schemas.verify_configuration_binding(
            &binding.configuration,
            expected,
            &bytes,
            self.input_limits,
            io,
        )
    }

    pub(crate) const fn input_grants(&self) -> &crate::Capabilities {
        &self.input_grants
    }

    /// Checks the retained volume grant on the optional work ledger.
    pub fn require_volume_grant(
        &self,
        volume: &crate::conversation::VolumeRef,
        operation: crate::conversation::VolumeOperation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if operation == crate::conversation::VolumeOperation::Write
            && work
                .as_deref()
                .and_then(|io| io.original_native_work())
                .is_some()
            && self
                .input_created
                .as_ref()
                .and_then(|access| {
                    access
                        .content
                        .iter()
                        .find(|binding| &binding.volume == volume)
                })
                .is_some_and(|binding| binding.binding.writer.is_none())
        {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "created volume has no retained WRITE custody",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        if self.readonly_original.is_some()
            && operation == crate::conversation::VolumeOperation::Write
        {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "historical READ owner cannot authorize namespace WRITE",
            ));
        }
        volume.validate(work)?;
        let observed = work.is_some();
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let grant = volume.capability(operation, quota.work())?;
        quota.adopt(grant.capacity() as u64)?;
        if !self.input_grants.contains(&grant)
            && !(observed
                && self
                    .input_created
                    .as_ref()
                    .is_some_and(|access| access.grants.contains(&grant)))
        {
            return Err(admission_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks required volume grant",
            ));
        }
        Ok(())
    }

    /// Obtains the original currently selected scratch allocation.
    pub fn child_scratch_creation_proof(
        &self,
    ) -> Result<crate::resource_preparation::ChildScratchCreationProof> {
        let access = self
            .input_created
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("task has no created scratch allocation".into()))?;
        let binding = access
            .current_content(&mut None)?
            .ok_or_else(|| Error::Unauthorized("task has no current scratch allocation".into()))?;
        let proof = binding.scratch_creation.as_ref().ok_or_else(|| {
            Error::Unauthorized("current scratch is not a verified filesystem allocation".into())
        })?;
        if crate::runtime::task_id_for_admission(proof.origin().intended_child) != self.task_id
            || !matches!(proof.volume().owner(), crate::conversation::VolumeOwner::Agent(agent)
                if Some(*agent) == self.input_agent)
            || proof.volume() != &binding.volume
        {
            return Err(Error::Unauthorized(
                "scratch allocation belongs to another original task".into(),
            ));
        }
        Ok(proof.clone())
    }

    pub(crate) fn require_interaction_grant(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if !self.input_grants.contains(capability::INTERACTION_ROUTE) {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks interaction:route",
            ));
        }
        Ok(())
    }

    pub(crate) fn task_binding(&self) -> (TaskId, &crate::scheduler::LeaseFence) {
        (self.task_id, &self.fence)
    }

    pub(crate) fn validate_storage(
        &self,
        verifier: &AuthorityVerifier,
        maximum_payload_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if verifier.audience() != self.host.verifier.audience()
            || maximum_payload_bytes > self.input_limits.file_bytes
        {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "journal storage exceeds admitted task authority or limits",
            ));
        }
        Ok(())
    }

    /// Returns the initial workflow identity derived from the retained task
    #[must_use]
    pub const fn workflow_admission(&self) -> &crate::workflow::WorkflowAdmission {
        &self.workflow
    }

    pub(crate) fn invocation_cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        self.invocation_cancellation.as_ref()
    }

    /// Binds this already-sealed owner to the caller's actual live operation token.
    pub fn with_invocation_cancellation(
        mut self,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Self {
        self.invocation_cancellation = Some(cancellation.clone());
        self
    }

    /// Shares an already SDK-observed owner without copying its original policy or deep metadata.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(
        mut self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        let owned = self.native_owned_bytes()?;
        if !self._native_custody.as_ref().is_some_and(|custody| {
            custody.work().shares_allocation_context(work) && custody.bytes() >= owned
        }) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native owner sharing requires its SDK metadata custody",
            });
        }
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1,
            ..Default::default()
        })?;
        self._native_shared = true;
        Ok(Arc::new(self))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native_shared_work_matches(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> bool {
        self._native_shared
            && self
                ._native_custody
                .as_ref()
                .is_some_and(|custody| custody.work().shares_allocation_context(work))
    }

    /// Reads the original owned task's actual terminal result on the same native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn outcome_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(
        Option<crate::Outcome<FileRef>>,
        acyclic_fs::NativeAllocation,
    )> {
        let mut io = crate::filesystem::PreparationIo::from_native_work(work)
            .map(crate::store::HistoryWork::new)?;
        let mut coordinator = self.host.coordinator.lock().await;
        coordinator.refresh(&mut Some(&mut io)).await?;
        let operation = coordinator.observe_operation(
            self.host.verifier.audience(),
            &self.host.owner_scope,
            &self.host.verifier,
            self.operation_id(),
            &mut Some(&mut io),
        )?;
        if operation.phase != crate::scheduler::OperationPhase::Terminal {
            return Ok((None, work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?));
        }
        let outcome = operation
            .outcome
            .as_ref()
            .ok_or_else(|| Error::Invalid("terminal original task lacks its outcome".into()))?;
        if matches!(outcome, crate::Outcome::Indeterminate { .. }) {
            return Err(Error::Invalid(
                "uncertain task outcome cannot be terminal".into(),
            ));
        }
        let output = outcome.clone_with_work(&mut Some(&mut io))?;
        Ok((Some(output), io.into_custody()?))
    }

    pub(crate) fn require_effect_grants(
        &self,
        provider: &str,
        planning: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if !self.input_grants.contains(capability::EFFECT_RUN) {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks effect:run",
            ));
        }
        let mut permitted = false;
        for grant in self.input_grants.iter() {
            if let Some(io) = work.as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            if grant.strip_prefix("effect:provider:") == Some(provider) {
                permitted = true;
                break;
            }
        }
        if !permitted {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks the effect provider grant",
            ));
        }
        if planning && !self.input_grants.contains(capability::EFFECT_PLAN) {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks effect:plan",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_input_file(
        &self,
        file: &FileRef,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(target_arch = "wasm32"))]
        let native = io.is_some_and(|io| io.original_native_work().is_some());
        #[cfg(target_arch = "wasm32")]
        let native = false;
        if native {
            if file.path().len() > self.input_limits.path_bytes
                || file.descriptor().byte_length() > self.input_limits.file_bytes
            {
                return Err(admission_boundary(
                    &work,
                    crate::wire::ErrorCode::Invalid,
                    "execution input file exceeds original task limits",
                ));
            }
        } else {
            self.input_limits.validate_file(file, &mut None)?;
        }
        let allowed = crate::runtime::read_granted_with_work(&self.input_grants, file, &mut work)?
            || (io.is_some()
                && match self.input_created.as_ref() {
                    Some(access) => {
                        crate::runtime::read_granted_with_work(&access.grants, file, &mut work)?
                    }
                    None => false,
                });
        if !allowed {
            return Err(admission_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "original task cannot read the execution input file",
            ));
        }
        Ok(())
    }
}
