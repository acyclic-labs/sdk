use crate::store::history::HistoryIo as _;
use super::{ContentResidencyVerifier, InteractionRouter, IssuedContextPurpose, Limits, RuntimeScope, TaskContext, ToolContext, ToolTaskContext};
use crate::{Error, NativeFutureBundle, Result, conversation::FileRef, durable_host::TaskJournalOwner};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use std::sync::Arc;

pub(super) struct NativeMetadataSource {
    work: NativeMaterializationWork,
    bytes: u64,
    pub(super) purpose: IssuedContextPurpose,
}

fn boundary(code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    Error::NativeBoundary { code, reason }
}

fn validate_scope_attenuation(
    original: &RuntimeScope, requested: &RuntimeScope, limits: Limits,
    work: &NativeMaterializationWork,
) -> Result<()> {
    original.validate_narrowing_with_native_work(&requested.grants, &limits, work)?;
    original.validate_run_limit_attenuation_with_native_work(&requested.run_limits, work)
}

/// The SDK-issued task context shared without cloning its sealed metadata.
pub struct NativeTaskContext {
    context: TaskContext,
    #[cfg(feature = "native-execution")]
    working_view: std::sync::OnceLock<Arc<crate::filesystem::NativeTaskWorkingView>>,
    custody: NativeAllocation,
}

impl TaskContext {
    /// Records custody after the SDK producer completes metadata allocation.
    pub(super) fn mark_native_metadata_source(
        &mut self,
        work: &NativeMaterializationWork,
        custody: &NativeAllocation,
    ) -> Result<()> {
        work.charge(WorkCounters::default())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if !custody.matches_work(work) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "issued task metadata custody belongs to another original ledger"));
        }
        self.native_metadata_source = Some(NativeMetadataSource {
            work: work.clone(), bytes: custody.bytes(), purpose: IssuedContextPurpose::Read,
        });
        Ok(())
    }

    /// Requires the SDK's original metadata source and cancellation ledger.
    pub(crate) fn require_original_native_work(&self, work: &NativeMaterializationWork) -> Result<()> {
        work.charge(WorkCounters::default())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if !self.native_metadata_source.as_ref()
            .is_some_and(|source| source.work.shares_allocation_context(work))
        {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "task context differs from its original native metadata source"));
        }
        Ok(())
    }
    pub(crate) fn require_original_native_dispatch(
        &self, work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_original_native_work(work)?;
        if !self.native_metadata_source.as_ref().is_some_and(|source| source.purpose == IssuedContextPurpose::Dispatch) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "native task metadata was not issued for fresh dispatch"));
        }
        Ok(())
    }

    /// Borrows the original SDK mailbox binding without copying its fence or deriving a principal from the task identity.
    pub fn owned_task_binding_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<(crate::TaskId, &crate::scheduler::LeaseFence)> {
        self.require_original_native_work(work)?;
        let mailbox = self.owned_mailbox.as_ref().ok_or_else(|| boundary(
            crate::wire::ErrorCode::Unauthorized, "native task has no original SDK mailbox",
        ))?;
        mailbox.validate_scope_with_native_work(&self.scope, work)?;
        let (task, fence) = mailbox.task_binding();
        if self.durable_task != Some(task) || !mailbox.matches_task_binding(task, fence) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "native context differs from its original mailbox binding"));
        }
        Ok((task, fence))
    }

    /// Revalidates the original issued execution owner before a native SDK read.
    pub fn verify_original_execution_owner_with_native_work<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, ()>> {
        self.require_original_native_dispatch(work)?;
        let (task, fence) = self.owned_task_binding_with_native_work(work)?;
        let host = self.harness.host.as_ref().ok_or_else(|| boundary(
            crate::wire::ErrorCode::Unsupported,
            "native execution has no original durable owner callback",
        ))?;
        self.harness.assert_durable_policy_bindings(Some(work))?;
        host.verify_dispatch_owner_with_native_work(task, fence, work)
    }

    /// Checks stock scope restrictions against the issued source before copying metadata or constructing a callback.
    pub(crate) fn validate_stock_scope_with_native_work(
        &self, requested: &RuntimeScope, limits: Limits, work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_original_native_work(work)?;
        validate_scope_attenuation(&self.scope, requested, limits, work)
    }

    /// Consumes the actual SDK-issued context and its observed metadata custody.
    pub fn into_native_shared(
        self,
        custody: NativeAllocation,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<NativeTaskContext>> {
        let mut payload = NativeTaskContext {
            context: self,
            #[cfg(feature = "native-execution")]
            working_view: std::sync::OnceLock::new(),
            custody,
        };
        let Some(source) = payload.context.native_metadata_source.as_ref() else {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "task context has no original native metadata source"));
        };
        if !source.work.shares_allocation_context(work) || !payload.custody.work().shares_allocation_context(work)
            || payload.custody.bytes() < source.bytes
        {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "task context metadata custody differs from its original source"));
        }
        let extra = (std::mem::size_of::<NativeTaskContext>() - std::mem::size_of::<TaskContext>()) as u64;
        let mut slot = match work.claim(extra) {
            Ok(slot) => slot,
            Err(cause) => {
                let NativeTaskContext { context, custody, .. } = payload;
                drop(context);
                return Err(Error::NativeWork { cause, custody: Some(custody) });
            }
        };
        payload.custody.absorb(&mut slot)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Arc::new(payload))
    }
}

impl NativeTaskContext {
    /// Borrows the same issued context used by the native command and provider.
    #[must_use]
    pub const fn context(&self) -> &TaskContext { &self.context }

    /// Selects an existing authenticated output writer without widening scope.
    pub async fn with_output_volume_with_native_work(
        self: &Arc<Self>, volume: &crate::conversation::VolumeRef,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        self.context.require_original_native_work(work)?;
        let purpose = self.context.native_metadata_source.as_ref()
            .expect("original native metadata source").purpose;
        if purpose == IssuedContextPurpose::Read {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "readonly metadata cannot select a native output writer"));
        }
        let binding = self.context.scope.created_content(Some(volume))
            .filter(|binding| binding.writer.is_some())
            .ok_or_else(|| boundary(crate::wire::ErrorCode::Unauthorized,
                "output volume has no authenticated created writer"))?;
        let mut permission_io = crate::store::history::native_work_io(work)?;
        let permission = volume.capability(
            crate::conversation::VolumeOperation::Write, &mut Some(&mut permission_io),
        )?;
        let allowed = self.context.scope.grants().contains(&permission);
        drop(permission);
        drop(permission_io);
        if !allowed {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "original task scope cannot write the selected output volume"));
        }
        let (task, fence) = self.context.owned_task_binding_with_native_work(work)?;
        let host = self.context.harness.host.as_ref().ok_or_else(|| boundary(
            crate::wire::ErrorCode::Unsupported, "output projection has no original task host",
        ))?;
        self.context.harness.assert_durable_policy_bindings(Some(work))?;
        match purpose {
            IssuedContextPurpose::Dispatch => {
                host.verify_dispatch_owner_with_native_work(task, fence, work)?.await?;
            }
            IssuedContextPurpose::Reconciliation => {
                host.verify_execution_owner_with_native_work(task, fence, work)?.await?;
            }
            IssuedContextPurpose::Read => unreachable!("readonly projection rejected"),
        }
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        io.retain_owned((std::mem::size_of::<TaskContext>()
            - std::mem::size_of::<RuntimeScope>()) as u64)?;
        let (scope, scope_custody) = self.context.scope.clone_with_native_work(work)?;
        io.absorb_native_custody(scope_custody)?;
        let policy_overrides = {
            let mut observer = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
            let mut quota = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
            let copied = crate::core::copy_vector(
                &self.context.policy_overrides, &mut quota, |(identity, policy), quota| {
                    Ok((crate::registry::ComponentIdentity {
                        name: crate::core::copy_text(&identity.name, quota)?,
                        version: crate::core::copy_text(&identity.version, quota)?,
                        digest: identity.digest,
                    }, Arc::clone(policy)))
                },
            )?;
            quota.transfer();
            copied
        };
        let mut context = TaskContext {
            harness: Arc::clone(&self.context.harness),
            task_id: self.context.task_id, durable_task: self.context.durable_task,
            descendants: self.context.descendants.clone(), scope, policy_overrides,
            owned_interactions: self.context.owned_interactions.clone(),
            owned_mailbox: self.context.owned_mailbox.clone(),
            output_content: Some(binding.clone()),
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            native_root_output: self.context.native_root_output.clone(),
            model_steps: Arc::clone(&self.context.model_steps),
            native_metadata_source: None,
        };
        let custody = io.into_custody();
        if let Err(error) = context.mark_native_metadata_source(work, &custody) {
            drop(context);
            drop(custody);
            return Err(error);
        }
        context.native_metadata_source.as_mut().expect("projected native metadata source")
            .purpose = purpose;
        let projected = context.into_native_shared(custody, work)?;
        #[cfg(feature = "native-execution")]
        if let Some(view) = self.working_view.get() {
            let _ = projected.working_view.set(Arc::clone(view));
        }
        Ok(projected)
    }

    /// Attaches one genuinely prepared working view to its original issued dispatch context.
    #[cfg(feature = "native-execution")]
    pub async fn bind_working_view_with_native_work(
        &self, view: crate::filesystem::NativeTaskWorkingView,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.context.require_original_native_dispatch(work)?;
        let (task, fence) = self.context.owned_task_binding_with_native_work(work)?;
        if view.original_task() != task || view.original_fence() != fence
            || !view.original_native_work().shares_allocation_context(work)
        {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "working view differs from its original issued task"));
        }
        if self.working_view.get().is_some_and(|installed| !installed.same_identity(&view)) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "working view cannot be rebound to a different preparation"));
        }
        view.verify_with_native_work(work)?.await?;
        if self.working_view.get().is_some() { return Ok(()); }
        work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let view = Arc::new(view);
        match self.working_view.set(view) {
            Ok(()) => Ok(()),
            Err(loser) => {
                let same = self.working_view.get().is_some_and(|winner| winner.same_identity(&loser));
                drop(loser);
                if same { Ok(()) } else {
                    Err(boundary(crate::wire::ErrorCode::Unauthorized,
                        "working view lost installation to a different preparation"))
                }
            }
        }
    }

    /// Installs the original SDK-created root result destination once on this same issued context.
    #[cfg(feature = "native-execution")]
    pub async fn bind_root_output_context_with_native_work(
        &self,
        source: crate::filesystem::NativeRootOutputSource,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.context.require_original_native_dispatch(work)?;
        let installed = self.context.native_root_output.get();
        if installed.is_some_and(|installed| !installed.0.same_identity(&source)) {
            return Err(boundary(crate::wire::ErrorCode::Conflict,
                "root result destination differs from its original binding"));
        }
        source.verify_context_with_native_work(&self.context, work).await?;
        if installed.is_some() { return Ok(()); }
        let source = source.into_native_shared(work)?;
        let content = source.retained_content_binding();
        let binding = (source, content);
        if let Err(binding) = self.context.native_root_output.set(binding) {
            if self.context.native_root_output.get()
                .is_some_and(|installed| installed.0.same_identity(&binding.0))
            {
                return Ok(());
            }
            return Err(boundary(crate::wire::ErrorCode::Conflict,
                "root result destination was bound to another original resource"));
        }
        Ok(())
    }

    /// Borrows the original metadata ledger without granting resource authority.
    pub(crate) const fn native_work(&self) -> &NativeMaterializationWork {
        self.custody.work()
    }

    /// Pure membership check for the original metadata ledger, not authority.
    pub(crate) fn native_metadata_matches(&self, work: &NativeMaterializationWork) -> bool {
        self.custody.work().shares_allocation_context(work)
    }

    /// Builds an original-owner READ facade over this same shared context.
    pub fn content_residency_verifier_with_native_work<P: acyclic_stream::StreamProvider + 'static>(
        self: &Arc<Self>,
        owner: Arc<TaskJournalOwner<P>>,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<dyn ContentResidencyVerifier>> {
        let (task, _) = owner.task_binding();
        if self.context.durable_task != Some(task) || self.context.task_id != owner.operation_id() {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "content READ owner differs from the issued task context"));
        }
        if !self.custody.work().shares_allocation_context(work) || !owner.native_shared_work_matches(work) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "content READ facade lacks original shared metadata custody"));
        }
        owner.require_mail_scope_with_native_work(self.context.scope(), work)?;
        let custody = work.claim(std::mem::size_of::<NativeContextVerifier<P>>() as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Arc::new(NativeContextVerifier {
            context: Arc::clone(self), owner, custody,
        }))
    }
}

impl ToolContext {
    /// Records protected ToolStarted acceptance.
    fn accepted_native_context(&self, work: &NativeMaterializationWork) -> Result<&TaskContext> {
        let context = self.validate_native_bound_context(work)?;
        if !context.native_metadata_source.as_ref()
            .is_some_and(|source| source.purpose == IssuedContextPurpose::Dispatch)
        {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "original admission observation requires dispatch purpose"));
        }
        let accepted = self.accepted_native_tool.get().ok_or_else(|| boundary(
            crate::wire::ErrorCode::Unauthorized,
            "original admission observation requires protected ToolStarted acceptance",
        ))?;
        if context.durable_task != Some(accepted.0) || self.operation_id != accepted.1 {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "working view invocation differs from protected ToolStarted"));
        }
        Ok(context)
    }

    /// Observes this accepted invocation's original protected task input.
    pub fn original_admission_with_native_work<'a>(
        &'a self, work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (super::TaskAdmissionRecord, NativeAllocation)>> {
        let context = self.accepted_native_context(work)?;
        let task = context.durable_task.expect("accepted original durable task");
        let host = context.harness.host.as_ref().ok_or_else(|| boundary(
            crate::wire::ErrorCode::Unsupported, "original admission has no durable host",
        ))?;
        NativeFutureBundle::new(work, || async move {
            context.verify_original_execution_owner_with_native_work(work)?.await?;
            let observed = host.observe_admission_with_native_work(task, work)?.await?;
            if observed.0.operation_id != context.task_id {
                return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                    "observed input differs from the original issued task"));
            }
            context.verify_original_execution_owner_with_native_work(work)?.await?;
            Ok(observed)
        })
    }

    #[cfg(feature = "native-execution")]
    fn accepted_native_working_view(
        &self, work: &NativeMaterializationWork,
    ) -> Result<(&TaskContext, &crate::filesystem::NativeTaskWorkingView)> {
        let context = self.accepted_native_context(work)?;
        let shared = self.native_task().expect("validated original native tool context");
        let view = shared.working_view.get().ok_or_else(|| boundary(
            crate::wire::ErrorCode::NotFound, "issued task has no prepared working view",
        ))?;
        if !context.original_task_binding_matches(view.original_task(), view.original_fence())
            || !view.original_native_work().shares_allocation_context(work)
        {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "working metadata differs from the original issued task"));
        }
        Ok((context, view.as_ref()))
    }

    /// Borrows only the actual protected mount metadata before normalization.
    #[cfg(feature = "native-execution")]
    pub fn native_working_manifest_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<&crate::filesystem::NativeViewManifest> {
        self.accepted_native_working_view(work).map(|(_, view)| view.manifest())
    }

    #[cfg(feature = "native-execution")]
    fn check_native_working_permission(
        &self, view: &crate::filesystem::NativeTaskWorkingView,
        relative: &std::path::Path, operation: crate::conversation::VolumeOperation,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let volume = view.volume_for_builtin_path(relative, operation, work)?;
        let (permission, permission_custody) = volume.capability_with_native_work(operation, work)?;
        let allowed = self.effective_scope().grants().contains(&permission);
        drop(permission);
        drop(permission_io);
        if allowed { Ok(()) } else {
            Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "effective tool scope cannot access the selected mounted volume"))
        }
    }

    /// Borrows the original held namespace only inside an accepted tool callback.
    #[cfg(feature = "native-execution")]
    pub(crate) async fn native_working_view_with_native_work<'a>(
        &'a self, relative: &'a std::path::Path,
        operation: crate::conversation::VolumeOperation,
        work: &'a NativeMaterializationWork,
    ) -> Result<&'a crate::filesystem::NativeTaskWorkingView> {
        let (context, view) = self.accepted_native_working_view(work)?;
        self.check_native_working_permission(view, relative, operation, work)?;
        context.verify_original_execution_owner_with_native_work(work)?.await?;
        view.verify_with_native_work(work)?.await?;
        Ok(view)
    }

    /// Checks the accepted action's path and effective permission.
    #[cfg(feature = "native-execution")]
    pub async fn authorize_working_file_with_native_work(
        &self, relative: &std::path::Path,
        operation: crate::conversation::VolumeOperation,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.native_working_view_with_native_work(relative, operation, work).await?;
        Ok(())
    }

    /// Reads the held selection under this invocation's effective scope.
    #[cfg(feature = "native-execution")]
    pub async fn read_working_file_with_native_work(
        &self, relative: &std::path::Path, maximum: usize,
        work: &NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let view = self.native_working_view_with_native_work(
            relative, crate::conversation::VolumeOperation::Read, work,
        ).await?;
        view.read_builtin_file(self.operation_id, relative, maximum, work)?.await
    }

    /// Mutates only the same selected held path after effective WRITE checks.
    #[cfg(feature = "native-execution")]
    pub async fn write_working_file_with_native_work(
        &self, relative: &std::path::Path, content: &str, create_parents: bool,
        expected_sha256: Option<[u8; 32]>, work: &NativeMaterializationWork,
    ) -> Result<()> {
        if expected_sha256.is_some() {
            let (_, view) = self.accepted_native_working_view(work)?;
            self.check_native_working_permission(
                view, relative, crate::conversation::VolumeOperation::Read, work,
            )?;
        }
        let view = self.native_working_view_with_native_work(
            relative, crate::conversation::VolumeOperation::Write, work,
        ).await?;
        view.write_builtin_file(
            self.operation_id, relative, content, create_parents, expected_sha256, work,
        )?.await
    }
    /// Binds native tool metadata to the same SDK-issued shared task context.
    pub fn new_with_native_work(
        task: Arc<NativeTaskContext>,
        operation_id: crate::OperationId,
        call_id: &str,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        if !task.native_metadata_matches(work) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "tool context differs from its original native metadata ledger"));
        }
        crate::tool::ToolInvocation::validate_call_id_with_native_work(call_id, work)?;
        let mut slot = work.retain_owned(std::mem::size_of::<Self>() as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut output = work.display(call_id)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        output.1.absorb(&mut slot)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Self {
            task: ToolTaskContext::Native(task),
            operation_id,
            call_id: output.0,
            native_interactions: None,
            effective_scope: None,
            accepted_native_tool: std::sync::OnceLock::new(),
            native_custody: Some(output.1),
        })
    }

    /// Checks stock restrictions against the effective scope.
    pub(crate) fn validate_stock_scope_with_native_work(
        &self, requested: &RuntimeScope, limits: Limits,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.task().require_original_native_work(work)?;
        if !self.native_metadata_matches(work) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "stock scope has no original native tool metadata source"));
        }
        validate_scope_attenuation(self.effective_scope(), requested, limits, work)
    }

    /// Narrows tool authority while preserving its issued task.
    pub fn with_native_scope(
        self, requested: &RuntimeScope, work: &NativeMaterializationWork,
    ) -> Result<Self> {
        self.with_native_effective_scope(requested, requested.limits, work)
    }

    /// Applies stock grants and run restrictions.
    pub fn with_native_stock_scope(
        self, requested: &RuntimeScope, execution_limits: Limits,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        self.with_native_effective_scope(requested, execution_limits, work)
    }

    fn with_native_effective_scope(
        mut self, requested: &RuntimeScope, execution_limits: Limits,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        self.validate_stock_scope_with_native_work(requested, execution_limits, work)?;
        let original = self.effective_scope();
        let grants = requested.grants.clone_with_native_work(work)?;
        let mut narrowed = original.narrow_with_native_work(grants, execution_limits, work)?;
        narrowed.0 = narrowed.0.with_run_limits_with_native_work(requested.run_limits, work)?;
        self.effective_scope = Some((narrowed.0, Some(narrowed.1)));
        Ok(self)
    }

    /// Retains the issued interaction router.
    pub(crate) fn with_native_interactions(
        mut self, router: Arc<dyn InteractionRouter>, work: &NativeMaterializationWork,
    ) -> Result<Self> {
        work.charge(WorkCounters::default())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if !self.native_metadata_matches(work) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "interaction override has no original native tool metadata source"));
        }
        self.native_interactions = Some(router);
        Ok(self)
    }

    /// Borrows the actual per-command router for the original policy pipeline.
    pub(crate) fn stock_native_interactions(&self) -> Option<&dyn InteractionRouter> {
        self.native_interactions.as_deref()
    }

    /// Borrows the original shared task without reconstructing its owner.
    pub(crate) fn native_task(&self) -> Option<&Arc<NativeTaskContext>> {
        match &self.task {
            ToolTaskContext::Native(task) => Some(task),
            ToolTaskContext::Ordinary(_) => None,
        }
    }

    /// Checks genuine SDK tool metadata funding without granting tool authority.
    pub(crate) fn native_metadata_matches(&self, work: &NativeMaterializationWork) -> bool {
        let ToolTaskContext::Native(task) = &self.task else { return false; };
        let Some(custody) = &self.native_custody else { return false; };
        task.native_metadata_matches(work) && custody.work().shares_allocation_context(work)
            && (std::mem::size_of::<Self>() as u64)
                .checked_add(self.call_id.capacity() as u64)
                .is_some_and(|bytes| custody.bytes() >= bytes)
            && self.effective_scope.as_ref().is_none_or(|(_, guard)| {
                guard.as_ref().is_some_and(|guard| guard.work().shares_allocation_context(work))
            })
    }

    /// Checks the SDK-issued native dispatch context before MCP allocation or IO.
    pub fn validate_native_mcp_context(&self, work: &NativeMaterializationWork) -> Result<()> {
        let context = self.validate_native_bound_context(work)?;
        if !context.native_metadata_source.as_ref()
            .is_some_and(|source| source.purpose == IssuedContextPurpose::Dispatch)
        {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "native context purpose does not permit MCP dispatch"));
        }
        Ok(())
    }

    /// Checks a genuine owner-bound native reconciliation context.
    pub fn validate_native_mcp_reconciliation_context(
        &self, work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.validate_native_bound_context(work).map(|_| ())
    }

    fn validate_native_bound_context(
        &self, work: &NativeMaterializationWork,
    ) -> Result<&TaskContext> {
        work.charge(WorkCounters::default())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        if !self.native_metadata_matches(work) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "MCP context has no original native tool metadata source"));
        }
        let Some(shared) = self.native_task() else {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "MCP context is not the original shared native task"));
        };
        let context = shared.context();
        context.require_original_native_work(work)?;
        let Some((task, fence)) = context.owned_mailbox.as_ref()
            .map(|owner| owner.task_binding())
        else {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "MCP context has no borrowed original owner binding"));
        };
        if !context.original_task_binding_matches(task, fence) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "MCP context differs from its original task and fence"));
        }
        Ok(context)
    }

    /// Checks the configured original process binding before preparing a native callback.
    #[allow(clippy::too_many_arguments, reason = "one exact approved MCP invocation binding")]
    pub(crate) fn validate_native_mcp_process_binding(
        &self,
        task: crate::TaskId,
        owner_fence: &crate::scheduler::LeaseFence,
        operation: crate::OperationId,
        server: &str,
        remote_name: Option<&str>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.validate_native_mcp_context(work)?;
        self.validate_native_mcp_process_fields(
            task, owner_fence, operation, server, remote_name, work,
        )
    }

    /// Checks that same approved invocation under the SDK's genuine reconciliation source.
    #[allow(clippy::too_many_arguments, reason = "one exact approved MCP invocation binding")]
    pub(crate) fn validate_native_mcp_process_reconciliation_binding(
        &self,
        task: crate::TaskId,
        owner_fence: &crate::scheduler::LeaseFence,
        operation: crate::OperationId,
        server: &str,
        remote_name: Option<&str>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.validate_native_mcp_reconciliation_context(work)?;
        self.validate_native_mcp_process_fields(
            task, owner_fence, operation, server, remote_name, work,
        )
    }

    #[allow(clippy::too_many_arguments, reason = "one exact approved MCP invocation binding")]
    fn validate_native_mcp_process_fields(
        &self,
        task: crate::TaskId,
        owner_fence: &crate::scheduler::LeaseFence,
        operation: crate::OperationId,
        server: &str,
        remote_name: Option<&str>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if !self.task().original_task_binding_matches(task, owner_fence)
            || self.operation_id() != operation
        {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "approved native MCP process differs from its task, fence or operation"));
        }
        let mut call_id = [0; 36];
        let uuid = uuid::Uuid::from_bytes(operation.into_bytes());
        let expected_call: &str = uuid.hyphenated().encode_lower(&mut call_id);
        if self.call_id() != expected_call {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "approved native MCP process differs from its original call identity"));
        }
        let mut server_allowed = false;
        let mut tool_allowed = remote_name.is_none();
        for grant in self.effective_scope().grants().iter() {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            server_allowed |= grant.strip_prefix("mcp:call:") == Some(server);
            if let Some(remote_name) = remote_name {
                tool_allowed |= grant.strip_prefix("tool:call:mcp.")
                    .and_then(|suffix| suffix.strip_prefix(server))
                    .and_then(|suffix| suffix.strip_prefix('.')) == Some(remote_name);
            }
        }
        if !server_allowed || !tool_allowed {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "approved native MCP process is outside the effective tool scope"));
        }
        Ok(())
    }
}

struct NativeContextVerifier<P: acyclic_stream::StreamProvider> {
    context: Arc<NativeTaskContext>,
    owner: Arc<TaskJournalOwner<P>>,
    custody: NativeAllocation,
}

impl<P: acyclic_stream::StreamProvider> NativeContextVerifier<P> {
    fn original_binding(&self) -> Result<()> {
        let (task, _) = self.owner.task_binding();
        let context = self.context.context();
        if context.durable_task != Some(task) || context.task_id != self.owner.operation_id() {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "content READ owner differs from the issued task context"));
        }
        self.owner.require_mail_scope_with_native_work(context.scope(), self.custody.work())
    }

    async fn authorize_native(
        &self, reference: &FileRef, maximum_bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(WorkCounters::default())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.original_binding()?;
        if !self.custody.work().shares_allocation_context(work) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "content READ callback differs from its original native ledger"));
        }
        if reference.descriptor().byte_length() > maximum_bytes {
            return Err(boundary(crate::wire::ErrorCode::Invalid,
                "content exceeds the native callback byte bound"));
        }
        self.owner.validate_input_file(reference, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        self.owner.verify_read_access(&mut Some(&mut crate::store::history::native_work_io(work)?)).await
    }

    async fn authorize(&self, reference: &FileRef) -> Result<()> {
        self.original_binding()?;
        self.owner.validate_input_file(reference, None)?;
        self.owner.verify_read_access(&mut None).await
    }
}

impl<P: acyclic_stream::StreamProvider + 'static> ContentResidencyVerifier for NativeContextVerifier<P> {
    fn verify<'a>(&'a self, reference: &'a FileRef, maximum_bytes: u64,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            drop(self.read(reference, maximum_bytes, io)?.await?);
            if let Some(work) = io.and_then(|io| io.original_native_work()) {
                self.authorize_native(reference, maximum_bytes, work).await
            } else { self.authorize(reference).await }
        })
    }

    fn read<'a>(&'a self, reference: &'a FileRef, maximum_bytes: u64,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if let Some(work) = io.and_then(|io| io.original_native_work()) {
                self.authorize_native(reference, maximum_bytes, work).await?;
                let bytes = self.context.context.read_file_with_native_work(reference, work).await?;
                self.authorize_native(reference, maximum_bytes, work).await?;
                Ok(bytes)
            } else {
                if reference.descriptor().byte_length() > maximum_bytes {
                    return Err(boundary(crate::wire::ErrorCode::Invalid, "content exceeds the callback byte bound"));
                }
                self.authorize(reference).await?;
                let bytes = self.context.context.read_file(reference).await?;
                self.authorize(reference).await?;
                Ok(bytes)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable_host::tests::created_resource_access::CreationFixture;

    fn budget() -> acyclic_fs::WorkBudget {
        let units = 16_384;
        let bytes = 16 * 1_024 * 1_024;
        acyclic_fs::WorkBudget {
            authority_records_read: units, authority_records_appended: units,
            authority_bytes_read: bytes, authority_bytes_written: bytes,
            object_probes: units, backend_read_operations: units, backend_write_operations: units,
            durability_operations: units, page_reads: units, page_writes: units,
            object_bytes_read: bytes, object_bytes_written: bytes, bytes_hashed: bytes,
            bytes_copied: bytes, bytes_encoded: bytes, source_bytes_read: bytes,
            source_path_components: units, source_entries_visited: units, output_bytes: bytes,
            items_examined: units, items_returned: units, allocation_operations: units,
            peak_allocation_bytes: bytes, materializations: units,
        }
    }

    fn work(cancellation: &acyclic_fs::CancellationToken) -> Result<NativeMaterializationWork> {
        NativeMaterializationWork::new(budget(), cancellation)
            .map_err(|failure| Error::NativeWork { cause: failure.error, custody: None })
    }

    #[tokio::test]
    async fn native_read_context_cannot_bind_an_approved_mcp_dispatch() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("finite original process binding ledger");
        let operation = fixture.root_lease.operation.operation_id;
        let (context, custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, operation, &original,
        ).await?;
        let shared = context.into_native_shared(custody, &original)?;
        let mut call_id = [0; 36];
        let uuid = uuid::Uuid::from_bytes(operation.into_bytes());
        let tool = ToolContext::new_with_native_work(
            shared, operation, uuid.hyphenated().encode_lower(&mut call_id), &original,
        )?;
        let fence = crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation);
        let before = original.receipt();
        assert!(matches!(tool.validate_native_mcp_process_binding(
            fixture.root, &fence, operation, "example", Some("echo"), &original,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert!(matches!(tool.validate_native_mcp_process_reconciliation_binding(
            fixture.root, &fence, operation, "example", Some("echo"), &original,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(original.receipt(), before);
        cancellation.cancel();
        assert!(matches!(tool.validate_native_mcp_process_binding(
            fixture.root, &fence, operation, "example", Some("echo"), &original,
        ), Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert!(matches!(tool.validate_native_mcp_process_reconciliation_binding(
            fixture.root, &fence, operation, "example", Some("echo"), &original,
        ), Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert_eq!(original.receipt(), before);
        Ok(())
    }

    #[tokio::test]
    async fn ordinary_context_cannot_be_qualified_by_an_opaque_allocation() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation)?;
        let context = fixture.harness.durable_context(
            fixture.root, fixture.root_lease.operation.operation_id,
        ).await?;
        let before = original.receipt();
        assert!(matches!(context.require_original_native_work(&original),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(original.receipt(), before);
        let custody = original.claim(65_536)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let before = original.receipt();
        let rejected = context.into_native_shared(custody, &original);
        assert!(matches!(rejected, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
        assert_eq!(original.receipt(), before);
        assert_eq!(original.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn native_metadata_receipt_rejects_clone_foreign_and_short_custody() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation)?;
        let (context, custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        assert!(custody.bytes() > 0);
        let clone = context.clone();
        let opaque = original.claim(custody.bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let before = original.receipt();
        let cloned = clone.into_native_shared(opaque, &original);
        assert!(matches!(cloned, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
        assert_eq!(original.receipt(), before);
        drop(context);
        drop(custody);

        let (context, custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        let foreign = work(&cancellation)?;
        let before = original.receipt();
        let rejected = context.into_native_shared(custody, &foreign);
        assert!(matches!(rejected, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
        assert_eq!(original.receipt(), before);

        let (context, mut custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        let source_bytes = context.native_metadata_source.as_ref()
            .expect("genuine native metadata source").bytes;
        assert!(source_bytes > 0);
        let removed = custody.bytes() - (source_bytes - 1);
        let released = custody.split_off(removed)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let before = original.receipt();
        let rejected = context.into_native_shared(custody, &original);
        assert!(matches!(rejected, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
        assert_eq!(original.receipt(), before);
        drop(released);
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_shared_context_preserves_original_cause_and_metadata_custody() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation)?;
        let (context, custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        let bytes = custody.bytes();
        cancellation.cancel();
        let before = original.receipt();
        let rejected = context.into_native_shared(custody, &original);
        match rejected {
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                custody: Some(custody),
            }) => {
                assert_eq!(custody.bytes(), bytes);
                assert!(custody.work().shares_allocation_context(&original));
                drop(custody);
            }
            _ => panic!("cancelled shared allocation must preserve the original cause and custody"),
        }
        assert_eq!(original.receipt(), before);
        Ok(())
    }

    #[tokio::test]
    async fn shared_native_content_uses_original_owner_and_keeps_real_output_custody() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let created = fixture.create_filesystem_child().await?;
        let task = crate::TaskId::from_bytes([84; 16]);
        assert!(matches!(fixture.admit_child(created.request.child.clone()).await?,
            crate::Admission::Accepted(admitted) if admitted == task));
        let observation = fixture.host.observe_task(task).await?;
        let reservation = observation.reservation
            .ok_or_else(|| Error::NotFound("created native context lease".into()))?;
        let fence = crate::scheduler::LeaseFence::from(&reservation);
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation)?;
        let (context, custody) = fixture.harness.durable_context_with_native_work(
            task, created.request.child.operation_id, &original,
        ).await?;
        let shared = context.into_native_shared(custody, &original)?;
        let mcp_tool = ToolContext::new_with_native_work(
            Arc::clone(&shared), crate::OperationId::from_bytes([120; 16]),
            "native-mcp-control", &original,
        )?;
        let before = original.receipt();
        assert!(matches!(mcp_tool.validate_native_mcp_context(&original),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert!(matches!(mcp_tool.validate_native_mcp_reconciliation_context(&original),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert!(matches!(mcp_tool.original_admission_with_native_work(&original),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(original.receipt(), before);
        let foreign = work(&cancellation)?;
        let foreign_before = foreign.receipt();
        assert!(matches!(mcp_tool.validate_native_mcp_reconciliation_context(&foreign),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert!(matches!(mcp_tool.original_admission_with_native_work(&foreign),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(foreign.receipt(), foreign_before);
        let unqualified = mcp_tool.clone();
        assert!(matches!(unqualified.validate_native_mcp_context(&original),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert!(matches!(unqualified.validate_native_mcp_reconciliation_context(&original),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert!(matches!(unqualified.original_admission_with_native_work(&original),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(original.receipt(), before);
        drop(unqualified);
        let owner = fixture.host.journal_owner_with_native_work(task, &fence, &original).await?
            .into_native_shared(&original)?;
        let reader = shared.content_residency_verifier_with_native_work(
            Arc::clone(&owner), &original,
        )?;
        let staged = shared.context().stage_file_once_with_native_work(
            crate::OperationId::from_bytes([118; 16]), "native/shared.txt",
            b"same issued context", "text/plain", "shared.txt", &original,
        ).await?;
        let before = original.live_allocation_bytes();
        let reads = original.receipt().backend_read_operations;
        let bytes = reader.read_with_native_work(&staged.0, 65_536, &original)?.await?;
        assert!(original.receipt().backend_read_operations > reads);
        assert_eq!(bytes.as_ref(), b"same issued context");
        assert!(bytes.matches_meter(&original.runtime_meter_owner()));
        let held = original.live_allocation_bytes() - before;
        assert!(held > 0);
        let physical = original.receipt();
        let shared_bytes = bytes.clone();
        drop(bytes);
        assert_eq!(shared_bytes.as_ref(), b"same issued context");
        assert!(shared_bytes.matches_meter(&original.runtime_meter_owner()));
        assert_eq!(original.live_allocation_bytes(), before + held);
        drop(shared_bytes);
        assert_eq!(original.live_allocation_bytes(), before);
        assert_eq!(original.receipt(), physical);
        reader.verify_with_native_work(&staged.0, 65_536, &original)?.await?;

        let root_fence = crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation);
        let root_owner = fixture.host.journal_owner_with_native_work(
            fixture.root, &root_fence, &original,
        ).await?.into_native_shared(&original)?;
        let before = original.receipt();
        let wrong_owner = shared.content_residency_verifier_with_native_work(
            Arc::clone(&root_owner), &original,
        );
        assert!(matches!(wrong_owner, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
        assert_eq!(original.receipt(), before);

        let unmeasured = Arc::new(fixture.host.journal_owner(task, fence.clone()).await?);
        let before = original.receipt();
        let rejected = shared.content_residency_verifier_with_native_work(unmeasured, &original);
        assert!(matches!(rejected, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
        assert_eq!(original.receipt(), before);

        let (root, custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        let root = root.into_native_shared(custody, &original)?;
        let root_reader = root.content_residency_verifier_with_native_work(root_owner, &original)?;
        let reads = original.receipt().backend_read_operations;
        assert!(matches!(root_reader.read_with_native_work(&staged.0, 65_536, &original)?.await,
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(original.receipt().backend_read_operations, reads);

        let available = budget().peak_allocation_bytes - original.live_allocation_bytes();
        let quota = original.claim(available)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut buffer = Vec::<u8>::new();
        buffer.try_reserve_exact(usize::try_from(available).expect("fixture byte bound"))
            .expect("finite fixture allocation");
        let blocker = (buffer, quota);
        let reads = original.receipt().backend_read_operations;
        let denied = reader.read_with_native_work(&staged.0, 65_536, &original);
        assert!(matches!(denied, Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }),
            ..
        })));
        assert_eq!(original.receipt().backend_read_operations, reads);
        drop(blocker);
        cancellation.cancel();
        let before = original.receipt();
        assert!(matches!(mcp_tool.validate_native_mcp_context(&original),
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert!(matches!(mcp_tool.validate_native_mcp_reconciliation_context(&original),
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert!(matches!(mcp_tool.original_admission_with_native_work(&original),
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert!(matches!(reader.read_with_native_work(&staged.0, 65_536, &original),
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert_eq!(original.receipt(), before);
        Ok(())
    }

    #[tokio::test]
    async fn native_tool_context_borrows_the_issued_context_and_invalidates_clone_funding() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation)?;
        let (context, custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        let shared = context.into_native_shared(custody, &original)?;
        assert!(shared.native_work().shares_allocation_context(&original));
        shared.context().require_original_native_work(&original)?;
        let operation = crate::OperationId::from_bytes([119; 16]);
        let call_id = operation.to_string();
        let baseline = original.live_allocation_bytes();
        let before = original.receipt();
        let tool = ToolContext::new_with_native_work(
            Arc::clone(&shared), operation, &call_id, &original,
        )?;
        assert!(std::ptr::eq(tool.task(), shared.context()));
        assert!(Arc::ptr_eq(tool.native_task().expect("original native task"), &shared));
        assert!(std::ptr::eq(&*tool, shared.context()));
        assert_eq!(tool.operation_id(), operation);
        assert_eq!(tool.call_id(), call_id);
        assert!(tool.native_metadata_matches(&original));
        assert!(tool.stock_native_interactions().is_none());
        assert!(original.receipt().bytes_copied >= before.bytes_copied + call_id.len() as u64);
        let held = tool.native_custody.as_ref().expect("genuine SDK tool metadata").bytes();
        assert!(held >= std::mem::size_of::<ToolContext>() as u64 + tool.call_id.capacity() as u64);
        assert_eq!(original.live_allocation_bytes(), baseline + held);

        let before = original.receipt();
        let clone = tool.clone();
        assert!(std::ptr::eq(clone.task(), shared.context()));
        assert!(Arc::ptr_eq(clone.native_task().expect("same shared task"), &shared));
        assert!(!clone.native_metadata_matches(&original));
        assert_eq!(clone.call_id(), tool.call_id());
        assert_eq!(original.receipt(), before);
        drop(tool);
        assert_eq!(original.live_allocation_bytes(), baseline);
        drop(clone);

        let foreign = work(&cancellation)?;
        let before = foreign.receipt();
        assert!(matches!(ToolContext::new_with_native_work(
            Arc::clone(&shared), operation, &call_id, &foreign,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(foreign.receipt(), before);
        let before = original.receipt().allocation_operations;
        assert!(matches!(ToolContext::new_with_native_work(
            Arc::clone(&shared), operation, "bad/call", &original,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        assert_eq!(original.receipt().allocation_operations, before);

        let available = budget().peak_allocation_bytes - original.live_allocation_bytes();
        let quota = original.claim(available)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut buffer = Vec::<u8>::new();
        buffer.try_reserve_exact(usize::try_from(available).expect("fixture byte bound"))
            .expect("finite fixture allocation");
        let blocker = (buffer, quota);
        let allocations = original.receipt().allocation_operations;
        assert!(matches!(ToolContext::new_with_native_work(
            Arc::clone(&shared), operation, &call_id, &original,
        ), Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }), ..
        })));
        assert_eq!(original.receipt().allocation_operations, allocations);
        drop(blocker);
        cancellation.cancel();
        let before = original.receipt();
        assert!(matches!(ToolContext::new_with_native_work(
            shared, operation, &call_id, &original,
        ), Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert_eq!(original.receipt(), before);
        Ok(())
    }

    #[tokio::test]
    async fn native_tool_scope_restricts_without_replacing_the_issued_context_or_lineage() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let created = fixture.create_filesystem_child().await?;
        let task = crate::TaskId::from_bytes([84; 16]);
        assert!(matches!(fixture.admit_child(created.request.child.clone()).await?,
            crate::Admission::Accepted(admitted) if admitted == task));
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation)?;
        let (context, custody) = fixture.harness.durable_context_with_native_work(
            task, created.request.child.operation_id, &original,
        ).await?;
        let shared = context.into_native_shared(custody, &original)?;
        let read = created.private.capability(crate::conversation::VolumeOperation::Read)?;
        let write = created.private.capability(crate::conversation::VolumeOperation::Write)?;
        assert!(shared.context().scope().grants().contains(&write));
        let limits = shared.context().scope().limits();
        let requested = RuntimeScope::new(crate::Capabilities::new([read.as_str()]), limits)?
            .with_run_limits(shared.context().scope().run_limits())?;
        let operation = crate::OperationId::from_bytes([121; 16]);
        let tool = ToolContext::new_with_native_work(
            Arc::clone(&shared), operation, "scoped-control", &original,
        )?.with_native_scope(&requested, &original)?;
        assert!(Arc::ptr_eq(tool.native_task().expect("same issued task"), &shared));
        assert!(std::ptr::eq(tool.task(), shared.context()));
        assert!(tool.effective_scope().grants().contains(&read));
        assert!(!tool.effective_scope().grants().contains(&write));
        assert!(shared.context().scope().grants().contains(&write));
        assert_eq!(tool.effective_scope().run_limits(), shared.context().scope().run_limits());
        assert_eq!(tool.effective_scope().resource_proofs, shared.context().scope().resource_proofs);
        let original_access = shared.context().scope().created_access.as_ref()
            .expect("genuine created-resource provenance");
        assert!(Arc::ptr_eq(tool.effective_scope().created_access.as_ref()
            .expect("derived original provenance"), original_access));
        assert!(requested.created_access.is_none());
        assert!(tool.native_metadata_matches(&original));
        let (_, guard) = tool.effective_scope.as_ref().expect("one effective scope");
        assert!(guard.as_ref().expect("scope copy guard").work().shares_ledger(&original));
        let before = original.receipt();
        tool.validate_stock_scope_with_native_work(&requested, limits, &original)?;
        assert!(matches!(tool.validate_stock_scope_with_native_work(
            shared.context().scope(), limits, &original,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(original.receipt(), before);
        let mut execution_limits = limits;
        execution_limits.file_bytes = 1;
        assert!(limits.file_bytes > execution_limits.file_bytes);
        let stock = ToolContext::new_with_native_work(
            Arc::clone(&shared), operation, "stock-execution-limits", &original,
        )?.with_native_stock_scope(&requested, execution_limits, &original)?;
        assert!(Arc::ptr_eq(stock.native_task().expect("same issued task"), &shared));
        assert_eq!(stock.effective_scope().limits(), execution_limits);
        assert_eq!(requested.limits(), limits);
        assert_eq!(stock.effective_scope().grants(), requested.grants());
        assert_eq!(stock.effective_scope().run_limits(), requested.run_limits());
        assert!(stock.native_metadata_matches(&original));
        let before = original.receipt();
        assert!(matches!(stock.validate_stock_scope_with_native_work(&requested, limits, &original),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(original.receipt(), before);
        drop(stock);

        let clone = tool.clone();
        assert_eq!(clone.effective_scope().grants(), tool.effective_scope().grants());
        assert_eq!(clone.effective_scope().limits(), tool.effective_scope().limits());
        assert!(!clone.native_metadata_matches(&original));
        let before = original.receipt();
        assert!(matches!(clone.validate_stock_scope_with_native_work(&requested, limits, &original),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(original.receipt(), before);
        assert!(Arc::ptr_eq(clone.native_task().expect("same issued task"), &shared));
        drop(clone);

        let widening = RuntimeScope::new(
            crate::Capabilities::new([read.as_str(), write.as_str()]), limits,
        )?.with_run_limits(shared.context().scope().run_limits())?;
        let candidate = ToolContext::new_with_native_work(
            Arc::clone(&shared), operation, "scope-widen", &original,
        )?.with_native_scope(&requested, &original)?;
        let allocations = original.receipt().allocation_operations;
        assert!(matches!(candidate.with_native_scope(&widening, &original),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(original.receipt().allocation_operations, allocations);

        let mut invalid = limits;
        invalid.file_bytes += 1;
        let allocations = original.receipt().allocation_operations;
        assert!(matches!(shared.context().validate_stock_scope_with_native_work(
            &requested, invalid, &original,
        ), Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized | crate::wire::ErrorCode::Invalid, ..
        })));
        assert_eq!(original.receipt().allocation_operations, allocations);
        let run_widening = RuntimeScope::new(
            crate::Capabilities::new([read.as_str()]), limits,
        )?;
        assert!(shared.context().scope().run_limits().concurrency.is_some());
        let allocations = original.receipt().allocation_operations;
        assert!(matches!(shared.context().validate_stock_scope_with_native_work(
            &run_widening, limits, &original,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(original.receipt().allocation_operations, allocations);
        Ok(())
    }
}
