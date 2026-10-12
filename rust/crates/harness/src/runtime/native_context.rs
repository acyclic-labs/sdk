use super::{
    InteractionRouter, IssuedContextPurpose, Limits, RuntimeScope, TaskContext, ToolContext,
};
use crate::store::history::HistoryIo as _;
use crate::{Error, NativeFutureBundle, Result};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use std::sync::Arc;

pub(super) struct NativeMetadataSource {
    work: NativeMaterializationWork,
    bytes: u64,
    pub(super) purpose: IssuedContextPurpose,
}

pub(super) fn boundary(code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    Error::NativeBoundary { code, reason }
}

fn validate_scope_attenuation(
    original: &RuntimeScope,
    requested: &RuntimeScope,
    limits: Limits,
    work: &NativeMaterializationWork,
) -> Result<()> {
    original.validate_narrowing_with_native_work(&requested.grants, &limits, work)?;
    original.validate_run_limit_attenuation(
        &requested.run_limits,
        &mut Some(&mut crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        )),
    )
}

impl TaskContext {
    /// Records custody after the SDK producer completes metadata allocation.
    pub(super) fn mark_native_metadata_source(
        &mut self,
        work: &NativeMaterializationWork,
        custody: &NativeAllocation,
    ) -> Result<()> {
        work.charge(WorkCounters::default())
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        if !custody.work().shares_allocation_context(work) {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "issued task metadata custody belongs to another original ledger",
            ));
        }
        self.native_metadata_source = Some(NativeMetadataSource {
            work: work.clone(),
            bytes: custody.bytes(),
            purpose: IssuedContextPurpose::Read,
        });
        Ok(())
    }

    /// Requires the SDK's original metadata source and cancellation ledger.
    pub(crate) fn require_original_native_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(WorkCounters::default())
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        if !self
            .native_metadata_source
            .as_ref()
            .is_some_and(|source| source.work.shares_allocation_context(work))
        {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "task context differs from its original native metadata source",
            ));
        }
        Ok(())
    }
    pub(crate) fn require_original_native_dispatch(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_original_native_work(work)?;
        if !self
            .native_metadata_source
            .as_ref()
            .is_some_and(|source| source.purpose == IssuedContextPurpose::Dispatch)
        {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "native task metadata was not issued for fresh dispatch",
            ));
        }
        Ok(())
    }

    /// Borrows the original SDK mailbox binding without copying its fence or deriving a principal from the task identity.
    pub fn owned_task_binding_with_native_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<(crate::TaskId, &crate::scheduler::LeaseFence)> {
        self.require_original_native_work(work)?;
        let mailbox = self.owned_mailbox.as_ref().ok_or_else(|| {
            boundary(
                crate::wire::ErrorCode::Unauthorized,
                "native task has no original SDK mailbox",
            )
        })?;
        mailbox.validate_scope_with_native_work(&self.scope, work)?;
        let (task, fence) = mailbox.task_binding();
        if self.durable_task != Some(task) || !mailbox.matches_task_binding(task, fence) {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "native context differs from its original mailbox binding",
            ));
        }
        Ok((task, fence))
    }

    /// Revalidates the original issued execution owner before a native SDK read.
    pub fn verify_original_execution_owner_with_native_work<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, ()>> {
        self.require_original_native_dispatch(work)?;
        let (task, fence) = self.owned_task_binding_with_native_work(work)?;
        let host = self.harness.host.as_ref().ok_or_else(|| {
            boundary(
                crate::wire::ErrorCode::Unsupported,
                "native execution has no original durable owner callback",
            )
        })?;
        self.harness.assert_durable_policy_bindings(Some(work))?;
        host.verify_dispatch_owner_with_native_work(task, fence, work)
    }

    /// Checks stock scope restrictions against the issued source before copying metadata or constructing a callback.
    pub(crate) fn validate_stock_scope_with_native_work(
        &self,
        requested: &RuntimeScope,
        limits: Limits,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_original_native_work(work)?;
        validate_scope_attenuation(&self.scope, requested, limits, work)
    }

    /// Consumes the actual SDK-issued context and its observed metadata custody.
    pub fn into_native_shared(
        mut self,
        mut custody: NativeAllocation,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<TaskContext>> {
        let Some(source) = self.native_metadata_source.as_ref() else {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "task context has no original native metadata source",
            ));
        };
        if !source.work.shares_allocation_context(work)
            || !custody.work().shares_allocation_context(work)
            || custody.bytes() < source.bytes
        {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "task context metadata custody differs from its original source",
            ));
        }
        let mut slot = match work.claim(0, 1, WorkCounters::UNCHARGED) {
            Ok(slot) => slot,
            Err(cause) => {
                drop(self);
                return Err(Error::NativeWork {
                    cause,
                    custody: Some(custody),
                });
            }
        };
        custody
            .absorb(&mut slot)
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        self.native_custody = Some(custody);
        Ok(Arc::new(self))
    }
}

impl TaskContext {
    /// Selects an existing authenticated output writer without widening scope.
    pub async fn with_output_volume_with_native_work(
        self: &Arc<Self>,
        volume: &crate::conversation::VolumeRef,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self.native_metadata_matches(work) {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "task context has no original shared metadata custody",
            ));
        }

        self.require_original_native_work(work)?;
        let purpose = self
            .native_metadata_source
            .as_ref()
            .expect("original native metadata source")
            .purpose;
        if purpose == IssuedContextPurpose::Read {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "readonly metadata cannot select a native output writer",
            ));
        }
        let binding = self
            .scope
            .created_content(Some(volume))
            .filter(|binding| binding.writer.is_some())
            .ok_or_else(|| {
                boundary(
                    crate::wire::ErrorCode::Unauthorized,
                    "output volume has no authenticated created writer",
                )
            })?;
        let mut permission_io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let permission = volume.capability(
            crate::conversation::VolumeOperation::Write,
            &mut Some(&mut permission_io),
        )?;
        let allowed = self.scope.grants().contains(&permission);
        drop(permission);
        drop(permission_io);
        if !allowed {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "original task scope cannot write the selected output volume",
            ));
        }
        let (task, fence) = self.owned_task_binding_with_native_work(work)?;
        let host = self.harness.host.as_ref().ok_or_else(|| {
            boundary(
                crate::wire::ErrorCode::Unsupported,
                "output projection has no original task host",
            )
        })?;
        self.harness.assert_durable_policy_bindings(Some(work))?;
        match purpose {
            IssuedContextPurpose::Dispatch => {
                host.verify_dispatch_owner_with_native_work(task, fence, work)?
                    .await?;
            }
            IssuedContextPurpose::Reconciliation => {
                host.verify_execution_owner_with_native_work(task, fence, work)?
                    .await?;
            }
            IssuedContextPurpose::Read => unreachable!("readonly projection rejected"),
        }
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        io.retain_owned(
            (std::mem::size_of::<TaskContext>() - std::mem::size_of::<RuntimeScope>()) as u64,
        )?;
        let (scope, scope_custody) = self.scope.clone_with_native_work(work)?;
        io.absorb_native_custody(scope_custody)?;
        let policy_overrides = {
            let mut observer = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
            let mut quota = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
            let copied = crate::core::copy_vector(
                &self.policy_overrides,
                &mut quota,
                |(identity, policy), quota| {
                    Ok((
                        crate::registry::ComponentIdentity {
                            name: crate::core::copy_text(&identity.name, quota)?,
                            version: crate::core::copy_text(&identity.version, quota)?,
                            digest: identity.digest,
                        },
                        Arc::clone(policy),
                    ))
                },
            )?;
            quota.transfer();
            copied
        };
        let mut context = TaskContext {
            harness: Arc::clone(&self.harness),
            task_id: self.task_id,
            durable_task: self.durable_task,
            descendants: self.descendants.clone(),
            scope,
            policy_overrides,
            owned_interactions: self.owned_interactions.clone(),
            owned_mailbox: self.owned_mailbox.clone(),
            output_content: Some(binding.clone()),
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            native_root_output: self.native_root_output.clone(),
            model_steps: Arc::clone(&self.model_steps),
            native_metadata_source: None,
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            working_view: std::sync::OnceLock::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        };
        let custody = io.into_custody()?;
        if let Err(error) = context.mark_native_metadata_source(work, &custody) {
            drop(context);
            drop(custody);
            return Err(error);
        }
        context
            .native_metadata_source
            .as_mut()
            .expect("projected native metadata source")
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
        &self,
        view: Arc<dyn crate::filesystem::NativeTaskWorkingView>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if !self.native_metadata_matches(work) {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "task context has no original shared metadata custody",
            ));
        }

        self.require_original_native_dispatch(work)?;
        let (task, fence) = self.owned_task_binding_with_native_work(work)?;
        if view.task_binding() != (task, fence)
            || !view.original_native_work().shares_allocation_context(work)
        {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "working view differs from its original issued task",
            ));
        }
        if self
            .working_view
            .get()
            .is_some_and(|installed| !installed.same_identity(view.as_ref()))
        {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "working view cannot be rebound to a different preparation",
            ));
        }
        view.verify_with_native_work(work)?.await?;
        if self.working_view.get().is_some() {
            return Ok(());
        }
        match self.working_view.set(view) {
            Ok(()) => Ok(()),
            Err(loser) => {
                let same = self
                    .working_view
                    .get()
                    .is_some_and(|winner| winner.same_identity(loser.as_ref()));
                drop(loser);
                if same {
                    Ok(())
                } else {
                    Err(boundary(
                        crate::wire::ErrorCode::Unauthorized,
                        "working view lost installation to a different preparation",
                    ))
                }
            }
        }
    }

    /// Installs the original SDK-created root result destination once on this same issued context.
    #[cfg(feature = "native-execution")]
    pub async fn bind_root_output_context_with_native_work(
        &self,
        source: Arc<crate::filesystem::NativeRootOutputSource>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if !self.native_metadata_matches(work) {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "task context has no original shared metadata custody",
            ));
        }

        self.require_original_native_dispatch(work)?;
        let installed = self.native_root_output.get();
        if installed.is_some_and(|installed| !installed.0.same_identity(&source)) {
            return Err(boundary(
                crate::wire::ErrorCode::Conflict,
                "root result destination differs from its original binding",
            ));
        }
        source.verify_context_with_native_work(self, work).await?;
        if installed.is_some() {
            return Ok(());
        }
        let source = source.into_native_shared(work)?;
        let content = source.retained_content_binding();
        let binding = (source, content);
        if let Err(binding) = self.native_root_output.set(binding) {
            if self
                .native_root_output
                .get()
                .is_some_and(|installed| installed.0.same_identity(&binding.0))
            {
                return Ok(());
            }
            return Err(boundary(
                crate::wire::ErrorCode::Conflict,
                "root result destination was bound to another original resource",
            ));
        }
        Ok(())
    }

    /// Borrows the original metadata ledger without granting resource authority.
    pub(crate) fn native_work(&self) -> Result<&NativeMaterializationWork> {
        self.native_custody
            .as_ref()
            .filter(|custody| self.native_metadata_matches(custody.work()))
            .map(NativeAllocation::work)
            .ok_or_else(|| {
                boundary(
                    crate::wire::ErrorCode::Unauthorized,
                    "task context has no original native custody",
                )
            })
    }

    /// Checks custody for this context's original metadata.
    pub(crate) fn native_metadata_matches(&self, work: &NativeMaterializationWork) -> bool {
        self.native_custody.as_ref().is_some_and(|custody| {
            custody.work().shares_allocation_context(work)
                && self.native_metadata_source.as_ref().is_some_and(|source| {
                    source.work.shares_allocation_context(work) && custody.bytes() >= source.bytes
                })
        })
    }
}

impl ToolContext {
    /// Records this invocation's committed ToolStarted acceptance.
    pub(crate) fn record_native_tool_acceptance(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.validate_native_mcp_context(work)?;
        let _ = self.accepted_native_tool.set(());
        Ok(())
    }

    fn accepted_native_context(&self, work: &NativeMaterializationWork) -> Result<&TaskContext> {
        let context = self.validate_native_bound_context(work)?;
        if !context
            .native_metadata_source
            .as_ref()
            .is_some_and(|source| source.purpose == IssuedContextPurpose::Dispatch)
        {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "original admission observation requires dispatch purpose",
            ));
        }
        self.accepted_native_tool.get().ok_or_else(|| {
            boundary(
                crate::wire::ErrorCode::Unauthorized,
                "original admission observation requires protected ToolStarted acceptance",
            )
        })?;
        Ok(context)
    }

    /// Observes this accepted invocation's original protected task input.
    pub fn original_admission_with_native_work<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (super::TaskAdmissionRecord, NativeAllocation)>> {
        let context = self.accepted_native_context(work)?;
        let task = context
            .durable_task
            .expect("accepted original durable task");
        let host = context.harness.host.as_ref().ok_or_else(|| {
            boundary(
                crate::wire::ErrorCode::Unsupported,
                "original admission has no durable host",
            )
        })?;
        NativeFutureBundle::new(work, || async move {
            context
                .verify_original_execution_owner_with_native_work(work)?
                .await?;
            let observed = host.observe_admission_with_native_work(task, work)?.await?;
            if observed.0.operation_id != context.task_id {
                return Err(boundary(
                    crate::wire::ErrorCode::Unauthorized,
                    "observed input differs from the original issued task",
                ));
            }
            context
                .verify_original_execution_owner_with_native_work(work)?
                .await?;
            Ok(observed)
        })
    }

    #[cfg(feature = "native-execution")]
    fn accepted_native_working_view(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<(&TaskContext, &dyn crate::filesystem::NativeTaskWorkingView)> {
        let context = self.accepted_native_context(work)?;
        let shared = self
            .native_task()
            .expect("validated original native tool context");
        let view = shared.working_view.get().ok_or_else(|| {
            boundary(
                crate::wire::ErrorCode::NotFound,
                "issued task has no prepared working view",
            )
        })?;
        let (task, fence) = view.task_binding();
        if !context.original_task_binding_matches(task, fence)
            || !view.original_native_work().shares_allocation_context(work)
        {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "working metadata differs from the original issued task",
            ));
        }
        Ok((context, view.as_ref()))
    }

    /// Borrows only the actual protected mount metadata before normalization.
    #[cfg(feature = "native-execution")]
    pub fn native_working_manifest_with_native_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<&crate::filesystem::NativeViewManifest> {
        self.accepted_native_working_view(work)
            .map(|(_, view)| view.manifest())
    }

    #[cfg(feature = "native-execution")]
    fn check_native_working_permission(
        &self,
        view: &dyn crate::filesystem::NativeTaskWorkingView,
        relative: &std::path::Path,
        operation: crate::conversation::VolumeOperation,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let volume = view.volume_for_builtin_path(relative, operation, work)?;
        let mut permission_io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let permission = volume.capability(operation, &mut Some(&mut permission_io))?;
        let allowed = self.effective_scope().grants().contains(&permission);
        drop(permission);
        drop(permission_io);
        if allowed {
            Ok(())
        } else {
            Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "effective tool scope cannot access the selected mounted volume",
            ))
        }
    }

    /// Borrows the original held namespace only inside an accepted tool callback.
    #[cfg(feature = "native-execution")]
    pub(crate) async fn native_working_view_with_native_work<'a>(
        &'a self,
        relative: &'a std::path::Path,
        operation: crate::conversation::VolumeOperation,
        work: &'a NativeMaterializationWork,
    ) -> Result<&'a dyn crate::filesystem::NativeTaskWorkingView> {
        let (context, view) = self.accepted_native_working_view(work)?;
        self.check_native_working_permission(view, relative, operation, work)?;
        context
            .verify_original_execution_owner_with_native_work(work)?
            .await?;
        view.verify_with_native_work(work)?.await?;
        Ok(view)
    }

    /// Checks the accepted action's path and effective permission.
    #[cfg(feature = "native-execution")]
    pub async fn authorize_working_file_with_native_work(
        &self,
        relative: &std::path::Path,
        operation: crate::conversation::VolumeOperation,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.native_working_view_with_native_work(relative, operation, work)
            .await?;
        Ok(())
    }

    /// Reads the held selection under this invocation's effective scope.
    #[cfg(feature = "native-execution")]
    pub async fn read_working_file_with_native_work(
        &self,
        relative: &std::path::Path,
        maximum: usize,
        work: &NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let view = self
            .native_working_view_with_native_work(
                relative,
                crate::conversation::VolumeOperation::Read,
                work,
            )
            .await?;
        view.read_builtin_file(self.operation_id, relative, maximum, work)?
            .await
    }

    /// Mutates only the same selected held path after effective WRITE checks.
    #[cfg(feature = "native-execution")]
    pub async fn write_working_file_with_native_work(
        &self,
        relative: &std::path::Path,
        content: &str,
        create_parents: bool,
        expected_sha256: Option<[u8; 32]>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if expected_sha256.is_some() {
            let (_, view) = self.accepted_native_working_view(work)?;
            self.check_native_working_permission(
                view,
                relative,
                crate::conversation::VolumeOperation::Read,
                work,
            )?;
        }
        let view = self
            .native_working_view_with_native_work(
                relative,
                crate::conversation::VolumeOperation::Write,
                work,
            )
            .await?;
        view.write_builtin_file(
            self.operation_id,
            relative,
            content,
            create_parents,
            expected_sha256,
            work,
        )?
        .await
    }

    /// Checks stock restrictions against the effective scope.
    pub(crate) fn validate_stock_scope_with_native_work(
        &self,
        requested: &RuntimeScope,
        limits: Limits,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.task().require_original_native_work(work)?;
        if !self.native_metadata_matches(work) {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "stock scope has no original native tool metadata source",
            ));
        }
        validate_scope_attenuation(self.effective_scope(), requested, limits, work)
    }

    /// Narrows tool authority and execution limits while preserving its issued task.
    pub fn with_native_scope(
        mut self,
        requested: &RuntimeScope,
        execution_limits: Limits,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        self.validate_stock_scope_with_native_work(requested, execution_limits, work)?;
        let original = self.effective_scope();
        let grants = requested.grants.clone_with_native_work(work)?;
        let mut narrowed = original.narrow_with_native_work(grants, execution_limits, work)?;
        narrowed.0 = narrowed.0.with_run_limits(
            requested.run_limits,
            &mut Some(&mut crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            )),
        )?;
        self.effective_scope = Some((narrowed.0, Some(narrowed.1)));
        Ok(self)
    }

    /// Retains the issued interaction router.
    pub(crate) fn with_native_interactions(
        mut self,
        router: Arc<dyn InteractionRouter>,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        work.charge(WorkCounters::default())
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        if !self.native_metadata_matches(work) {
            return Err(boundary(
                crate::wire::ErrorCode::Unauthorized,
                "interaction override has no original native tool metadata source",
            ));
        }
        self.native_interactions = Some(router);
        Ok(self)
    }

    /// Borrows the actual per-command router for the original policy pipeline.
    pub(crate) fn stock_native_interactions(&self) -> Option<&dyn InteractionRouter> {
        self.native_interactions.as_deref()
    }

    /// Borrows the original shared task without reconstructing its owner.
    pub(crate) fn native_task(&self) -> Option<&Arc<TaskContext>> {
        self.task.native_custody.as_ref().map(|_| &self.task)
    }

    /// Checks genuine SDK tool metadata funding without granting tool authority.
    pub(crate) fn native_metadata_matches(&self, work: &NativeMaterializationWork) -> bool {
        let task = &self.task;
        let Some(custody) = &self.native_custody else {
            return false;
        };
        task.native_metadata_matches(work)
            && custody.work().shares_allocation_context(work)
            && (std::mem::size_of::<Self>() as u64)
                .checked_add(self.call_id.capacity() as u64)
                .is_some_and(|bytes| custody.bytes() >= bytes)
            && self.effective_scope.as_ref().is_none_or(|(_, guard)| {
                guard
                    .as_ref()
                    .is_some_and(|guard| guard.work().shares_allocation_context(work))
            })
    }
}

#[cfg(test)]
mod tests;
