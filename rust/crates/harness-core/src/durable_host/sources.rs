//! Owner-bound project source policy.
use super::source_graph::SourceBudget;
use super::*;
use crate::Capabilities;
use crate::core::AuthorityIssuer;
use crate::source_authorization::{
    ProjectSourceAuthorization, ProjectSourceReadHandle, ProjectSourceSnapshot,
    RootProjectSourceContext, RootProjectSourceProvider,
};
impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Authorizes a bounded project selection under the original owner.
    #[allow(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "Explicit authority and bounds feed one funded authorization future without a request carrier."
    )]
    pub async fn authorize_project_source(
        &self,
        context: &crate::runtime::TaskContext,
        issuer: &AuthorityIssuer,
        provider: &dyn RootProjectSourceProvider,
        source_task: TaskId,
        operation_id: OperationId,
        snapshot: &ProjectSourceSnapshot,
        maximum_work: u64,
        maximum_bytes: u64,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<ProjectSourceReadHandle> {
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        #[cfg(not(target_arch = "wasm32"))]
        let native = io
            .map(|io| {
                io.original_native_work().ok_or_else(|| {
                    crate::store::guarded_boundary(
                        quota.work(),
                        crate::wire::ErrorCode::Unsupported,
                        "project source requires original native work",
                    )
                })
            })
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let bound = match native {
            Some(native) => {
                self.require_mail_scope(context.scope(), io)?;
                context.owned_task_binding(Some(
                    &crate::executor::PreparationIo::from_native_work(native)?,
                ))? == (self.task_id(), &self.fence)
            }
            None => {
                context.durable_task_id() == Some(self.task_id())
                    && context.owned_task_binding(None)?.1 == &self.fence
            }
        };
        #[cfg(target_arch = "wasm32")]
        let bound = context.durable_task_id() == Some(self.task_id())
            && context.owned_task_binding(None)?.1 == &self.fence;
        if !bound
            || context.scope().grants() != &self.admission.grants
            || maximum_work == 0
            || maximum_work == u64::MAX
            || maximum_bytes == 0
            || maximum_bytes == u64::MAX
            || operation_id == self.operation_id()
        {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source bounds or original owner differs",
            ));
        }
        let cancellation = self.invocation_cancellation.as_ref().ok_or_else(|| {
            crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source requires actual invocation cancellation",
            )
        })?;
        let check = || {
            cancellation.check().map_err(|cause| {
                #[cfg(not(target_arch = "wasm32"))]
                if native.is_some() {
                    return Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled);
                }
                Error::from(cause)
            })
        };
        check()?;
        self.verify(false, quota.work()).await?;
        let verifier = issuer.verifier(quota.work())?;
        let verifier_held = if quota.work().is_some() {
            verifier.logical_owned_bytes()?
        } else {
            0
        };
        quota.adopt(verifier_held)?;
        if verifier != self.host.verifier {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source signer differs from installed owner",
            ));
        }
        drop(verifier);
        quota.relinquish(verifier_held)?;
        snapshot.project.validate(quota.work())?;
        snapshot.generation.validate(quota.work())?;
        if snapshot.project.class() != crate::conversation::VolumeClass::Project
            || snapshot.project.provider() != snapshot.generation.as_resource().provider()
        {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "source selection is not one provider's project generation",
            ));
        }
        let bytes = crate::executor::json::json_byte_length(snapshot, maximum_bytes, quota.work())?;
        let mut budget = SourceBudget {
            work: maximum_work,
            bytes: maximum_bytes,
        };
        budget.charge(1, bytes, quota.work())?;
        let (caller, caller_held) = self
            .host
            .source_admission(self.task_id(), &mut budget, quota.work())
            .await?;
        quota.adopt(caller_held)?;
        if crate::executor::json::canonical_json_digest(
            &caller,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            quota.work(),
        )? != self.admission.canonical_digest_with_work(quota.work())?
        {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "original project source caller admission changed",
            ));
        }
        let (source, source_held) = self
            .host
            .source_admission(source_task, &mut budget, quota.work())
            .await?;
        quota.adopt(source_held)?;
        let graph_root = self
            .host
            .source_graph_root(self.task_id(), &mut budget, quota.work())
            .await?;
        if self
            .host
            .source_graph_root(source_task, &mut budget, quota.work())
            .await?
            != graph_root
        {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source belongs to another actual session graph",
            ));
        }
        let created_project_authenticated = self
            .host
            .source_created_project(&source, &snapshot.project, &mut budget, quota.work())
            .await?;
        if (!created_project_authenticated && source_task != graph_root)
            || budget.work == 0
            || budget.bytes == 0
        {
            return Err(crate::store::guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source has no bounded authenticated creator",
            ));
        }
        let policy_context = RootProjectSourceContext {
            owner: self.host.verifier.audience(),
            caller: &caller,
            source: &source,
            graph_root,
            operation_id,
            maximum_work: budget.work,
            maximum_bytes: budget.bytes,
            created_project_authenticated,
            cancellation,
        };
        #[cfg(not(target_arch = "wasm32"))]
        match native {
            Some(native) => {
                provider
                    .verify_with_native_work(policy_context, snapshot, native)?
                    .await?;
            }
            None => provider.verify(policy_context, snapshot).await?,
        }
        #[cfg(target_arch = "wasm32")]
        provider.verify(policy_context, snapshot).await?;
        self.verify(false, quota.work()).await?;
        check()?;
        drop(caller);
        drop(source);
        quota.relinquish(caller_held)?;
        quota.relinquish(source_held)?;
        let owner = crate::core::copy_authority(self.host.verifier.audience(), &mut quota)?;
        let caller_fence = self.fence.clone_with_work(quota.work())?;
        let project = snapshot.project.copy(quota.work())?;
        let reference = crate::core::copy_resource(snapshot.generation.as_resource(), &mut quota)?;
        let generation = crate::resources::GenerationRef::from_resource(reference, quota.work())?;
        let native_snapshot = snapshot
            .native_snapshot
            .as_ref()
            .map(|value| {
                crate::core::copy(value, &mut quota, |value, _| {
                    crate::executor::json::json_value_logical_owned_bytes(value)
                })
            })
            .transpose()?;
        let mut authorization = ProjectSourceAuthorization {
            owner,
            caller_task: self.task_id(),
            caller_admission_digest: self.admission.canonical_digest_with_work(quota.work())?,
            caller_fence,
            operation_id,
            source_task,
            project,
            generation,
            native_snapshot,
            graph_root,
            maximum_work,
            maximum_bytes,
            policy_scope: Scope::from_wire(
                String::new(),
                Capabilities::default(),
                String::new(),
                None,
                None,
                [0; 32],
                Vec::new(),
            ),
        };
        // The policy label authenticates a descriptor; it grants no volume access.
        let label = authorization.scope_label(quota.work())?;
        let label_held = if quota.work().is_some() {
            label.capacity() as u64
        } else {
            0
        };
        quota.adopt(label_held)?;
        let capability = crate::core::copy_text(&label, &mut quota)?;
        if let Some(io) = quota.work().as_deref_mut() {
            io.admit_owned(std::mem::size_of::<String>() as u64)?;
            io.retain_owned(std::mem::size_of::<String>() as u64)?;
        }
        let capabilities = Capabilities::new([capability]);
        let capabilities_held = if quota.work().is_some() {
            capabilities.logical_owned_bytes()?
        } else {
            0
        };
        authorization.policy_scope =
            issuer.issue(&label, &capabilities, None, None, quota.work())?;
        drop(capabilities);
        drop(label);
        quota.relinquish(label_held)?;
        // copy_text adopted the capability allocation; the vector slot was retained above.
        if quota.work().is_some() {
            quota.adopt(std::mem::size_of::<String>() as u64)?;
        }
        quota.relinquish(capabilities_held)?;
        let handle =
            ProjectSourceReadHandle::seal(authorization, &self.host.verifier, quota.work())?;
        quota.transfer();
        Ok(handle)
    }

    /// Restores a retained source authorization under its original caller.
    pub async fn restore_project_source_authorization(
        self: &Arc<Self>,
        context: &crate::runtime::TaskContext,
        authorization: &ProjectSourceAuthorization,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ProjectSourceReadHandle> {
        let mut history = io.map(|io| io.history_io());
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        self.require_mail_scope(context.scope(), io)?;
        if !context.original_task_binding_matches(self.task_id(), &self.fence)
            || authorization.caller_admission_digest
                != self.admission.canonical_digest_with_work(&mut work)?
        {
            return Err(Error::Unauthorized("project source caller differs".into()));
        }
        self.check_project_source_invocation()?;
        self.verify(false, &mut work).await?;
        let handle = self
            .host
            .seal_project_source_authorization(authorization, self.task_id(), io)
            .await?;
        self.verify(false, &mut work).await?;
        self.check_project_source_invocation()?;
        Ok(handle)
    }
    fn check_project_source_invocation(&self) -> Result<()> {
        self.invocation_cancellation
            .as_ref()
            .ok_or_else(|| {
                Error::Unauthorized("project source requires invocation cancellation".into())
            })?
            .check()
            .map_err(Error::from)
    }
    pub(crate) async fn verify_project_source_handle(
        &self,
        context: &crate::runtime::TaskContext,
        handle: &ProjectSourceReadHandle,
        native: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        let io = native
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io: Option<crate::executor::PreparationIo> = None;
        self.require_task_context_binding(context, io.as_ref())?;
        self.verify_project_source_read(handle, native).await
    }

    /// Revalidates the sealed reader captured by the original owner.
    pub(crate) async fn verify_project_source_read(
        &self,
        handle: &ProjectSourceReadHandle,
        _native: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        let mut io = _native
            .map(|work| {
                crate::executor::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut work = io.as_mut().map(|io| io as &mut dyn crate::store::HistoryIo);
        #[cfg(target_arch = "wasm32")]
        let mut work: Option<&mut dyn crate::store::HistoryIo> = None;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = _native {
            work.remaining()?;
        }
        let cancellation = self.invocation_cancellation.as_ref().ok_or_else(|| {
            crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "selected project read requires actual invocation cancellation",
            )
        })?;
        let check = || {
            #[cfg(not(target_arch = "wasm32"))]
            if _native.is_some() {
                return cancellation.check().map_err(|_| {
                    Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled)
                });
            }
            cancellation.check().map_err(Error::from)
        };
        check()?;
        if handle.authorization().caller_admission_digest
            != self.admission.canonical_digest_with_work(&mut work)?
        {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "selected project read differs from its original admission",
            ));
        }
        self.verify(false, &mut work).await?;
        self.host
            .verify_project_source_authorization(handle.authorization(), self.task_id(), &mut work)
            .await?;
        self.verify(false, &mut work).await?;
        check()
    }
}
