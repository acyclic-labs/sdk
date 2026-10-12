//! Project selection under the configured source policy.
use super::resources::Verification;
use super::*;
use crate::core::AuthorityIssuer;
use crate::source_authorization::{
    ProjectSourceAuthorization, ProjectSourceReadHandle, ProjectSourceSnapshot,
    RootProjectSourceContext, RootProjectSourceProvider,
};
use crate::wire::ErrorCode;

impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Authorizes a bounded selection under the current owner and configured policy.
    #[allow(clippy::too_many_arguments)]
    pub async fn authorize_project_source(
        self: &Arc<Self>,
        context: &crate::runtime::TaskContext,
        issuer: &AuthorityIssuer,
        provider: &dyn RootProjectSourceProvider,
        source_task: TaskId,
        operation_id: OperationId,
        snapshot: ProjectSourceSnapshot,
        maximum_work: u64,
        maximum_bytes: u64,
    ) -> Result<ProjectSourceReadHandle> {
        self.select_project_source(
            context,
            issuer,
            provider,
            source_task,
            operation_id,
            &snapshot,
            maximum_work,
            maximum_bytes,
            None,
        )
        .await
    }

    async fn select_project_source(
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
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        #[cfg(not(target_arch = "wasm32"))]
        let native = io
            .map(|io| {
                io.original_native_work().ok_or_else(|| {
                    admission_boundary(
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
                self.require_mail_scope_with_native_work(context.scope(), native)?;
                context.owned_task_binding_with_native_work(native)? == (self.task_id, &self.fence)
            }
            None => {
                context.durable_task_id() == Some(self.task_id)
                    && context.owned_task_fence()? == self.fence
            }
        };
        #[cfg(target_arch = "wasm32")]
        let bound = context.durable_task_id() == Some(self.task_id)
            && context.owned_task_fence()? == self.fence;
        if !bound
            || context.scope().grants() != &self.input_grants
            || maximum_work == 0
            || maximum_work == u64::MAX
            || maximum_bytes == 0
            || maximum_bytes == u64::MAX
            || operation_id == self.operation_id()
        {
            return Err(admission_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source bounds or original owner differs",
            ));
        }
        let cancellation = self.invocation_cancellation.as_ref().ok_or_else(|| {
            admission_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source requires actual invocation cancellation",
            )
        })?;
        let check = || {
            cancellation.check().map_err(|_| {
                #[cfg(not(target_arch = "wasm32"))]
                if native.is_some() {
                    return Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled);
                }
                Error::Unauthorized("project source invocation is cancelled".into())
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
            return Err(admission_boundary(
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
            return Err(admission_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "source selection is not one provider's project generation",
            ));
        }
        let bytes = crate::contract::json_byte_length(snapshot, maximum_bytes, quota.work())?;
        let mut budget = Verification {
            remaining: Some((maximum_work, maximum_bytes)),
            path: Vec::new(),
        };
        budget.charge(1, bytes, quota.work())?;
        let (caller, caller_held) = self
            .host
            .source_admission(self.task_id, &mut budget, quota.work())
            .await?;
        quota.adopt(caller_held)?;
        if crate::contract::canonical_json_digest(
            &caller,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            quota.work(),
        )? != self.original_admission_digest
        {
            return Err(admission_boundary(
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
            .source_graph_root(self.task_id, &mut budget, quota.work())
            .await?;
        if self
            .host
            .source_graph_root(source_task, &mut budget, quota.work())
            .await?
            != graph_root
        {
            return Err(admission_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "project source belongs to another actual session graph",
            ));
        }
        let created_project_authenticated = self
            .host
            .source_created_project(&source, &snapshot.project, &mut budget, quota.work())
            .await?;
        let (remaining_work, remaining_bytes) =
            budget.remaining.expect("bounded project verification");
        if (!created_project_authenticated && source_task != graph_root)
            || remaining_work == 0
            || remaining_bytes == 0
        {
            return Err(admission_boundary(
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
            maximum_work: remaining_work,
            maximum_bytes: remaining_bytes,
            created_project_authenticated,
            cancellation,
        };
        #[cfg(not(target_arch = "wasm32"))]
        match native {
            Some(native) => {
                provider
                    .verify_with_native_work(policy_context, snapshot, native)?
                    .await?
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
        let generation = super::headless::copy_generation(&snapshot.generation, quota.work())?;
        let native_snapshot = snapshot
            .native_snapshot
            .as_ref()
            .map(|value| crate::core::copy_value(value, &mut quota))
            .transpose()?;
        let mut authorization = ProjectSourceAuthorization {
            owner,
            caller_task: self.task_id,
            caller_admission_digest: self.original_admission_digest,
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

    /// Authorizes a selection using its original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn authorize_project_source_with_native_work<'a>(
        self: &'a Arc<Self>,
        context: &'a crate::runtime::TaskContext,
        issuer: &'a AuthorityIssuer,
        provider: &'a dyn RootProjectSourceProvider,
        source_task: TaskId,
        operation_id: OperationId,
        snapshot: &'a ProjectSourceSnapshot,
        maximum_work: u64,
        maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, (ProjectSourceReadHandle, acyclic_fs::NativeAllocation)>,
    > {
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let handle = self
                .select_project_source(
                    context,
                    issuer,
                    provider,
                    source_task,
                    operation_id,
                    snapshot,
                    maximum_work,
                    maximum_bytes,
                    Some(&io),
                )
                .await?;
            let held = io.owned_bytes()?;
            Ok((handle, io.into_native_custody(held)?))
        })
    }
}
