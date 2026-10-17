use super::content_write::bound_content_writer;
use super::*;

/// One admitted task's mailbox borrowed from its current owner token.
pub(crate) trait OwnedTaskMailbox: acyclic_stream::ProviderPlatform + Any {
    fn task_id(&self) -> TaskId;
    #[cfg(feature = "filesystem")]
    fn invocation_cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        None
    }

    fn validate_scope(
        &self,
        scope: &RuntimeScope,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()>;
    #[cfg(feature = "filesystem")]
    fn task_binding(&self) -> (TaskId, &crate::scheduler::LeaseFence);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn admission(&self) -> (&TaskAdmissionRecord, &WorkflowAdmission);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn claim_model_dispatch<'a>(
        &'a self,
        context: &'a TaskContext,
        dispatch: crate::model::ModelDispatch,
        fence: &'a crate::scheduler::LeaseFence,
    ) -> BoxFuture<'a, Result<()>>;
    fn verify_owner<'a>(
        &'a self,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>>;
    fn file_publication_condition(
        &self,
    ) -> BoxFuture<'_, Result<(acyclic_stream::CommitCondition, Option<u64>)>>;
    fn send<'a>(
        &'a self,
        recipient: TaskId,
        operation: OperationId,
        payload: FileRef,
    ) -> BoxFuture<'a, Result<crate::conversation::FileRef>>;
    fn inbox<'a>(&'a self, after: u64, maximum: usize) -> BoxFuture<'a, Result<Vec<InboxItem>>>;
    fn read_received<'a>(
        &'a self,
        receive_operation: OperationId,
        item: &'a InboxItem,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>;
}

/// Capability-scoped context passed to live task code.
pub struct TaskContext {
    pub(crate) harness: Arc<AgentHarness>,
    pub(super) task_id: OperationId,
    pub(super) durable_task: Option<TaskId>,
    pub(super) descendants: TaskGroup,
    pub(super) scope: RuntimeScope,
    pub(super) policy_overrides: Vec<(ComponentIdentity, Arc<dyn ToolPolicy>)>,
    pub(crate) owned_interactions: Option<Arc<dyn InteractionRouter>>,
    pub(super) model_steps: Arc<(AtomicUsize, Option<NativeCustody>)>,
    pub(crate) owned_mailbox: Option<Arc<dyn OwnedTaskMailbox>>,
    pub(super) output_content: Option<ContentBindings>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_purpose: Option<IssuedContextPurpose>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl Clone for TaskContext {
    fn clone(&self) -> Self {
        self.copy_with_scope(self.scope.clone())
    }
}

impl TaskContext {
    #[cfg(feature = "filesystem")]
    pub(crate) fn invocation_cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        self.owned_mailbox
            .as_ref()
            .and_then(|owner| owner.invocation_cancellation())
    }
    #[cfg(feature = "filesystem")]
    pub(crate) fn bind_ordinary_created_owner<P: acyclic_stream::StreamProvider + 'static>(
        &mut self,
        owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if self.native_custody.is_some() {
            return Err(Error::Unauthorized(
                "ordinary publication owner requires an ordinary context".into(),
            ));
        }
        if self.scope.created_access.is_none() {
            return Ok(());
        }
        owner.require_task_context_binding(self, None)?;
        if let Some(access) = self.scope.created_access.as_mut() {
            if Arc::get_mut(access).is_none() {
                *access = Arc::new(crate::resource_preparation::CreatedResourceAccess {
                    operation: access.operation,
                    admission_digest: access.admission_digest,
                    grants: access.grants.clone(),
                    content: access
                        .content
                        .iter()
                        .map(
                            |binding| crate::resource_preparation::CreatedContentBinding {
                                volume: binding.volume.clone(),
                                authority: binding.authority.clone(),
                                binding: binding.binding.clone(),
                                default_writer: binding.default_writer,
                                artifact_writer: binding.artifact_writer,
                                scratch_creation: binding.scratch_creation.clone(),
                            },
                        )
                        .collect(),
                    references: access.references.clone(),
                    proofs: access.proofs.clone(),
                    publication_lease: access.publication_lease.clone(),
                });
            }
            let access = Arc::get_mut(access).ok_or_else(|| {
                Error::Conflict("created task scope is not uniquely owned".into())
            })?;
            if access.operation.into_bytes() != owner.task_id().into_bytes()
                || access.admission_digest
                    != owner.admission().canonical_digest_with_work(&mut None)?
            {
                return Err(Error::Unauthorized(
                    "created publication owner differs from the admission seal".into(),
                ));
            }
            access.publication_lease = Some(owner);
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn require_ordinary_metadata(&self) -> Result<()> {
        if self.native_custody.is_some() {
            return Err(Error::Unsupported(
                "ordinary task files require ordinary ownership".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn copy_with_scope(&self, scope: RuntimeScope) -> Self {
        Self {
            harness: Arc::clone(&self.harness),
            task_id: self.task_id,
            durable_task: self.durable_task,
            descendants: self.descendants.clone(),
            scope,
            policy_overrides: self.policy_overrides.clone(),
            owned_interactions: self.owned_interactions.clone(),
            owned_mailbox: self.owned_mailbox.clone(),
            output_content: self.output_content.clone(),
            model_steps: Arc::clone(&self.model_steps),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_purpose: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        }
    }

    pub(crate) fn into_shared(self) -> Result<Arc<TaskContext>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut context = self;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let context = self;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(mut custody) = context.native_custody.take() {
            let mut slot = match custody.work().claim(
                (2 * std::mem::size_of::<usize>()) as u64,
                1,
                acyclic_fs::WorkCounters::UNCHARGED,
            ) {
                Ok(slot) => slot,
                Err(cause) => {
                    drop(context);
                    return Err(Error::NativeWork {
                        cause,
                        custody: Some(custody),
                    });
                }
            };
            custody.absorb(&mut slot)?;
            context.native_custody = Some(custody);
        }
        Ok(Arc::new(context))
    }

    /// Selects an authenticated created destination without widening task authority.
    pub async fn with_output_volume(self: &Arc<Self>, volume: &VolumeRef) -> Result<Arc<Self>> {
        let io = self.preparation_io()?;
        let binding = self.scope.created_content(Some(volume)).ok_or_else(|| {
            crate::conversation::content_error(
                io.as_ref(),
                crate::wire::ErrorCode::Unauthorized,
                "output volume has no admitted creation binding",
            )
        })?;
        self.with_output_bindings(binding.clone()).await
    }

    /// Selects provider-authenticated content within this task's existing grants.
    pub async fn with_output_bindings(
        self: &Arc<Self>,
        binding: ContentBindings,
    ) -> Result<Arc<Self>> {
        let io = self.preparation_io()?;
        bound_content_writer(Some(&binding), &self.scope, io.as_ref())?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io {
            return self
                .project_output_binding(
                    &binding,
                    io.original_native_work().ok_or_else(|| {
                        native_context::boundary(
                            crate::wire::ErrorCode::Invalid,
                            "original context work is missing",
                        )
                    })?,
                )
                .await;
        }
        let mut context = self.as_ref().clone();
        context.output_content = Some(binding);
        context.into_shared()
    }

    /// Borrows the immutable admission retained by this context's original owner.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn admission(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&TaskAdmissionRecord>> {
        self.require_original_native_work(work)?;
        let Some(owner) = &self.owned_mailbox else {
            return Ok(None);
        };
        self.owned_task_binding(Some(&crate::executor::PreparationIo::from_native_work(
            work,
        )?))?;
        Ok(Some(owner.admission().0))
    }

    /// Borrows the task and fence from this context's original owner.
    #[cfg(feature = "filesystem")]
    pub fn owned_task_binding(
        &self,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(TaskId, &crate::scheduler::LeaseFence)> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(|io| io.original_native_work()) {
            self.require_original_native_work(work)?;
        }
        let owner = self
            .owned_mailbox
            .as_ref()
            .filter(|owner| self.durable_task == Some(owner.task_binding().0))
            .ok_or_else(|| {
                crate::conversation::content_error(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "task context has no matching original owner",
                )
            })?;
        owner.validate_scope(&self.scope, io)?;
        Ok(owner.task_binding())
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn original_task_binding_matches(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
    ) -> bool {
        self.durable_task == Some(task)
            && self
                .owned_mailbox
                .as_ref()
                .is_some_and(|owner| owner.task_binding() == (task, fence))
    }

    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    pub(crate) fn ancestors(
        &self,
        task: TaskId,
    ) -> Result<impl futures::Stream<Item = Result<TaskId>>> {
        let state = Arc::clone(
            self.harness
                .state
                .as_ref()
                .ok_or_else(|| Error::Invalid("task state is absent".into()))?,
        );
        Ok(ancestor_stream(task, move |task| {
            let state = Arc::clone(&state);
            async move { Ok(state.observe_admission(task).await?.parent) }
        }))
    }

    /// Returns this invocation's stable local operation identity.
    #[must_use]
    pub const fn id(&self) -> OperationId {
        self.task_id
    }

    /// Admitted durable owner, absent for a process-local task.
    #[must_use]
    pub const fn durable_task_id(&self) -> Option<TaskId> {
        self.durable_task
    }

    /// Returns the exact effective authority and bounds.
    #[must_use]
    pub const fn scope(&self) -> &RuntimeScope {
        &self.scope
    }

    /// Produces an attenuated view for descendants and tool calls.
    pub fn scoped(&self, grants: Capabilities, limits: Limits) -> Result<Self> {
        let mut child = self.clone();
        child.scope = self.scope.narrow(grants, limits)?;
        Ok(child)
    }

    /// Resolves one registered task without widening its scope.
    pub fn task<I: 'static, O: 'static>(&self, name: &str) -> Result<Arc<TaskDefinition<I, O>>> {
        self.harness.task(name)
    }

    /// Resolves one pinned tool contract.
    pub fn tool(&self, name: &str) -> Result<ToolDefinition> {
        self.harness.tool(name)
    }

    /// Spawns a local child; durable work must use stable host admission instead.
    pub async fn spawn<I, O>(
        &self,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
    ) -> Result<RuntimeTask<O>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable descendants need stable admission".into(),
            ));
        }
        require_task_spawn(&self.scope, definition)?;
        self.harness
            .spawn_in_group(
                &self.descendants,
                self.scope.clone(),
                self.policy_overrides.clone(),
                definition,
                input,
                None,
            )
            .await
    }

    /// Returns a separately cancellable bounded group for additional local work.
    #[must_use]
    #[allow(
        clippy::expect_used,
        clippy::missing_panics_doc,
        reason = "A child with no native work performs no fallible quota operations."
    )]
    pub fn group(&self) -> RuntimeGroup {
        RuntimeGroup {
            harness: Arc::clone(&self.harness),
            group: self
                .descendants
                .child(self.scope.concurrency_bound(self.harness.concurrency), None)
                .expect("ordinary child group"),
            scope: self.scope.clone(),
            policy_overrides: self.policy_overrides.clone(),
            durable_parent: self.durable_task,
        }
    }

    /// Admits a durable child using an explicit stable operation identity.
    pub async fn admit<I, O>(
        &self,
        operation_id: OperationId,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
    ) -> Result<Admission<RuntimeTask<O>>>
    where
        I: Serialize + Send + 'static,
        O: DeserializeOwned + Send + 'static,
    {
        let parent = self.durable_task.ok_or_else(|| {
            Error::Unsupported("a live task cannot claim durable descendant ownership".into())
        })?;
        self.harness
            .admit_scoped(
                operation_id,
                definition,
                input,
                Some(parent),
                self.scope.clone(),
            )
            .await
    }
}

pub(super) fn require_task_spawn<I, O>(
    scope: &RuntimeScope,
    definition: &TaskDefinition<I, O>,
) -> Result<()> {
    require_descendant_grant(scope.grants(), &definition.identity)
}

pub(crate) fn require_descendant_grant(
    grants: &Capabilities,
    identity: &ComponentIdentity,
) -> Result<()> {
    let capability = format!("task:spawn:{}@{}", identity.name, identity.version,);
    if !grants.contains(&capability) {
        return Err(Error::Unauthorized(format!("scope lacks {capability}")));
    }
    Ok(())
}

pub(crate) fn read_granted(grants: &Capabilities, file: &FileRef) -> Result<bool> {
    read_granted_with_work(grants, file, &mut None)
}

pub(crate) fn read_granted_with_work(
    grants: &Capabilities,
    file: &FileRef,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    if let Some(io) = work.as_deref_mut() {
        io.check_cancellation()?;
    }
    file.validate(work)?;
    if contains_content_grant_with_work(grants, file, acyclic_fs::access::FILE_READ_PREFIX, work)?
        || contains_content_grant_with_work(
            grants,
            file.volume(),
            acyclic_fs::access::VolumeOperation::Read.capability_prefix(),
            work,
        )?
        || contains_content_grant_with_work(
            grants,
            &(file.volume(), ""),
            acyclic_fs::access::DIRECTORY_READ_PREFIX,
            work,
        )?
    {
        return Ok(true);
    }
    for prefix in file
        .path()
        .match_indices('/')
        .filter_map(|(slash, _)| file.path().get(..slash))
    {
        if !crate::conversation::is_internal_path(prefix)
            && contains_content_grant_with_work(
                grants,
                &(file.volume(), prefix),
                acyclic_fs::access::DIRECTORY_READ_PREFIX,
                work,
            )?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn contains_content_grant_with_work<T: Serialize + ?Sized>(
    grants: &Capabilities,
    value: &T,
    prefix: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<bool> {
    if let Some(io) = work.as_deref_mut() {
        io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
    }
    if !grants
        .0
        .range::<str, _>((
            std::ops::Bound::Included(prefix),
            std::ops::Bound::Unbounded,
        ))
        .next()
        .is_some_and(|grant| grant.starts_with(prefix))
    {
        return Ok(false);
    }
    let digest = crate::executor::json::canonical_json_digest(
        value,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        work,
    )?;
    if let Some(io) = work.as_deref_mut() {
        io.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        io.copy_bytes((prefix.len() + 64) as u64)?;
    }
    Ok(crate::conversation::contains_content_label(
        grants, prefix, digest,
    ))
}

#[cfg(any(
    all(test, not(acyclic_harness_loom)),
    all(feature = "filesystem-local", not(target_arch = "wasm32"))
))]
fn ancestor_stream<F, Fut>(task: TaskId, parent: F) -> impl futures::Stream<Item = Result<TaskId>>
where
    F: FnMut(TaskId) -> Fut,
    Fut: std::future::Future<Output = Result<Option<TaskId>>>,
{
    futures::stream::try_unfold(
        (task, true, std::collections::HashSet::new(), parent),
        |(task, first, mut visited, mut parent)| async move {
            let current = if first {
                task
            } else {
                let Some(parent) = parent(task).await? else {
                    return Ok(None);
                };
                parent
            };
            if !visited.insert(current) {
                return Err(Error::Invalid("cyclic task lineage".into()));
            }
            Ok(Some((current, (current, false, visited, parent))))
        },
    )
}
#[cfg(all(test, not(acyclic_harness_loom)))]
#[path = "task_context/ancestor_tests.rs"]
mod ancestor_tests;
