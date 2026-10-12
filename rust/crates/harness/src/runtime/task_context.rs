use super::*;

/// One admitted task's mailbox borrowed from its current owner token.

pub(crate) trait OwnedTaskMailbox: acyclic_stream::ProviderPlatform {
    fn validate_scope(&self, scope: &RuntimeScope) -> Result<()>;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn validate_scope_with_native_work(
        &self,
        _scope: &RuntimeScope,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "mailbox has no original-ledger admitted scope verifier",
        })
    }
    #[cfg(feature = "filesystem")]
    fn task_binding(&self) -> (TaskId, &crate::scheduler::LeaseFence);
    #[cfg(feature = "filesystem")]
    fn matches_task_binding(&self, task: TaskId, fence: &crate::scheduler::LeaseFence) -> bool;
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
    ) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>>;

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn send_with_native_work<'a>(
        &'a self,
        _recipient: TaskId,
        _operation: OperationId,
        _payload: &'a FileRef,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, (crate::conversation::FileRef, acyclic_fs::NativeAllocation)>,
    > {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "owned mailbox has no original native-ledger sender",
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn inbox_with_native_work<'a>(
        &'a self,
        _after: u64,
        _maximum: usize,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<InboxItem>, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "owned mailbox has no original native-ledger inbox",
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn read_received_with_native_work<'a>(
        &'a self,
        _receive_operation: OperationId,
        _item: &'a InboxItem,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "owned mailbox has no original native-ledger received parcel reader",
        })
    }
}

/// Capability-scoped context passed to live task code.

pub struct TaskContext {
    pub(super) harness: Arc<AgentHarness>,
    pub(super) task_id: OperationId,
    pub(super) durable_task: Option<TaskId>,
    pub(super) descendants: TaskGroup,
    pub(super) scope: RuntimeScope,
    pub(super) policy_overrides: Vec<(ComponentIdentity, Arc<dyn ToolPolicy>)>,
    pub(super) owned_interactions: Option<Arc<dyn InteractionRouter>>,
    pub(super) model_steps: Arc<(AtomicUsize, Option<NativeCustody>)>,
    pub(super) owned_mailbox: Option<Arc<dyn OwnedTaskMailbox>>,
    pub(super) output_content: Option<ContentBindings>,
    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    pub(super) native_root_output: std::sync::OnceLock<(
        Arc<crate::filesystem::NativeRootOutputSource>,
        ContentBindings,
    )>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_metadata_source: Option<native_context::NativeMetadataSource>,
    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    pub(super) working_view: std::sync::OnceLock<Arc<dyn crate::filesystem::NativeTaskWorkingView>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl Clone for TaskContext {
    fn clone(&self) -> Self {
        Self {
            harness: Arc::clone(&self.harness),
            task_id: self.task_id,
            durable_task: self.durable_task,
            descendants: self.descendants.clone(),
            scope: self.scope.clone(),
            policy_overrides: self.policy_overrides.clone(),
            owned_interactions: self.owned_interactions.clone(),
            owned_mailbox: self.owned_mailbox.clone(),
            output_content: self.output_content.clone(),
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            native_root_output: self.native_root_output.clone(),
            model_steps: Arc::clone(&self.model_steps),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_source: None,
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            working_view: std::sync::OnceLock::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        }
    }
}

impl TaskContext {
    /// Selects a trusted host-provided output binding under the unchanged task authority.

    pub fn with_content_bindings(mut self, binding: ContentBindings) -> Result<Self> {
        let writer = binding
            .writer
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("output binding has no writer".into()))?;
        if !self.scope.grants().contains(
            &writer
                .volume()
                .capability(VolumeOperation::Write, &mut None)?,
        ) {
            return Err(Error::Unauthorized(
                "task cannot select this output volume".into(),
            ));
        }
        if let Some(sealed) = self.scope.created_content(Some(writer.volume())) {
            if sealed
                .writer
                .as_ref()
                .is_none_or(|original| !Arc::ptr_eq(original, writer))
                || !Arc::ptr_eq(&sealed.reader, &binding.reader)
            {
                return Err(Error::Unauthorized(
                    "output differs from authenticated created binding".into(),
                ));
            }
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            self.native_metadata_source = None;
        }
        self.output_content = Some(binding);
        Ok(self)
    }

    /// Selects an existing authenticated created destination without cloning the harness, widening grants or acquiring another owner reservation.

    pub fn with_output_volume(self, volume: &VolumeRef) -> Result<Self> {
        let binding = self
            .scope
            .created_content(Some(volume))
            .ok_or_else(|| {
                Error::Unauthorized("output volume has no admitted creation binding".into())
            })?
            .clone();
        self.with_content_bindings(binding)
    }

    /// Returns only the exact creator-authenticated handles already admitted to this task.

    pub fn created_content_bindings(&self, volume: &VolumeRef) -> Result<ContentBindings> {
        volume.validate(&mut None)?;
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            if !self
                .scope
                .grants()
                .contains(&volume.capability(operation, &mut None)?)
            {
                return Err(Error::Unauthorized(
                    "task lacks original created-content read/write authority".into(),
                ));
            }
        }
        let binding = self.scope.created_content(Some(volume)).ok_or_else(|| {
            Error::Unauthorized("volume has no authenticated task creation binding".into())
        })?;
        if binding
            .writer
            .as_ref()
            .is_none_or(|writer| writer.volume() != volume)
        {
            return Err(Error::Unauthorized(
                "created content lacks its exact admitted writer".into(),
            ));
        }
        Ok(binding.clone())
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn with_owned_mailbox(
        mut self,
        task: TaskId,
        mailbox: Arc<dyn OwnedTaskMailbox>,
    ) -> Result<Self> {
        if self.durable_task != Some(task) {
            return Err(Error::Unauthorized(
                "mailbox owner differs from task context".into(),
            ));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            self.native_metadata_source = None;
        }
        self.owned_mailbox = Some(mailbox);
        Ok(self)
    }

    /// Returns only the original owner fence already bound to this exact task context.

    #[cfg(feature = "filesystem")]
    pub fn owned_task_fence(&self) -> Result<crate::scheduler::LeaseFence> {
        let owner = self
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("task context has no original owner".into()))?;
        owner.validate_scope(&self.scope)?;
        let (task, fence) = owner.task_binding();
        if self.durable_task != Some(task) {
            return Err(Error::Unauthorized(
                "task context differs from original owner".into(),
            ));
        }
        Ok(fence.clone())
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
                .is_some_and(|owner| owner.matches_task_binding(task, fence))
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn with_owned_interactions(
        mut self,
        task: TaskId,
        router: Arc<dyn InteractionRouter>,
    ) -> Result<Self> {
        if self.durable_task != Some(task) {
            return Err(Error::Unauthorized(
                "interaction owner differs from task context".into(),
            ));
        }
        self.owned_interactions = Some(router);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        {
            self.native_metadata_source = None;
        }
        Ok(self)
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

    #[cfg(feature = "filesystem")]
    pub(crate) fn original_host_binding_matches(
        &self,
        host: &dyn DurableTaskHost,
        policy: Option<&dyn ToolPolicy>,
    ) -> bool {
        self.harness.original_host_binding_matches(host, policy)
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

    /// Attenuates task execution bounds for this context and its descendants.

    pub fn scoped_run_limits(&self, limits: TaskRunLimits) -> Result<Self> {
        let mut child = self.clone();
        child.scope = self.scope.clone().with_run_limits(limits, &mut None)?;
        child.descendants = self.descendants.child(
            child.scope.concurrency_bound(self.harness.concurrency),
            None,
        )?;
        Ok(child)
    }

    /// Overrides only this live task's model binding for itself and local descendants.

    pub fn scoped_model(
        &self,
        grants: Capabilities,
        limits: Limits,
        model: Model,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<Self> {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable model override requires a recorded effect".into(),
            ));
        }
        let mut child = self.scoped(grants, limits)?;
        child.harness = child.harness.bind_model(model, provider)?;
        Ok(child)
    }

    /// Places newly admitted descendants on another qualified provider.

    pub fn scoped_execution(
        &self,
        grants: Capabilities,
        limits: Limits,
        execution: Arc<dyn ExecutionProvider>,
    ) -> Result<Self> {
        if self.durable_task.is_some() {
            return Err(Error::Unsupported(
                "durable execution override requires a recorded effect".into(),
            ));
        }
        let mut child = self.scoped(grants, limits)?;
        child.harness = self.harness.scoped_execution(
            child.scope.grants().clone(),
            child.scope.limits(),
            execution,
        )?;
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
    if contains_content_grant_with_work(grants, file, "file:read:", work)?
        || contains_content_grant_with_work(grants, file.volume(), "volume:read:", work)?
        || contains_content_grant_with_work(grants, &(file.volume(), ""), "directory:read:", work)?
    {
        return Ok(true);
    }
    for (slash, _) in file.path().match_indices('/') {
        let prefix = &file.path()[..slash];
        if !crate::conversation::is_internal_path(prefix)
            && contains_content_grant_with_work(
                grants,
                &(file.volume(), prefix),
                "directory:read:",
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
    let digest = crate::contract::canonical_json_digest_with_work(
        value,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        work,
    )?;
    let digest = blake3::Hash::from(digest).to_hex();
    let mut capability = [0; "directory:read:".len() + 64];
    let end = prefix.len() + digest.len();
    if let Some(io) = work.as_deref_mut() {
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        io.copy_bytes(end as u64)?;
    }
    capability[..prefix.len()].copy_from_slice(prefix.as_bytes());
    capability[prefix.len()..end].copy_from_slice(digest.as_bytes());
    let capability = std::str::from_utf8(&capability[..end])
        .expect("canonical capability prefixes and hash are ASCII");
    Ok(grants.contains(capability))
}
