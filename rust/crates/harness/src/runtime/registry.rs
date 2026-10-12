use super::*;

#[cfg(target_arch = "wasm32")]
pub(super) type StoredDefinition = dyn Any;

pub(super) fn task_registered_portable_admission(
    operation: OperationId, input: Value, parent: Option<TaskId>, scope: &RuntimeScope,
    entry: &TaskEntry, machine: &MachineIdentity,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<TaskAdmissionRecord> {
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    if let Some(io) = quota.work().as_deref_mut() {
        let bytes = (std::mem::size_of::<TaskAdmissionRecord>() - std::mem::size_of::<Value>()) as u64;
        io.retain_owned(bytes)?;
        quota.adopt(bytes)?;
    }
    let task = task_copy_component(entry.workflow_definition.identity(), &mut quota)?;
    let machine = task_copy_machine(machine, &mut quota)?;
    let input_schema = crate::core::copy_value(entry.workflow_definition.input_schema(), &mut quota)?;
    let output_schema = crate::core::copy_value(entry.workflow_definition.output_schema(), &mut quota)?;
    let grants = crate::core::copy_capabilities(scope.grants(), &mut quota)?;
    let resource_proofs = RuntimeScope::copy_resource_proofs(scope.resource_proofs(), &mut quota)?;
    let extensions = scope.extensions().map(|extension| {
        let (copied, retained) = extension.copy(quota.work())?;
        quota.adopt(retained)?;
        Ok::<_, Error>(copied)
    }).transpose()?;
    let record = TaskAdmissionRecord {
        operation_id: operation, task, machine, input, input_schema, output_schema,
        parent, resource_proofs, grants, limits: scope.limits(), run_limits: scope.run_limits(),
        policy: None, extensions, execution: None,
    };
    record.validate(quota.work())?;
    quota.transfer();
    Ok(record)
}

pub(super) struct TaskEntry {
    pub(super) machine: Option<MachineIdentity>,
    #[cfg(target_arch = "wasm32")]
    pub(super) definition: Arc<StoredDefinition>,
    pub(super) workflow_definition: Arc<dyn RegisteredTaskDefinition>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl TaskEntry {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn require_native_definition(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.workflow_definition.require_native_identity(work)
    }

    pub(super) fn clone_with_io(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Self> {
        Ok(Self {
            machine: task_copy_with_io(io, |quota| self.machine.as_ref()
                .map(|machine| task_copy_machine(machine, quota)).transpose())?,
            #[cfg(target_arch = "wasm32")]
            definition: self.definition.clone(),
            workflow_definition: self.workflow_definition.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        })
    }
}

pub(super) fn task_definition_arc<I: 'static, O: 'static>(entry: &TaskEntry) -> Option<Arc<TaskDefinition<I, O>>> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let definition: Arc<dyn Any + Send + Sync> = entry.workflow_definition.clone();
        definition.downcast::<TaskDefinition<I, O>>().ok()
    }
    #[cfg(target_arch = "wasm32")]
    { entry.definition.downcast_ref::<Arc<TaskDefinition<I, O>>>().cloned() }
}

pub(super) trait RegisteredTaskDefinition: Any + acyclic_stream::ProviderPlatform {
    fn identity(&self) -> &ComponentIdentity;
    fn input_schema(&self) -> &Value;
    fn output_schema(&self) -> &Value;
    fn requirements(&self) -> &BTreeSet<String>;
    fn open_workflow(
        self: Arc<Self>,
        context: TaskContext,
        fence: crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
    ) -> BoxFuture<'static, Result<ResumableTaskSession>>;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_native_identity(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()>;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn open_workflow_with_native_work<'a>(
        self: Arc<Self>, context: Arc<NativeTaskContext>,
        fence: &'a crate::scheduler::LeaseFence, journal: Arc<dyn WorkflowJournal>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ResumableTaskSession>>;
}

impl<I: 'static, O: 'static> RegisteredTaskDefinition for TaskDefinition<I, O>
where
    TaskDefinition<I, O>: acyclic_stream::ProviderPlatform,
{
    fn identity(&self) -> &ComponentIdentity { &self.identity }
    fn input_schema(&self) -> &Value { &self.input_schema }
    fn output_schema(&self) -> &Value { &self.output_schema }
    fn requirements(&self) -> &BTreeSet<String> { &self.requirements }

    fn open_workflow(
        self: Arc<Self>,
        context: TaskContext,
        fence: crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
    ) -> BoxFuture<'static, Result<ResumableTaskSession>> {
        Box::pin(async move { self.open(&context, fence, journal).await })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn require_native_identity(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        TaskDefinition::require_native_identity(self, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn open_workflow_with_native_work<'a>(
        self: Arc<Self>, context: Arc<NativeTaskContext>,
        fence: &'a crate::scheduler::LeaseFence, journal: Arc<dyn WorkflowJournal>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ResumableTaskSession>> {
        TaskDefinition::require_native_identity(self.as_ref(), work)?;
        if !journal.supports_native_work() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "workflow journal has no native callbacks",
            });
        }
        crate::NativeFutureBundle::new(work, || async move {
            self.open_with_native_work(context, fence, journal, work).await
        })
    }
}

/// Immutable typed definitions indexed by their exact name and version.

#[derive(Default)]
pub struct TaskRegistry(
    pub(super) BTreeMap<(String, String), TaskEntry>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) Option<acyclic_fs::NativeAllocation>,
);

impl Clone for TaskRegistry {
    fn clone(&self) -> Self {
        self.clone_with_io(None).expect("unobserved task registry cloning is infallible")
    }
}

impl TaskRegistry {
    /// Registers one exact task version and retains its native metadata guard.
    pub fn register<I: 'static, O: 'static>(&mut self, definition: TaskDefinition<I, O>) -> Result<()>
    where TaskDefinition<I, O>: acyclic_stream::ProviderPlatform,
    {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = definition.native_metadata_custody.as_ref().map(|guard| guard.work().clone()) {
            definition.require_native_identity(&work)?;
            let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
            let result = (|| {
                if self.1.is_none() { io.retain_owned(std::mem::size_of::<Self>() as u64)?; }
                self.prepare_registration(definition, Some(&io))
            })();
            let retained = io.owned_bytes()?;
            let ((key, mut entry), mut custody) = crate::workflow::finish_native_io(result, io, retained)?;
            if self.1.is_none() {
                self.1 = Some(custody.split_off(std::mem::size_of::<Self>() as u64).map_err(Error::from)?);
            }
            entry.native_custody = Some(custody);
            self.0.insert(key, entry);
            return Ok(());
        }
        let (key, entry) = self.prepare_registration(definition, None)?;
        self.0.insert(key, entry);
        Ok(())
    }

    pub(super) fn prepare_registration<I: 'static, O: 'static>(
        &self, definition: TaskDefinition<I, O>, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<((String, String), TaskEntry)>
    where TaskDefinition<I, O>: acyclic_stream::ProviderPlatform,
    {
        let name = task_copy_text(&definition.identity.name, io)?;
        let version = task_copy_text(&definition.identity.version, io)?;
        let key = (name, version);
        if io.is_some() { inspect_task_metadata(io, self.0.len() as u64)?; }
        if self.0.contains_key(&key) {
            return Err(task_workflow_error(task_original_work(io)?,
                crate::wire::ErrorCode::Conflict, "task version is already registered"));
        }
        let machine = match &definition.implementation {
            TaskImplementation::Resumable(machine) => Some(task_copy_with_io(io, |quota|
                task_copy_machine(task_machine_identity(machine.as_ref(), io)?, quota))?),
            TaskImplementation::Live(_) if io.is_some() => return Err(task_workflow_error(
                task_original_work(io)?, crate::wire::ErrorCode::Unsupported, "live tasks have no native workflow")),
            TaskImplementation::Live(_) => None,
        };
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.retain_owned((std::mem::size_of::<TaskEntry>() + std::mem::size_of::<(String, String)>()) as u64)?;
            io.allocation_attempt(std::mem::size_of::<TaskDefinition<I, O>>() as u64)?;
        }
        let definition = Arc::new(definition);
        let workflow_definition: Arc<dyn RegisteredTaskDefinition> = definition.clone();
        #[cfg(target_arch = "wasm32")]
        let stored_definition: Arc<StoredDefinition> = Arc::new(definition);
        Ok((key, TaskEntry {
            machine, workflow_definition,
            #[cfg(target_arch = "wasm32")]
            definition: stored_definition,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        }))
    }

    /// Resolves one Rust input/output pair.
    pub fn get<I: 'static, O: 'static>(&self, name: &str) -> Result<Arc<TaskDefinition<I, O>>> {
        if let Some((logical, version)) = name.rsplit_once('@') {
            return self.get_version(logical, version, #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None);
        }
        let mut matches = self.0.iter().filter(|((logical, _), _)| logical == name);
        let (_, stored) = matches.next()
            .ok_or_else(|| Error::NotFound(format!("task {name}")))?;
        if matches.next().is_some() {
            return Err(Error::Conflict(format!("task {name} requires an exact version")));
        }
        task_definition_arc::<I, O>(stored)
            .ok_or_else(|| Error::Conflict(format!("task {name} has different input/output types")))
    }

    /// Resolves an exact pinned version without relying on name parsing.
    pub fn get_version<I: 'static, O: 'static>(
        &self, name: &str, version: &str,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        work: Option<&acyclic_fs::NativeMaterializationWork>,
    ) -> Result<Arc<TaskDefinition<I, O>>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let mut selected = None;
            for ((logical, pinned_version), entry) in &self.0 {
                inspect_task_metadata(Some(&io), 1)?;
                if logical == name && pinned_version == version { selected = Some(entry); break; }
            }
            let stored = selected.ok_or_else(|| task_workflow_error(Some(work),
                crate::wire::ErrorCode::NotFound, "native task version is unavailable"))?;
            if stored.native_custody.is_none() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                    reason: "registry entry was not copied with native custody" });
            }
            stored.require_native_definition(work)?;
            return task_definition_arc::<I, O>(stored).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "task version has different input/output types",
            });
        }
        let stored = self.0.get(&(name.to_owned(), version.to_owned()))
            .ok_or_else(|| Error::NotFound(format!("task {name}@{version}")))?;
        task_definition_arc::<I, O>(stored)
            .ok_or_else(|| Error::Conflict(format!("task {name}@{version} has different input/output types")))
    }

    /// Copies actual registry metadata under the original work before growth.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = self.clone_with_io(Some(&io));
        let (mut registry, custody) = crate::workflow::finish_native_io(
            result, io, std::mem::size_of::<Self>() as u64,
        )?;
        registry.1 = Some(custody);
        Ok(registry)
    }

    pub(super) fn clone_with_io(&self, io: Option<&crate::executor::PreparationIo>) -> Result<Self> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { io.retain_owned(std::mem::size_of::<Self>() as u64)?; }
        let mut registry = Self::default();
        for (key, entry) in &self.0 {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = task_original_work(io)? {
                entry.require_native_definition(work)?;
                let entry_io = crate::filesystem::PreparationIo::from_native_work(work)?;
                let result = (|| {
                    entry_io.retain_owned((std::mem::size_of::<TaskEntry>()
                        + std::mem::size_of::<(String, String)>()) as u64)?;
                    let key = task_copy_key(key, Some(&entry_io))?;
                    Ok((key, entry.clone_with_io(Some(&entry_io))?))
                })();
                let retained = entry_io.owned_bytes()?;
                let ((key, mut copied), custody) = crate::workflow::finish_native_io(
                    result, entry_io, retained,
                )?;
                copied.native_custody = Some(custody);
                registry.0.insert(key, copied);
                continue;
            }
            registry.0.insert(task_copy_key(key, io)?, entry.clone_with_io(io)?);
        }
        Ok(registry)
    }

    /// Uses the same registered metadata for an erased stock child command.
    pub(super) fn registered_admission(
        &self, operation: OperationId, name: &str, version: &str, input: Value,
        parent: Option<TaskId>, expected_digest: Option<[u8; 32]>, scope: &RuntimeScope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<TaskAdmissionRecord> {
        let entry = if work.is_none() {
            self.0.get(&(name.to_owned(), version.to_owned()))
        } else {
            let mut found = None;
            for ((registered_name, registered_version), entry) in &self.0 {
                work.as_deref_mut().expect("observed registry lookup")
                    .codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                if registered_name == name && registered_version == version {
                    found = Some(entry);
                    break;
                }
            }
            found
        }.ok_or_else(|| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) { return Error::NativeBoundary {
                code: crate::wire::ErrorCode::NotFound, reason: "registered child task version is absent",
            }; }
            Error::NotFound(format!("task {name}@{version}"))
        })?;
        let machine = entry.machine.as_ref().ok_or_else(|| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) { return Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "live task cannot be a durable child command",
            }; }
            Error::Unsupported("live task cannot be a durable command".into())
        })?;
        if expected_digest.is_some_and(|digest| digest != entry.workflow_definition.identity().digest) {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) { return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "root definition differs from the registered task",
            }); }
            return Err(Error::Conflict("root definition differs from the registered task".into()));
        }
        let mut descendant = parent.is_none();
        for grant in scope.grants().iter() {
            if let Some(io) = work.as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            if grant.strip_prefix("task:spawn:").and_then(|grant| grant.strip_prefix(name))
                .and_then(|grant| grant.strip_prefix('@')) == Some(version)
            {
                descendant = true;
                break;
            }
        }
        if !descendant {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) { return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized, reason: "original scope lacks the registered descendant grant",
            }); }
            require_descendant_grant(scope.grants(), entry.workflow_definition.identity())?;
        }
        if parent.is_some() && (scope.extensions().is_some() || entry.workflow_definition.requirements().iter().any(|requirement| requirement.starts_with("extension:"))) {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) { return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported, reason: "stock child extension runner is not composed",
            }); }
            return Err(Error::Unsupported("stock child extension runner is not composed".into()));
        }
        if work.is_none() {
            return TaskAdmissionRecord::from_parts(
                operation, name, version, input, entry.workflow_definition.input_schema().clone(), entry.workflow_definition.output_schema().clone(),
                entry.workflow_definition.requirements(), &machine.digest, parent, scope.grants().clone(),
                scope.limits(), scope.run_limits(), None, scope.extensions().cloned(), None,
            ).and_then(|record| record.with_resource_proofs(scope.resource_proofs().to_vec()));
        }
        task_registered_portable_admission(operation, input, parent, scope, entry, machine, work)
    }

    pub(crate) fn validate_durable_admission(
        &self, identity: &ComponentIdentity, machine: &MachineIdentity,
        input_schema: Option<&Value>, output_schema: &Value, input: Option<&Value>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut quota = crate::wire_codec::CodecQuota::new(work,0)?;
        let name = crate::core::copy_text(&identity.name, &mut quota)?;
        let version = crate::core::copy_text(&identity.version, &mut quota)?;
        let lookup = (name,version);
        let entry = match self.0.get(&lookup) {
            Some(entry) => entry,
            None => {
                #[cfg(all(feature = "filesystem",not(target_arch = "wasm32")))]
                if quota.work().as_ref().is_some_and(|io| io.original_native_work().is_some()) {
                    return Err(Error::NativeBoundary {code:crate::wire::ErrorCode::NotFound,
                        reason:"pinned durable task revision is unavailable"});
                }
                return Err(Error::NotFound(format!("task {}@{}",identity.name,identity.version)));
            }
        };
        if entry.workflow_definition.identity() != identity
            || entry.machine.is_none()
            || entry.machine.as_ref() != Some(machine)
            || input_schema.is_some_and(|schema| schema != entry.workflow_definition.input_schema())
            || entry.workflow_definition.output_schema() != output_schema
        {
            return Err(Error::Conflict(
                "durable admission differs from its registered task definition".into(),
            ));
        }
        if let Some(input) = input {
            crate::contract::validate_json_schema_value(
                entry.workflow_definition.input_schema(),input,"task input",quota.work(),
            )?;
        }
        Ok(())
    }
}
