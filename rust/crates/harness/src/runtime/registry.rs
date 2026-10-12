use super::*;

pub(super) trait RegisteredTaskDefinition: Any + acyclic_stream::ProviderPlatform {
    fn identity(&self) -> &ComponentIdentity;
    fn input_schema(&self) -> &Value;
    fn output_schema(&self) -> &Value;
    fn requirements(&self) -> &BTreeSet<String>;
    fn machine(&self) -> Option<&dyn ResumableMachine>;
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
        self: Arc<Self>,
        context: Arc<TaskContext>,
        fence: &'a crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ResumableTaskSession>>;
}

impl<I: 'static, O: 'static> RegisteredTaskDefinition for TaskDefinition<I, O>
where
    TaskDefinition<I, O>: acyclic_stream::ProviderPlatform,
{
    fn identity(&self) -> &ComponentIdentity {
        &self.identity
    }
    fn input_schema(&self) -> &Value {
        &self.input_schema
    }
    fn output_schema(&self) -> &Value {
        &self.output_schema
    }
    fn requirements(&self) -> &BTreeSet<String> {
        &self.requirements
    }

    fn machine(&self) -> Option<&dyn ResumableMachine> {
        match &self.implementation {
            TaskImplementation::Resumable(machine) => Some(machine.as_ref()),
            TaskImplementation::Live(_) => None,
        }
    }

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
        self: Arc<Self>,
        context: Arc<TaskContext>,
        fence: &'a crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
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
            self.open_with_native_work(context, fence, journal, work)
                .await
        })
    }
}

/// Immutable typed definitions with exact name and version lookup.
#[derive(Default)]
pub struct TaskRegistry(
    pub(super) Vec<Arc<dyn RegisteredTaskDefinition>>,
    #[cfg(target_arch = "wasm32")]
    Vec<Arc<dyn Any>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super)  Option<acyclic_fs::NativeAllocation>,
);

impl Clone for TaskRegistry {
    fn clone(&self) -> Self {
        Self(
            self.0.clone(),
            #[cfg(target_arch = "wasm32")]
            self.1.clone(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            None,
        )
    }
}

impl TaskRegistry {
    fn definition<I: 'static, O: 'static>(&self, definition: &Arc<dyn RegisteredTaskDefinition>) -> Option<Arc<TaskDefinition<I, O>>> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let definition: Arc<dyn Any + Send + Sync> = definition.clone();
            definition.downcast().ok()
        }
        #[cfg(target_arch = "wasm32")]
        {
            let slot = self.0.iter().position(|stored| Arc::ptr_eq(stored, definition))?;
            self.1[slot].downcast_ref::<Arc<TaskDefinition<I, O>>>().cloned()
        }
    }

    /// Registers an exact task version.
    pub fn register<I: 'static, O: 'static>(
        &mut self,
        mut definition: TaskDefinition<I, O>,
    ) -> Result<()>
    where
        TaskDefinition<I, O>: acyclic_stream::ProviderPlatform,
    {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = definition
            .native_metadata_custody
            .as_ref()
            .map(|held| held.work().clone());
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let native: Option<&NativeWork> = None;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let registration_work = native
            .clone()
            .or_else(|| self.1.as_ref().map(|held| held.work().clone()));
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = registration_work
            .as_ref()
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = None;
        for stored in &self.0 {
            inspect_task_metadata(io.as_ref(), 1)?;
            if stored.identity().name == definition.identity.name
                && stored.identity().version == definition.identity.version
            {
                return Err(task_workflow_error(
                    task_original_work(io.as_ref())?,
                    crate::wire::ErrorCode::Conflict,
                    "task version is already registered",
                ));
            }
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = registration_work.as_ref() {
            if let Some(original) = native.as_ref() {
                definition.require_native_identity(original)?;
                // The definition retains its value; registration allocates the Arc header.
                definition
                    .native_metadata_custody
                    .as_mut()
                    .expect("native definition")
                    .grow((2 * std::mem::size_of::<usize>()) as u64)
                    .map_err(Error::from)?;
            }
            let slots = self.0.len().checked_add(1).ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "task registry capacity overflows",
            })?;
            let bytes = (slots.max(self.0.capacity())
                * std::mem::size_of::<Arc<dyn RegisteredTaskDefinition>>()
                + std::mem::size_of::<Self>()) as u64;
            if self.1.is_none() {
                self.1 = Some(
                    work.claim(bytes, 1, acyclic_fs::WorkCounters::UNCHARGED)
                        .map_err(Error::from)?,
                );
            } else if slots > self.0.capacity() {
                let held = self.1.as_mut().expect("registry allocation");
                held.grow(bytes.saturating_sub(held.bytes()))
                    .map_err(Error::from)?;
            }
            if slots > self.0.capacity() {
                self.0
                    .try_reserve_exact(slots - self.0.len())
                    .map_err(|_| Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "task registry allocation failed",
                    })?;
            }
            let actual = (self.0.capacity()
                * std::mem::size_of::<Arc<dyn RegisteredTaskDefinition>>()
                + std::mem::size_of::<Self>()) as u64;
            self.1
                .as_mut()
                .expect("registry allocation")
                .reconcile_capacity(actual)
                .map_err(Error::from)?;
        }
        let _ = native;
        let definition = Arc::new(definition);
        #[cfg(target_arch = "wasm32")]
        self.1.push(Arc::new(definition.clone()));
        self.0.push(definition);
        Ok(())
    }

    pub(super) fn find(
        &self,
        name: &str,
        version: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<&Arc<dyn RegisteredTaskDefinition>> {
        for definition in &self.0 {
            if let Some(io) = work.as_deref_mut() {
                io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
            }
            let identity = definition.identity();
            if identity.name == name && identity.version == version {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(original) = work.as_deref().and_then(|io| io.original_native_work()) {
                    if self.1.is_none() {
                        return Err(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unsupported,
                            reason: "registry metadata has no native custody",
                        });
                    }
                    definition.require_native_identity(original)?;
                }
                return Ok(definition);
            }
        }
        Err(crate::store::guarded_boundary(
            work,
            crate::wire::ErrorCode::NotFound,
            "registered task version is absent",
        ))
    }

    /// Resolves one Rust input/output pair.
    pub fn get<I: 'static, O: 'static>(&self, name: &str) -> Result<Arc<TaskDefinition<I, O>>> {
        if let Some((name, version)) = name.rsplit_once('@') {
            return self.get_version(
                name,
                version,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                None,
            );
        }
        let mut matches = self
            .0
            .iter()
            .filter(|definition| definition.identity().name == name);
        let stored = matches
            .next()
            .ok_or_else(|| Error::NotFound(format!("task {name}")))?;
        if matches.next().is_some() {
            return Err(Error::Conflict(format!(
                "task {name} requires an exact version"
            )));
        }
        self.definition(stored)
            .ok_or_else(|| Error::Conflict(format!("task {name} has different input/output types")))
    }

    /// Resolves an exact pinned version.
    pub fn get_version<I: 'static, O: 'static>(
        &self,
        name: &str,
        version: &str,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] work: Option<
            &acyclic_fs::NativeMaterializationWork,
        >,
    ) -> Result<Arc<TaskDefinition<I, O>>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = None;
        crate::executor::with_history_io(io.as_ref(), |observer| {
            let definition = self.find(name, version, observer)?;
            self.definition(definition).ok_or_else(|| {
                crate::store::guarded_boundary(
                    observer,
                    crate::wire::ErrorCode::Conflict,
                    "task version has different input/output types",
                )
            })
        })
    }

    /// Copies the registry's vector while sharing funded definitions.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        for definition in &self.0 {
            inspect_task_metadata(Some(&io), 1)?;
            definition.require_native_identity(work)?;
        }
        let bytes = (self.0.len() * std::mem::size_of::<Arc<dyn RegisteredTaskDefinition>>()
            + std::mem::size_of::<Self>()) as u64;
        let mut held = work
            .claim(bytes, 1, acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(Error::from)?;
        let mut definitions = Vec::new();
        definitions
            .try_reserve_exact(self.0.len())
            .map_err(|_| Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "task registry allocation failed",
            })?;
        held.reconcile_capacity(
            (definitions.capacity() * std::mem::size_of::<Arc<dyn RegisteredTaskDefinition>>()
                + std::mem::size_of::<Self>()) as u64,
        )
        .map_err(Error::from)?;
        definitions.extend(self.0.iter().cloned());
        Ok(Self(definitions, Some(held)))
    }

    pub(super) fn registered_admission(
        &self,
        operation: OperationId,
        name: &str,
        version: &str,
        input: Value,
        parent: Option<TaskId>,
        expected_digest: Option<[u8; 32]>,
        scope: &RuntimeScope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<TaskAdmissionRecord> {
        let definition = self.find(name, version, work)?;
        let machine = definition.machine().ok_or_else(|| {
            crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Unsupported,
                "live task cannot be a durable child command",
            )
        })?;
        let machine = {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(original) = work.as_deref().and_then(|io| io.original_native_work()) {
                machine.identity_with_native_work(original)?
            } else {
                machine.identity()
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            machine.identity()
        };
        if expected_digest.is_some_and(|digest| digest != definition.identity().digest) {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "root definition differs from the registered task",
            ));
        }
        if parent.is_some() {
            let mut descendant = false;
            for grant in scope.grants().iter() {
                if let Some(io) = work.as_deref_mut() {
                    io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect { items: 1 })?;
                }
                if grant
                    .strip_prefix("task:spawn:")
                    .and_then(|grant| grant.strip_prefix(name))
                    .and_then(|grant| grant.strip_prefix('@'))
                    == Some(version)
                {
                    descendant = true;
                    break;
                }
            }
            if !descendant {
                return Err(crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Unauthorized,
                    "original scope lacks the registered descendant grant",
                ));
            }
            if scope.extensions().is_some()
                || definition
                    .requirements()
                    .iter()
                    .any(|requirement| requirement.starts_with("extension:"))
            {
                return Err(crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Unsupported,
                    "stock child extension runner is not composed",
                ));
            }
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        if let Some(io) = quota.work().as_deref_mut() {
            let bytes =
                (std::mem::size_of::<TaskAdmissionRecord>() - std::mem::size_of::<Value>()) as u64;
            io.retain_owned(bytes)?;
            quota.adopt(bytes)?;
        }
        let extensions = scope
            .extensions()
            .map(|extension| {
                let (copied, retained) = extension.copy(quota.work())?;
                quota.adopt(retained)?;
                Ok::<_, Error>(copied)
            })
            .transpose()?;
        let record = TaskAdmissionRecord {
            operation_id: operation,
            task: task_copy_component(definition.identity(), &mut quota)?,
            machine: task_copy_machine(machine, &mut quota)?,
            input,
            input_schema: crate::core::copy_value(definition.input_schema(), &mut quota)?,
            output_schema: crate::core::copy_value(definition.output_schema(), &mut quota)?,
            parent,
            resource_proofs: RuntimeScope::copy_resource_proofs(
                scope.resource_proofs(),
                &mut quota,
            )?,
            grants: crate::core::copy_capabilities(scope.grants(), &mut quota)?,
            limits: scope.limits(),
            run_limits: scope.run_limits(),
            policy: None,
            extensions,
            execution: None,
        };
        record.validate(quota.work())?;
        quota.transfer();
        Ok(record)
    }

    pub(crate) fn validate_durable_admission(
        &self,
        identity: &ComponentIdentity,
        machine: &MachineIdentity,
        input_schema: Option<&Value>,
        output_schema: &Value,
        input: Option<&Value>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let definition = self.find(&identity.name, &identity.version, work)?;
        let registered_machine = definition
            .machine()
            .map(|machine| {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(original) = work.as_deref().and_then(|io| io.original_native_work()) {
                    return machine.identity_with_native_work(original);
                }
                Ok(machine.identity())
            })
            .transpose()?;
        if definition.identity() != identity
            || registered_machine != Some(machine)
            || input_schema.is_some_and(|schema| schema != definition.input_schema())
            || definition.output_schema() != output_schema
        {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "durable admission differs from its registered task definition",
            ));
        }
        if let Some(input) = input {
            crate::executor::validate_json_schema_value(
                definition.input_schema(),
                input,
                "task input",
                work,
            )?;
        }
        Ok(())
    }
}
