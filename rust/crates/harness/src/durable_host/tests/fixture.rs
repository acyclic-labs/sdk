//! Shared real-provider fixture for durable host behavior tests.
use super::*;
use crate::filesystem::{FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost};
type Execution<P, A, O> = crate::filesystem::FilesystemExecutionJournal<P, A, O>;
type Workflow<P, A, O> = crate::filesystem::FilesystemWorkflowJournal<P, A, O>;
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, Fs, MemoryAuthorityBackend, MemoryObjectBackend,
};

pub(crate) struct Fixture<
    P: StreamProvider = MemoryStream,
    A = MemoryAuthorityBackend,
    O = MemoryObjectBackend,
> {
    pub(crate) stream: Arc<P>,
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    pub(crate) backend: Fs<A, O>,
    pub(crate) files: Arc<FilesystemHost<A, O>>,
    pub(crate) issuer: AuthorityIssuer,
    pub(crate) host: Arc<CoordinatorTaskHost<P>>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) destination: VolumeRef,
    pub(crate) storage: VolumeRef,
    pub(crate) reader: Arc<FilesystemContentVerifier<A, O>>,
    pub(crate) publisher: Arc<FilesystemContentPublisher<A, O>>,
    pub(crate) task: ComponentIdentity,
    pub(crate) machine: MachineIdentity,
    pub(crate) schema: Value,
}

impl<P, A, O> Fixture<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    pub(crate) async fn owner(&self, byte: u8) -> Result<TaskJournalOwner<P>> {
        let lease = match self
            .host
            .recover_work(TaskId::from_bytes([byte; 16]), &mut None)
            .await?
        {
            crate::distributed::WorkPull::Claimed(lease)
            | crate::distributed::WorkPull::Unresolved { lease, .. } => lease,
            crate::distributed::WorkPull::Idle => {
                return Err(Error::NotFound("running fixture owner".into()));
            }
        };
        self.host
            .journal_owner(
                TaskId::from_bytes([byte; 16]),
                &((&lease.reservation).into()),
                None,
            )
            .await
    }

    pub(crate) async fn task_files(&self) -> Result<Arc<crate::task_volume::TaskFiles<P>>> {
        Ok(Arc::new(
            crate::task_volume::TaskFiles::open(
                self.stream.clone(),
                &self.storage,
                &self.issuer.verifier(&mut None)?,
                &self.host.owner_scope,
                65_536,
            )
            .await?,
        ))
    }

    pub(crate) async fn native_execution(
        &self,
        byte: u8,
        operation: OperationId,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<Execution<P, A, O>> {
        let fence = self.owner(byte).await?.task_binding().1.clone();
        Execution::for_task(
            self.host
                .journal_owner(TaskId::from_bytes([byte; 16]), &fence, Some(io))
                .await?,
            operation,
            operation,
            self.files.clone(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &self.storage,
                verifier: &self.issuer.verifier(&mut None)?,
                scope: &self.host.owner_scope,
                maximum_bytes: 65_536,
                io: Some(io),
            },
        )
    }

    pub(crate) fn execution(
        &self,
        owner: TaskJournalOwner<P>,
        operation: OperationId,
    ) -> Result<Execution<P, A, O>> {
        Execution::for_task(
            owner,
            operation,
            operation,
            self.files.clone(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &self.storage,
                verifier: &self.issuer.verifier(&mut None)?,
                scope: &self.host.owner_scope,
                maximum_bytes: 65_536,
                io: None,
            },
        )
    }

    pub(crate) fn workflow(&self, owner: TaskJournalOwner<P>) -> Result<Arc<Workflow<P, A, O>>> {
        Workflow::for_task(
            Arc::new(owner),
            self.files.clone(),
            crate::filesystem::FilesystemJournalBinding {
                volume: &self.storage,
                verifier: &self.issuer.verifier(&mut None)?,
                scope: &self.host.owner_scope,
                maximum_bytes: 65_536,
                io: None,
            },
        )
    }

    pub(crate) async fn replace(&self, byte: u8) -> Result<TaskJournalOwner<P>> {
        let task = TaskId::from_bytes([byte; 16]);
        self.host
            .suspend_workflow(
                task,
                self.owner(byte).await?.task_binding().1,
                (1, None),
                None,
                None,
                &mut None,
            )
            .await?;
        let (input, _) = crate::conversation::ContentPublisher::stage(
            self.publisher.as_ref(),
            OperationId::from_bytes([byte; 16]),
            "replacement.json",
            b"0",
            "application/json",
            "replacement.json",
            None,
        )?
        .await?;
        self.host
            .resume_workflow(
                task,
                1,
                input,
                IdempotencyKey::new("replace")?,
                None,
                &mut None,
            )
            .await?;
        self.claim(byte, "replacement").await?;
        self.owner(byte).await
    }

    pub(crate) async fn running(&self, byte: u8) -> Result<TaskJournalOwner<P>> {
        self.host.admit(self.admission(byte, json!(0))).await?;
        self.claim(byte, "original").await?;
        self.owner(byte).await
    }
    pub(crate) async fn new(
        stream: Arc<P>,
        backend: Fs<A, O>,
        schema: Value,
        session: Option<crate::scheduler::SessionLimits>,
        extra: &[&str],
    ) -> Result<Self> {
        let provider =
            ProviderRef::new("fixture".into(), "filesystem".into(), "2".into(), &mut None)?;
        let files = FilesystemHost::new(backend.clone(), &provider, None)?;
        let volume = |name: &str| {
            VolumeRef::new(
                provider.clone(),
                name.into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([1; 16])),
                &mut None,
            )
        };
        let destination = volume("destination")?;
        let storage = volume("storage")?;
        for volume in [&destination, &storage] {
            let reference = crate::resources::WorkspaceRef::new(
                provider.clone(),
                volume.storage_name(&mut None)?.into_bytes(),
                None,
            )?;
            match files.resolve(&reference, None).await {
                Ok(_) => (),
                Err(Error::NotFound(_)) => {
                    files.create_volume(volume).await?;
                }
                Err(error) => return Err(error),
            }
        }
        let issuer = AuthorityIssuer::new(
            "fixture",
            [7; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "fixture-owner".into(),
            },
        );
        let mut grants: Vec<String> = [
            "operation:declare",
            "operation:observe",
            "operation:cancel",
            "operation:wake",
            "task:spawn:test.fixture@1",
            "model:generate",
            "mail:send",
            "mail:read",
            "effect:run",
            "effect:plan",
            "effect:provider:test.native",
            "interaction:route",
            "fork:publish",
            "resource:create:custom",
            "conversation:bind",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        grants.extend(extra.iter().map(|grant| (*grant).to_owned()));
        for volume in [&destination, &storage] {
            grants.push(volume.capability(VolumeOperation::Read, &mut None)?);
        }
        for volume in [&destination, &storage] {
            grants.push(volume.capability(VolumeOperation::Write, &mut None)?);
        }
        let scope = RuntimeScope::new(Capabilities::new(grants), Limits::default())?;
        let signed = issuer.issue(
            "owner",
            scope.grants(),
            Some(AgentId::from_bytes([1; 16])),
            None,
            &mut None,
        )?;
        let implementation = Arc::new(FixtureMachine {
            identity: MachineIdentity {
                name: "test.fixture".into(),
                version: "1".into(),
                digest: [2; 32],
            },
            schema: schema.clone(),
        });
        #[cfg(not(target_arch = "wasm32"))]
        let metadata_work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(65_536, 64 * 1_024 * 1_024),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        #[cfg(not(target_arch = "wasm32"))]
        let metadata_io = Some(crate::filesystem::PreparationIo::from_native_work(
            &metadata_work,
        )?);
        #[cfg(target_arch = "wasm32")]
        let metadata_io = None;
        let (task, machine, tasks, machines) = super::created_fixture::test_registries(
            implementation,
            schema.clone(),
            schema.clone(),
            metadata_io.as_ref(),
        )?;
        let verifier = issuer.verifier(&mut None)?;
        let reader = FilesystemContentVerifier::new(
            files.clone(),
            &verifier,
            &signed,
            scope.limits().file_bytes,
            None,
        )?;
        let publisher = FilesystemContentPublisher::new(
            files.clone(),
            &storage,
            &verifier,
            &signed,
            scope.limits().file_bytes,
            None,
        )?;
        let payloads = FilesystemContentPublisher::new(
            files.clone(),
            &storage,
            &verifier,
            &signed,
            scope.limits().file_bytes,
            None,
        )?;
        let coordinator = DistributedCoordinator::open(
            &stream,
            reader.clone(),
            Some(payloads.clone()),
            &mut None,
        )
        .await?;
        let mut host = CoordinatorTaskHost::new(
            coordinator,
            stream.clone(),
            payloads,
            reader.clone(),
            issuer.audience().clone(),
            signed,
            verifier,
            scope,
            tasks,
            machines,
            Arc::new(SystemUnixMillisClock),
        )?;
        if let Some(limits) = session {
            host = host.with_session_limits(limits)?;
        }
        Ok(Self {
            stream,
            #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
            backend,
            files,
            issuer,
            host: Arc::new(host),
            #[cfg(not(target_arch = "wasm32"))]
            destination,
            storage,
            reader,
            publisher,
            task,
            machine,
            schema,
        })
    }

    pub(crate) fn admission(&self, byte: u8, input: Value) -> TaskAdmissionRecord {
        TaskAdmissionRecord {
            operation_id: OperationId::from_bytes([byte; 16]),
            resource_proofs: Vec::new(),
            task: self.task.clone(),
            machine: self.machine.clone(),
            input,
            input_schema: self.schema.clone(),
            output_schema: self.schema.clone(),
            parent: None,
            grants: self.host.root_scope.grants().clone(),
            limits: self.host.root_scope.limits(),
            run_limits: self.host.root_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        }
    }

    pub(crate) async fn claim(
        &self,
        byte: u8,
        worker: &str,
    ) -> Result<crate::distributed::WorkLease> {
        let crate::distributed::WorkPull::Claimed(lease) = self
            .host
            .pull_work(
                &crate::distributed::Worker {
                    id: worker.into(),
                    available: Default::default(),
                    labels: BTreeMap::new(),
                },
                Some(OperationId::from_bytes([byte; 16])),
                &mut None,
            )
            .await?
        else {
            return Err(Error::NotFound("fixture lease".into()));
        };
        self.host.start_task(&lease, &mut None).await?;
        Ok(lease)
    }

    pub(crate) async fn reopen(
        &self,
        issuer: &AuthorityIssuer,
        subject: &str,
    ) -> Result<Arc<CoordinatorTaskHost<P>>> {
        let signed = issuer.issue(
            subject,
            self.host.root_scope.grants(),
            Some(AgentId::from_bytes([1; 16])),
            None,
            &mut None,
        )?;
        let verifier = issuer.verifier(&mut None)?;
        let payloads = FilesystemContentPublisher::new(
            self.files.clone(),
            &self.storage,
            &verifier,
            &signed,
            self.host.root_scope.limits().file_bytes,
            None,
        )?;
        let mut host = CoordinatorTaskHost::new(
            DistributedCoordinator::open(
                &self.stream,
                self.reader.clone(),
                Some(payloads.clone()),
                &mut None,
            )
            .await?,
            self.stream.clone(),
            payloads,
            self.reader.clone(),
            issuer.audience().clone(),
            signed,
            verifier,
            self.host.root_scope.clone(),
            self.host.tasks.clone(),
            self.host.machines.clone(),
            Arc::new(SystemUnixMillisClock),
        )?;
        if let Some(limits) = self.host.session_limits {
            host = host.with_session_limits(limits)?;
        }
        Ok(Arc::new(host))
    }

    pub(crate) fn batch(&self, byte: u8, parent: Option<TaskId>) -> DurableBatchRequest {
        DurableBatchRequest {
            group_id: GroupId::from_bytes([9; 16]),
            batch_id: BatchId::from_bytes([byte; 16]),
            group_policy: crate::runtime::BatchGroupPolicy::CollectAll,
            task: self.task.clone(),
            machine: self.machine.clone(),
            inputs: vec![json!(1), json!(2), json!(3)],
            input_schema: self.schema.clone(),
            output_schema: self.schema.clone(),
            parent,
            scope: self.host.root_scope.clone(),
            policy: None,
            execution: None,
        }
    }
}

pub(super) async fn fixture() -> Result<Fixture> {
    Fixture::new(
        Arc::new(MemoryStream::default()),
        Fs::memory(),
        json!({"type":"integer"}),
        None,
        &[],
    )
    .await
}

pub(super) struct FixtureMachine {
    pub(super) identity: MachineIdentity,
    pub(super) schema: Value,
}
impl ResumableMachine for FixtureMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        self.transition_on(state, input, None)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_work(&self) -> bool {
        true
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn identity_with_native_work<'a>(
        &'a self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<&'a MachineIdentity> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)?;
        Ok(&self.identity)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn initialize_with_native_work(
        &self,
        input: &Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Value, acyclic_fs::NativeAllocation)> {
        crate::tool::clone_value(input, None, work)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn transition_with_native_work(
        &self,
        state: &Value,
        input: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let transition = self.transition_on(state, &input, Some(&io))?;
        io.retain_owned(
            std::mem::size_of::<MachineTransition>().saturating_sub(std::mem::size_of::<Value>())
                as u64,
        )?;
        let custody = io.take_native_custody(io.owned_bytes()?)?;
        Ok((transition, custody))
    }
}
impl FixtureMachine {
    fn transition_on(
        &self,
        state: &Value,
        input: &Value,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<MachineTransition> {
        let copy = |value: &Value| {
            crate::executor::with_history_io(io, |work| {
                crate::wire_codec::CodecQuota::produce(work, |quota| {
                    crate::core::copy(value, quota, |value, _| {
                        crate::executor::json::json_value_logical_owned_bytes(value)
                    })
                })
                .map(|(value, _)| value)
            })
        };
        let literal = |bytes: &[u8]| match io {
            Some(io) => io.decode_json::<Value>(bytes),
            None => crate::contract::json_from_slice(bytes)
                .map_err(|error| Error::Invalid(error.to_string())),
        };
        if state.get("emitted") == Some(&Value::Bool(true)) {
            return Ok(MachineTransition {
                state: copy(state)?,
                commands: Vec::new(),
                status: MachineStatus::Completed {
                    value: literal(br#"{"ok":true}"#)?,
                },
            });
        }
        let command = input
            .get("created_command")
            .map(|value| match io {
                Some(io) => Ok(crate::filesystem::retain_decoded(
                    io.value_decode_preparation(value, crate::wire::ErrorCode::Invalid)?,
                )?),
                None => serde_json::from_value(value.clone())
                    .map_err(|error| Error::Invalid(error.to_string())),
            })
            .transpose()?;
        let state = if command.is_some() {
            literal(br#"{"emitted":true}"#)?
        } else {
            copy(state)?
        };
        let mut commands = Vec::new();
        if let Some(command) = command {
            crate::executor::stock_vec_push(&mut commands, command, io)?;
        }
        Ok(MachineTransition {
            state,
            commands,
            status: MachineStatus::Suspended,
        })
    }
}
