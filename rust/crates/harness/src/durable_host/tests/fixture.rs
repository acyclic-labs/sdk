//! Shared real-provider fixture for durable host behavior tests.
use super::*;
use crate::filesystem::{
    FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost,
    FilesystemSchedulerPayloadStore,
};
use crate::runtime::{TaskDefinition, TaskRegistry};
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
    pub(crate) backend: Fs<A, O>,
    pub(crate) files: Arc<FilesystemHost<A, O>>,
    pub(crate) issuer: AuthorityIssuer,
    pub(crate) signed: Scope,
    pub(crate) scope: RuntimeScope,
    pub(crate) host: Arc<CoordinatorTaskHost<P>>,
    pub(crate) source: VolumeRef,
    pub(crate) destination: VolumeRef,
    pub(crate) storage: VolumeRef,
    pub(crate) reader: Arc<FilesystemContentVerifier<A, O>>,
    pub(crate) publisher: Arc<FilesystemContentPublisher<A, O>>,
    pub(crate) task: ComponentIdentity,
    pub(crate) machine: MachineIdentity,
    pub(crate) schema: Value,
    pub(crate) tasks: TaskRegistry,
    pub(crate) machines: MachineRegistry,
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
            .recover_work(TaskId::from_bytes([byte; 16]))
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
            &self.storage,
            &self.issuer.verifier(&mut None)?,
            &self.signed,
            65_536,
            None,
        )
    }

    pub(crate) fn workflow(&self, owner: TaskJournalOwner<P>) -> Result<Arc<Workflow<P, A, O>>> {
        Workflow::for_task(
            owner,
            self.files.clone(),
            &self.storage,
            &self.issuer.verifier(&mut None)?,
            &self.signed,
            65_536,
            None,
        )
    }

    pub(crate) async fn replace(&self, byte: u8) -> Result<TaskJournalOwner<P>> {
        use crate::conversation::ContentPublisher;
        let task = TaskId::from_bytes([byte; 16]);
        self.host
            .suspend_workflow(
                task,
                self.owner(byte).await?.task_binding().1,
                1,
                None,
                None,
                None,
                &mut None,
            )
            .await?;
        let (input, _) = self
            .publisher
            .stage(
                OperationId::from_bytes([byte; 16]),
                "replacement.json",
                b"0",
                "application/json",
                "replacement.json",
                None,
            )?
            .await?;
        self.host
            .resume_workflow(task, 1, input, IdempotencyKey::new("replace")?)
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
        let source = VolumeRef::new(
            provider.clone(),
            "source".into(),
            VolumeClass::Project,
            VolumeOwner::Project("fixture-project".into()),
            &mut None,
        )?;
        let destination = volume("destination")?;
        let storage = volume("storage")?;
        for volume in [&source, &destination, &storage] {
            let reference = crate::resources::WorkspaceRef::new(
                provider.clone(),
                volume.storage_name(&mut None)?.into_bytes(),
                None,
            )?;
            match files.resolve(&reference).await {
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
        for volume in [&source, &destination, &storage] {
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
        let definition = TaskDefinition::<Value, Value>::resumable(
            implementation.clone(),
            schema.clone(),
            schema.clone(),
        )?;
        let task = definition.identity().clone();
        let machine = implementation.identity().clone();
        let mut tasks = TaskRegistry::default();
        tasks.register(definition)?;
        let mut machines = MachineRegistry::default();
        machines.register(implementation, &mut None)?;
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
        let payloads = Arc::new(FilesystemSchedulerPayloadStore::new(
            files.clone(),
            &storage,
            &verifier,
            &signed,
            scope.limits().file_bytes,
            None,
        )?);
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
            signed.clone(),
            verifier,
            scope.clone(),
            tasks.clone(),
            machines.clone(),
            Arc::new(SystemUnixMillisClock),
        )?;
        if let Some(limits) = session {
            host = host.with_session_limits(limits)?;
        }
        Ok(Self {
            stream,
            backend,
            files,
            issuer,
            signed,
            scope,
            host: Arc::new(host),
            source,
            destination,
            storage,
            reader,
            publisher,
            task,
            machine,
            schema,
            tasks,
            machines,
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
            grants: self.scope.grants().clone(),
            limits: self.scope.limits(),
            run_limits: self.scope.run_limits(),
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
            .pull_operation(
                &crate::distributed::Worker {
                    id: worker.into(),
                    available: Default::default(),
                    labels: BTreeMap::new(),
                },
                OperationId::from_bytes([byte; 16]),
            )
            .await?
        else {
            return Err(Error::NotFound("fixture lease".into()));
        };
        self.host.start_task(&lease).await?;
        Ok(lease)
    }

    pub(crate) async fn reopen(
        &self,
        issuer: &AuthorityIssuer,
        subject: &str,
    ) -> Result<Arc<CoordinatorTaskHost<P>>> {
        let signed = issuer.issue(
            subject,
            self.scope.grants(),
            Some(AgentId::from_bytes([1; 16])),
            None,
            &mut None,
        )?;
        let verifier = issuer.verifier(&mut None)?;
        let payloads = Arc::new(FilesystemSchedulerPayloadStore::new(
            self.files.clone(),
            &self.storage,
            &verifier,
            &signed,
            self.scope.limits().file_bytes,
            None,
        )?);
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
            self.scope.clone(),
            self.tasks.clone(),
            self.machines.clone(),
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
            scope: self.scope.clone(),
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

struct FixtureMachine {
    identity: MachineIdentity,
    schema: Value,
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
        if state.get("emitted") == Some(&Value::Bool(true)) {
            return Ok(MachineTransition {
                state: state.clone(),
                commands: Vec::new(),
                status: MachineStatus::Completed {
                    value: json!({"ok":true}),
                },
            });
        }
        let commands = input
            .get("created_command")
            .map(|command| {
                serde_json::from_value(command.clone()).map_err(|e| Error::Invalid(e.to_string()))
            })
            .transpose()?;
        Ok(MachineTransition {
            state: if commands.is_some() {
                json!({"emitted":true})
            } else {
                state.clone()
            },
            commands: commands.into_iter().collect(),
            status: MachineStatus::Suspended,
        })
    }
}
