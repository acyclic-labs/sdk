use super::*;
use acyclic_fs::{MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_harness::core::{AuthorityVerifier, Scope};

type Host = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;
type Runtime = FilesystemTaskRuntime<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>;

pub(crate) struct Fixture {
    pub stream: Arc<MemoryStream>,
    pub runtime: Runtime,
    pub files: Arc<Host>,
    pub volume: VolumeRef,
    pub verifier: AuthorityVerifier,
    pub signed: Scope,
    pub tasks: TaskRegistry,
    pub revision: String,
    pub publisher: Arc<FilesystemContentPublisher<MemoryAuthorityBackend, MemoryObjectBackend>>,
    pub requests: TaskVolume<MemoryStream>,
    pub results: TaskVolume<MemoryStream>,
}

impl Fixture {
    pub fn new(binding: VolumeBinding) -> impl Future<Output = Result<Self>> {
        Self::at_revision(binding, "2")
    }

    pub fn at_revision(
        binding: VolumeBinding,
        revision: &str,
    ) -> impl Future<Output = Result<Self>> {
        Box::pin(Self::open(binding, revision))
    }

    async fn open(binding: VolumeBinding, selected_revision: &str) -> Result<Self> {
        let provider = ProviderRef::new(
            "session-worker".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let files = FilesystemHost::new(Fs::memory(), &provider, None)?;
        let agent = AgentId::from_bytes([61; 16]);
        let volume = VolumeRef::new(
            provider,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        files.create_volume(&volume).await?;
        let issuer = AuthorityIssuer::new(
            "session-worker",
            [62; 32],
            Authority {
                kind: AggregateKind::Task,
                id: "session-worker".into(),
            },
        );
        let grants = Capabilities::new([
            "operation:declare".to_owned(),
            "operation:observe".to_owned(),
            "operation:wake".to_owned(),
            "operation:cancel".to_owned(),
            "task:spawn:acyclic.command_session@1".to_owned(),
            "tool:call:write".to_owned(),
            "mail:read".to_owned(),
            "mail:send".to_owned(),
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
            volume.directory_read_capability("", &mut None)?,
        ]);
        let signed = issuer.issue("owner", &grants, Some(agent), None, &mut None)?;
        let verifier = issuer.verifier(&mut None)?;
        let scope = RuntimeScope::new(grants, Limits::default())?;
        let machine = Arc::new(CommandSessionMachine::new());
        let mut tasks = TaskRegistry::default();
        tasks.register(machine.definition()?)?;
        let mut machines = MachineRegistry::default();
        machines.register(machine, &mut None)?;
        let mut tools = acyclic_harness::tool::ToolRegistry::default();
        let write = acyclic_harness::tool::files::write()?;
        let revision = write.definition.revision.clone();
        let replacement = (binding == VolumeBinding::RetainedRevision).then(|| write.clone());
        tools.register((write, None), &mut None, None)?;
        if let Some(mut replacement) = replacement {
            replacement.definition.revision = selected_revision.into();
            tools.register((replacement, None), &mut None, None)?;
            tools.select_model_version("write", selected_revision)?;
        }
        let stream = Arc::new(MemoryStream::default());
        let requests = TaskVolume::open(
            stream.clone(),
            StreamPath::new("requests").map_err(storage)?,
        )
        .await
        .map_err(storage)?;
        let results = TaskVolume::open(
            if binding == VolumeBinding::Foreign {
                Arc::new(MemoryStream::default())
            } else {
                stream.clone()
            },
            StreamPath::new(if binding == VolumeBinding::Shared {
                "requests"
            } else {
                "results"
            })
            .map_err(storage)?,
        )
        .await
        .map_err(storage)?;
        let runtime = Box::pin(FilesystemTaskRuntime::open(
            stream.clone(),
            files.clone(),
            &volume,
            &verifier,
            &signed,
            &scope,
            &tasks,
            &machines,
            &tools,
            if binding == VolumeBinding::Recursive {
                SessionLimits {
                    active_tasks: 8,
                    total_tasks: 16,
                    depth: 4,
                    model_steps: 16,
                }
            } else {
                SessionLimits {
                    active_tasks: 1,
                    total_tasks: 1,
                    depth: 1,
                    model_steps: 1,
                }
            },
            1,
            65_536,
            Arc::new(SystemUnixMillisClock),
            None,
            &[],
            &ResumableToolRegistry::default(),
            None,
        ))
        .await?
        .with_output_volume(&volume, None)?;
        let publisher = FilesystemContentPublisher::new(
            files.clone(),
            &volume,
            &verifier,
            &signed,
            65_536,
            None,
        )?;
        Ok(Self {
            stream,
            runtime,
            files,
            volume,
            verifier,
            signed,
            tasks,
            revision,
            publisher,
            requests,
            results,
        })
    }
}

#[tokio::test]
async fn fixture_creation_future_is_boxed_at_the_runtime_boundary() -> Result<()> {
    {
        let raw = Fixture::open(VolumeBinding::Separate, "2");
        let future = Fixture::new(VolumeBinding::Separate);
        eprintln!(
            "fixture bootstrap futures: raw {} bytes, boxed {} bytes",
            std::mem::size_of_val(&raw),
            std::mem::size_of_val(&future)
        );
        assert!(std::mem::size_of_val(&future) < 16 * 1024);
    }
    let fixture = Fixture::new(VolumeBinding::Separate).await?;
    let definition = fixture
        .tasks
        .get::<Value, Value>("acyclic.command_session@1")?;
    {
        let future = fixture.runtime.harness().admit(
            OperationId::from_bytes([80; 16]),
            &definition,
            Value::Null,
            None,
        );
        eprintln!(
            "task admission future: {} bytes",
            std::mem::size_of_val(&future)
        );
    }
    {
        let future = fixture.runtime.harness().durable_context(
            TaskId::from_bytes([80; 16]),
            OperationId::from_bytes([80; 16]),
            None,
        );
        eprintln!(
            "task context future: {} bytes",
            std::mem::size_of_val(&future)
        );
    }
    {
        let future = fixture.files.create_volume(&fixture.volume);
        eprintln!(
            "volume creation future: {} bytes",
            std::mem::size_of_val(&future)
        );
    }
    Ok(())
}
