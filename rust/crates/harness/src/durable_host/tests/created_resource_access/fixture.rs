use super::*;

impl CreationFixture {
    pub(crate) async fn new() -> Result<Self> {
        Self::new_with_grants(&[]).await
    }
    pub(crate) async fn new_with_grants(grants: &[&str]) -> Result<Self> {
        Self::open(grants, None).await
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn new_with_original_root_births(
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        Self::open(&[], Some(work)).await
    }

    async fn open(extra: &[&str], native: crate::NativeWork<'_>) -> Result<Self> {
        let stream = Arc::new(MemoryStream::default());
        let filesystem_provider = ProviderRef::new(
            "created-controls".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let backend = Fs::memory();
        let filesystem = FilesystemHost::new(backend.clone(), &filesystem_provider, None)?;
        let payload = VolumeRef::new(
            filesystem_provider.clone(),
            "scheduler".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
            &mut None,
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let original_root_births = if let Some(work) = native {
            Some(super::roots::create(&backend, &filesystem, &payload, work).await?)
        } else {
            filesystem.create_volume(&payload).await?;
            None
        };
        #[cfg(target_arch = "wasm32")]
        let original_root_births = {
            filesystem.create_volume(&payload).await?;
            None
        };
        let _ = native;
        let authority = Authority {
            kind: AggregateKind::Task,
            id: "created-owner".into(),
        };
        let issuer = AuthorityIssuer::new("created-owner", [80; 32], authority.clone());
        let verifier = issuer.verifier(&mut None)?;
        let mut grants: Vec<String> = [
            "operation:declare",
            "operation:observe",
            "operation:cancel",
            "task:spawn:test.created@1",
            "fork:publish",
            "conversation:bind",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        grants.extend(extra.iter().map(|grant| (*grant).to_owned()));
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            grants.push(payload.capability(operation, &mut None)?);
        }
        for (name, class, owner) in [
            (
                "parent-project",
                VolumeClass::Project,
                VolumeOwner::Project("created-project".into()),
            ),
            (
                "parent-private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([90; 16])),
            ),
        ] {
            grants.push(
                VolumeRef::new(
                    filesystem_provider.clone(),
                    name.into(),
                    class,
                    owner,
                    &mut None,
                )?
                .capability(VolumeOperation::Read, &mut None)?,
            );
        }
        let grants = Capabilities::new(grants);
        let signed = issuer.issue(
            "owner",
            &grants,
            Some(AgentId::from_bytes([80; 16])),
            None,
            &mut None,
        )?;
        let root_scope = RuntimeScope::new(grants, Limits::default())?.with_run_limits(
            crate::runtime::TaskRunLimits {
                concurrency: Some(16),
                max_steps: Some(4),
                deadline_epoch_ms: None,
            },
            &mut None,
        )?;
        let schema = serde_json::json!({"type":"object"});
        let (task, machine, tasks, machines) = test_registries::<Value>(
            Arc::new(super::machine::CreatedControlMachine {
                identity: MachineIdentity {
                    name: "test.created".into(),
                    version: "1".into(),
                    digest: *blake3::hash(include_str!("machine.rs").as_bytes()).as_bytes(),
                },
                schema: schema.clone(),
            }),
            schema.clone(),
            schema,
        )?;
        let creator = FilesystemResourceCreationProvider::new(
            filesystem.clone(),
            &issuer,
            #[cfg(not(target_arch = "wasm32"))]
            native,
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let filesystem_creator = match native {
            Some(work) => creator.into_native_shared(work)?,
            None => Arc::new(creator),
        };
        #[cfg(target_arch = "wasm32")]
        let filesystem_creator = Arc::new(creator);
        let runtime = NativeCustomerRuntime::open(
            stream.clone(),
            filesystem.clone(),
            &payload,
            &verifier,
            &signed,
            &root_scope,
            &tasks,
            &machines,
            &crate::tool::ToolRegistry::default(),
            crate::scheduler::SessionLimits {
                active_tasks: 16,
                total_tasks: 32,
                depth: 4,
                model_steps: 4,
            },
            16,
            root_scope.limits().file_bytes,
            Arc::new(SystemUnixMillisClock),
            None,
            &[filesystem_creator.clone()],
            &crate::durable_tool::ResumableToolRegistry::default(),
            None,
        )
        .await?;
        let host = runtime.task_host().clone();
        let harness = runtime.harness().clone();
        let worker = crate::distributed::Worker {
            id: "created-worker".into(),
            available: Default::default(),
            labels: BTreeMap::new(),
        };
        let root = TaskId::from_bytes([82; 16]);
        let admission = TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id: OperationId::from_bytes([82; 16]),
            task: task.clone(),
            machine: machine.clone(),
            input: serde_json::json!({}),
            input_schema: serde_json::json!({"type":"object"}),
            output_schema: serde_json::json!({"type":"object"}),
            parent: None,
            grants: root_scope.grants().clone(),
            limits: root_scope.limits(),
            run_limits: root_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        };
        assert!(matches!(host.admit(admission).await?, Admission::Accepted(id) if id == root));
        let crate::distributed::WorkPull::Claimed(root_lease) = host.pull_work(&worker).await?
        else {
            return Err(Error::NotFound("created root lease".into()));
        };
        host.start_task(&root_lease, None).await?;
        Ok(Self {
            host,
            harness,
            filesystem,
            stream,
            filesystem_provider,
            filesystem_creator,
            root_scope,
            root,
            root_lease,
            worker,
            original_root_births,
            task,
            machine,
        })
    }

    pub(crate) fn admission(&self, byte: u8, input: Value) -> TaskAdmissionRecord {
        TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id: OperationId::from_bytes([byte; 16]),
            task: self.task.clone(),
            machine: self.machine.clone(),
            input,
            input_schema: serde_json::json!({"type":"object"}),
            output_schema: serde_json::json!({"type":"object"}),
            parent: Some(self.root),
            grants: self.root_scope.grants().clone(),
            limits: self.root_scope.limits(),
            run_limits: self.root_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeCustomerFixture {
    pub(crate) async fn new_with_grants(
        grants: &[&str],
        tools: crate::tool::ToolRegistry,
    ) -> Result<Self> {
        let creation = CreationFixture::new_with_grants(grants).await?;
        let runtime = creation.runtime(tools)?;
        Ok(Self { creation, runtime })
    }
}

impl CreationFixture {
    pub(super) fn runtime(
        &self,
        tools: crate::tool::ToolRegistry,
    ) -> Result<NativeCustomerRuntime> {
        let volume = VolumeRef::new(
            self.filesystem_provider.clone(),
            "scheduler".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
            &mut None,
        )?;
        NativeCustomerRuntime::from_host(
            self.stream.clone(),
            self.harness.clone(),
            self.host.clone(),
            self.filesystem.clone(),
            volume,
            self.host.verifier.clone(),
            self.host.owner_scope.clone(),
            tools,
            self.root_scope.limits().file_bytes,
            None,
            None,
        )
    }
}
