use super::*;

impl CreationFixture {
    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    pub(crate) fn signed_authority(&self) -> (&AuthorityVerifier, &Scope) {
        (&self.host.verifier, &self.host.owner_scope)
    }

    pub(crate) async fn new() -> Result<Self> {
        Self::new_with_grants(&[]).await
    }
    pub(crate) async fn new_with_grants(extra: &[&str]) -> Result<Self> {
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
        filesystem.create_volume(&payload).await?;
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
                VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
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
        let schema = serde_json::json!({"type":"object"});
        let (task, machine, tasks, machines) = test_registries(
            Arc::new(super::super::fixture::FixtureMachine {
                identity: MachineIdentity {
                    name: "test.created".into(),
                    version: "1".into(),
                    digest: [83; 32],
                },
                schema: schema.clone(),
            }),
            schema.clone(),
            schema,
            metadata_io.as_ref(),
        )?;
        let creator = FilesystemResourceCreationProvider::new(
            filesystem.clone(),
            &issuer,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )?;
        let filesystem_creator = Arc::new(creator);
        let runtime = FilesystemTaskRuntime::open(
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
            &[filesystem_creator.clone()
                as Arc<dyn crate::resource_preparation::ResourceCreationProvider>],
            &crate::durable_tool::ResumableToolRegistry::default(),
            metadata_io.as_ref(),
        )
        .await?;
        let host = runtime.task_host().clone();
        #[cfg(not(target_arch = "wasm32"))]
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
        let crate::distributed::WorkPull::Claimed(root_lease) =
            host.pull_work(&worker, None, &mut None).await?
        else {
            return Err(Error::NotFound("created root lease".into()));
        };
        host.start_task(&root_lease, &mut None).await?;
        Ok(Self {
            host,
            #[cfg(not(target_arch = "wasm32"))]
            harness,
            #[cfg(not(target_arch = "wasm32"))]
            runtime,
            filesystem,
            stream,
            filesystem_provider,
            root,
            root_lease,
            worker,
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
            grants: self.host.root_scope.grants().clone(),
            limits: self.host.root_scope.limits(),
            run_limits: self.host.root_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        }
    }
}

impl CreationFixture {
    pub(crate) async fn observation(
        &self,
        task: TaskId,
    ) -> Result<crate::scheduler::OperationState> {
        let mut coordinator = self.host.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        Ok(coordinator
            .observe_operation(
                &self.host.owner,
                &self.host.owner_scope,
                &self.host.verifier,
                OperationId::from_bytes(task.into_bytes()),
                &mut None,
            )?
            .clone())
    }
}
