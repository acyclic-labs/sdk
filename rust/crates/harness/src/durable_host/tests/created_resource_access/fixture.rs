use super::*;

#[cfg(not(target_arch = "wasm32"))]
impl NativeCustomerFixture {
    /// Setup is ordinary configured construction, before native observation.
    pub(crate) async fn new_with_grants(
        grants: &[&str],
        tools: crate::tool::ToolRegistry,
    ) -> Result<Self> {
        let creation = CreationFixture::new_with_grants(grants).await?;
        let volume = VolumeRef::new(
            creation.filesystem_provider.clone(),
            "scheduler".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
            &mut None,
        )?;
        let runtime = NativeCustomerRuntime::from_host(
            creation.stream.clone(),
            Arc::clone(&creation.harness),
            Arc::clone(&creation.host),
            Arc::clone(&creation.filesystem),
            volume,
            creation.host.verifier.clone(),
            creation.host.owner_scope.clone(),
            tools,
            creation.root_scope.limits().file_bytes,
            None,
            None,
        )?;
        Ok(Self { creation, runtime })
    }
}

impl CreationFixture {
    pub(crate) async fn new() -> Result<Self> {
        Self::new_with_grants(&[]).await
    }

    pub(crate) async fn new_with_grants(extra: &[&str]) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Self::new_with_creation(extra, None).await
        }
        #[cfg(target_arch = "wasm32")]
        {
            Self::new_with_creation(extra).await
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn new_with_original_root_births(
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        Self::new_with_creation(&[], Some(work)).await
    }

    pub(super) async fn new_with_creation(
        extra: &[&str],
        #[cfg(not(target_arch = "wasm32"))] bootstrap: Option<
            &acyclic_fs::NativeMaterializationWork,
        >,
    ) -> Result<Self> {
        let stream = Arc::new(MemoryStream::default());
        let filesystem_provider = ProviderRef::new(
            "created-controls".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?;
        let fs = Fs::memory();
        let filesystem = FilesystemHost::new(fs.clone(), &filesystem_provider, None)?;
        let payload_volume = VolumeRef::new(
            filesystem_provider.clone(),
            "scheduler".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
            &mut None,
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let original_root_births = if let Some(work) = bootstrap {
            let project = VolumeRef::new(
                filesystem_provider.clone(),
                "parent-project".into(),
                VolumeClass::Project,
                VolumeOwner::Project("created-project".into()),
                &mut None,
            )?;
            let config = acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Durable);
            let mut births = Vec::new();
            for (volume, operation, contents) in [
                (
                    project,
                    acyclic_fs::OperationId::from_bytes([177; 16]),
                    b"original project import".as_slice(),
                ),
                (
                    payload_volume.clone(),
                    acyclic_fs::OperationId::from_bytes([178; 16]),
                    b"original root private".as_slice(),
                ),
            ] {
                let mut name_io = crate::store::history::native_work_io(work)?;
                let name = volume.storage_name(&mut Some(&mut name_io))?;
                let name_custody = name_io.into_custody();
                let initial = fs
                    .create_workspace_with_operation_with_native_work(
                        &name,
                        config,
                        operation,
                        work,
                        work.cancellation(),
                    )
                    .await
                    .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
                let initial_generation = initial.id();
                let workspace = workspace_ref(filesystem_provider.clone(), &name)?;
                let initial_ref = GenerationRef::new(
                    filesystem_provider.clone(),
                    initial_generation.digest().into_bytes().to_vec(),
                    None,
                )?;
                let selected = filesystem
                    .apply(
                        &workspace,
                        Some(&initial_ref),
                        &[WorkspaceMutation::PutFile {
                            path: "/root.txt".into(),
                            bytes: contents.to_vec().into(),
                        }],
                        &IdempotencyKey::new(format!(
                            "original-root-import-{}",
                            uuid::Uuid::from_bytes(operation.into_bytes())
                        ))?,
                    )
                    .await?;
                let selected_generation =
                    acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(
                        selected
                            .as_resource()
                            .key()
                            .try_into()
                            .map_err(|_| Error::Invalid("fixture generation size".into()))?,
                    ));
                assert_ne!(initial_generation, selected_generation);
                births.push(OriginalRootFilesystemBirth {
                    volume,
                    operation,
                    config,
                    initial_generation,
                    selected_generation,
                });
                drop(initial);
                drop(name);
                drop(name_custody);
            }
            Some(
                births
                    .try_into()
                    .map_err(|_| Error::Invalid("fixture Root birth count".into()))?,
            )
        } else {
            filesystem.create_volume(&payload_volume).await?;
            None
        };
        #[cfg(target_arch = "wasm32")]
        let original_root_births = {
            filesystem.create_volume(&payload_volume).await?;
            None
        };
        let authority = Authority {
            kind: AggregateKind::Task,
            id: "created-owner".into(),
        };
        let issuer = AuthorityIssuer::new("created-owner", [80; 32], authority.clone());
        let grants = Capabilities::new(
            [
                "operation:declare".into(),
                "operation:observe".into(),
                "operation:cancel".into(),
                "task:spawn:test.created@1".into(),
                "resource:create:custom".into(),
                "fork:publish".into(),
                payload_volume.capability(VolumeOperation::Read, &mut None)?,
                "conversation:bind".into(),
                payload_volume.capability(VolumeOperation::Write, &mut None)?,
                VolumeRef::new(
                    filesystem_provider.clone(),
                    "parent-project".into(),
                    VolumeClass::Project,
                    VolumeOwner::Project("created-project".into()),
                    &mut None,
                )?
                .capability(VolumeOperation::Read, &mut None)?,
                VolumeRef::new(
                    filesystem_provider.clone(),
                    "parent-private".into(),
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(AgentId::from_bytes([90; 16])),
                    &mut None,
                )?
                .capability(VolumeOperation::Read, &mut None)?,
            ]
            .into_iter()
            .chain(extra.iter().map(|grant| (*grant).to_owned())),
        );
        let signed = issuer
            .issue(
                "owner",
                &grants,
                Some(AgentId::from_bytes([80; 16])),
                None,
                &mut None,
            )
            .expect("scope proof");
        let root_scope = RuntimeScope::new(grants, Limits::default())?.with_run_limits(
            crate::runtime::TaskRunLimits {
                concurrency: Some(16),
                max_steps: Some(4),
                deadline_epoch_ms: None,
            },
            &mut None,
        )?;
        let payloads = Arc::new(FilesystemSchedulerPayloadStore::new(
            filesystem.clone(),
            &payload_volume,
            &issuer.verifier(&mut None).expect("verifier copy"),
            &signed,
            root_scope.limits().file_bytes,
            None,
        )?);
        let reader = FilesystemContentVerifier::new(
            filesystem.clone(),
            &issuer.verifier(&mut None).expect("verifier copy"),
            &signed,
            root_scope.limits().file_bytes,
            None,
        )?;
        let schema = serde_json::json!({"type":"object"});
        let implementation = Arc::new(CreatedControlMachine {
            identity: MachineIdentity {
                name: "test.created".into(),
                version: "1".into(),
                digest: *blake3::hash(include_str!("machine.rs").as_bytes()).as_bytes(),
            },
            schema: schema.clone(),
        });
        let (task, machine, tasks, machines) =
            test_registries::<Value>(implementation, schema.clone(), schema.clone())?;
        let creator = Arc::new(SandboxCreator {
            provider: ProviderRef::new(
                "created-controls".into(),
                "sandbox".into(),
                "1".into(),
                &mut None,
            )?,
            filesystem_provider: filesystem_provider.clone(),
            filesystem: filesystem.clone(),
            allocations: Mutex::new(BTreeMap::new()),
        });
        #[cfg(not(target_arch = "wasm32"))]
        let fs_creator = if let Some(work) = bootstrap {
            FilesystemResourceCreationProvider::new(filesystem.clone(), &issuer, Some(work))?
                .into_native_shared(work)?
        } else {
            Arc::new(FilesystemResourceCreationProvider::new(
                filesystem.clone(),
                &issuer,
                #[cfg(not(target_arch = "wasm32"))]
                None,
            )?)
        };
        #[cfg(target_arch = "wasm32")]
        let fs_creator = Arc::new(FilesystemResourceCreationProvider::new(
            filesystem.clone(),
            &issuer,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )?);
        let host = Arc::new(
            CoordinatorTaskHost::new(
                DistributedCoordinator::open(&stream, reader.clone(), None, &mut None).await?,
                stream,
                payloads,
                reader,
                &(authority),
                signed,
                issuer.verifier(&mut None).expect("verifier copy"),
                root_scope.clone(),
                tasks.clone(),
                machines,
                Arc::new(SystemUnixMillisClock),
                None,
            )?
            .with_session_limits(
                crate::scheduler::SessionLimits {
                    active_tasks: 16,
                    total_tasks: 32,
                    depth: 4,
                    model_steps: 4,
                },
                None,
            )?
            .with_resource_creation_providers(&[creator.clone(), fs_creator.clone()], None)?,
        );
        let mut bindings = Bindings::local();
        bindings.tasks = tasks;
        bindings.scope = root_scope.clone();
        bindings.durable_host = Some(host.clone());
        let harness = bindings.build()?;
        let root_op = OperationId::from_bytes([82; 16]);
        let root = TaskId::from_bytes(root_op.into_bytes());
        let admission = TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id: root_op,
            task: task.clone(),
            machine: machine.clone(),
            input: serde_json::json!({}),
            input_schema: schema.clone(),
            output_schema: schema.clone(),
            parent: None,
            grants: root_scope.grants().clone(),
            limits: root_scope.limits(),
            run_limits: root_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        };
        assert!(matches!(host.admit(admission).await?, Admission::Accepted(id) if id == root));
        let worker = crate::distributed::Worker {
            id: "created-worker".into(),
            available: Default::default(),
            labels: BTreeMap::new(),
        };
        let crate::distributed::WorkPull::Claimed(root_lease) = host.pull_work(&worker).await?
        else {
            return Err(Error::NotFound("created root lease".into()));
        };
        host.start_task(&root_lease, None).await?;
        let stream = host.stream.clone();
        Ok(Self {
            host,
            harness,
            filesystem,
            stream,
            filesystem_provider,
            creator,
            root_scope,
            task,
            machine,
            schema,
            root,
            root_lease,
            worker,
            original_root_births,
            filesystem_creator: fs_creator,
        })
    }

    pub(crate) fn admission(&self, byte: u8, input: Value) -> TaskAdmissionRecord {
        TaskAdmissionRecord {
            resource_proofs: Vec::new(),
            operation_id: OperationId::from_bytes([byte; 16]),
            task: self.task.clone(),
            machine: self.machine.clone(),
            input,
            input_schema: self.schema.clone(),
            output_schema: self.schema.clone(),
            parent: Some(self.root),
            grants: self.root_scope.grants().clone(),
            limits: self.root_scope.limits(),
            run_limits: self.root_scope.run_limits(),
            policy: None,
            extensions: None,
            execution: None,
        }
    }

    pub(super) fn sandbox_request(&self) -> Result<ResourcePreparationRequest> {
        let preparation = TaskId::from_bytes([83; 16]);
        let proof = self.creator.proof(preparation)?;
        let mut child = self.admission(84, serde_json::json!({"action":"execute"}));
        child.grants = Capabilities::new([
            created_resource_capability(&proof.resource, "execute", &mut None)?,
            "task:spawn:test.created@1".into(),
        ]);
        child = child.with_resource_proofs(vec![proof.clone()])?;
        Ok(ResourcePreparationRequest {
            command: None,
            source_checkpoint: None,
            provider: self.creator.provider.clone(),
            child,
            resources: vec![proof.resource],
            input_files: Vec::new(),
            read_authorization: None,
            creation_grant: "resource:create:custom".into(),
            maximum_resources: 1,
            maximum_work: 4,
            maximum_bytes: 65_536,
            payload: serde_json::json!({"image":"isolated"}),
        })
    }

    pub(super) async fn prepare(
        &self,
        request: &ResourcePreparationRequest,
    ) -> Result<(
        TaskResourcePreparation,
        crate::distributed::WorkLease,
        crate::filesystem::InvocationCancellation,
    )> {
        let invocation =
            crate::filesystem::InvocationCancellation(acyclic_fs::CancellationToken::new());
        let prep = self.admission(
            83,
            serde_json::to_value(request).map_err(|e| Error::Invalid(e.to_string()))?,
        );
        assert!(
            matches!(self.host.admit_owned(prep, (&self.root_lease.reservation).into()).await?,
            Admission::Accepted(id) if id == TaskId::from_bytes([83; 16]))
        );
        let crate::distributed::WorkPull::Claimed(lease) =
            self.host.pull_work(&self.worker).await?
        else {
            return Err(Error::NotFound("created preparation lease".into()));
        };
        self.host.start_task(&lease, None).await?;
        let owner = Arc::new(
            self.host
                .journal_owner(
                    TaskId::from_bytes(lease.operation.operation_id.into_bytes()),
                    (&lease.reservation).into(),
                )
                .await?
                .with_invocation_cancellation(&invocation.0),
        );
        Ok((
            owner
                .resource_preparation(request.clone(), &mut None)
                .await?,
            lease,
            invocation,
        ))
    }

    pub(crate) async fn admit_child(
        &self,
        child: TaskAdmissionRecord,
    ) -> Result<Admission<TaskId>> {
        let preparation = TaskId::from_bytes([83; 16]);
        let current = self.host.observe_task(preparation).await?;
        if current.phase == crate::scheduler::OperationPhase::Running {
            let reservation = current
                .reservation
                .ok_or_else(|| Error::NotFound("preparation completion lease".into()))?;
            self.host
                .settle_task(
                    preparation,
                    &(&reservation).into(),
                    Outcome::Succeeded(serde_json::json!({"prepared":true})),
                    &mut None,
                )
                .await?;
        }
        let admitted = self
            .host
            .admit_owned(child, (&self.root_lease.reservation).into())
            .await?;
        if let Admission::Accepted(task) = &admitted {
            self.start_next(*task).await?;
        }
        Ok(admitted)
    }

    pub(super) async fn start_next(&self, expected: TaskId) -> Result<()> {
        let crate::distributed::WorkPull::Claimed(lease) =
            self.host.pull_work(&self.worker).await?
        else {
            return Err(Error::NotFound("created child publication lease".into()));
        };
        assert_eq!(
            lease.operation.operation_id.into_bytes(),
            expected.into_bytes()
        );
        self.host.start_task(&lease, None).await
    }
}

impl CreationFixture {
    pub(crate) async fn create_filesystem_child(&self) -> Result<PreparedFilesystemChild> {
        self.create_filesystem_child_with_grants(&[]).await
    }

    pub(crate) async fn create_filesystem_child_with_grants(
        &self,
        additional: &[&str],
    ) -> Result<PreparedFilesystemChild> {
        self.create_filesystem_child_with_options(additional, false)
            .await
    }

    pub(super) async fn create_filesystem_child_with_options(
        &self,
        additional: &[&str],
        with_result: bool,
    ) -> Result<PreparedFilesystemChild> {
        use crate::core::{Action, Command, SchemaRegistry};
        use crate::fork::{
            ForkPreparation, ForkPreparer, ForkRequest, ForkSelection, ResourceRevision,
        };
        use crate::resources::StreamRef;
        use crate::store::StreamAggregate;

        let parent = Authority {
            kind: AggregateKind::Conversation,
            id: "created-fork-parent".into(),
        };
        let parent_agent = AgentId::from_bytes([80; 16]);
        let child_agent = AgentId::from_bytes([91; 16]);
        let fork_issuer = AuthorityIssuer::new("created-owner", [80; 32], parent.clone());
        let parent_project = VolumeRef::new(
            self.filesystem_provider.clone(),
            "parent-project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("created-project".into()),
            &mut None,
        )?;
        let project_head = self.filesystem.create_volume(&parent_project).await?;
        let parent_private = VolumeRef::new(
            self.filesystem_provider.clone(),
            "parent-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(parent_agent),
            &mut None,
        )?;
        let private_head = self.filesystem.create_volume(&parent_private).await?;
        let fork_scope = fork_issuer
            .issue(
                "fork-parent",
                &Capabilities::new([
                    "conversation:bind".into(),
                    "fork:publish".into(),
                    parent_project.capability(VolumeOperation::Read, &mut None)?,
                    parent_private.capability(VolumeOperation::Read, &mut None)?,
                ]),
                Some(parent_agent),
                None,
                &mut None,
            )
            .expect("scope proof");
        let resolver = FilesystemContentVerifier::new(
            self.filesystem.clone(),
            &fork_issuer.verifier(&mut None).expect("verifier copy"),
            &fork_scope,
            65_536,
            None,
        )?;
        let fork_stream = self.host.stream.clone();
        let mut aggregate = StreamAggregate::open(
            &fork_stream,
            parent.clone(),
            fork_issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
            None,
            crate::store::default_projection_read_limits(),
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None,
        )
        .await?;
        aggregate
            .execute(
                Command {
                    operation_id: OperationId::from_bytes([90; 16]),
                    idempotency_key: IdempotencyKey::new("created-fork-bind")?,
                    expected_revision: 0,
                    scope: fork_scope.clone(),
                    causal_parent: None,
                    action: Action::BindConversation {
                        agent: parent_agent,
                    },
                },
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
            )
            .await?;
        let project = VolumeRef::new(
            self.filesystem_provider.clone(),
            "child-project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("created-project".into()),
            &mut None,
        )?;
        let private = VolumeRef::new(
            self.filesystem_provider.clone(),
            "child-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(child_agent),
            &mut None,
        )?;
        let result = with_result
            .then(|| {
                VolumeRef::new(
                    self.filesystem_provider.clone(),
                    "child-result".into(),
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(child_agent),
                    &mut None,
                )
            })
            .transpose()?;
        let stream_provider = ProviderRef::new(
            "created-controls".into(),
            "stream".into(),
            "2".into(),
            &mut None,
        )?;
        let fork = ForkRequest {
            authorized_source: None,
            operation_id: OperationId::from_bytes([92; 16]),
            parent: parent.clone(),
            parent_revision: aggregate.reducer().revision(),
            child: Authority {
                kind: AggregateKind::Conversation,
                id: "created-fork-child".into(),
            },
            child_agent,
            attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_project_volume: project.clone(),
                child_private_volume: private.clone(),
                child_result_volume: result.clone(),
                summary: None,
                inherited_through_sequence: 0,
                maximum_inherited_messages: 1,
                maximum_inherited_bytes: 65_536,
                maximum_inherited_references: 1,
            },
            selections: vec![
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::History(StreamRef::new(
                        stream_provider.clone(),
                        parent.stream_path(&mut None)?.into_bytes(),
                        Some(aggregate.reducer().revision().to_string()),
                    )?),
                },
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::Project {
                        volume: parent_project,
                        generation: project_head.generation,
                    },
                },
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::PrivateVolume {
                        volume: parent_private,
                        generation: private_head.generation,
                        paths: Vec::new(),
                    },
                },
            ],
            boundary: None,
        };
        let history = crate::store::HistoryReader::new(
            &fork_stream,
            &parent,
            &(fork_issuer.verifier(&mut None).expect("verifier copy")),
            &mut None,
        )?
        .fork_history(
            aggregate.reducer(),
            0,
            crate::store::HistoryReadLimits {
                maximum_events: 2,
                maximum_bytes: 65_536,
            },
            &mut None,
        )
        .await?;
        let preparer = FilesystemForkPreparer::new(
            self.filesystem.clone(),
            aggregate.reducer().copy(&mut None)?,
            fork_issuer.verifier(&mut None).expect("verifier copy"),
            fork_scope.clone(),
            fork.selections
                .iter()
                .find_map(|selection| match &selection.revision {
                    ResourceRevision::Project { volume, .. } => Some(volume.clone()),
                    _ => None,
                })
                .ok_or_else(|| Error::Invalid("fork fixture project missing".into()))?,
            stream_provider,
            resolver,
            Some(history),
            None,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )?;
        let mut child = self.admission(84, serde_json::json!({"action":"filesystem-io"}));
        child.grants = Capabilities::new([
            "task:spawn:test.created@1".into(),
            private.capability(VolumeOperation::Read, &mut None)?,
            private.capability(VolumeOperation::Write, &mut None)?,
            project.capability(VolumeOperation::Read, &mut None)?,
            project.capability(VolumeOperation::Write, &mut None)?,
        ]);
        child.grants = Capabilities::new(child.grants.iter().chain(additional.iter().copied()));
        if let Some(result) = &result {
            child.grants = Capabilities::new(
                child.grants.iter().chain(
                    [
                        result.capability(VolumeOperation::Read, &mut None)?,
                        result.capability(VolumeOperation::Write, &mut None)?,
                    ]
                    .iter()
                    .map(String::as_str),
                ),
            );
        }
        let (resources, proofs) =
            preparer.creation_allocations(&fork, TaskId::from_bytes([83; 16]), None)?;
        child = child.with_resource_proofs(proofs)?;
        let request = ResourcePreparationRequest {
            command: None,
            source_checkpoint: None,
            provider: self.filesystem_provider.clone(),
            child,
            resources,
            creation_grant: "fork:publish".into(),
            maximum_resources: if with_result { 3 } else { 2 },
            maximum_work: 16_384,
            maximum_bytes: 16 * 1_024 * 1_024,
            payload: serde_json::to_value(&fork).map_err(|e| Error::Invalid(e.to_string()))?,
            input_files: Vec::new(),
            read_authorization: fork.authorized_source.clone(),
        };
        let (preparation, _, _invocation) = self.prepare(&request).await?;
        let report = preparer
            .prepare_for_task(fork, &Arc::new(preparation))
            .await?;
        let seed = report.clone().into_seed(&mut None)?;
        Ok(PreparedFilesystemChild {
            request,
            private,
            project,
            seed,
            report,
            parent: aggregate.reducer().copy(&mut None)?,
            parent_scope: fork_scope,
            parent_verifier: fork_issuer.verifier(&mut None).expect("verifier copy"),
        })
    }
}
