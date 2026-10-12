use super::*;
use acyclic_harness::runtime::TaskSpawner as _;
impl Producer {
    pub(super) async fn native_command(&mut self, caller: &Caller) -> Result<Value> {
        use acyclic_harness::{
            InteractionId,
            core::EffectGuarantee,
            effect_host::TaskEffectPlan,
            filesystem::{
                BorrowedNativeVolumeBinding, FilesystemInteractionHost, NATIVE_PROCESS_EFFECT_KIND,
                NativePreparationRoot, NativeProcessRequest, NativeViewOptions,
                NativeVolumeNamespace, NativeVolumeView,
            },
            interaction::{ApprovalBinding, Interaction, InteractionKind, InteractionTicket},
        };
        let operation = OperationId::new();
        let approval = InteractionId::new_v4();
        let issuer = AuthorityIssuer::new(ISSUER, TEST_KEY, caller.binding.authority.clone());
        let verifier = issuer.verifier(&mut None)?;
        let budget = self.config.tools.content_budget;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &self.cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let fence = LeaseFence::from(&caller.lease.reservation);
        let owner = self
            .runtime
            .task_host()
            .journal_owner_with_native_work(caller.task, &fence, &work)
            .await?
            .into_native_shared(&work)?;
        let context = self
            .runtime
            .task_context_with_native_work(caller.task, &fence, &work)
            .await?;
        let guards = [context
            .volume_callback_guard_with_native_work(&owner, &work)
            .await?];
        let namespace = self.root.join("native-command-view");
        let preparation = self.root.join("native-command-preparation");
        for directory in [
            &namespace,
            &preparation,
            &preparation.join("target"),
            &preparation.join("backup"),
        ] {
            tokio::fs::create_dir_all(directory)
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
        }
        let options = NativeViewOptions {
            root: namespace.clone(),
            maximum_volumes: 1,
            work_per_volume: budget,
        };
        let (allocation, _allocation_custody) =
            NativePreparationRoot::witness(&namespace, &preparation, &work)?.await?;
        let generation = self.generation(&caller.project).await?;
        let volumes = [BorrowedNativeVolumeBinding {
            host: self.files.as_ref(),
            volume: &caller.project,
            generation: &generation,
            namespace: &NativeVolumeNamespace::MaterializedDirectory,
            path: "project",
            writable: false,
            verifier: &verifier,
            scope: &caller.binding.scope,
            publication: acyclic_fs::PublicationPermit::Unrestricted,
        }];
        let view = NativeVolumeView::prepare(
            &owner,
            &guards,
            &allocation,
            &options,
            &volumes,
            &[None],
            &work,
        )?
        .await?;
        #[cfg(windows)]
        let (executable, argv) = (
            PathBuf::from(
                std::env::var_os("SystemRoot")
                    .ok_or_else(|| Error::NotFound("SystemRoot".into()))?,
            )
            .join("System32/cmd.exe"),
            vec![
                "/d".into(),
                "/c".into(),
                "echo graphcoder-approved-command".into(),
            ],
        );
        #[cfg(not(windows))]
        let (executable, argv) = (
            PathBuf::from("/bin/sh"),
            vec!["-c".into(), "printf graphcoder-approved-command".into()],
        );
        let request = NativeProcessRequest {
            executable,
            argv,
            cwd: namespace.join("project"),
            environment: Default::default(),
            timeout_ms: 5000,
            control_timeout_ms: 1000,
            cancellation_poll_ms: 20,
            maximum_output_bytes: 1024,
            maximum_result_bytes: 16 * 1024,
            mcp_stdio: None,
            standard_input: None,
            view: view.manifest().clone(),
        };
        let request_file = write(
            &self.files,
            &caller.binding,
            &caller.private,
            "command/request.json",
            &serde_json::to_vec(&request).map_err(|error| Error::Invalid(error.to_string()))?,
            "application/json",
            "request.json",
            "native-command-request",
        )
        .await?;
        let schema = write(
            &self.files,
            &caller.binding,
            &caller.private,
            "command/schema.json",
            br#"{"type":"object"}"#,
            "application/schema+json",
            "schema.json",
            "native-command-schema",
        )
        .await?;
        let digest = request.approval_digest(caller.task, operation)?;
        let interaction = Interaction::approval(
            "Run the exact native qualification command".into(),
            operation,
            digest,
        )?;
        let prompt = write(
            &self.files,
            &caller.binding,
            &caller.private,
            "command/approval.json",
            &serde_json::to_vec(&interaction).map_err(|error| Error::Invalid(error.to_string()))?,
            "application/json",
            "approval.json",
            "native-command-approval",
        )
        .await?;
        let interactions = FilesystemInteractionHost::new(
            self.stream.clone(),
            self.files.clone(),
            &caller.binding.authority,
            &verifier,
            &SchemaRegistry::default(),
            &caller.binding.scope,
            &caller.private,
            PAYLOAD_BYTES,
            None,
        )?;
        let ticket = InteractionTicket {
            id: approval,
            kind: InteractionKind::Approval,
            request: prompt,
            deadline_unix_ms: None,
            approval: Some(ApprovalBinding {
                operation_id: operation,
                action_digest: digest,
            }),
        };
        interactions
            .open(
                OperationId::new(),
                caller.binding.scope.clone(),
                ticket.clone(),
            )
            .await?;
        let plan = TaskEffectPlan {
            provider: "graphcoder.native".into(),
            guarantee: EffectGuarantee::AtMostOnce,
            effect_kind: NATIVE_PROCESS_EFFECT_KIND.into(),
            request: request_file,
            result_schema: schema,
        };
        self.config
            .tools
            .commands
            .push(crate::native_tools::NativeCommandConfig {
                caller: caller.task,
                operation,
                plan,
                approval,
            });
        Ok(
            json!({ "caller": caller.task, "lease": caller.lease, "operation": operation,
            "approval": approval, "source": self.references["right_project"] }),
        )
    }
    pub(super) async fn seed(
        &mut self,
        caller: &mut Caller,
        label: &str,
        messages: usize,
    ) -> Result<()> {
        let scratch = write(
            &self.files,
            &caller.binding,
            &caller.private,
            "scratch/caller-marker.txt",
            format!("{label} private scratch; must never be copied as child scratch\n").as_bytes(),
            "text/plain",
            "caller-marker.txt",
            &format!("{label}-scratch"),
        )
        .await?;
        self.references.insert(format!("{label}_scratch"), scratch);
        for index in 0..messages {
            let file = write(
                &self.files,
                &caller.binding,
                &caller.private,
                &format!("messages/{label}-{index}.txt"),
                format!("{label} ORIGINAL CALLER transcript marker {index}\n").as_bytes(),
                "text/plain",
                "transcript.txt",
                &format!("{label}-message-{index}"),
            )
            .await?;
            if index == 0 {
                self.references
                    .insert(format!("{label}_transcript"), file.clone());
            }
            let sequence = caller
                .aggregate
                .reducer()
                .conversation()
                .ok_or_else(|| Error::Invalid("caller conversation was not initialized".into()))?
                .logical_revision()
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("caller conversation sequence overflow".into()))?;
            caller
                .aggregate
                .execute(
                    Command {
                        operation_id: OperationId::new(),
                        idempotency_key: IdempotencyKey::new(format!("{label}-append-{index}"))?,
                        expected_revision: caller.aggregate.reducer().revision(),
                        scope: caller.binding.scope.clone(),
                        causal_parent: None,
                        action: Action::AppendConversationMessage {
                            message: Box::new(ConversationMessage {
                                id: uuid::Uuid::new_v4(),
                                sequence,
                                kind: MessageKind::User,
                                content: file,
                                attachments: Default::default(),
                                reply_to: None,
                                tool_call_id: None,
                                extensions: Default::default(),
                            }),
                        },
                    },
                    None,
                    &mut None,
                )
                .await?;
        }
        for (suffix, text, key) in [
            ("before", "baseline", "baseline"),
            ("after", "selected generation", "selected"),
        ] {
            let file = write(
                &self.files,
                &caller.binding,
                &caller.project,
                "sentinel.txt",
                format!("{label} project {text}\n").as_bytes(),
                "text/plain",
                "sentinel.txt",
                &format!("{label}-project-{key}"),
            )
            .await?;
            self.references
                .insert(format!("{label}_project_{suffix}"), file);
        }
        self.references.insert(
            format!("{label}_project"),
            self.references[&format!("{label}_project_after")].clone(),
        );
        Ok(())
    }
    pub(super) async fn fork(
        &mut self,
        parent: &mut Caller,
        label: &str,
        source: Option<TaskId>,
    ) -> Result<Option<Caller>> {
        let execute = source.is_none();
        let agent = AgentId::new();
        let child_authority = Authority {
            kind: AggregateKind::Conversation,
            id: format!("qualification-{label}-{agent}"),
        };
        let project = VolumeRef::new(
            self.config.filesystem_provider.clone(),
            (format!("{label}-project")).into(),
            VolumeClass::Project,
            VolumeOwner::Project("qualification-project".into()),
            &mut None,
        )?;
        let private = VolumeRef::new(
            self.config.filesystem_provider.clone(),
            (format!("{label}-private")).into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let result = VolumeRef::new(
            self.config.filesystem_provider.clone(),
            (format!("{label}-result")).into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        let operation = OperationId::new();
        let preparation_operation = OperationId::new();
        let preparation_task =
            acyclic_harness::runtime::task_id_for_admission(preparation_operation);
        let child_operation = OperationId::new();
        let initial_prompt = format!("real isolated {label} qualification task");
        let verifier = AuthorityIssuer::new(ISSUER, TEST_KEY, parent.binding.authority.clone())
            .verifier(&mut None)
            .expect("verifier copy");
        let admission = self
            .runtime
            .task_host()
            .observe_admission(parent.task)
            .await?;
        let context = self
            .runtime
            .task_context(parent.task, LeaseFence::from(&parent.lease.reservation))
            .await?;
        let source_read = if let Some(source_task) = source.filter(|task| *task != parent.task) {
            let inspection = self
                .runtime
                .inspect_source(parent.task, source_task)
                .await?;
            let selected = inspection.project()?;
            let graph_root = self.config.project_source_root.ok_or_else(|| {
                Error::Unsupported(
                    "actual coordinator graph root source policy is not configured".into(),
                )
            })?;
            let policy = acyclic_harness::filesystem::FsRootProjectSourceProvider::new(
                self.files.clone(),
                &self.config.owner.authority,
                graph_root,
                None,
            )?;
            let service_issuer = AuthorityIssuer::new(
                &self.config.owner.issuer,
                self.config.owner.key,
                self.config.owner.authority.clone(),
            );
            let owner = Arc::new(
                self.runtime
                    .task_host()
                    .journal_owner(parent.task, LeaseFence::from(&parent.lease.reservation))
                    .await?
                    .with_invocation_cancellation(&self.cancellation),
            );
            Some(
                owner
                    .authorize_project_source(
                        &context,
                        &service_issuer,
                        &policy,
                        source_task,
                        operation,
                        acyclic_harness::source_authorization::ProjectSourceSnapshot {
                            project: selected.volume.clone(),
                            generation: selected.generation.clone(),
                            native_snapshot: None,
                        },
                        16 * 1024 * 1024,
                        16 * 1024 * 1024,
                    )
                    .await?,
            )
        } else {
            None
        };
        let binding = recursive_fork_source(
            self.files.clone(),
            &self.stream,
            &parent.aggregate,
            verifier,
            parent.binding.scope.clone(),
            &context,
            RecursiveSourceSnapshot {
                task: parent.task,
                project: parent.project.clone(),
                project_generation: self.generation(&parent.project).await?,
                private_volume: parent.private.clone(),
                private_generation: self.generation(&parent.private).await?,
            },
            self.config.stream_provider.clone(),
            operation,
            &ForkToolArguments {
                id: agent,
                msg: initial_prompt.clone(),
                source,
            },
            RecursiveForkConfig {
                child: child_authority.clone(),
                child_project: project.clone(),
                child_private: private.clone(),
                child_result_volume: Some(result.clone()),
                limits: self.config.limits,
                inherited_messages: 64,
                inherited_bytes: PAYLOAD_BYTES,
                inherited_references: 64,
                history_limits: HistoryReadLimits {
                    maximum_events: 256,
                    maximum_bytes: 4 * 1024 * 1024,
                },
            },
            source_read,
            &self.cancellation,
        )
        .await?;
        let projected_reference_grants = binding
            .preparer
            .required_reference_grants(&binding.request, None)
            .await?;
        let checkpoint = ForkSourceConfig {
            caller: parent.task,
            owner: parent.binding.clone(),
            project: parent.project.clone(),
            snapshot: parent
                .aggregate
                .reducer()
                .snapshot(usize::MAX, &mut None)?
                .0,
            request: binding.request.clone(),
        };
        let checkpoint_path = self
            .root
            .join("inputs")
            .join(format!("{label}-source.json"));
        persist(&checkpoint_path, &checkpoint).await?;
        self.config.fork_sources.push(checkpoint);
        let mut child = self.template.clone();
        child.operation_id = child_operation;
        child.parent = Some(parent.task);
        child.input = serde_json::to_value(&self.planned_commands[label])
            .map_err(|error| Error::Invalid(error.to_string()))?;
        // Parent-owned scratch rights never become generic child rights.
        let mut grants = COMMON_GRANTS
            .iter()
            .map(|grant| (*grant).to_owned())
            .collect::<Vec<_>>();
        let parent_project_read = parent
            .project
            .capability(VolumeOperation::Read, &mut None)?;
        if admission.grants.contains(&parent_project_read) {
            grants.push(parent_project_read);
        }
        for command in self.planned_commands.values() {
            let capability = command.payload.read_capability(&mut None)?;
            if admission.grants.contains(&capability) {
                grants.push(capability);
            }
        }
        for grant in &projected_reference_grants {
            if grant.reader != agent {
                return Err(Error::Unauthorized(
                    "canonical projection unexpectedly grants another reader".into(),
                ));
            }
            grants.push(grant.capability()?);
        }
        child.grants = Capabilities::new(grants);
        let fork_bytes = serde_json::to_vec(&binding.request)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        if fork_bytes.len() as u64 > PAYLOAD_BYTES {
            return Err(Error::Invalid(
                "retained fork command exceeds fixture artifact bound".into(),
            ));
        }
        let command_file = context
            .stage_file_once(
                OperationId::new(),
                &format!(".qualification-forks/{operation}.json"),
                &fork_bytes,
                "application/json",
                "fork-request.json",
            )
            .await?;
        let command = WorkflowCommand {
            operation_id: operation,
            kind: FORK_PREPARE_TASK_COMMAND_KIND.into(),
            payload: command_file.clone(),
        };
        let request = fork_preparation_request(
            &binding.preparer,
            binding.request,
            preparation_task,
            child,
            command,
            ForkPreparationLimits {
                resources: 3,
                work: 16 * 1024 * 1024,
                bytes: 16 * 1024 * 1024,
            },
            None,
            vec![command_file, self.planned_commands[label].payload.clone()],
        )?;
        let request_path = self
            .root
            .join("inputs")
            .join(format!("{label}-prepare.json"));
        persist(
            &request_path,
            &json!({ "operation": preparation_operation, "parent": parent.task,
            "caller_lease": parent.lease, "request": request }),
        )
        .await?;
        let report_path = self
            .root
            .join("inputs")
            .join(format!("{label}-report.json"));
        let preparation_lease_path = self
            .root
            .join("inputs")
            .join(format!("{label}-preparation-lease.json"));
        self.preparations.push(json!({ "label": label, "parent": parent.task, "operation": preparation_operation,
            "task": preparation_task, "child_task_operation": child_operation, "child_agent": agent,
            "request": request_path, "report": if execute { Some(report_path.clone()) } else { None },
            "projected_reference_grants": projected_reference_grants,
            "source_snapshot": checkpoint_path, "executed": execute,
            "original_lease": if execute { Some(preparation_lease_path.clone()) } else { None } }));
        let child_issuer = AuthorityIssuer::new(ISSUER, TEST_KEY, child_authority.clone());
        let grants = Capabilities::new(request.child.grants.iter().map(str::to_owned).chain([
            "conversation:bind".to_owned(),
            "conversation:append".to_owned(),
        ]));
        let purpose = if execute {
            "explicit-trusted-created-child"
        } else {
            "explicit-trusted-pending-child"
        };
        let owner = AuthorityBinding {
            authority: child_authority,
            issuer: ISSUER.into(),
            key: TEST_KEY,
            scope: child_issuer.issue(purpose, &grants, Some(agent), None, &mut None)?,
        };
        if !execute {
            self.config.observed_authorities.push(owner.clone());
            self.config.tools.forks.push(NativeToolFork {
                caller: parent.task,
                child: owner,
                project,
                private_volume: private,
                admission: request.child.clone(),
                limits: ForkPreparationLimits {
                    resources: 3,
                    work: 16 * 1024 * 1024,
                    bytes: 16 * 1024 * 1024,
                },
                result_volume: Some(result),
            });
            return Ok(None);
        }
        let definition: Arc<TaskDefinition<ResourcePreparationRequest, ForkReport>> =
            self.runtime.harness().task("acyclic.fork_preparation@1")?;
        let admitted = admitted_task(
            context
                .admit(preparation_operation, &definition, request.clone())
                .await?,
        )?;
        if admitted != preparation_task {
            return Err(Error::Conflict(
                "ordinary preparation task identity differs".into(),
            ));
        }
        let preparation_lease = claim(&self.runtime, preparation_task, &self.worker).await?;
        persist(&preparation_lease_path, &preparation_lease).await?;
        let commands = self.runtime.commands().with_fork_preparer(binding.preparer);
        let attempt = self
            .runtime
            .resume_task(preparation_lease.clone(), &commands, 16, None)
            .await;
        if let acyclic_harness::filesystem::TaskWorkerAttempt::Unresolved { error, .. } = attempt {
            return Err(error);
        }
        let report: ForkReport = match self.runtime.task_host().outcome(preparation_task).await? {
            Some(Outcome::Succeeded(value)) => {
                serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?
            }
            Some(Outcome::Failed { message }) => {
                return Err(Error::Conflict(format!(
                    "real fork {label} failed: {message}"
                )));
            }
            _ => {
                return Err(Error::Unsupported(format!(
                    "real fork {label} did not authoritatively complete; no synthetic report is allowed"
                )));
            }
        };
        // Capture cannot widen the already admitted exact child rights.
        for grant in &report.reference_grants {
            if !request.child.grants.contains(&grant.capability()?) {
                return Err(Error::Unauthorized(
                    "capture requires an unadmitted original reference grant".into(),
                ));
            }
        }
        let mut child_aggregate =
            aggregate(&self.stream, self.files.clone(), &owner, &self.config).await?;
        let seed = child_aggregate
            .spawn_from_report(
                &mut parent.aggregate,
                report.clone(),
                parent.binding.scope.clone(),
                owner.scope.clone(),
            )
            .await?;
        let child_task = match self
            .runtime
            .task_host()
            .admit_owned(
                request.child.clone(),
                LeaseFence::from(&parent.lease.reservation),
            )
            .await?
        {
            Admission::Accepted(task) => task,
            Admission::Rejected { reason } => return Err(Error::Conflict(reason)),
            Admission::Indeterminate { operation_id } => {
                return Err(Error::Indeterminate(operation_id));
            }
        };
        let child_lease = claim(&self.runtime, child_task, &self.worker).await?;
        let child_lease_path = self
            .root
            .join("inputs")
            .join(format!("{label}-original-lease.json"));
        persist(&child_lease_path, &child_lease).await?;
        self.leases.insert(label.to_owned(), child_lease_path);
        persist(
            &report_path,
            &json!({ "report": report, "seed": seed,
            "child": self.runtime.task_host().observe_admission(child_task).await?,
            "parent_lease": parent.lease, "preparation_lease": preparation_lease }),
        )
        .await?;
        self.config.observed_authorities.push(owner.clone());
        println!(
            "native-scenario real-fork label={label} parent={} preparation={preparation_task} child={child_task}",
            parent.task
        );
        Ok(Some(Caller {
            task: child_task,
            binding: owner,
            project,
            private,
            result: Some(result),
            aggregate: child_aggregate,
            lease: child_lease,
        }))
    }
    async fn generation(
        &self,
        volume: &VolumeRef,
    ) -> Result<acyclic_harness::resources::GenerationRef> {
        Ok(self
            .files
            .resolve(&workspace_ref(
                self.config.filesystem_provider.clone(),
                &volume.storage_name(&mut None)?,
            )?)
            .await?
            .generation)
    }
}
