use super::*;
use acyclic_harness::{
    filesystem::{
        BorrowedNativeVolumeBinding, FilesystemContentVerifier, FilesystemInteractionHost,
        FsRootProjectSourceProvider, NativePreparationRoot, NativeProcessRequest,
        NativeViewOptions, NativeVolumeNamespace, NativeVolumeView, ParentProjectController,
        PreparationIo,
    },
    source_authorization::ProjectSourceSnapshot,
    store::HistoryReader,
};

impl Producer {
    pub(super) async fn file(
        &self,
        caller: &Caller,
        volume: &VolumeRef,
        path: &str,
        bytes: &[u8],
        media: &str,
    ) -> Result<FileRef> {
        let writer = ContentGrant::verify(
            &caller.binding.verifier()?,
            &caller.binding.scope,
            volume,
            VolumeOperation::Write,
            u64::MAX,
            &mut None,
        )?;
        self.files
            .put_content(
                volume,
                &writer,
                path,
                bytes,
                media,
                path,
                PAYLOAD_BYTES,
                &IdempotencyKey::new(OperationId::new().to_string())?,
                None,
                None,
            )
            .await
    }

    pub(super) async fn native_command(&mut self, caller: &Caller) -> Result<Value> {
        use acyclic_harness::{
            InteractionId,
            core::EffectGuarantee,
            effect_host::TaskEffectPlan,
            filesystem::NATIVE_PROCESS_EFFECT_KIND,
            interaction::{ApprovalBinding, Interaction, InteractionKind, InteractionTicket},
        };
        let operation = OperationId::new();
        let approval = InteractionId::new();
        let verifier = caller.binding.verifier()?;
        let budget = self.config.tools.content_budget;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &self.cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let io = PreparationIo::from_native_work(&work)?;
        let fence = LeaseFence::from(&caller.lease.reservation);
        let owner = self
            .runtime
            .task_host()
            .journal_owner(caller.task, &fence, Some(&io))
            .await?
            .into_native_shared(&work)?;
        let context = self
            .runtime
            .task_context_with_native_work(caller.task, &fence, &work)
            .await?;
        context.verify_volume_owner(&owner, &work).await?;
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
                .map_err(storage)?;
        }
        let options = NativeViewOptions {
            root: namespace.clone(),
            maximum_volumes: 1,
            work_per_volume: budget,
        };
        let (allocation, _custody) =
            NativePreparationRoot::witness(&namespace, &preparation, &work)?.await?;
        let generation = self.generation(&caller.project).await?;
        let view = NativeVolumeView::prepare(
            &owner,
            &[context],
            &allocation,
            &options,
            &[BorrowedNativeVolumeBinding {
                host: self.files.as_ref(),
                volume: &caller.project,
                generation: &generation,
                namespace: &NativeVolumeNamespace::MaterializedDirectory,
                path: "project",
                writable: false,
                verifier: &verifier,
                scope: &caller.binding.scope,
                publication: acyclic_fs::PublicationPermit::Unrestricted,
            }],
            &work,
        )?
        .await?;
        let executable = if cfg!(windows) {
            PathBuf::from(
                std::env::var_os("SystemRoot")
                    .ok_or_else(|| Error::NotFound("SystemRoot".into()))?,
            )
            .join("System32/cmd.exe")
        } else {
            PathBuf::from("/bin/sh")
        };
        let mut argv = if cfg!(windows) {
            vec!["/d".into(), "/c".into()]
        } else {
            vec!["-c".into()]
        };
        argv.push("echo graphcoder-approved-command".into());
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
        let request_file = self
            .file(
                caller,
                &caller.private,
                "command/request.json",
                &serde_json::to_vec(&request).map_err(invalid)?,
                "application/json",
            )
            .await?;
        let schema = self
            .file(
                caller,
                &caller.private,
                "command/schema.json",
                br#"{"type":"object"}"#,
                "application/schema+json",
            )
            .await?;
        let digest = request.approval_digest(caller.task, operation, None)?;
        let prompt = self
            .file(
                caller,
                &caller.private,
                "command/approval.json",
                &serde_json::to_vec(&Interaction::approval(
                    "Run the exact native qualification command".into(),
                    operation,
                    digest,
                )?)
                .map_err(invalid)?,
                "application/json",
            )
            .await?;
        FilesystemInteractionHost::new(
            self.stream.clone(),
            self.files.clone(),
            &caller.binding.authority,
            &verifier,
            &SchemaRegistry::default(),
            &caller.binding.scope,
            &caller.private,
            PAYLOAD_BYTES,
            None,
        )?
        .open(
            OperationId::new(),
            Some(&caller.binding.scope),
            &InteractionTicket {
                id: uuid::Uuid::from_bytes(approval.into_bytes()),
                kind: InteractionKind::Approval,
                request: prompt,
                deadline_unix_ms: None,
                approval: Some(ApprovalBinding {
                    operation_id: operation,
                    action_digest: digest,
                }),
            },
            None,
            None,
        )
        .await?;
        self.config
            .tools
            .commands
            .push(crate::native_tools::NativeCommandConfig {
                caller: caller.task,
                operation,
                approval,
                operation_directory: allocation.operation_directory.clone(),
                root_identity: allocation.expected_identity.to_bytes(),
                preparation_identity: allocation.expected_preparation_identity,
                plan: TaskEffectPlan {
                    provider: "graphcoder.native".into(),
                    guarantee: EffectGuarantee::AtMostOnce,
                    effect_kind: NATIVE_PROCESS_EFFECT_KIND.into(),
                    request: request_file,
                    result_schema: schema,
                },
            });
        Ok(json!({ "caller": caller.task, "lease": caller.lease, "operation": operation }))
    }

    pub(super) async fn seed(
        &mut self,
        caller: &mut Caller,
        label: &str,
        messages: usize,
    ) -> Result<()> {
        let scratch = self
            .file(
                caller,
                &caller.private,
                "scratch/caller-marker.txt",
                format!("{label} private scratch; must never be copied as child scratch\n")
                    .as_bytes(),
                "text/plain",
            )
            .await?;
        self.references.insert(format!("{label}_scratch"), scratch);
        for index in 0..messages {
            let file = self
                .file(
                    caller,
                    &caller.private,
                    &format!("messages/{label}-{index}.txt"),
                    format!("{label} ORIGINAL CALLER transcript marker {index}\n").as_bytes(),
                    "text/plain",
                )
                .await?;
            if index == 0 {
                self.references
                    .insert(format!("{label}_transcript"), file.clone());
            }
            caller
                .apply(Action::AppendConversationMessage {
                    message: Box::new(caller.message(MessageKind::User, file)?),
                })
                .await?;
        }
        for (suffix, text) in [("before", "baseline"), ("after", "selected generation")] {
            let file = self
                .file(
                    caller,
                    &caller.project,
                    "sentinel.txt",
                    format!("{label} project {text}\n").as_bytes(),
                    "text/plain",
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
        let agent = AgentId::new();
        let child_authority = Authority {
            kind: AggregateKind::Conversation,
            id: format!("qualification-{label}-{agent}"),
        };
        let [project, private, result] = [
            (
                "project",
                VolumeOwner::Project("qualification-project".into()),
            ),
            ("private", VolumeOwner::Agent(agent)),
            ("result", VolumeOwner::Agent(agent)),
        ]
        .map(|(suffix, owner)| {
            VolumeRef::new(
                self.config.filesystem_provider.clone(),
                format!("{label}-{suffix}").into(),
                if matches!(owner, VolumeOwner::Agent(_)) {
                    VolumeClass::AgentPrivate
                } else {
                    VolumeClass::Project
                },
                owner,
                &mut None,
            )
        });
        let (project, private, result) = (project?, private?, result?);
        let operation = OperationId::new();
        let preparation_operation = OperationId::new();
        let preparation_task =
            acyclic_harness::runtime::task_id_for_admission(preparation_operation);
        let child_operation = OperationId::new();
        let verifier = parent.binding.verifier()?;
        let admission = self
            .runtime
            .task_host()
            .observe_admission(parent.task)
            .await?;
        let fence = LeaseFence::from(&parent.lease.reservation);
        let context = self
            .runtime
            .task_context(parent.task, fence.clone())
            .await?;
        let source_read = if let Some(source_task) = source.filter(|task| *task != parent.task) {
            let inspection = self
                .runtime
                .inspect_source(parent.task, source_task)
                .await?;
            let selected = inspection.project()?;
            let graph_root = self.config.project_source_root.ok_or_else(|| {
                Error::Unsupported("coordinator graph root source policy is not configured".into())
            })?;
            let policy = FsRootProjectSourceProvider::new(
                self.files.clone(),
                &self.config.owner.authority,
                graph_root,
                None,
            )?;
            let issuer = AuthorityIssuer::new(
                &self.config.owner.issuer,
                self.config.owner.key,
                self.config.owner.authority.clone(),
            );
            let owner = self
                .runtime
                .task_host()
                .journal_owner(parent.task, &fence, None)
                .await?
                .with_invocation_cancellation(&self.cancellation);
            Some(
                owner
                    .authorize_project_source(
                        &context,
                        &issuer,
                        policy.as_ref(),
                        source_task,
                        operation,
                        &ProjectSourceSnapshot {
                            project: selected.volume.clone(),
                            generation: selected.generation.clone(),
                            native_snapshot: None,
                        },
                        FORK_LIMITS.work,
                        FORK_LIMITS.bytes,
                        None,
                    )
                    .await?,
            )
        } else {
            None
        };
        let resolver = FilesystemContentVerifier::new(
            self.files.clone(),
            &verifier,
            &parent.binding.scope,
            PAYLOAD_BYTES,
            None,
        )?;
        let binding = recursive_fork_source(
            self.files.clone(),
            &self.stream,
            &parent.aggregate,
            verifier.copy(&mut None)?,
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
                msg: format!("real isolated {label} qualification task"),
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
                history_limits: HISTORY_LIMITS,
            },
            source_read,
            resolver.clone(),
            &self.cancellation,
        )
        .await?;
        self.config.fork_sources.push(ForkSourceConfig {
            caller: parent.task,
            request: binding.request.clone(),
        });
        let mut child = self.template.clone();
        child.operation_id = child_operation;
        child.parent = Some(parent.task);
        child.input = serde_json::to_value(&self.planned_commands[label]).map_err(invalid)?;
        let mut grants = COMMON_GRANTS
            .iter()
            .map(|grant| (*grant).to_owned())
            .collect::<Vec<_>>();
        let capabilities =
            std::iter::once(parent.project.capability(VolumeOperation::Read, &mut None)).chain(
                self.planned_commands
                    .values()
                    .map(|command| command.payload.read_capability(&mut None)),
            );
        for capability in capabilities {
            let capability = capability?;
            if admission.grants.contains(&capability) {
                grants.push(capability);
            }
        }
        let state = parent.aggregate.reducer();
        let preparation = &binding.request.preparation;
        let history = HistoryReader::new(
            &self.stream,
            &parent.binding.authority,
            &verifier,
            &mut None,
        )?
        .fork_history(
            state,
            preparation.inherited_through_sequence,
            HISTORY_LIMITS,
            &mut None,
        )
        .await?;
        let (references, _) = ParentProjectController::new(
            &self.files,
            state,
            &verifier,
            &parent.binding.scope,
            &parent.project,
            None,
        )?
        .inherited_reference_grants(
            state,
            agent,
            &binding.request.attached_agents,
            preparation.inherited_through_sequence,
            preparation.maximum_inherited_messages as usize,
            preparation.maximum_inherited_bytes,
            preparation.maximum_inherited_references as usize,
            resolver.as_ref(),
            Some(&history),
        )
        .await?;
        for grant in references {
            if grant.reader != agent {
                return Err(Error::Unauthorized(
                    "canonical projection grants another reader".into(),
                ));
            }
            grants.push(grant.capability()?);
        }
        child.grants = Capabilities::new(grants);
        let bytes = serde_json::to_vec(&binding.request).map_err(invalid)?;
        if bytes.len() as u64 > PAYLOAD_BYTES {
            return Err(Error::Invalid(
                "retained fork command exceeds fixture artifact bound".into(),
            ));
        }
        let (command_file, _) = context
            .stage_file_once(
                OperationId::new(),
                &format!(".qualification-forks/{operation}.json"),
                &bytes,
                "application/json",
                "fork-request.json",
                None,
            )
            .await?;
        let request = fork_preparation_request(
            &binding.preparer,
            &binding.request,
            preparation_task,
            child,
            WorkflowCommand {
                operation_id: operation,
                kind: FORK_PREPARE_TASK_COMMAND_KIND.into(),
                payload: command_file.clone(),
            },
            FORK_LIMITS,
            None,
            vec![command_file, self.planned_commands[label].payload.clone()],
            None,
        )?;
        let request_path = self
            .root
            .join("inputs")
            .join(format!("{label}-prepare.json"));
        let report_path = self
            .root
            .join("inputs")
            .join(format!("{label}-report.json"));
        persist(
            &request_path,
            &json!({
                "operation": preparation_operation, "parent": parent.task,
                "caller_lease": parent.lease, "request": request,
            }),
        )
        .await?;
        self.preparations.push(json!({
            "label": label, "parent": parent.task, "operation": preparation_operation,
            "task": preparation_task, "child_task_operation": child_operation, "request": request_path,
            "report": if source.is_none() { Some(&report_path) } else { None }, "executed": source.is_none(),
        }));
        let grants = Capabilities::new(request.child.grants.iter().map(str::to_owned).chain([
            "conversation:bind".to_owned(),
            "conversation:append".to_owned(),
        ]));
        let owner = AuthorityBinding {
            authority: child_authority.clone(),
            issuer: ISSUER.into(),
            key: TEST_KEY,
            scope: AuthorityIssuer::new(ISSUER, TEST_KEY, child_authority).issue(
                "explicit-trusted-child",
                &grants,
                Some(agent),
                None,
                &mut None,
            )?,
        };
        if source.is_some() {
            self.config.tools.forks.push(NativeToolFork {
                caller: parent.task,
                child: owner,
                project,
                private_volume: private,
                admission: request.child,
                limits: FORK_LIMITS,
                result_volume: Some(result),
            });
            return Ok(None);
        }
        let definition: Arc<TaskDefinition<ResourcePreparationRequest, ForkReport>> =
            self.runtime.harness().task("acyclic.fork_preparation@1")?;
        if admitted_task(
            context
                .admit(preparation_operation, &definition, request.clone())
                .await?,
        )? != preparation_task
        {
            return Err(Error::Conflict(
                "ordinary preparation task identity differs".into(),
            ));
        }
        let lease = claim(&self.runtime, preparation_task, &self.worker).await?;
        let commands = self.runtime.commands().with_fork_preparer(binding.preparer);
        if let acyclic_harness::filesystem::TaskWorkerAttempt::Unresolved { error, .. } =
            self.runtime.resume_task(lease, &commands, 16, None).await
        {
            return Err(error);
        }
        let report: ForkReport = match self.runtime.task_host().outcome(preparation_task).await? {
            Some(Outcome::Succeeded(value)) => serde_json::from_value(value).map_err(invalid)?,
            Some(Outcome::Failed { message }) => return Err(Error::Conflict(message)),
            _ => {
                return Err(Error::Unsupported(format!(
                    "real fork {label} has no successful report"
                )));
            }
        };
        for grant in &report.reference_grants {
            if !request.child.grants.contains(&grant.capability()?) {
                return Err(Error::Unauthorized(
                    "capture requires an unadmitted original reference grant".into(),
                ));
            }
        }
        let mut aggregate =
            aggregate(&self.stream, self.files.clone(), &owner, &self.config).await?;
        let seed = aggregate
            .spawn_from_report(
                &mut parent.aggregate,
                report.clone(),
                parent.binding.scope.clone(),
                owner.scope.clone(),
            )
            .await?;
        let task = match self
            .runtime
            .task_host()
            .admit_owned(request.child, fence)
            .await?
        {
            Admission::Accepted(task) => task,
            Admission::Rejected { reason } => return Err(Error::Conflict(reason)),
            Admission::Indeterminate { operation_id } => {
                return Err(Error::Indeterminate(operation_id));
            }
        };
        let lease = claim(&self.runtime, task, &self.worker).await?;
        let lease_path = self
            .root
            .join("inputs")
            .join(format!("{label}-original-lease.json"));
        persist(&lease_path, &lease).await?;
        self.leases.insert(label.to_owned(), lease_path);
        persist(
            &report_path,
            &json!({
                "report": report, "seed": seed,
                "child": self.runtime.task_host().observe_admission(task).await?,
            }),
        )
        .await?;
        Ok(Some(Caller {
            task,
            binding: owner,
            project,
            private,
            result: Some(result),
            aggregate,
            lease,
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
            )?, None)
            .await?
            .generation)
    }
}
