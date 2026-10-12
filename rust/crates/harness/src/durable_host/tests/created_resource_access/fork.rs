use super::*;
use crate::core::{Action, Command, SchemaRegistry};
use crate::fork::{ForkPreparation, ForkPreparer, ForkRequest, ForkSelection, ResourceRevision};
use crate::resources::StreamRef;
use crate::store::StreamAggregate;

impl CreationFixture {
    pub(crate) async fn create_filesystem_child(&self) -> Result<PreparedFilesystemChild> {
        self.create_filesystem_child_with_grants(&[]).await
    }
    pub(crate) async fn create_filesystem_child_with_grants(
        &self,
        additional: &[&str],
    ) -> Result<PreparedFilesystemChild> {
        let parent = Authority {
            kind: AggregateKind::Conversation,
            id: "created-fork-parent".into(),
        };
        let parent_agent = AgentId::from_bytes([80; 16]);
        let child_agent = AgentId::from_bytes([91; 16]);
        let issuer = AuthorityIssuer::new("created-owner", [80; 32], parent.clone());
        let verifier = issuer.verifier(&mut None)?;
        let parent_project = VolumeRef::new(
            self.filesystem_provider.clone(),
            "parent-project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("created-project".into()),
            &mut None,
        )?;
        let parent_private = VolumeRef::new(
            self.filesystem_provider.clone(),
            "parent-private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(parent_agent),
            &mut None,
        )?;
        let project_head = self.filesystem.create_volume(&parent_project).await?;
        let private_head = self.filesystem.create_volume(&parent_private).await?;
        let parent_scope = issuer.issue(
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
        )?;
        let resolver = FilesystemContentVerifier::new(
            self.filesystem.clone(),
            &verifier,
            &parent_scope,
            65_536,
            None,
        )?;
        let mut aggregate = StreamAggregate::open(
            &self.stream,
            parent.clone(),
            verifier.clone(),
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
                    scope: parent_scope.clone(),
                    causal_parent: None,
                    action: Action::BindConversation {
                        agent: parent_agent,
                    },
                },
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
            boundary: None,
            preparation: ForkPreparation {
                child_project_volume: project.clone(),
                child_private_volume: private.clone(),
                child_result_volume: None,
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
                        volume: parent_project.clone(),
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
        };
        let history =
            crate::store::HistoryReader::new(&self.stream, &parent, &verifier, &mut None)?
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
            verifier.clone(),
            parent_scope.clone(),
            parent_project,
            stream_provider,
            resolver,
            Some(history),
            None,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )?;
        let mut child = self.admission(84, serde_json::json!({"action":"filesystem-io"}));
        let mut grants = vec!["task:spawn:test.created@1".to_owned()];
        for volume in [&private, &project] {
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                grants.push(volume.capability(operation, &mut None)?);
            }
        }
        grants.extend(additional.iter().map(|grant| (*grant).to_owned()));
        child.grants = Capabilities::new(grants);
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
            maximum_resources: 2,
            maximum_work: 16_384,
            maximum_bytes: 16 * 1_024 * 1_024,
            payload: serde_json::to_value(&fork)
                .map_err(|error| Error::Invalid(error.to_string()))?,
            input_files: Vec::new(),
            read_authorization: fork.authorized_source.clone(),
        };
        let (preparation, _lease, _invocation) = self.prepare(&request).await?;
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
            parent_scope,
            parent_verifier: verifier,
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
        let admission = self.admission(
            83,
            serde_json::to_value(request).map_err(|error| Error::Invalid(error.to_string()))?,
        );
        assert!(
            matches!(self.host.admit_owned(admission, (&self.root_lease.reservation).into()).await?, Admission::Accepted(id) if id==TaskId::from_bytes([83;16]))
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
        if let Admission::Accepted(task) = admitted {
            self.start_next(task).await?;
        }
        Ok(admitted)
    }

    pub(super) async fn start_next(&self, expected: TaskId) -> Result<()> {
        let crate::distributed::WorkPull::Claimed(lease) =
            self.host.pull_work(&self.worker).await?
        else {
            return Err(Error::NotFound("created child lease".into()));
        };
        assert_eq!(
            lease.operation.operation_id.into_bytes(),
            expected.into_bytes()
        );
        self.host.start_task(&lease, None).await
    }
}
