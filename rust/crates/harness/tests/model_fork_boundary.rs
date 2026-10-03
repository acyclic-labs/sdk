//! Exact model boundaries passed through real durable workspace/conversation forks.
#![cfg(feature = "filesystem-local")]
#![allow(clippy::too_many_lines)]

use acyclic_fs::{Fs, LocalAuthorityBackend, LocalObjectBackend, LocalOptions};
use acyclic_harness::{
    batch_publication::{ModelBatchPublication, ModelBatchPublisher},
    conversation::{
        ContentResidencyVerifier, ConversationMessage, Limits, MessageKind, VolumeClass,
        VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{
        Action, AggregateKind, Authority, AuthorityIssuer, Command, EffectGuarantee, SchemaRegistry,
    },
    executor::ExecutionEvent,
    filesystem::{
        workspace_ref, DurableHarnessStorage, FilesystemContentVerifier, FilesystemForkPreparer,
        FilesystemForkVerifier, FilesystemHost, FilesystemProjectMergeVerifier, HarnessStorage,
        WorkspaceMutation,
    },
    fork::{
        AttestedBoundary, CompositeForkVerifier, ForkPreparation, ForkRequest, ForkSelection,
        ResourceRevision, StreamHistoryForkVerifier,
    },
    model::{
        FileProjectionPolicy, Model, ModelAttempt, ModelContent, ModelContentPart, ModelEvent,
        ModelMessage, ModelProvider, ModelRequest, ModelRole,
    },
    model_input::{CompletedModelBoundary, FrozenModelPrefix, PreparedModelInput},
    registry::ComponentIdentity,
    resources::{ProviderRef, StreamRef},
    store::StreamAggregate,
    AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

type Host = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;

struct CapturedModel {
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    root: bool,
    read_first: bool,
}
impl ModelProvider for CapturedModel {
    fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request);
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        let events = if !self.root && self.read_first && first {
            let file = self.requests.lock().unwrap().last().and_then(|request| {
                request
                    .messages
                    .iter()
                    .find_map(|message| match &message.content {
                        ModelContent::Parts(parts) => parts.iter().find_map(|part| match part {
                            ModelContentPart::File { file, .. } => Some(file.clone()),
                            _ => None,
                        }),
                        _ => None,
                    })
            });
            let Some(file) = file else {
                return Box::pin(stream::iter(vec![Err(Error::Invalid(
                    "recursive fixture request has no readable file reference".into(),
                ))]));
            };
            vec![
                ModelEvent::ToolCall {
                    call_id: "read-inherited".into(),
                    name: "acyclic.read_file".into(),
                    arguments: json!({"file": file}),
                },
                ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]
        } else if self.root && first {
            vec![
                ModelEvent::Content {
                    delta: " \nα🦀\t retained\n".into(),
                },
                ModelEvent::ToolCall {
                    call_id: "invalid".into(),
                    name: "acyclic.stage_file".into(),
                    arguments: json!({"parameters": {"text": "wrong"}}),
                },
                ModelEvent::ToolCall {
                    call_id: "edit".into(),
                    name: "acyclic.stage_file".into(),
                    arguments: json!({"path":"notes/real.txt","text":"real content","media_type":"text/plain","display_name":"real.txt"}),
                },
                ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]
        } else {
            vec![
                ModelEvent::Content {
                    delta: "final only".into(),
                },
                ModelEvent::Completed {
                    metadata: Value::Null,
                },
            ]
        };
        Box::pin(stream::iter(events.into_iter().map(Ok)))
    }
    fn reconcile<'a>(&'a self, _: ModelAttempt) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

struct ForkAtBatch {
    storage: Arc<DurableHarnessStorage>,
    host: Arc<Host>,
    stream: StreamClient<LocalStream>,
    project: VolumeRef,
    issuer: AuthorityIssuer,
    stream_provider: ProviderRef,
    children: Vec<Arc<CapturedModel>>,
    grandchild: Arc<CapturedModel>,
    limits: Limits,
    publications: AtomicUsize,
    paused: bool,
}
impl ForkAtBatch {
    async fn assert_child_unbound(
        &self,
        seed: &acyclic_harness::fork::ForkSeed,
        issuer: &AuthorityIssuer,
    ) -> Result<()> {
        let child = StreamAggregate::open(
            &self.stream,
            seed.child.clone(),
            issuer.verifier(),
            SchemaRegistry::new(),
        )
        .await?;
        assert_eq!(child.reducer().revision(), 0);
        assert_eq!(
            child.reducer().conversation().and_then(|state| state.agent),
            None
        );
        Ok(())
    }

    async fn prebind_rejections(
        &self,
        parent: &StreamAggregate<LocalStream>,
        report: &acyclic_harness::fork::ForkReport,
        issuer: &AuthorityIssuer,
    ) -> Result<()> {
        let seed = report.clone().into_seed()?;
        self.assert_child_unbound(&seed, issuer).await?;
        let mut foreign = seed.clone();
        let grant = foreign
            .reference_grants
            .first_mut()
            .ok_or_else(|| Error::Storage("fixture reference grant missing".into()))?;
        let volume = VolumeRef::new(
            ProviderRef::new("foreign-filesystem", "filesystem", "2")?,
            "foreign-source",
            grant.file.volume().class(),
            grant.file.volume().owner().clone(),
        )?;
        grant.file = acyclic_harness::conversation::FileRef::new(
            volume,
            grant.file.path(),
            grant.file.version(),
            grant.file.descriptor().clone(),
            grant.file.display_name(),
        )?;
        foreign.validate()?;
        assert!(matches!(HarnessStorage::from_published_fork(
            self.limits.file_bytes, self.host.clone(), self.stream.clone(),
            issuer.clone(), parent, &foreign,
        ).await, Err(Error::Unauthorized(message)) if message.contains("another provider")));
        self.assert_child_unbound(&seed, issuer).await?;
        let mut unallocated = seed.clone();
        unallocated.operation_id = OperationId::from_bytes([252; 16]);
        unallocated.validate()?;
        assert!(matches!(HarnessStorage::from_published_fork(
            self.limits.file_bytes, self.host.clone(), self.stream.clone(),
            issuer.clone(), parent, &unallocated,
        ).await, Err(Error::Conflict(message)) if message.contains("another preparation")));
        self.assert_child_unbound(&seed, issuer).await?;
        let mut changed_project = seed.clone();
        for resource in &mut changed_project.resources {
            if let ResourceRevision::Project { volume, .. } = &mut resource.revision {
                *volume = VolumeRef::new(
                    volume.provider().clone(),
                    "unallocated-project",
                    VolumeClass::Project,
                    volume.owner().clone(),
                )?;
            }
        }
        changed_project.validate()?;
        assert!(matches!(HarnessStorage::from_published_fork(
            self.limits.file_bytes, self.host.clone(), self.stream.clone(),
            issuer.clone(), parent, &changed_project,
        ).await, Err(Error::Conflict(message)) if message.contains("allocated publication")));
        self.assert_child_unbound(&seed, issuer).await?;
        Ok(())
    }

    async fn attached_reader(
        &self,
        seed: &acyclic_harness::fork::ForkSeed,
        issuer: &AuthorityIssuer,
        admission: &ModelBatchPublication,
    ) -> Result<FilesystemContentVerifier<LocalAuthorityBackend, LocalObjectBackend>> {
        let attached = AgentId::from_bytes([220; 16]);
        let attached_scope = issuer.root_for_agent(
            attached,
            "exact-prefix-reader",
            seed.reference_capabilities(attached)?,
        );
        let attached_reader = FilesystemContentVerifier::new(
            self.host.clone(),
            issuer.verifier(),
            attached_scope,
            self.limits.file_bytes,
        )?;
        for prefix in &seed.inherited_context {
            attached_reader.read(prefix).await?;
        }
        assert!(matches!(
            attached_reader.read(&admission.request).await,
            Err(Error::Unauthorized(_))
        ));
        let ungranted_scope = issuer.root_for_agent(
            attached,
            "no-prefix-grants",
            Capabilities::new(std::iter::empty::<String>()),
        );
        let ungranted_reader = FilesystemContentVerifier::new(
            self.host.clone(),
            issuer.verifier(),
            ungranted_scope,
            self.limits.file_bytes,
        )?;
        for prefix in &seed.inherited_context {
            assert!(matches!(
                ungranted_reader.read(prefix).await,
                Err(Error::Unauthorized(_))
            ));
        }

        Ok(attached_reader)
    }

    async fn verified_child_storage(
        &self,
        parent: &StreamAggregate<LocalStream>,
        seed: &acyclic_harness::fork::ForkSeed,
        issuer: AuthorityIssuer,
        boundary: &CompletedModelBoundary,
        admission: &ModelBatchPublication,
        index: u8,
    ) -> Result<DurableHarnessStorage> {
        let attached_reader = self.attached_reader(seed, &issuer, admission).await?;
        let mut forged = seed.clone();
        forged.operation_id = OperationId::from_bytes([251; 16]);
        assert!(matches!(
            HarnessStorage::from_published_fork(
                self.limits.file_bytes,
                self.host.clone(),
                self.stream.clone(),
                issuer.clone(),
                parent,
                &forged,
            )
            .await,
            Err(Error::Unauthorized(_) | Error::Conflict(_))
        ));
        let storage = HarnessStorage::from_published_fork(
            self.limits.file_bytes,
            self.host.clone(),
            self.stream.clone(),
            issuer.clone(),
            parent,
            seed,
        )
        .await?;
        let primary = boundary
            .request
            .messages
            .iter()
            .find_map(|message| match &message.content {
                ModelContent::Parts(parts) => parts.iter().find_map(|part| {
                    if let ModelContentPart::File { file, .. } = part {
                        Some(file.clone())
                    } else {
                        None
                    }
                }),
                _ => None,
            })
            .ok_or_else(|| Error::Storage("inherited primary file missing".into()))?;
        let original = self.storage.read(&primary).await?;
        assert_eq!(storage.read(&primary).await?, original);
        let later = self
            .storage
            .stage(
                OperationId::from_bytes([index + 110; 16]),
                &format!("later/{index}.txt"),
                b"later parent bytes",
                "text/plain",
                "later.txt",
            )
            .await?;
        assert!(matches!(
            storage.read(&later).await,
            Err(Error::Unauthorized(_))
        ));
        assert_eq!(storage.read(&primary).await?, original);
        let child_file = storage
            .stage(
                OperationId::from_bytes([index + 120; 16]),
                "scratch/private.txt",
                b"child scratch",
                "text/plain",
                "private.txt",
            )
            .await?;
        assert!(matches!(
            attached_reader.read(&child_file).await,
            Err(Error::Unauthorized(_))
        ));
        for prefix in &seed.inherited_context {
            attached_reader.read(prefix).await?;
        }
        let reopened = HarnessStorage::from_published_fork(
            self.limits.file_bytes,
            self.host.clone(),
            self.stream.clone(),
            issuer,
            parent,
            seed,
        )
        .await?;
        assert_eq!(reopened.read(&primary).await?, original);
        assert!(matches!(
            reopened.read(&later).await,
            Err(Error::Unauthorized(_))
        ));
        Ok(storage)
    }

    async fn publish_children(&self, admission: ModelBatchPublication) -> Result<()> {
        // Each refusal precedes workspace preparation or child activation.
        let mut changed = admission.clone();
        changed.operation_id = OperationId::from_bytes([250; 16]);
        assert!(matches!(
            self.storage
                .verified_model_fork_boundary(&changed, self.limits)
                .await,
            Err(Error::Conflict(_))
        ));
        let mut changed = admission.clone();
        changed.request = admission.boundary.clone();
        assert!(matches!(
            self.storage
                .verified_model_fork_boundary(&changed, self.limits)
                .await,
            Err(Error::Conflict(_))
        ));
        let mut changed = admission.clone();
        changed.publisher.version.push_str("-substituted");
        assert!(matches!(
            self.storage
                .verified_model_fork_boundary(&changed, self.limits)
                .await,
            Err(Error::Conflict(_))
        ));
        let (boundary, mut parent) = self
            .storage
            .verified_model_fork_boundary(&admission, self.limits)
            .await?
            .into_parts();
        let provider = self.project.provider().clone();
        let parent_scope = self.issuer.root_for_agent(
            self.storage
                .owner_scope()
                .agent()
                .ok_or_else(|| Error::Storage("test owner agent missing".into()))?,
            "parent-publishing",
            Capabilities::new([
                "conversation:append".to_owned(),
                "fork:publish".to_owned(),
                self.project.capability(VolumeOperation::Read)?,
                self.project.capability(VolumeOperation::Write)?,
                self.storage.volume().capability(VolumeOperation::Read)?,
            ]),
        );
        let forks = Arc::new(CompositeForkVerifier::new(vec![
            Arc::new(FilesystemForkVerifier::new(
                self.host.clone(),
                self.limits.file_bytes,
            )?),
            Arc::new(StreamHistoryForkVerifier::new(
                self.stream_provider.clone(),
            )?),
        ])?);
        parent = parent.with_fork_verifier(forks.clone());
        let project_head = self.host.create_volume(&self.project).await?;
        for index in 0..2_u8 {
            let child_agent = AgentId::from_bytes([index + 20; 16]);
            let child_authority = Authority {
                kind: AggregateKind::Conversation,
                id: format!("child-{index}"),
            };
            let child_issuer =
                AuthorityIssuer::new("model-fork-e2e", [7; 32], child_authority.clone());
            let private = VolumeRef::new(
                provider.clone(),
                format!("private-{index}"),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(child_agent),
            )?;
            let project = VolumeRef::new(
                provider.clone(),
                format!("project-{index}"),
                VolumeClass::Project,
                self.project.owner().clone(),
            )?;
            let resolver = Arc::new(FilesystemContentVerifier::new(
                self.host.clone(),
                self.issuer.verifier(),
                parent_scope.clone(),
                self.limits.file_bytes,
            )?);
            let preparer = FilesystemForkPreparer::new(
                self.host.clone(),
                parent.reducer().clone(),
                self.issuer.verifier(),
                parent_scope.clone(),
                self.project.clone(),
                self.stream_provider.clone(),
                resolver,
            )?;
            let mut request = ForkRequest {
                operation_id: OperationId::from_bytes([index + 40; 16]),
                parent: parent.reducer().authority().clone(),
                parent_revision: parent.reducer().revision(),
                child: child_authority.clone(),
                child_agent,
                attached_agents: vec![AgentId::from_bytes([220; 16])],
                preparation: ForkPreparation {
                    child_project_volume: project.clone(),
                    child_private_volume: private.clone(),
                    inherited_through_sequence: parent
                        .reducer()
                        .conversation()
                        .ok_or_else(|| Error::Storage("test parent conversation missing".into()))?
                        .messages
                        .len() as u64,
                    maximum_inherited_messages: 64,
                    maximum_inherited_bytes: self.limits.file_bytes,
                    maximum_inherited_references: 128,
                },
                selections: vec![
                    ForkSelection {
                        required: true,
                        revision: ResourceRevision::History(StreamRef::new(
                            self.stream_provider.clone(),
                            parent.reducer().authority().stream_path()?.into_bytes(),
                            Some(parent.reducer().revision().to_string()),
                        )?),
                    },
                    ForkSelection {
                        required: true,
                        revision: ResourceRevision::Project {
                            volume: self.project.clone(),
                            generation: project_head.generation.clone(),
                        },
                    },
                ],
                boundary: None,
                model_boundary: None,
            };
            let verified = self
                .storage
                .verified_model_fork_boundary(&admission, self.limits)
                .await?;
            let mut future = request.clone();
            future.parent_revision += 1;
            for selection in &mut future.selections {
                if let ResourceRevision::History(reference) = &mut selection.revision {
                    *reference = StreamRef::new(
                        self.stream_provider.clone(),
                        future.parent.stream_path()?.into_bytes(),
                        Some(future.parent_revision.to_string()),
                    )?;
                }
            }
            future.validate()?;
            let future_error = self
                .storage
                .attach_model_fork_references(&verified, &mut future)
                .await
                .expect_err("future revision cannot be signed");
            assert!(
                matches!(future_error, Error::Conflict(_)),
                "unexpected future revision error: {future_error:?}"
            );
            assert!(future.model_boundary.is_none());
            self.storage
                .attach_model_fork_references(&verified, &mut request)
                .await?;
            let references = request
                .model_boundary
                .as_ref()
                .ok_or_else(|| Error::Storage("model boundary attestation missing".into()))?;
            assert_eq!(references.publication, admission.operation_id);
            assert_ne!(references.boundary_digest, [0; 32]);
            assert!(!references.files.is_empty());
            assert_ne!(references.attestation, [0; 32]);
            let report = parent.prepare_fork(&preparer, request).await?;
            self.prebind_rejections(&parent, &report, &child_issuer)
                .await?;

            let preview_seed = report.clone().into_seed()?;
            let reference_capabilities = preview_seed.reference_capabilities(child_agent)?;
            let mut child_capabilities = vec![
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                "fork:publish".to_owned(),
                private.capability(VolumeOperation::Read)?,
                private.capability(VolumeOperation::Write)?,
                // A child must be able to inspect its captured project when it
                // prepares the next recursive fork. This is an explicit
                // source-project grant, never an implicit transcript grant.
                project.capability(VolumeOperation::Read)?,
            ];
            child_capabilities.extend(reference_capabilities.iter().map(str::to_owned));
            let child_scope = child_issuer.root_for_agent(
                child_agent,
                "child",
                Capabilities::new(child_capabilities),
            );
            let mut child = StreamAggregate::open(
                &self.stream,
                child_authority.clone(),
                child_issuer.verifier(),
                SchemaRegistry::new(),
            )
            .await?
            .with_fork_verifier(forks.clone())
            .with_content_verifier(Arc::new(FilesystemContentVerifier::new(
                self.host.clone(),
                child_issuer.verifier(),
                child_scope.clone(),
                self.limits.file_bytes,
            )?))
            .with_merge_verifier(Arc::new(FilesystemProjectMergeVerifier::new(
                self.host.clone(),
            )));
            let seed = child
                .spawn_from_report(
                    &mut parent,
                    report,
                    parent_scope.clone(),
                    child_scope.clone(),
                )
                .await?;
            let storage = self
                .verified_child_storage(&parent, &seed, child_issuer.clone(), &boundary, &admission, index)
                .await?;
            let suffix = vec![ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(format!(
                    "fork child {index}; task: verify; workspace: project-{index}; fresh scratch"
                )),
            }];
            let child_model = self
                .children
                .get(index as usize)
                .cloned()
                .ok_or_else(|| Error::Invalid("missing deterministic child provider".into()))?;
            let bundle = storage
                .inherited_builder(boundary.clone(), suffix, child_model, self.limits)?
                .tools(storage.default_tools(self.limits)?)
                .grant("tool:call:acyclic.read_file")
                .grant("tool:call:acyclic.stage_file")
                .grant("tool:call:acyclic.list_files")
                .limits(self.limits)
                .build()?;
            let child_operation = OperationId::from_bytes([index + 60; 16]);
            let child_input = storage
                .stage(
                    child_operation,
                    &format!("input/child-{index}.txt"),
                    format!("explicit recursive child input {index}").as_bytes(),
                    "text/plain",
                    &format!("child-{index}.txt"),
                )
                .await?;
            storage
                .run_conversation(&bundle, child_operation, child_input, Vec::new(), 3)
                .await?;
            if index == 0 {
                let child_boundary = storage
                    .completed_model_boundary(child_operation, 0, self.limits)
                    .await?
                    .ok_or_else(|| Error::Storage("recursive child boundary missing".into()))?;
                // Reconstruct the direct parent's authoritative post-turn
                // conversation. The aggregate used for spawn is intentionally
                // only the pre-turn binding and must not become a stale fork
                // parent after the child model exchange.
                let child_parent = storage
                    .completed_conversation(child_operation, 0, self.limits)
                    .await?;
                self.publish_grandchild(
                    child_parent,
                    child_authority,
                    child_issuer,
                    child_scope,
                    project,
                    child_boundary,
                )
                .await?;
            }
        }
        self.publications.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn publish_grandchild(
        &self,
        mut parent: StreamAggregate<LocalStream>,
        parent_authority: Authority,
        parent_issuer: AuthorityIssuer,
        parent_scope: acyclic_harness::core::Scope,
        parent_project: VolumeRef,
        boundary: CompletedModelBoundary,
    ) -> Result<()> {
        let provider = parent_project.provider().clone();
        let forks = Arc::new(CompositeForkVerifier::new(vec![
            Arc::new(FilesystemForkVerifier::new(
                self.host.clone(),
                self.limits.file_bytes,
            )?),
            Arc::new(StreamHistoryForkVerifier::new(
                self.stream_provider.clone(),
            )?),
        ])?);
        parent = parent.with_fork_verifier(forks.clone());
        let project_head = self.host.create_volume(&parent_project).await?;
        let pinned_generation = project_head.generation.clone();
        let grandchild_agent = AgentId::from_bytes([90; 16]);
        let grandchild_authority = Authority {
            kind: AggregateKind::Conversation,
            id: "grandchild-0".into(),
        };
        let grandchild_issuer =
            AuthorityIssuer::new("model-fork-e2e", [7; 32], grandchild_authority.clone());
        let private = VolumeRef::new(
            provider.clone(),
            "grandchild-private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(grandchild_agent),
        )?;
        let project = VolumeRef::new(
            provider.clone(),
            "grandchild-project",
            VolumeClass::Project,
            parent_project.owner().clone(),
        )?;
        let resolver = Arc::new(FilesystemContentVerifier::new(
            self.host.clone(),
            parent_issuer.verifier(),
            parent_scope.clone(),
            self.limits.file_bytes,
        )?);
        let preparer = FilesystemForkPreparer::new(
            self.host.clone(),
            parent.reducer().clone(),
            parent_issuer.verifier(),
            parent_scope.clone(),
            parent_project.clone(),
            self.stream_provider.clone(),
            resolver,
        )?;
        let request = ForkRequest {
            operation_id: OperationId::from_bytes([91; 16]),
            parent: parent_authority.clone(),
            parent_revision: parent.reducer().revision(),
            child: grandchild_authority.clone(),
            child_agent: grandchild_agent,
            attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_project_volume: project.clone(),
                child_private_volume: private.clone(),
                inherited_through_sequence: parent.reducer().conversation().unwrap().messages.len()
                    as u64,
                maximum_inherited_messages: 64,
                maximum_inherited_bytes: self.limits.file_bytes,
                maximum_inherited_references: 128,
            },
            selections: vec![
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::History(StreamRef::new(
                        self.stream_provider.clone(),
                        parent.reducer().authority().stream_path()?.into_bytes(),
                        Some(parent.reducer().revision().to_string()),
                    )?),
                },
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::Project {
                        volume: parent_project.clone(),
                        generation: pinned_generation.clone(),
                    },
                },
            ],
            boundary: None,
            model_boundary: None,
        };

        // A provider-shaped boundary without an attestation from the bound
        // provider must be refused before the child aggregate is visible.
        let mut forged = request.clone();
        forged.boundary = Some(AttestedBoundary {
            provider: self.stream_provider.clone(),
            evidence: b"forged recursive boundary".to_vec(),
        });
        assert!(parent.prepare_fork(&preparer, forged).await.is_err());

        let report = parent.prepare_fork(&preparer, request).await?;
        let preview_seed = report.clone().into_seed()?;
        let reference_capabilities = preview_seed.reference_capabilities(grandchild_agent)?;
        let mut capabilities = vec![
            "conversation:bind".to_owned(),
            "conversation:append".to_owned(),
            private.capability(VolumeOperation::Read)?,
            private.capability(VolumeOperation::Write)?,
            project.capability(VolumeOperation::Read)?,
        ];
        capabilities.extend(reference_capabilities.iter().map(str::to_owned));
        let child_scope = grandchild_issuer.root_for_agent(
            grandchild_agent,
            "grandchild",
            Capabilities::new(capabilities),
        );
        let mut child = StreamAggregate::open(
            &self.stream,
            grandchild_authority.clone(),
            grandchild_issuer.verifier(),
            SchemaRegistry::new(),
        )
        .await?
        .with_fork_verifier(forks)
        .with_content_verifier(Arc::new(FilesystemContentVerifier::new(
            self.host.clone(),
            grandchild_issuer.verifier(),
            child_scope.clone(),
            self.limits.file_bytes,
        )?))
        .with_merge_verifier(Arc::new(FilesystemProjectMergeVerifier::new(
            self.host.clone(),
        )));
        child
            .spawn_from_report(&mut parent, report, parent_scope, child_scope.clone())
            .await?;

        // Mutating the source path after capture advances its generation. A
        // stale reopen is denied, while the already pinned fork remains valid.
        let source = self.host.resolve(&project_head.workspace).await?;
        self.host
            .apply(
                &source.workspace,
                Some(&source.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/recursive-same-path.txt".into(),
                    bytes: b"new parent generation".to_vec(),
                }],
                &IdempotencyKey::new("recursive-parent-mutation")?,
            )
            .await?;
        assert_ne!(source.generation, pinned_generation);
        assert!(self
            .host
            .read(
                &source.workspace,
                Some(&pinned_generation),
                "/recursive-same-path.txt",
                self.limits.file_bytes,
            )
            .await
            .is_err());

        let storage = HarnessStorage::from_providers(
            grandchild_agent,
            self.limits.file_bytes,
            self.host.clone(),
            self.stream.clone(),
            private,
            grandchild_authority,
            grandchild_issuer,
        )
        .await?;
        let suffix = vec![ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text(
                "grandchild task: read the inherited file and verify the pinned workspace".into(),
            ),
        }];
        let bundle = storage
            .inherited_builder(
                boundary.clone(),
                suffix,
                self.grandchild.clone(),
                self.limits,
            )?
            .tools(storage.default_tools(self.limits)?)
            .grant("tool:call:acyclic.read_file")
            .grant("tool:call:acyclic.stage_file")
            .grant("tool:call:acyclic.list_files")
            .limits(self.limits)
            .build()?;
        let operation = OperationId::from_bytes([92; 16]);
        let input = storage
            .stage(
                operation,
                "input/grandchild.txt",
                "grandchild explicit UTF-8 input α🦀\n".as_bytes(),
                "text/plain",
                "grandchild.txt",
            )
            .await?;
        storage
            .run_conversation(&bundle, operation, input, Vec::new(), 3)
            .await?;
        let records = storage.journal().replay(operation).await?;
        let read_started = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ToolStarted { call_id, .. } if call_id == "read-inherited" => {
                Some(call_id.clone())
            }
            _ => None,
        });
        let read_completed = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ToolCompleted {
                call_id,
                result,
                projection,
                ..
            } if call_id == "read-inherited" => {
                Some((call_id.clone(), result.clone(), projection.clone()))
            }
            _ => None,
        });
        let (completed_call, result, projection) = read_completed
            .ok_or_else(|| Error::Storage("recursive read_file result missing".into()))?;
        assert_eq!(read_started.as_deref(), Some(completed_call.as_str()));
        let result: Value = serde_json::from_slice(&storage.journal().load(&result).await?)
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(
            result.get("text").and_then(Value::as_str),
            Some("root request")
        );
        let projection: Value = serde_json::from_slice(&storage.journal().load(&projection).await?)
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(projection, Value::String("root request".into()));
        let captured = self.grandchild.requests.lock().unwrap();
        assert!(captured.len() >= 2);
        let actual = PreparedModelInput::prepare(captured[0].clone(), self.limits)?;
        let inherited = FrozenModelPrefix::capture(&actual, boundary.request.messages.len())?;
        assert_eq!(inherited.message_bytes(), boundary.prefix.message_bytes());
        Ok(())
    }
}
impl ModelBatchPublisher for ForkAtBatch {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            name: "test.filesystem-model-fork".into(),
            version: "1".into(),
            digest: [6; 32],
        }
    }
    fn guarantee(&self) -> EffectGuarantee {
        EffectGuarantee::AtMostOnce
    }
    fn publish<'a>(&'a self, request: ModelBatchPublication) -> BoxFuture<'a, Result<()>> {
        if self.paused {
            return Box::pin(async { Err(Error::Storage("publication interrupted".into())) });
        }
        Box::pin(self.publish_children(request))
    }
    fn reconcile<'a>(&'a self, _: ModelBatchPublication) -> BoxFuture<'a, Result<Option<()>>> {
        Box::pin(async { Ok((self.publications.load(Ordering::SeqCst) > 0).then_some(())) })
    }
}

#[tokio::test]
async fn native_forks_capture_completed_authoritative_exchange_and_exact_model_prefix() -> Result<()>
{
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = ProviderRef::new("model-fork-e2e", "filesystem", "2")?;
    let stream_provider = ProviderRef::new("model-fork-e2e", "stream", "2")?;
    let host = Arc::new(FilesystemHost::new(
        Fs::local(LocalOptions::new(directory.path().join("fs")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(
            directory.path().join("streams"),
            LocalStreamLimits::default(),
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?,
    ));
    let agent = AgentId::from_bytes([1; 16]);
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "root".into(),
    };
    let issuer = AuthorityIssuer::new("model-fork-e2e", [7; 32], authority.clone());
    let private = VolumeRef::new(
        provider.clone(),
        "root-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let project = VolumeRef::new(
        provider,
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
    )?;
    host.create_volume(&private).await?;
    host.create_volume(&project).await?;
    let limits = Limits::default();
    let storage = Arc::new(
        HarnessStorage::from_providers(
            agent,
            limits.file_bytes,
            host.clone(),
            stream.clone(),
            private,
            authority,
            issuer.clone(),
        )
        .await?,
    );
    let model = Model::new("test", "frozen", "1", Value::Null)?;
    let root_model = Arc::new(CapturedModel {
        root: true,
        read_first: false,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let children = (0..2)
        .map(|_| {
            Arc::new(CapturedModel {
                root: false,
                read_first: true,
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            })
        })
        .collect::<Vec<_>>();
    let grandchild = Arc::new(CapturedModel {
        root: false,
        read_first: true,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let publisher = Arc::new(ForkAtBatch {
        storage: storage.clone(),
        host,
        stream,
        project,
        issuer,
        stream_provider,
        children: children.clone(),
        grandchild,
        limits,
        publications: AtomicUsize::new(0),
        paused: false,
    });
    let bundle = storage
        .builder()
        .model(model, root_model.clone())
        .grant("model:generate")
        .tools(storage.default_tools(limits)?)
        .grant("tool:call:acyclic.read_file")
        .grant("tool:call:acyclic.stage_file")
        .grant("tool:call:acyclic.list_files")
        .batch_publisher(publisher.clone())
        .limits(limits)
        .build()?;
    let operation = OperationId::from_bytes([2; 16]);
    let input = storage
        .stage(
            operation,
            "input/root.txt",
            b"root request",
            "text/plain",
            "root.txt",
        )
        .await?;
    let output = storage
        .run_conversation(&bundle, operation, input.clone(), Vec::new(), 3)
        .await?;
    assert_eq!(output.text, " \nα🦀\t retained\nfinal only");
    assert_eq!(publisher.publications.load(Ordering::SeqCst), 1);
    let boundary = storage
        .completed_model_boundary(operation, 0, limits)
        .await?
        .ok_or_else(|| Error::Storage("test completed boundary missing".into()))?;
    // Each sibling receives one read_file turn followed by its terminal
    // continuation. The first request for each sibling is the exact fork
    // boundary; the continuation is allowed to contain the paired result.
    let child_requests = children
        .iter()
        .map(|child| child.requests.lock().unwrap().clone())
        .collect::<Vec<_>>();
    assert_eq!(child_requests.len(), 2);
    assert!(child_requests.iter().all(|requests| requests.len() == 2));
    let first_sibling = &child_requests[0][0];
    let second_sibling = &child_requests[1][0];
    assert_eq!(
        &first_sibling.messages[..boundary.request.messages.len()],
        boundary.request.messages
    );
    assert_eq!(
        &second_sibling.messages[..boundary.request.messages.len()],
        boundary.request.messages
    );
    let first_prepared = PreparedModelInput::prepare(first_sibling.clone(), limits)?;
    let second_prepared = PreparedModelInput::prepare(second_sibling.clone(), limits)?;
    let first_prefix =
        FrozenModelPrefix::capture(&first_prepared, boundary.request.messages.len())?;
    let second_prefix =
        FrozenModelPrefix::capture(&second_prepared, boundary.request.messages.len())?;
    assert_eq!(
        first_prefix.message_bytes(),
        boundary.prefix.message_bytes()
    );
    assert_eq!(
        second_prefix.message_bytes(),
        boundary.prefix.message_bytes()
    );
    assert_eq!(first_prefix.digest(), second_prefix.digest());
    for requests in &child_requests {
        let request = &requests[0];
        assert_eq!(
            request.messages.get(..boundary.request.messages.len()),
            Some(boundary.request.messages.as_slice())
        );
        assert_eq!(request.messages.len(), boundary.request.messages.len() + 2);
        let actual = PreparedModelInput::prepare(request.clone(), limits)?;
        let inherited = FrozenModelPrefix::capture(&actual, boundary.request.messages.len())?;
        assert_eq!(inherited.message_bytes(), boundary.prefix.message_bytes());
    }
    storage
        .run_conversation(&bundle, operation, input, Vec::new(), 3)
        .await?;
    assert_eq!(publisher.publications.load(Ordering::SeqCst), 1);
    let next_operation = OperationId::from_bytes([3; 16]);
    let next_input = storage
        .stage(
            next_operation,
            "input/follow-up.txt",
            b"follow-up",
            "text/plain",
            "follow-up.txt",
        )
        .await?;
    storage
        .run_conversation(&bundle, next_operation, next_input.clone(), Vec::new(), 3)
        .await?;
    let requests = root_model
        .requests
        .lock()
        .map_err(|error| Error::Storage(error.to_string()))?
        .clone();
    assert_eq!(requests.len(), 3);
    let mut expected = boundary.request.messages.clone();
    expected.push(ModelMessage {
        role: ModelRole::Assistant,
        content: ModelContent::Text("final only".into()),
    });
    expected.push(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Parts(vec![ModelContentPart::File {
            file: next_input,
            policy: FileProjectionPolicy::BoundedFull,
        }]),
    });
    assert_eq!(
        requests.get(2).map(|request| &request.messages),
        Some(&expected)
    );
    drop(requests);
    assert!(matches!(
        storage.completed_conversation(operation, 0, limits).await,
        Err(Error::Conflict(_))
    ));
    let records = storage.journal().replay(operation).await?;
    assert!(records.iter().any(|record| matches!(
        record.event,
        ExecutionEvent::BatchPublicationCompleted { .. }
    )));
    Ok(())
}

#[tokio::test]
async fn stale_completed_boundary_is_refused_before_publication_files_are_written() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let provider = ProviderRef::new("model-fork-e2e", "filesystem", "2")?;
    let stream_provider = ProviderRef::new("model-fork-e2e", "stream", "2")?;
    let host = Arc::new(FilesystemHost::new(
        Fs::local(LocalOptions::new(directory.path().join("fs")))
            .await
            .map_err(|error| Error::Storage(error.to_string()))?,
        provider.clone(),
    )?);
    let stream = StreamClient::new(Arc::new(
        LocalStream::open(
            directory.path().join("streams"),
            LocalStreamLimits::default(),
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?,
    ));
    let agent = AgentId::from_bytes([1; 16]);
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "root".into(),
    };
    let issuer = AuthorityIssuer::new("model-fork-e2e", [7; 32], authority.clone());
    let private = VolumeRef::new(
        provider.clone(),
        "root-private",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
    )?;
    let project = VolumeRef::new(
        provider,
        "root-project",
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
    )?;
    host.create_volume(&private).await?;
    host.create_volume(&project).await?;
    let limits = Limits::default();
    let storage = Arc::new(
        HarnessStorage::from_providers(
            agent,
            limits.file_bytes,
            host.clone(),
            stream.clone(),
            private,
            authority,
            issuer.clone(),
        )
        .await?,
    );
    let model = Model::new("test", "frozen", "1", Value::Null)?;
    let root_model = Arc::new(CapturedModel {
        root: true,
        read_first: false,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let children = (0..2)
        .map(|_| {
            Arc::new(CapturedModel {
                root: false,
                read_first: false,
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
            })
        })
        .collect::<Vec<_>>();
    let grandchild = Arc::new(CapturedModel {
        root: false,
        read_first: false,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
    });
    let publisher = Arc::new(ForkAtBatch {
        storage: storage.clone(),
        host: host.clone(),
        stream: stream.clone(),
        project,
        issuer: issuer.clone(),
        stream_provider,
        children: children.clone(),
        grandchild,
        limits,
        publications: AtomicUsize::new(0),
        paused: true,
    });
    let bundle = storage
        .builder()
        .model(model, root_model.clone())
        .grant("model:generate")
        .tools(storage.default_tools(limits)?)
        .grant("tool:call:acyclic.read_file")
        .grant("tool:call:acyclic.stage_file")
        .grant("tool:call:acyclic.list_files")
        .batch_publisher(publisher.clone())
        .limits(limits)
        .build()?;

    let operation = OperationId::from_bytes([2; 16]);
    let input = storage
        .stage(
            operation,
            "input/root.txt",
            b"root request",
            "text/plain",
            "root.txt",
        )
        .await?;
    assert!(
        matches!(storage.run_conversation(&bundle, operation, input, Vec::new(), 3).await,
        Err(Error::Storage(message)) if message == "publication interrupted")
    );
    let concurrent = OperationId::from_bytes([4; 16]);
    let content = storage
        .stage(
            concurrent,
            "input/concurrent.txt",
            b"concurrent user input",
            "text/plain",
            "concurrent.txt",
        )
        .await?;
    let authority = storage.conversation().clone();
    let mut aggregate =
        StreamAggregate::open(&stream, authority, issuer.verifier(), SchemaRegistry::new())
            .await?
            .with_content_verifier(Arc::new(FilesystemContentVerifier::new(
                host.clone(),
                issuer.verifier(),
                storage.owner_scope().clone(),
                limits.file_bytes,
            )?));
    let message = ConversationMessage {
        id: uuid::Uuid::from_bytes([4; 16]),
        sequence: aggregate
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Storage("test conversation missing".into()))?
            .messages
            .len() as u64
            + 1,
        kind: MessageKind::User,
        content,
        attachments: Vec::new().into(),
        reply_to: None,
        tool_call_id: None,
        extensions: Default::default(),
    };
    aggregate
        .execute(Command {
            operation_id: concurrent,
            idempotency_key: IdempotencyKey::new("concurrent-message")?,
            expected_revision: aggregate.reducer().revision(),
            scope: storage.owner_scope().clone(),
            causal_parent: None,
            action: Action::AppendConversationMessage {
                message: Box::new(message),
            },
        })
        .await?;
    let private = workspace_ref(
        storage.volume().provider().clone(),
        &storage.volume().storage_name()?,
    )?;
    let before = host.resolve(&private).await?.generation;
    for _ in 0..2 {
        assert!(matches!(
            storage.completed_conversation(operation, 0, limits).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(host.resolve(&private).await?.generation, before);
    }
    Ok(())
}
