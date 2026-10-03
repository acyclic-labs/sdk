//! Exact model boundaries passed through real durable workspace/conversation forks.
#![cfg(feature = "filesystem-local")]
#![allow(clippy::too_many_lines)]

use acyclic_fs::{Fs, LocalAuthorityBackend, LocalObjectBackend, LocalOptions};
use acyclic_harness::{
    AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result,
    batch_publication::{ModelBatchPublication, ModelBatchPublisher},
    conversation::{
        Attachment, ContentResidencyVerifier, ConversationMessage, Limits, MessageKind,
        VolumeClass,
        VolumeOperation, VolumeOwner, VolumeRef,
    },
    core::{
        Action, AggregateKind, Authority, AuthorityIssuer, Command, EffectGuarantee, SchemaRegistry,
    },
    executor::ExecutionEvent,
    filesystem::{
        DurableHarnessStorage, FilesystemContentVerifier, FilesystemForkPreparer,
        FilesystemForkVerifier, FilesystemHost, FilesystemProjectMergeVerifier, HarnessStorage,
        WorkspaceMutation, workspace_ref,
    },
    fork::{
        CompositeForkVerifier, ForkPreparation, ForkRequest, ForkSelection,
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
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};
use futures::{
    future::BoxFuture,
    stream::{self, BoxStream},
    StreamExt,
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Barrier, Notify};

type Host = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;

struct CapturedModel {
    calls: AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
    serialized_requests: Mutex<Vec<Vec<u8>>>,
    binding_digests: Mutex<Vec<[u8; 32]>>,
    root: bool,
    read_first: bool,
    overlap_barrier: Option<Arc<Barrier>>,
}
impl ModelProvider for CapturedModel {
    fn generate<'a>(&'a self, request: ModelRequest) -> BoxStream<'a, Result<ModelEvent>> {
        let prepared = match PreparedModelInput::prepare(request.clone(), Limits::default()) {
            Ok(prepared) => prepared,
            Err(error) => return Box::pin(stream::iter(vec![Err(error)])),
        };
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request);
        self.serialized_requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(prepared.bytes().to_vec());
        self.binding_digests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(prepared.manifest().binding_digest);
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
        if let Some(barrier) = self.overlap_barrier.clone().filter(|_| !self.root && first) {
            let mut events = events;
            let first_event = events.remove(0);
            return Box::pin(
                stream::once(async move {
                    barrier.wait().await;
                    Ok::<ModelEvent, Error>(first_event)
                })
                .chain(stream::iter(events.into_iter().map(Ok))),
            );
        }
        Box::pin(stream::iter(events.into_iter().map(Ok)))
    }
    fn reconcile<'a>(&'a self, _: ModelAttempt) -> BoxFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async { Ok(None) })
    }
}

impl CapturedModel {
    fn evidence(&self) -> Result<(Vec<ModelRequest>, Vec<Vec<u8>>, Vec<[u8; 32]>)> {
        let requests = self
            .requests
            .lock()
            .map_err(|error| Error::Storage(error.to_string()))?
            .clone();
        let serialized = self
            .serialized_requests
            .lock()
            .map_err(|error| Error::Storage(error.to_string()))?
            .clone();
        let bindings = self
            .binding_digests
            .lock()
            .map_err(|error| Error::Storage(error.to_string()))?
            .clone();
        Ok((requests, serialized, bindings))
    }
}

/// Captures the executor's durable publication admission without inventing a
/// second fork path. The child publication is subsequently verified through
/// HarnessStorage and attached to the typed recursive fork request.
struct RecordingPublisher {
    identity: ComponentIdentity,
    admission: Mutex<Option<ModelBatchPublication>>,
    ready: Option<Arc<Notify>>,
    release: Option<Arc<Barrier>>,
}
impl RecordingPublisher {
    fn new(index: u8, ready: Option<Arc<Notify>>, release: Option<Arc<Barrier>>) -> Self {
        Self {
            identity: ComponentIdentity {
                name: format!("test.recursive-model-batch-{index}"),
                version: "1".into(),
                digest: [index.saturating_add(1); 32],
            },
            admission: Mutex::new(None),
            ready,
            release,
        }
    }

    fn admission(&self) -> Result<ModelBatchPublication> {
        self.admission
            .lock()
            .map_err(|_| Error::Storage("recursive publication lock is poisoned".into()))?
            .clone()
            .ok_or_else(|| Error::Storage("recursive model publication was not admitted".into()))
    }
}
impl ModelBatchPublisher for RecordingPublisher {
    fn identity(&self) -> ComponentIdentity {
        self.identity.clone()
    }

    fn guarantee(&self) -> EffectGuarantee {
        EffectGuarantee::AtMostOnce
    }

    fn publish<'a>(&'a self, request: ModelBatchPublication) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut admission = self
                .admission
                .lock()
                .map_err(|_| Error::Storage("recursive publication lock is poisoned".into()))?;
            if let Some(existing) = &*admission {
                if existing != &request {
                    return Err(Error::Conflict(
                        "recursive model publication identity changed".into(),
                    ));
                }
            } else {
                *admission = Some(request);
            }
            drop(admission);
            if let Some(ready) = &self.ready {
                ready.notify_one();
            }
            if let Some(release) = &self.release {
                release.wait().await;
            }
            Ok(())
        })
    }

    fn reconcile<'a>(
        &'a self,
        request: ModelBatchPublication,
    ) -> BoxFuture<'a, Result<Option<()>>> {
        Box::pin(async move {
            let admission = self
                .admission
                .lock()
                .map_err(|_| Error::Storage("recursive publication lock is poisoned".into()))?;
            Ok(admission
                .as_ref()
                .filter(|existing| *existing == &request)
                .map(|_| ()))
        })
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
    fn assert_model_evidence(
        &self,
        model: &CapturedModel,
        expected_binding: [u8; 32],
    ) -> Result<Vec<ModelRequest>> {
        let (requests, serialized, bindings) = model.evidence()?;
        assert_eq!(requests.len(), serialized.len());
        assert_eq!(requests.len(), bindings.len());
        for ((request, bytes), binding) in requests.iter().zip(&serialized).zip(&bindings) {
            let prepared = PreparedModelInput::prepare(request.clone(), self.limits)?;
            assert_eq!(bytes, prepared.bytes());
            assert_eq!(*binding, prepared.manifest().binding_digest);
            assert_eq!(*binding, expected_binding);
        }
        Ok(requests)
    }

    async fn assert_model_read(
        &self,
        storage: &DurableHarnessStorage,
        operation: OperationId,
        expected: &str,
    ) -> Result<()> {
        let records = storage.journal().replay(operation).await?;
        let result = records.iter().find_map(|record| match &record.event {
            ExecutionEvent::ToolCompleted {
                call_id,
                result,
                ..
            } if call_id == "read-inherited" => Some(result.clone()),
            _ => None,
        });
        let result = result
            .ok_or_else(|| Error::Storage("recursive read_file result missing".into()))?;
        let result: Value = serde_json::from_slice(&storage.journal().load(&result).await?)
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(result.get("text").and_then(Value::as_str), Some(expected));
        Ok(())
    }

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
        let model_files = seed
            .model_boundary
            .as_ref()
            .ok_or_else(|| Error::Storage("model boundary manifest missing".into()))?
            .files
            .clone();
        for file in &model_files {
            attached_reader.read(file).await?;
        }
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
        for file in &model_files {
            assert!(matches!(
                ungranted_reader.read(file).await,
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
        let model_files = seed
            .model_boundary
            .as_ref()
            .ok_or_else(|| Error::Storage("model boundary manifest missing".into()))?
            .files
            .clone();
        for file in &model_files {
            let expected = self.storage.read(file).await?;
            assert_eq!(storage.read(file).await?, expected);
        }
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
        let sibling_barrier = Arc::new(Barrier::new(2));
        let child_zero_ready = Arc::new(Notify::new());
        let child_zero_release = Arc::new(Barrier::new(2));
        let mut child_zero_context = None;
        let mut child_runs = Vec::new();
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
            let mut forged = request.clone();
            forged.operation_id = OperationId::from_bytes([45 + index; 16]);
            forged
                .model_boundary
                .as_mut()
                .ok_or_else(|| Error::Storage("model boundary attestation missing".into()))?
                .attestation[0] ^= 1;
            let forged_report = parent.prepare_fork(&preparer, forged).await?;
            let forged_seed = forged_report.clone().into_seed()?;
            self.assert_child_unbound(&forged_seed, &child_issuer).await?;
            let forged_error = parent
                .publish_fork_report(forged_report, parent_scope.clone())
                .await
                .expect_err("forged model boundary was published");
            assert!(
                matches!(forged_error, Error::Invalid(message) if message.contains("manifest")),
                "unexpected forged model boundary error: {forged_error:?}"
            );
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
            let storage = Arc::new(self
                .verified_child_storage(
                    &parent,
                    &seed,
                    child_issuer.clone(),
                    &boundary,
                    &admission,
                    index,
                )
                .await?);
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
            let child_publisher = Arc::new(RecordingPublisher::new(
                index,
                (index == 0).then(|| child_zero_ready.clone()),
                (index == 0).then(|| child_zero_release.clone()),
            ));
            let bundle = storage
                .inherited_builder(boundary.clone(), suffix, child_model, self.limits)?
                .tools(storage.default_tools(self.limits)?)
                .grant("tool:call:acyclic.read_file")
                .grant("tool:call:acyclic.stage_file")
                .grant("tool:call:acyclic.list_files")
                .batch_publisher(child_publisher.clone())
                .limits(self.limits)
                .build()?;
            if index == 0 {
                child_zero_context = Some((
                    storage.clone(),
                    child_publisher.clone(),
                    child_authority.clone(),
                    child_issuer.clone(),
                    child_scope.clone(),
                    project.clone(),
                ));
            }
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
            let barrier = sibling_barrier.clone();
            child_runs.push(async move {
                barrier.wait().await;
                storage
                    .run_conversation(&bundle, child_operation, child_input, Vec::new(), 3)
                    .await?;
                Ok::<_, Error>((
                    index,
                    storage,
                    child_publisher,
                    child_authority,
                    child_issuer,
                    child_scope,
                    project,
                    child_operation,
                ))
            });
        }
        let all_children = futures::future::join_all(child_runs);
        tokio::pin!(all_children);
        let (
            child_zero_storage,
            child_zero_publisher,
            child_zero_authority,
            child_zero_issuer,
            child_zero_scope,
            child_zero_project,
        ) = child_zero_context
            .ok_or_else(|| Error::Storage("child zero context missing".into()))?;
        tokio::select! {
            _ = child_zero_ready.notified() => {}
            _ = &mut all_children => {
                return Err(Error::Conflict(
                    "child completed before recursive boundary handoff".into(),
                ));
            }
        }
        self.publish_grandchild(
            &child_zero_storage,
            child_zero_publisher.admission()?,
            child_zero_authority,
            child_zero_issuer,
            child_zero_scope,
            child_zero_project,
        )
        .await?;
        child_zero_release.wait().await;

        let mut completed_children = Vec::with_capacity(2);
        for result in all_children.await {
            completed_children.push(result?);
        }
        let boundary_binding =
            PreparedModelInput::prepare(boundary.request.clone(), self.limits)?
                .manifest()
                .binding_digest;
        for (index, storage, _, _, _, _, _, operation) in &completed_children {
            self.assert_model_read(storage, *operation, "root request").await?;
            let child_model = self
                .children
                .get(*index as usize)
                .ok_or_else(|| Error::Invalid("missing deterministic child provider".into()))?;
            let requests = self.assert_model_evidence(child_model, boundary_binding)?;
            assert_eq!(requests.len(), 2);
            assert_eq!(
                &requests[0].messages[..boundary.request.messages.len()],
                boundary.request.messages
            );
            let actual = PreparedModelInput::prepare(requests[0].clone(), self.limits)?;
            let inherited = FrozenModelPrefix::capture(&actual, boundary.request.messages.len())?;
            assert_eq!(inherited.message_bytes(), boundary.prefix.message_bytes());
        }
        self.publications.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn publish_grandchild(
        &self,
        storage: &DurableHarnessStorage,
        admission: ModelBatchPublication,
        parent_authority: Authority,
        parent_issuer: AuthorityIssuer,
        parent_scope: acyclic_harness::core::Scope,
        parent_project: VolumeRef,
    ) -> Result<()> {
        let verified = storage
            .verified_model_fork_boundary(&admission, self.limits)
            .await?;
        let boundary = verified.boundary().clone();
        let (_, mut parent) = verified.into_parts();
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
        let source_before = self.host.resolve(&project_head.workspace).await?;
        let seeded_source = self
            .host
            .apply(
                &source_before.workspace,
                Some(&source_before.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/recursive-same-path.txt".into(),
                    bytes: b"old parent generation".to_vec(),
                }],
                &IdempotencyKey::new("recursive-parent-seed")?,
            )
            .await?;
        let pinned_generation = seeded_source.clone();
        let old_source_bytes = self
            .host
            .read(
                &source_before.workspace,
                Some(&pinned_generation),
                "/recursive-same-path.txt",
                self.limits.file_bytes,
            )
            .await?;
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

        let verified_references = storage
            .verified_model_fork_boundary(&admission, self.limits)
            .await?;
        let mut request = request;
        storage
            .attach_model_fork_references(&verified_references, &mut request)
            .await?;

        // Model-boundary attestations bind the exact child identity and must
        // be rejected at publication admission before the child is visible.
        let mut forged = request.clone();
        forged.operation_id = OperationId::from_bytes([145; 16]);
        forged
            .model_boundary
            .as_mut()
            .ok_or_else(|| Error::Storage("model boundary attestation missing".into()))?
            .attestation[0] ^= 1;
        let forged_report = parent.prepare_fork(&preparer, forged).await?;
        let forged_seed = forged_report.clone().into_seed()?;
        self.assert_child_unbound(&forged_seed, &grandchild_issuer)
            .await?;
        let forged_error = parent
            .publish_fork_report(forged_report, parent_scope.clone())
            .await
            .expect_err("forged recursive model boundary was published");
        assert!(
            matches!(forged_error, Error::Invalid(message) if message.contains("manifest")),
            "unexpected forged recursive boundary error: {forged_error:?}"
        );

        let report = parent.prepare_fork(&preparer, request).await?;
        let seed = report.clone().into_seed()?;
        self.assert_child_unbound(&seed, &grandchild_issuer).await?;
        parent.publish_fork_report(report, parent_scope).await?;

        // Mutating the source path after capture advances its generation. A
        // stale reopen is denied, while the already pinned fork remains valid.
        let source = self.host.resolve(&project_head.workspace).await?;
        let updated_generation = self
            .host
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
        assert_ne!(updated_generation, pinned_generation);
        assert_eq!(
            self.host
                .read(
                    &source.workspace,
                    Some(&updated_generation),
                    "/recursive-same-path.txt",
                    self.limits.file_bytes,
                )
                .await?
                .as_ref(),
            b"new parent generation".as_slice()
        );
        assert_eq!(
            self.host
                .read(
                    &source.workspace,
                    Some(&pinned_generation),
                    "/recursive-same-path.txt",
                    self.limits.file_bytes,
                )
                .await?,
            old_source_bytes
        );

        let storage = HarnessStorage::from_published_fork(
            self.limits.file_bytes,
            self.host.clone(),
            self.stream.clone(),
            grandchild_issuer.clone(),
            &parent,
            &seed,
        )
        .await?;
        let model_files = seed
            .model_boundary
            .as_ref()
            .ok_or_else(|| Error::Storage("grandchild model boundary manifest missing".into()))?
            .files
            .clone();
        for file in &model_files {
            assert_eq!(storage.read(file).await?, self.storage.read(file).await?);
        }
        let ungranted_scope = grandchild_issuer.root_for_agent(
            grandchild_agent,
            "grandchild-no-model-grants",
            Capabilities::new(std::iter::empty::<String>()),
        );
        let ungranted_reader = FilesystemContentVerifier::new(
            self.host.clone(),
            grandchild_issuer.verifier(),
            ungranted_scope,
            self.limits.file_bytes,
        )?;
        for file in &model_files {
            assert!(matches!(
                ungranted_reader.read(file).await,
                Err(Error::Unauthorized(_))
            ));
        }
        let child_project_workspace =
            workspace_ref(project.provider().clone(), &project.storage_name()?)?;
        let child_project = self.host.resolve(&child_project_workspace).await?;
        assert_eq!(
            self.host
                .read(
                    &child_project.workspace,
                    Some(&child_project.generation),
                    "/recursive-same-path.txt",
                    self.limits.file_bytes,
                )
                .await?,
            old_source_bytes
        );
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
        assert_eq!(
            projection,
            Value::String("root request".into())
        );
        let (captured, serialized, bindings) = self.grandchild.evidence()?;
        assert!(captured.len() >= 2);
        assert_eq!(captured.len(), serialized.len());
        assert_eq!(captured.len(), bindings.len());
        let boundary_binding =
            PreparedModelInput::prepare(boundary.request.clone(), self.limits)?
                .manifest()
                .binding_digest;
        for ((request, bytes), binding) in captured.iter().zip(&serialized).zip(&bindings) {
            let prepared = PreparedModelInput::prepare(request.clone(), self.limits)?;
            assert_eq!(bytes, prepared.bytes());
            assert_eq!(*binding, prepared.manifest().binding_digest);
            assert_eq!(*binding, boundary_binding);
        }
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
    let sibling_overlap_barrier = Arc::new(Barrier::new(2));
    let root_model = Arc::new(CapturedModel {
        overlap_barrier: None,
        root: true,
        read_first: false,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
        serialized_requests: Mutex::new(Vec::new()),
        binding_digests: Mutex::new(Vec::new()),
    });
    let children = (0..2)
        .map(|_| {
            Arc::new(CapturedModel {
                overlap_barrier: Some(sibling_overlap_barrier.clone()),
                root: false,
                read_first: true,
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
                serialized_requests: Mutex::new(Vec::new()),
                binding_digests: Mutex::new(Vec::new()),
            })
        })
        .collect::<Vec<_>>();
    let grandchild = Arc::new(CapturedModel {
        overlap_barrier: None,
        root: false,
        read_first: true,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
        serialized_requests: Mutex::new(Vec::new()),
        binding_digests: Mutex::new(Vec::new()),
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
    let root_attachment = storage
        .stage(
            operation,
            "input/root-attachment.bin",
            b"root attachment bytes",
            "application/octet-stream",
            "root-attachment.bin",
        )
        .await?;
    let root_attachments = vec![Attachment {
        file: root_attachment.clone(),
        label: Some("root attachment".into()),
    }];
    let output = storage
        .run_conversation(
            &bundle,
            operation,
            input.clone(),
            root_attachments.clone(),
            3,
        )
        .await?;
    assert_eq!(output.text, " \nα🦀\t retained\nfinal only");
    assert_eq!(publisher.publications.load(Ordering::SeqCst), 1);
    let (root_requests, root_serialized, root_bindings) = root_model.evidence()?;
    assert_eq!(root_requests.len(), root_serialized.len());
    assert_eq!(root_requests.len(), root_bindings.len());
    let root_boundary_binding = root_bindings
        .first()
        .copied()
        .ok_or_else(|| Error::Storage("root model binding digest missing".into()))?;
    for ((request, bytes), binding) in root_requests
        .iter()
        .zip(&root_serialized)
        .zip(&root_bindings)
    {
        let prepared = PreparedModelInput::prepare(request.clone(), limits)?;
        assert_eq!(bytes, prepared.bytes());
        assert_eq!(*binding, prepared.manifest().binding_digest);
        assert_eq!(*binding, root_boundary_binding);
    }
    let boundary = storage
        .completed_model_boundary(operation, 0, limits)
        .await?
        .ok_or_else(|| Error::Storage("test completed boundary missing".into()))?;
    assert_eq!(boundary.rejection_evidence.len(), 1);
    assert_eq!(boundary.rejection_evidence[0].call_id, "invalid");
    assert_eq!(boundary.rejection_evidence[0].name, "acyclic.stage_file");
    assert_eq!(
        PreparedModelInput::prepare(boundary.request.clone(), limits)?
            .manifest()
            .binding_digest,
        root_boundary_binding
    );
    assert!(boundary.request.messages.iter().any(|message| {
        message
            .content
            .file_refs()
            .iter()
            .any(|file| *file == &root_attachment)
    }));
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
        .run_conversation(&bundle, operation, input, root_attachments.clone(), 3)
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
        overlap_barrier: None,
        root: true,
        read_first: false,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
        serialized_requests: Mutex::new(Vec::new()),
        binding_digests: Mutex::new(Vec::new()),
    });
    let children = (0..2)
        .map(|_| {
            Arc::new(CapturedModel {
                overlap_barrier: None,
                root: false,
                read_first: false,
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
                serialized_requests: Mutex::new(Vec::new()),
                binding_digests: Mutex::new(Vec::new()),
            })
        })
        .collect::<Vec<_>>();
    let grandchild = Arc::new(CapturedModel {
        overlap_barrier: None,
        root: false,
        read_first: false,
        calls: AtomicUsize::new(0),
        requests: Mutex::new(Vec::new()),
        serialized_requests: Mutex::new(Vec::new()),
        binding_digests: Mutex::new(Vec::new()),
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
