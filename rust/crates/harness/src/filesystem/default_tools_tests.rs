use super::*;
use crate::{
    AgentId, Capabilities,
    conversation::{Attachment, MessageKind, ReferencedAttachments, VolumeClass, VolumeOwner},
    core::{AggregateKind, AuthorityIssuer, SchemaRegistry},
    filesystem::{
        ExactTextReplacement, FilesystemContentVerifier, WorkspaceMutation, workspace_ref,
    },
    resources::ProviderRef,
};
use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_stream::MemoryStream;

type Host = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;

struct Fixture {
    host: Arc<Host>,
    parent: StreamAggregate<MemoryStream>,
    issuer: AuthorityIssuer,
    scope: Scope,
    project: VolumeRef,
    source: VolumeRef,
    private: VolumeRef,
    source_generation: GenerationRef,
}

impl Fixture {
    fn controller(
        &self,
    ) -> Result<ParentProjectController<'_, MemoryAuthorityBackend, MemoryObjectBackend>> {
        ParentProjectController::new(
            &self.host,
            self.parent.reducer(),
            &self.issuer.verifier(&mut None)?,
            &self.scope,
            &self.project,
            None,
        )
    }

    fn grant(&self, operation: VolumeOperation) -> Result<ContentGrant> {
        ContentGrant::verify(
            &self.issuer.verifier(&mut None)?,
            &self.scope,
            &self.private,
            operation,
            u64::MAX,
            &mut None,
        )
    }

    async fn put(
        &self,
        path: &str,
        bytes: &[u8],
        media: &str,
        maximum: u64,
        key: &str,
    ) -> Result<FileRef> {
        self.host
            .put_content(
                &self.private,
                &self.grant(VolumeOperation::Write)?,
                path,
                bytes,
                media,
                path.rsplit('/').next().unwrap(),
                maximum,
                &IdempotencyKey::new(key)?,
                None,
                None,
            )
            .await
    }
}

async fn fixture() -> Result<Fixture> {
    let provider = ProviderRef::new(
        "recursive-default-test".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let agent = AgentId::from_bytes([1; 16]);
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "recursive-default-parent".into(),
    };
    let project = VolumeRef::new(
        provider.clone(),
        "recursive-default-project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("parent".into()),
        &mut None,
    )?;
    let source = VolumeRef::new(
        provider.clone(),
        "recursive-default-source".into(),
        VolumeClass::Project,
        VolumeOwner::Project("child".into()),
        &mut None,
    )?;
    let private = VolumeRef::new(
        provider,
        "recursive-default-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&project).await?;
    host.create_volume(&private).await?;
    let issuer = AuthorityIssuer::new("recursive-default-owner", [4; 32], authority.clone());
    let mut grants = vec![
        "conversation:bind".to_owned(),
        "conversation:append".to_owned(),
        "fork:publish".to_owned(),
        "project:merge".to_owned(),
    ];
    for volume in [&project, &source, &private] {
        grants.push(volume.capability(VolumeOperation::Read, &mut None)?);
        grants.push(volume.capability(VolumeOperation::Write, &mut None)?);
    }
    let scope = issuer
        .issue(
            "parent-owner",
            &Capabilities::new(grants),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let stream = Arc::new(MemoryStream::default());
    let mut parent = StreamAggregate::open(
        &stream,
        authority,
        issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    parent
        .execute(
            Command {
                operation_id: OperationId::from_bytes([2; 16]),
                idempotency_key: IdempotencyKey::new("bind-parent")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let project_ref = workspace_ref(
        project.provider().clone(),
        &project.storage_name(&mut None)?,
    )?;
    let base = host
        .apply(
            &project_ref,
            None,
            &[WorkspaceMutation::PutFile {
                path: "/value.txt".into(),
                bytes: b"base".to_vec().into(),
            }],
            &IdempotencyKey::new("base-value")?,
        )
        .await?;
    let controller = ParentProjectController::new(
        host.as_ref(),
        parent.reducer(),
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &project,
        None,
    )?;
    controller
        .fork_project(&base, &source, &IdempotencyKey::new("fork-source")?)
        .await?;
    let source_ref = workspace_ref(source.provider().clone(), &source.storage_name(&mut None)?)?;
    let source_generation = host
        .apply(
            &source_ref,
            None,
            &[WorkspaceMutation::PutFile {
                path: "/value.txt".into(),
                bytes: b"child".to_vec().into(),
            }],
            &IdempotencyKey::new("source-value")?,
        )
        .await?;
    Ok(Fixture {
        host,
        parent,
        issuer,
        scope,
        project,
        source,
        private,
        source_generation,
    })
}

#[tokio::test]
async fn restored_merge_keeps_the_original_target_and_publication() -> Result<()> {
    for applied in [false, true] {
        let fixture = fixture().await?;
        let controller = fixture.controller()?;
        let inspected = controller
            .prepare_project_import(&fixture.source, &fixture.source_generation, None)
            .await?;
        let target = fixture.host.generation_ref_id(inspected.target_head())?;
        let project = workspace_ref(
            fixture.project.provider().clone(),
            &fixture.project.storage_name(&mut None)?,
        )?;
        let effect = OperationId::from_bytes([21; 16]);
        let expected = if applied {
            let JoinOutcome::Applied(first) = controller
                .apply_project_merge(&inspected, effect, BTreeMap::new())
                .await?
                .0
            else {
                panic!("expected initial publication");
            };
            fixture.host.generation_ref_id(first.generation.id())?
        } else {
            fixture
                .host
                .apply(
                    &project,
                    Some(&target),
                    &[WorkspaceMutation::PutFile {
                        path: "/value.txt".into(),
                        bytes: b"concurrent user".to_vec().into(),
                    }],
                    &IdempotencyKey::new("concurrent-user-edit")?,
                )
                .await?
        };
        let restored = controller
            .prepare_project_import(&fixture.source, &fixture.source_generation, Some(&target))
            .await?;
        assert_eq!(restored.target_head(), inspected.target_head());
        let retried = controller
            .apply_project_merge(&restored, effect, BTreeMap::new())
            .await?
            .0;
        if applied {
            let JoinOutcome::AlreadyApplied(retried) = retried else {
                panic!("retry must observe original publication");
            };
            assert_eq!(
                fixture.host.generation_ref_id(retried.generation.id())?,
                expected
            );
        } else {
            assert!(matches!(retried, JoinOutcome::StaleTarget(_)));
            assert_eq!(
                fixture
                    .host
                    .read(&project, Some(&expected), "/value.txt", 64)
                    .await?
                    .as_ref(),
                b"concurrent user"
            );
        }
        assert_eq!(fixture.host.resolve(&project).await?.generation, expected);
    }
    Ok(())
}

#[tokio::test]
async fn restored_plan_requires_original_signed_source_read_grant() -> Result<()> {
    let fixture = fixture().await?;
    let caller_only = fixture
        .issuer
        .issue(
            "destination-only",
            &Capabilities::new([
                "project:merge".to_owned(),
                fixture
                    .project
                    .capability(VolumeOperation::Write, &mut None)?,
            ]),
            Some(AgentId::from_bytes([1; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    let controller = ParentProjectController::new(
        fixture.host.as_ref(),
        fixture.parent.reducer(),
        &fixture.issuer.verifier(&mut None).expect("verifier copy"),
        &caller_only,
        &fixture.project,
        None,
    )?;
    let project = workspace_ref(
        fixture.project.provider().clone(),
        &fixture.project.storage_name(&mut None)?,
    )?;
    let target = fixture.host.resolve(&project).await?.generation;
    assert!(matches!(
        controller
            .prepare_project_import(&fixture.source, &fixture.source_generation, Some(&target))
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(fixture.host.resolve(&project).await?.generation, target);
    Ok(())
}

#[tokio::test]
async fn exact_edit_uses_source_generation_and_rejects_ambiguous_overlap() -> Result<()> {
    let fixture = fixture().await?;
    let read = fixture.grant(VolumeOperation::Read)?;
    let write = fixture.grant(VolumeOperation::Write)?;
    let source = fixture
        .put(
            "scratch/value.txt",
            b"aaa",
            "text/plain",
            64,
            "initial-edit-source",
        )
        .await?;
    assert!(
        fixture
            .host
            .edit_text(
                &source,
                &read,
                &write,
                &ExactTextReplacement {
                    old_text: "aa".into(),
                    new_text: "b".into()
                },
                64,
                &IdempotencyKey::new("ambiguous-edit")?
            )
            .await
            .is_err()
    );
    let changed = fixture
        .put("scratch/value.txt", b"user", "text/plain", 64, "user-edit")
        .await?;
    assert!(matches!(
        fixture
            .host
            .edit_text(
                &source,
                &read,
                &write,
                &ExactTextReplacement {
                    old_text: "aaa".into(),
                    new_text: "child".into()
                },
                64,
                &IdempotencyKey::new("stale-exact-edit")?
            )
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        fixture
            .host
            .read_content(&changed, &read, 64, None)
            .await?
            .as_ref(),
        b"user"
    );
    assert_eq!(
        fixture
            .host
            .read_content(&source, &read, 64, None)
            .await?
            .as_ref(),
        b"aaa"
    );
    Ok(())
}

async fn merge_plan(
    fixture: &Fixture,
    effect: OperationId,
) -> Result<(RecursiveMergeToolPlan, FileRef)> {
    let file = fixture
        .put(
            "merge/notice.txt",
            b"explicit project import",
            "text/plain",
            4096,
            "merge-notice",
        )
        .await?;
    let controller = fixture.controller()?;
    let inspected = controller
        .prepare_project_import(&fixture.source, &fixture.source_generation, None)
        .await?;
    let plan = RecursiveMergeToolPlan {
        arguments: MergeToolArguments {
            source: TaskId::from_bytes([9; 16]),
            selections: Vec::new(),
        },
        caller: TaskId::from_bytes([8; 16]),
        operation_id: effect,
        destination: fixture.parent.reducer().authority().clone(),
        source: Authority {
            kind: AggregateKind::Conversation,
            id: "recursive-default-child".into(),
        },
        source_project: fixture.source.clone(),
        source_generation: fixture.source_generation.clone(),
        target_project: fixture.project.clone(),
        expected_target_generation: fixture.host.generation_ref_id(inspected.target_head())?,
        authorized_source: None,
        notice: ConversationMessage {
            id: uuid::Uuid::from_bytes([30; 16]),
            sequence: 1,
            kind: MessageKind::Merge,
            content: file,
            attachments: crate::conversation::ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        },
    };
    let command_payload = plan.notice.content.clone();
    Ok((plan, command_payload))
}

#[tokio::test]
async fn merge_rejects_foreign_receipts_and_success_shaped_nonoutcomes() -> Result<()> {
    let fixture = fixture().await?;
    let effect = OperationId::from_bytes([31; 16]);
    let (plan, _) = merge_plan(&fixture, effect).await?;
    assert!(merge_result(&plan, &json!({"status":"success"}), None).is_err());
    assert!(merge_result(&plan, &json!({"status":"no_changes"}), None)?.is_none());
    let controller = fixture.controller()?;
    let inspected = controller
        .prepare_project_import(
            &plan.source_project,
            &plan.source_generation,
            Some(&plan.expected_target_generation),
        )
        .await?;
    let outcome = controller
        .apply_project_merge(&inspected, effect, BTreeMap::new())
        .await?
        .0;
    let receipt = controller
        .merge_receipt(&inspected, &outcome, &plan.source, effect, &plan.notice)?
        .0;
    let mut foreign = receipt.clone();
    foreign.operation_id = OperationId::from_bytes([98; 16]);
    assert!(merge_result(&plan, &json!({"status":"applied","receipt":foreign}), None).is_err());
    assert_eq!(
        merge_result(&plan, &json!({"status":"applied","receipt":receipt}), None)?,
        Some(receipt)
    );
    Ok(())
}

#[test]
fn fork_initialization_retains_raw_child_message_and_independent_source_choice() -> Result<()> {
    let arguments = ForkToolArguments {
        id: AgentId::from_bytes([40; 16]),
        msg: "exact é\0🦀\r\nchild task".into(),
        source: Some(TaskId::from_bytes([41; 16])),
    };
    let value = encode(&arguments, None)?;
    recursive_byte_bound(&value, 4096, None)?;
    let retained: ForkToolArguments = decode(&value, None)?;
    assert_eq!(encode(&retained, None)?, value);
    assert_eq!(
        arguments.source_task(
            TaskId::from_bytes([42; 16]),
            crate::conversation::Limits::default(),
            &mut None
        )?,
        TaskId::from_bytes([41; 16])
    );
    let mut oversized = arguments;
    oversized.msg = "x".repeat(4097);
    assert!(recursive_byte_bound(&encode(&oversized, None)?, 4096, None).is_err());
    Ok(())
}

#[tokio::test]
async fn actual_registry_keeps_shared_file_tools_and_refuses_contextless_recursive_execution()
-> Result<()> {
    let storage = super::super::MemoryHarnessStorage::new(AgentId::new(), 65536).await?;
    let ordinary = storage.default_tools(
        crate::conversation::Limits {
            file_bytes: 65536,
            ..Default::default()
        },
        None,
    )?;
    let registry =
        recursive_default_tools::<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>(
            None, 65536,
        )?;
    for expected in [
        "acyclic.read_file",
        "acyclic.write_file",
        "acyclic.edit_file",
        "acyclic.fork",
        "acyclic.merge",
    ] {
        assert!(registry.get(expected, None, None)?.is_some());
        assert_eq!(
            ordinary.get(expected, None, None)?.unwrap().definition,
            registry.get(expected, None, None)?.unwrap().definition
        );
    }
    for name in ["acyclic.fork", "acyclic.merge"] {
        let tool = registry
            .get(name, None, None)?
            .ok_or_else(|| Error::NotFound(name.into()))?;
        let invocation = ToolInvocation {
            operation_id: OperationId::from_bytes([50; 16]),
            call_id: "exact-call".into(),
            name: name.into(),
            arguments: json!({}),
        };
        assert!(tool.executor.authorize(None, &invocation).is_err());
        assert!(
            tool.executor
                .execute(invocation.clone(), None)
                .await
                .is_err()
        );
        assert!(tool.executor.reconcile(invocation, None).await.is_err());
    }
    assert!(registry.get("bash", None, None)?.is_none());
    Ok(())
}

#[tokio::test]
async fn dropped_invocation_guard_rejects_the_next_real_filesystem_request_before_work()
-> Result<()> {
    let fixture = fixture().await?;
    let read = fixture.grant(VolumeOperation::Read)?;
    let file = fixture
        .put(
            "cancel/original.txt",
            b"original",
            "text/plain",
            64,
            "cancel-original",
        )
        .await?;
    let cancellation = CancellationToken::new();
    let guard = InvocationCancellation(cancellation.clone());
    let budget = acyclic_fs::performance::work_budget(4096, 1 << 20);
    let io = crate::filesystem::PreparationIo::from_budget(budget, &cancellation)?;
    let live = fixture
        .host
        .read_content(&file, &read, 64, Some(&io))
        .await?;
    assert_eq!(live.as_ref(), b"original");
    let io = crate::filesystem::PreparationIo::from_budget(budget, &cancellation)?;
    drop(guard);
    fixture
        .host
        .read_content(&file, &read, 64, Some(&io))
        .await
        .expect_err("actual invoking token must reject a new backend request");
    assert_eq!(io.receipt(), acyclic_fs::WorkCounters::default());
    assert!(cancellation.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn inherited_grant_preflight_reuses_real_materializer_without_child_effects() -> Result<()> {
    let mut fixture = fixture().await?;
    let write = fixture.grant(VolumeOperation::Write)?;
    let content = fixture
        .put(
            "history/content.txt",
            b"original caller history",
            "text/plain",
            8192,
            "history-content",
        )
        .await?;
    let attachment = fixture
        .put(
            "history/attachment.bin",
            b"immutable attachment",
            "application/octet-stream",
            8192,
            "history-attachment",
        )
        .await?;
    let attachments = fixture
        .host
        .put_attachment_manifest(
            &fixture.private,
            &write,
            "history/attachments.json",
            &[Attachment {
                file: attachment.clone(),
                label: Some("original".into()),
            }],
            8192,
            &IdempotencyKey::new("history-manifest")?,
        )
        .await?;
    let manifest = match &attachments {
        ReferencedAttachments::Manifest { manifest, .. } => manifest.clone(),
        _ => {
            return Err(Error::Invalid(
                "real manifest publisher did not return a manifest".into(),
            ));
        }
    };
    let reader = FilesystemContentVerifier::new(
        fixture.host.clone(),
        &fixture.issuer.verifier(&mut None).expect("verifier copy"),
        &fixture.scope,
        8192,
        None,
    )?;
    fixture.parent = fixture.parent.with_content_verifier(reader.clone());
    fixture
        .parent
        .execute(
            Command {
                operation_id: OperationId::from_bytes([60; 16]),
                idempotency_key: IdempotencyKey::new("history-message")?,
                expected_revision: fixture.parent.reducer().revision(),
                scope: fixture.scope.clone(),
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: uuid::Uuid::from_bytes([60; 16]),
                        sequence: 1,
                        kind: MessageKind::User,
                        content: content.clone(),
                        attachments,
                        reply_to: None,
                        tool_call_id: None,
                        extensions: BTreeMap::new(),
                    }),
                },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let child_agent = AgentId::from_bytes([61; 16]);
    let child = VolumeRef::new(
        fixture.private.provider().clone(),
        "preflight-child".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(child_agent),
        &mut None,
    )?;
    let child_workspace = workspace_ref(child.provider().clone(), &child.storage_name(&mut None)?)?;
    let controller = fixture.controller()?;
    assert!(fixture.host.resolve(&child_workspace).await.is_err());
    let projected = controller
        .inherited_reference_grants(
            fixture.parent.reducer(),
            child_agent,
            &[],
            1,
            8,
            8192,
            16,
            reader.as_ref(),
            None,
        )
        .await?
        .0;
    assert!(fixture.host.resolve(&child_workspace).await.is_err());
    assert_eq!(projected.len(), 3);
    let member = projected
        .iter()
        .find(|grant| grant.file == attachment)
        .ok_or_else(|| Error::NotFound("canonical attachment-member grant".into()))?;
    assert_eq!(member.attachment_manifest.as_ref(), Some(&manifest));
    assert!(projected.iter().all(|grant| grant.reader == child_agent));
    let donor = Capabilities::new([manifest.read_capability(&mut None)?]);
    crate::filesystem::fork_preparer::require_reference_donor(&donor, member, None)?;
    let direct = projected
        .iter()
        .find(|grant| grant.file == content)
        .ok_or_else(|| Error::NotFound("canonical content grant".into()))?;
    assert!(
        crate::filesystem::fork_preparer::require_reference_donor(&donor, direct, None).is_err()
    );
    for (through, bytes, references) in [(1, 8192, 2), (1, 1, 16), (2, 8192, 16)] {
        assert!(
            controller
                .inherited_reference_grants(
                    fixture.parent.reducer(),
                    child_agent,
                    &[],
                    through,
                    8,
                    bytes,
                    references,
                    reader.as_ref(),
                    None,
                )
                .await
                .is_err()
        );
    }
    let private = fixture.host.create_volume(&child).await?;
    let actual = controller
        .materialize_inherited_conversation(
            fixture.parent.reducer(),
            &child,
            &private.generation,
            child_agent,
            &[],
            1,
            8,
            8192,
            16,
            reader.as_ref(),
            &IdempotencyKey::new("real-inherited-prefix")?,
            &[],
            None,
            None,
            None,
            #[cfg(not(target_arch = "wasm32"))]
            None,
        )
        .await?;
    assert_eq!(actual.reference_grants, projected);
    Ok(())
}

#[tokio::test]
async fn durable_source_pin_keeps_original_cut_when_real_parent_history_advances() -> Result<()> {
    let mut fixture = fixture().await?;
    let caller = TaskId::from_bytes([62; 16]);
    let child_agent = AgentId::from_bytes([63; 16]);
    let effect = OperationId::from_bytes([64; 16]);
    let arguments = ForkToolArguments {
        id: child_agent,
        msg: "sealed original é\0🦀\r\n".into(),
        source: None,
    };
    let value = encode(&arguments, None)?;
    let project = workspace_ref(
        fixture.project.provider().clone(),
        &fixture.project.storage_name(&mut None)?,
    )?;
    let private = workspace_ref(
        fixture.private.provider().clone(),
        &fixture.private.storage_name(&mut None)?,
    )?;
    let stream_provider = ProviderRef::new(
        "recursive-default-test".into(),
        "stream".into(),
        "1".into(),
        &mut None,
    )?;
    let request = super::super::recursive::fork_request(
        fixture.parent.reducer(),
        effect,
        &arguments,
        (
            caller,
            &fixture.project,
            &fixture.host.resolve(&project).await?.generation,
            &fixture.private,
            &fixture.host.resolve(&private).await?.generation,
        ),
        super::super::RecursiveForkConfig {
            child: Authority {
                kind: AggregateKind::Conversation,
                id: "new-source-pin-child".into(),
            },
            child_project: VolumeRef::new(
                fixture.project.provider().clone(),
                "new-source-pin-project".into(),
                VolumeClass::Project,
                VolumeOwner::Project("new-source-pin-child".into()),
                &mut None,
            )?,
            child_private: VolumeRef::new(
                fixture.private.provider().clone(),
                "new-source-pin-private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(child_agent),
                &mut None,
            )?,
            child_result_volume: None,
            limits: crate::conversation::Limits::default(),
            inherited_messages: 8,
            inherited_bytes: 8192,
            inherited_references: 16,
            history_limits: crate::store::HistoryReadLimits {
                maximum_events: 8,
                maximum_bytes: 8192,
            },
        },
        &stream_provider,
        None,
        None,
    )?;
    let intent = RecursiveForkSourceIntent {
        arguments: arguments.clone(),
        caller,
        source_task: caller,
        child_operation_id: OperationId::from_bytes([65; 16]),
        allocation_digest: [66; 32],
        request,
    };
    let file = fixture
        .put(
            "source/source-checkpoint.json",
            &crate::contract::canonical_json_bytes(&intent)?,
            "application/json",
            8192,
            "source-checkpoint",
        )
        .await?;
    let original_file = file.clone();
    fixture.parent = fixture
        .parent
        .with_content_verifier(FilesystemContentVerifier::new(
            fixture.host.clone(),
            &fixture.issuer.verifier(&mut None).expect("verifier copy"),
            &fixture.scope,
            8192,
            None,
        )?);
    fixture
        .parent
        .execute(
            Command {
                operation_id: OperationId::from_bytes([67; 16]),
                idempotency_key: IdempotencyKey::new("later-parent-history")?,
                expected_revision: fixture.parent.reducer().revision(),
                scope: fixture.scope.clone(),
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(ConversationMessage {
                        id: uuid::Uuid::from_bytes([67; 16]),
                        sequence: 1,
                        kind: MessageKind::User,
                        content: file,
                        attachments: crate::conversation::ReferencedAttachments::Inline {
                            items: Vec::new(),
                        },
                        reply_to: None,
                        tool_call_id: None,
                        extensions: BTreeMap::new(),
                    }),
                },
            },
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
    let bytes = fixture
        .host
        .read_content(
            &original_file,
            &fixture.grant(VolumeOperation::Read)?,
            8192,
            None,
        )
        .await?;
    let original: RecursiveForkSourceIntent = recursive_decode_bytes(&bytes, None)?;
    assert_eq!(original, intent);
    assert_eq!(encode(&original.arguments, None)?, value);
    assert!(original.request.parent_revision < fixture.parent.reducer().revision());
    Ok(())
}
