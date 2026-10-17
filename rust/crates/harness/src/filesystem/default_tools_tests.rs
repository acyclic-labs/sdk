use super::*;
use crate::conversation::ContentGrant;
use crate::{
    AgentId, Capabilities,
    conversation::{MessageKind, VolumeClass, VolumeOwner},
    core::{AggregateKind, AuthorityIssuer, SchemaRegistry},
    filesystem::{WorkspaceMutation, workspace_ref},
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
        (
            authority,
            issuer.verifier(&mut None).expect("verifier copy"),
            SchemaRegistry::new(),
        ),
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

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn merge_rejects_foreign_receipts_and_success_shaped_nonoutcomes() -> Result<()> {
    let fixture = fixture().await?;
    let effect = OperationId::from_bytes([31; 16]);
    let file = fixture
        .host
        .put_content(
            &fixture.private,
            &fixture.grant(VolumeOperation::Write)?,
            "merge/notice.txt",
            b"explicit project import",
            "text/plain",
            "notice.txt",
            4096,
            &IdempotencyKey::new("merge-notice")?,
            None,
            None,
            None,
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
        expected_target_generation: fixture
            .host
            .generation_ref_id(inspected.target_head(), None)?,
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
    assert!(merge_result(&plan, &json!({"status":"success"}), None).is_err());
    assert!(merge_result(&plan, &json!({"status":"no_changes"}), None)?.is_none());
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

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn fork_initialization_retains_raw_child_message_and_independent_source_choice() -> Result<()> {
    let arguments = ForkToolArguments {
        id: AgentId::from_bytes([40; 16]),
        msg: "exact Ã©\0ðŸ¦€\r\nchild task".into(),
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

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn actual_registry_keeps_shared_file_tools_and_refuses_contextless_recursive_execution()
-> Result<()> {
    let ordinary = crate::tools::defaults()?;
    let mut registry =
        recursive_tools::<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>(None, 65536)?;
    registry.extend(crate::tools::defaults()?)?;
    for expected in [
        "bash",
        "fs",
        "read",
        "write",
        "edit",
        "acyclic.fork",
        "acyclic.merge",
    ] {
        assert!(registry.get(expected, None, None)?.is_some());
        if let Some(tool) = ordinary.get(expected, None, None)? {
            assert_eq!(
                tool.definition,
                registry.get(expected, None, None)?.unwrap().definition
            );
        }
    }
    for name in ["bash", "fs", "acyclic.fork", "acyclic.merge"] {
        let tool = registry
            .get(name, None, None)?
            .ok_or_else(|| Error::NotFound(name.into()))?;
        let invocation = ToolInvocation {
            operation_id: OperationId::from_bytes([50; 16]),
            call_id: "exact-call".into(),
            name: name.into(),
            arguments: json!({}),
        };
        assert!(
            tool.executor
                .authorize(None, None, &invocation, None)
                .is_err()
        );
        assert!(
            tool.executor
                .execute(invocation.clone(), None)
                .await
                .is_err()
        );
        assert!(tool.executor.reconcile(invocation, None).await.is_err());
    }
    Ok(())
}
