use super::*;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one real content-provider scenario exercises scoped tool execution, replay and authority controls"
)]
async fn portable_tool_context_preserves_owner_authority_and_pinned_retry() -> Result<()> {
    use acyclic_harness::{
        filesystem::{FilesystemContentPublisher, FilesystemContentVerifier},
        runtime::ToolContext,
        tool::{ToolInvocation, files},
    };
    use serde_json::json;
    let provider = ProviderRef::new(
        "tool-context".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let agent = AgentId::new();
    let volume = VolumeRef::new(
        provider,
        "workspace".into(),
        VolumeClass::Project,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "tool-context",
        [11; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "tool-context".into(),
        },
    );
    let read_cap = volume.capability(VolumeOperation::Read, &mut None)?;
    let write_cap = volume.capability(VolumeOperation::Write, &mut None)?;
    let scope = issuer
        .issue(
            "owner",
            &Capabilities::new([read_cap.clone(), write_cap.clone()]),
            Some(agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let writer = FilesystemContentPublisher::new(
        host.clone(),
        &volume,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        4096,
        None,
    )?;
    let reader = FilesystemContentVerifier::new(
        host,
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        4096,
        None,
    )?;
    let source = writer
        .stage(
            OperationId::new(),
            "source.txt",
            b"one two",
            "text/plain",
            "source.txt",
            None,
            None,
        )?
        .await?
        .0;
    let task = content_context(
        reader.clone(),
        Some(writer.clone()),
        Capabilities::new([read_cap.clone(), write_cap.clone()]),
    )
    .await?;
    let read_tool = files::read()?;
    let read_operation = OperationId::new();
    let read_invocation = ToolInvocation {
        operation_id: read_operation,
        call_id: "read".into(),
        name: read_tool.definition.name.clone(),
        arguments: json!({"file":source}),
    };
    assert_eq!(
        read_tool
            .executor
            .execute(
                read_invocation.clone(),
                Some(ToolContext::new(
                    task.clone(),
                    read_operation,
                    "read",
                    None
                )?)
            )
            .await?
            .value,
        json!("one two")
    );
    for wrong_context in [
        ToolContext::new(task.clone(), OperationId::new(), "read", None)?,
        ToolContext::new(task.clone(), read_operation, "another-call", None)?,
    ] {
        assert!(matches!(
            read_tool
                .executor
                .execute(read_invocation.clone(), Some(wrong_context))
                .await,
            Err(Error::Unauthorized(_))
        ));
    }
    let mut tool = files::edit()?;
    tool.definition.name = "example.exact_edit".into();
    tool.definition.description =
        "Consumer-defined edit contract using the portable executor".into();
    tool.definition.validate(&mut None)?;
    let operation = OperationId::new();
    let invocation = ToolInvocation {
        operation_id: operation,
        call_id: "edit".into(),
        name: tool.definition.name.clone(),
        arguments: json!({"file":source,"old_text":"two","new_text":"three"}),
    };
    let context = ToolContext::new(task.clone(), operation, "edit", None)?;
    let first = tool
        .executor
        .execute(invocation.clone(), Some(context.clone()))
        .await?;
    let edited: FileRef = serde_json::from_value(
        first
            .value
            .get("file")
            .ok_or_else(|| Error::Invalid("tool result lacks its file reference".into()))?
            .clone(),
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        task.read_file(&edited, None).await?.as_ref(),
        b"one three".as_ref()
    );
    assert_eq!(
        task.read_file(&source, None).await?.as_ref(),
        b"one two".as_ref()
    );
    let user = writer
        .stage(
            OperationId::new(),
            "source.txt",
            b"user edit",
            "text/plain",
            "source.txt",
            None,
            None,
        )?
        .await?
        .0;
    assert_eq!(
        tool.executor
            .reconcile(invocation.clone(), Some(context))
            .await?,
        Some(first)
    );
    assert_eq!(
        task.read_file(&user, None).await?.as_ref(),
        b"user edit".as_ref()
    );
    let write_tool = files::write()?;
    let write_operation = OperationId::new();
    let write_invocation = ToolInvocation {
        operation_id: write_operation,
        call_id: "write".into(),
        name: write_tool.definition.name.clone(),
        arguments: json!({"path":"second.txt","text":"exact \u{e9}\r\n",
            "media_type":"text/plain","display_name":"second.txt"}),
    };
    let write_context = ToolContext::new(task.clone(), write_operation, "write", None)?;
    let written = write_tool
        .executor
        .execute(write_invocation.clone(), Some(write_context.clone()))
        .await?;
    let written_ref: FileRef = serde_json::from_value(
        written
            .value
            .get("file")
            .ok_or_else(|| Error::Invalid("write result lacks its file reference".into()))?
            .clone(),
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        task.read_file(&written_ref, None).await?.as_ref(),
        "exact \u{e9}\r\n".as_bytes()
    );
    assert_eq!(
        write_tool
            .executor
            .reconcile(write_invocation, Some(write_context))
            .await?,
        Some(written)
    );
    let patch_tool = files::patch_file(8192, 8)?;
    assert_ne!(
        patch_tool.definition.digest()?,
        files::patch_file(8193, 8)?.definition.digest()?
    );
    let patch_operation = OperationId::new();
    let patch_invocation = ToolInvocation {
        operation_id: patch_operation,
        call_id: "patch".into(),
        name: patch_tool.definition.name.clone(),
        arguments: json!({"file":written_ref,"diff":"@@\n-exact \u{e9}\n+patched\n"}),
    };
    let patch_context = ToolContext::new(task.clone(), patch_operation, "patch", None)?;
    let patched = patch_tool
        .executor
        .execute(patch_invocation.clone(), Some(patch_context.clone()))
        .await?;
    let patched_ref: FileRef = serde_json::from_value(
        patched
            .value
            .get("file")
            .ok_or_else(|| Error::Invalid("patch result lacks file".into()))?
            .clone(),
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        task.read_file(&patched_ref, None).await?.as_ref(),
        b"patched\r\n".as_ref()
    );
    let later = writer
        .stage(
            OperationId::new(),
            "second.txt",
            b"later user edit",
            "text/plain",
            "second.txt",
            None,
            None,
        )?
        .await?
        .0;
    assert_eq!(
        patch_tool
            .executor
            .reconcile(patch_invocation.clone(), Some(patch_context.clone()))
            .await?,
        Some(patched)
    );
    assert_eq!(
        task.read_file(&later, None).await?.as_ref(),
        b"later user edit".as_ref()
    );
    let mut changed_patch = patch_invocation.clone();
    changed_patch.arguments = json!({"file":written_ref,"diff":"@@\n-exact \u{e9}\n+different\n"});
    assert!(matches!(
        patch_tool
            .executor
            .reconcile(changed_patch, Some(patch_context))
            .await,
        Err(Error::Conflict(_))
    ));
    let stale_patch_operation = OperationId::new();
    assert!(matches!(
        patch_tool
            .executor
            .execute(
                ToolInvocation {
                    operation_id: stale_patch_operation,
                    ..patch_invocation
                },
                Some(ToolContext::new(
                    task.clone(),
                    stale_patch_operation,
                    "patch",
                    None
                )?)
            )
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        task.read_file(&later, None).await?.as_ref(),
        b"later user edit".as_ref()
    );
    assert_eq!(
        task.read_file(&user, None).await?.as_ref(),
        b"user edit".as_ref()
    );
    assert_eq!(
        read_tool
            .executor
            .reconcile(
                read_invocation,
                Some(ToolContext::new(
                    task.clone(),
                    read_operation,
                    "read",
                    None
                )?)
            )
            .await?
            .ok_or_else(|| Error::Invalid("pinned read was not reconciled".into()))?
            .value,
        json!("one two")
    );
    let fresh = OperationId::new();
    let stale = ToolInvocation {
        operation_id: fresh,
        ..invocation.clone()
    };
    assert!(matches!(
        tool.executor
            .execute(
                stale,
                Some(ToolContext::new(task.clone(), fresh, "edit", None)?)
            )
            .await,
        Err(Error::Conflict(_))
    ));
    for (writer_binding, grants) in [
        (
            None,
            Capabilities::new([read_cap.clone(), write_cap.clone()]),
        ),
        (Some(writer.clone()), Capabilities::new([write_cap.clone()])),
        (Some(writer.clone()), Capabilities::new([read_cap.clone()])),
    ] {
        let restricted = content_context(
            reader.clone(),
            writer_binding.map(|writer| writer as Arc<dyn ContentPublisher>),
            grants,
        )
        .await?;
        assert!(matches!(
            tool.executor
                .execute(
                    invocation.clone(),
                    Some(ToolContext::new(restricted, operation, "edit", None)?)
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
    }
    assert_eq!(
        task.read_file(&user, None).await?.as_ref(),
        b"user edit".as_ref()
    );
    let ordinary = Arc::new(OrdinaryPublisher {
        volume: volume.clone(),
        calls: AtomicUsize::new(0),
    });
    let write_context = content_context(
        reader.clone(),
        Some(ordinary.clone()),
        Capabilities::new([write_cap]),
    )
    .await?;
    let write_tool = files::write()?;
    for path in ["../escape.txt", ".system/private.json"] {
        let op = OperationId::new();
        let invalid = ToolInvocation {
            operation_id: op,
            call_id: "invalid-path".into(),
            name: write_tool.definition.name.clone(),
            arguments: json!({"path":path,"text":"secret","media_type":"text/plain","display_name":"text"}),
        };
        assert!(matches!(
            write_tool
                .executor
                .execute(
                    invalid,
                    Some(ToolContext::new(
                        write_context.clone(),
                        op,
                        "invalid-path",
                        None
                    )?)
                )
                .await,
            Err(Error::Invalid(_)) | Err(Error::Unauthorized(_))
        ));
    }
    assert_eq!(ordinary.calls.load(Ordering::SeqCst), 0);
    let foreign_volume = VolumeRef::new(
        volume.provider().clone(),
        "foreign".into(),
        VolumeClass::Project,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    let foreign = Arc::new(OrdinaryPublisher {
        volume: foreign_volume.clone(),
        calls: AtomicUsize::new(0),
    });
    let foreign_context = content_context(
        reader,
        Some(foreign.clone()),
        Capabilities::new([
            read_cap,
            foreign_volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
    )
    .await?;
    assert!(matches!(
        tool.executor
            .execute(
                invocation.clone(),
                Some(ToolContext::new(foreign_context, operation, "edit", None)?)
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(foreign.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        tool.executor.execute(invocation.clone(), None).await,
        Err(Error::Unsupported(_))
    ));
    let mut changed = invocation;
    *changed
        .arguments
        .get_mut("new_text")
        .ok_or_else(|| Error::Invalid("edit input lacks its replacement".into()))? =
        json!("different");
    assert!(matches!(
        tool.executor
            .reconcile(
                changed,
                Some(ToolContext::new(task, operation, "edit", None)?)
            )
            .await,
        Err(Error::Conflict(_))
    ));
    Ok(())
}
