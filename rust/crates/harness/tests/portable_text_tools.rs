//! Portable text effects use owner grants, pinned generations and retained receipts.
#![cfg(feature = "filesystem")]

use acyclic_fs::Fs;
use acyclic_harness::{
    AgentId, Capabilities, Error, IdempotencyKey, OperationId, Result,
    conversation::{
        ContentGrant, ContentPublisher, FileRef, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityIssuer},
    filesystem::{ExactTextReplacement, FilesystemHost, workspace_ref},
    resources::ProviderRef,
};
use acyclic_stream::BoxProviderFuture;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct ResumedScope(acyclic_harness::runtime::RuntimeScope);

impl acyclic_harness::runtime::TaskStateProvider for ResumedScope {
    fn policy_identity(&self) -> Option<acyclic_harness::registry::ComponentIdentity> {
        None
    }
    fn observe_admission<'a>(
        &'a self,
        _: acyclic_harness::TaskId,
    ) -> BoxProviderFuture<'a, Result<acyclic_harness::runtime::TaskAdmissionRecord>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "fixture tests content authority, not task admission".into(),
            ))
        })
    }
    fn resume_scope<'a>(
        &'a self,
        _: acyclic_harness::TaskId,
        _: OperationId,
    ) -> BoxProviderFuture<'a, Result<acyclic_harness::runtime::RuntimeScope>> {
        Box::pin(async move { Ok(self.0.clone()) })
    }
    fn outcome<'a>(
        &'a self,
        _: acyclic_harness::TaskId,
    ) -> BoxProviderFuture<'a, Result<Option<acyclic_harness::Outcome<serde_json::Value>>>> {
        Box::pin(async { Ok(None) })
    }
    fn cancel<'a>(&'a self, _: acyclic_harness::TaskId) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

async fn content_context(
    reader: Arc<dyn acyclic_harness::conversation::ContentResidencyVerifier>,
    writer: Option<Arc<dyn ContentPublisher>>,
    capabilities: Capabilities,
) -> Result<acyclic_harness::runtime::TaskContext> {
    use acyclic_harness::runtime::{Bindings, ContentBindings, RuntimeScope};
    let scope = RuntimeScope::new(
        capabilities,
        acyclic_harness::conversation::Limits::default(),
    )?;
    let mut bindings = Bindings::local();
    bindings.scope = scope.clone();
    bindings.state = Some(Arc::new(ResumedScope(scope)));
    bindings.content = Some(ContentBindings { reader, writer });
    bindings
        .build()?
        .durable_context(acyclic_harness::TaskId::new(), OperationId::new())
        .await
}

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
    let provider = ProviderRef::new("tool-context", "filesystem", "2")?;
    let host = Arc::new(FilesystemHost::new(Fs::memory(), provider.clone())?);
    let agent = AgentId::new();
    let volume = VolumeRef::new(
        provider,
        "workspace",
        VolumeClass::Project,
        VolumeOwner::Agent(agent),
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
    let read_cap = volume.capability(VolumeOperation::Read)?;
    let write_cap = volume.capability(VolumeOperation::Write)?;
    let scope = issuer.root_for_agent(
        agent,
        "owner",
        Capabilities::new([read_cap.clone(), write_cap.clone()]),
    );
    let writer = Arc::new(FilesystemContentPublisher::new(
        host.clone(),
        volume.clone(),
        &issuer.verifier(),
        &scope,
        4096,
    )?);
    let reader = Arc::new(FilesystemContentVerifier::new(
        host,
        issuer.verifier(),
        scope,
        4096,
    )?);
    let source = writer
        .stage(
            OperationId::new(),
            "source.txt",
            b"one two",
            "text/plain",
            "source.txt",
        )
        .await?;
    let task = content_context(
        reader.clone(),
        Some(writer.clone()),
        Capabilities::new([read_cap.clone(), write_cap.clone()]),
    )
    .await?;
    let read_tool = files::read_file()?;
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
            .execute_with_context(
                ToolContext::new(task.clone(), read_operation, "read")?,
                read_invocation.clone(),
            )
            .await?
            .value,
        json!("one two")
    );
    // A copied invocation cannot use a context for another operation or call.
    for wrong_context in [
        ToolContext::new(task.clone(), OperationId::new(), "read")?,
        ToolContext::new(task.clone(), read_operation, "another-call")?,
    ] {
        assert!(matches!(
            read_tool
                .executor
                .execute_with_context(wrong_context, read_invocation.clone())
                .await,
            Err(Error::Unauthorized(_))
        ));
    }
    let mut tool = files::edit_file()?;
    tool.definition.name = "example.exact_edit".into();
    tool.definition.description =
        "Consumer-defined edit contract using the portable executor".into();
    tool.definition.validate()?;
    let operation = OperationId::new();
    let invocation = ToolInvocation {
        operation_id: operation,
        call_id: "edit".into(),
        name: tool.definition.name.clone(),
        arguments: json!({"file":source,"old_text":"two","new_text":"three"}),
    };
    let context = ToolContext::new(task.clone(), operation, "edit")?;
    let first = tool
        .executor
        .execute_with_context(context.clone(), invocation.clone())
        .await?;
    let edited: FileRef = serde_json::from_value(
        first
            .value
            .get("file")
            .ok_or_else(|| Error::Invalid("tool result lacks its file reference".into()))?
            .clone(),
    )
    .map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(task.read_file(&edited).await?, b"one three");
    assert_eq!(task.read_file(&source).await?, b"one two");
    let user = writer
        .stage(
            OperationId::new(),
            "source.txt",
            b"user edit",
            "text/plain",
            "source.txt",
        )
        .await?;
    assert_eq!(
        tool.executor
            .reconcile_with_context(context, invocation.clone())
            .await?,
        Some(first)
    );
    assert_eq!(task.read_file(&user).await?, b"user edit");
    // The same portable composition also supports independent read and write
    // contracts; writing a second path preserves the edited path's user head.
    let write_tool = files::write_file()?;
    let write_operation = OperationId::new();
    let write_invocation = ToolInvocation {
        operation_id: write_operation,
        call_id: "write".into(),
        name: write_tool.definition.name.clone(),
        arguments: json!({"path":"second.txt","text":"exact \u{e9}\r\n",
            "media_type":"text/plain","display_name":"second.txt"}),
    };
    let write_context = ToolContext::new(task.clone(), write_operation, "write")?;
    let written = write_tool
        .executor
        .execute_with_context(write_context.clone(), write_invocation.clone())
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
        task.read_file(&written_ref).await?,
        "exact \u{e9}\r\n".as_bytes()
    );
    assert_eq!(
        write_tool
            .executor
            .reconcile_with_context(write_context, write_invocation)
            .await?,
        Some(written)
    );
    assert_eq!(task.read_file(&user).await?, b"user edit");
    assert_eq!(
        read_tool
            .executor
            .reconcile_with_context(
                ToolContext::new(task.clone(), read_operation, "read")?,
                read_invocation,
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
            .execute_with_context(ToolContext::new(task.clone(), fresh, "edit")?, stale)
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
                .execute_with_context(
                    ToolContext::new(restricted, operation, "edit")?,
                    invocation.clone()
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
    }
    assert_eq!(task.read_file(&user).await?, b"user edit");
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
    let write_tool = files::write_file()?;
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
                .execute_with_context(
                    ToolContext::new(write_context.clone(), op, "invalid-path")?,
                    invalid
                )
                .await,
            Err(Error::Invalid(_)) | Err(Error::Unauthorized(_))
        ));
    }
    assert_eq!(ordinary.calls.load(Ordering::SeqCst), 0);
    let foreign_volume = VolumeRef::new(
        volume.provider().clone(),
        "foreign",
        VolumeClass::Project,
        VolumeOwner::Agent(agent),
    )?;
    let foreign = Arc::new(OrdinaryPublisher {
        volume: foreign_volume.clone(),
        calls: AtomicUsize::new(0),
    });
    let foreign_context = content_context(
        reader,
        Some(foreign.clone()),
        Capabilities::new([read_cap, foreign_volume.capability(VolumeOperation::Write)?]),
    )
    .await?;
    assert!(matches!(
        tool.executor
            .execute_with_context(
                ToolContext::new(foreign_context, operation, "edit")?,
                invocation.clone()
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(foreign.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        tool.executor.execute(invocation.clone()).await,
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
            .reconcile_with_context(ToolContext::new(task, operation, "edit")?, changed)
            .await,
        Err(Error::Conflict(_))
    ));
    // Construction uses explicit runtime limits; this fixture does not qualify
    // the stock model loop or original task-admission authentication.
    Ok(())
}

struct OrdinaryPublisher {
    volume: VolumeRef,
    calls: AtomicUsize,
}

#[tokio::test]
async fn single_file_publication_never_falls_back_to_ordinary_staging() -> Result<()> {
    let ordinary = OrdinaryPublisher {
        volume: VolumeRef::new(
            ProviderRef::new("once", "filesystem", "2")?,
            "workspace",
            VolumeClass::Project,
            VolumeOwner::Agent(AgentId::new()),
        )?,
        calls: AtomicUsize::new(0),
    };
    assert!(matches!(
        ordinary
            .stage_once(
                OperationId::new(),
                "one.txt",
                b"one",
                "text/plain",
                "one.txt"
            )
            .await,
        Err(Error::Unsupported(_))
    ));
    assert_eq!(ordinary.calls.load(Ordering::SeqCst), 0);
    Ok(())
}

impl ContentPublisher for OrdinaryPublisher {
    fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    fn stage<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        _: &'a [u8],
        _: &'a str,
        _: &'a str,
    ) -> BoxProviderFuture<'a, Result<FileRef>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(Error::Storage(
                "ordinary publication must not be called".into(),
            ))
        })
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered scenario checks publication, exact retry, concurrent user edits and negative controls"
)]
async fn exact_edit_retains_pinned_result_and_rejects_stale_or_foreign_writes() -> Result<()> {
    let provider = ProviderRef::new("portable", "filesystem", "2")?;
    let filesystem = Fs::memory();
    let host = FilesystemHost::new(filesystem.clone(), provider.clone())?;
    let owner = AgentId::from_bytes([3; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "source",
        VolumeClass::Project,
        VolumeOwner::Agent(owner),
    )?;
    let other = VolumeRef::new(
        provider.clone(),
        "other",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
    )?;
    host.create_volume(&volume).await?;
    host.create_volume(&other).await?;
    let issuer = AuthorityIssuer::new(
        "portable",
        [4; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "portable".into(),
        },
    );
    let scope = issuer.root_for_agent(
        owner,
        "owner",
        Capabilities::new([
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
            other.capability(VolumeOperation::Write)?,
        ]),
    );
    let read = ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Read)?;
    let write = ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Write)?;
    let foreign = ContentGrant::verify(&issuer.verifier(), &scope, &other, VolumeOperation::Write)?;
    let source = host
        .put_content(
            &volume,
            &write,
            "test.txt",
            b"one\r\ntwo\n",
            "text/plain",
            "test.txt",
            64,
            &IdempotencyKey::new("source")?,
        )
        .await?;
    let ordinary = OrdinaryPublisher {
        volume: volume.clone(),
        calls: AtomicUsize::new(0),
    };
    assert!(matches!(
        ordinary
            .stage_at(OperationId::new(), &source, b"replacement")
            .await,
        Err(Error::Unsupported(_))
    ));
    assert_eq!(ordinary.calls.load(Ordering::SeqCst), 0);
    let replacement = ExactTextReplacement {
        old_text: "two".into(),
        new_text: "three".into(),
    };
    assert!(matches!(
        host.edit_text(
            &source,
            &read,
            &foreign,
            &replacement,
            64,
            &IdempotencyKey::new("foreign")?
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    let key = IdempotencyKey::new("edit")?;
    let edited = host
        .edit_text(&source, &read, &write, &replacement, 64, &key)
        .await?;
    assert_eq!(
        host.read_content(&edited, &read, 64).await?.as_ref(),
        b"one\r\nthree\n"
    );
    assert_eq!(
        host.read_content(&source, &read, 64).await?.as_ref(),
        b"one\r\ntwo\n"
    );
    let workspace = workspace_ref(provider.clone(), &volume.storage_name()?)?;
    let head = host.resolve(&workspace).await?.generation;
    // Exact retry survives host reconstruction and returns the prior immutable
    // result, even when a user has since advanced the workspace.
    let user = host
        .put_content(
            &volume,
            &write,
            "test.txt",
            b"user edit",
            "text/plain",
            "test.txt",
            64,
            &IdempotencyKey::new("user")?,
        )
        .await?;
    let host = FilesystemHost::new(filesystem, provider)?;
    assert_eq!(
        host.edit_text(&source, &read, &write, &replacement, 64, &key)
            .await?,
        edited
    );
    assert_eq!(
        host.read_content(&user, &read, 64).await?.as_ref(),
        b"user edit"
    );
    assert!(matches!(
        host.edit_text(
            &source,
            &read,
            &write,
            &replacement,
            64,
            &IdempotencyKey::new("stale")?
        )
        .await,
        Err(Error::Conflict(_))
    ));
    let changed = ExactTextReplacement {
        old_text: "two".into(),
        new_text: "different".into(),
    };
    assert!(matches!(
        host.edit_text(&source, &read, &write, &changed, 64, &key)
            .await,
        Err(Error::Conflict(_))
    ));
    // Same path and output under a different precondition cannot borrow the
    // first receipt. The generation itself is part of the retained identity.
    assert!(matches!(
        host.put_content_at(
            &volume,
            &write,
            "test.txt",
            b"one\r\nthree\n",
            "text/plain",
            "test.txt",
            64,
            &key,
            &head
        )
        .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        host.read_content(&user, &read, 64).await?.as_ref(),
        b"user edit"
    );
    Ok(())
}

#[cfg(feature = "filesystem-local")]
#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered persistent scenario retains the exact source, result and user head across provider drop"
)]
async fn exact_edit_reopens_persistent_receipt_without_overwriting_user_changes() -> Result<()> {
    let directory = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
    let options = acyclic_fs::LocalOptions::new(directory.path().join("filesystem"));
    let provider = ProviderRef::new("persistent-edit", "filesystem", "2")?;
    let owner = AgentId::from_bytes([3; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "files",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
    )?;
    let issuer = AuthorityIssuer::new(
        "persistent-edit",
        [4; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "persistent-edit".into(),
        },
    );
    let scope = issuer.root_for_agent(
        owner,
        "owner",
        Capabilities::new([
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]),
    );
    let read = ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Read)?;
    let write = ContentGrant::verify(&issuer.verifier(), &scope, &volume, VolumeOperation::Write)?;
    let replacement = ExactTextReplacement {
        old_text: "before".into(),
        new_text: "after".into(),
    };
    let key = IdempotencyKey::new("persistent-edit")?;
    let (source, edited, user) = {
        let filesystem = Fs::local(options.clone())
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let host = FilesystemHost::new(filesystem, provider.clone())?;
        host.create_volume(&volume).await?;
        let source = host
            .put_content(
                &volume,
                &write,
                "file.txt",
                b"before\n",
                "text/plain",
                "file.txt",
                64,
                &IdempotencyKey::new("source")?,
            )
            .await?;
        let edited = host
            .edit_text(&source, &read, &write, &replacement, 64, &key)
            .await?;
        let user = host
            .put_content(
                &volume,
                &write,
                "file.txt",
                b"user\n",
                "text/plain",
                "file.txt",
                64,
                &IdempotencyKey::new("user")?,
            )
            .await?;
        (source, edited, user)
    };
    // Drop every provider/host handle before reopening the same disk store.
    let filesystem = Fs::local(options)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let host = FilesystemHost::new(filesystem, provider.clone())?;
    let workspace = workspace_ref(provider, &volume.storage_name()?)?;
    let head = host.resolve(&workspace).await?.generation;
    for maximum_bytes in [0, u64::MAX] {
        assert!(matches!(
            host.edit_text(&source, &read, &write, &replacement, maximum_bytes, &key)
                .await,
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            host.put_content_at(
                &volume,
                &write,
                "file.txt",
                b"after\n",
                "text/plain",
                "file.txt",
                maximum_bytes,
                &key,
                &head
            )
            .await,
            Err(Error::Invalid(_))
        ));
    }
    assert_eq!(
        host.edit_text(&source, &read, &write, &replacement, 64, &key)
            .await?,
        edited
    );
    assert_eq!(host.resolve(&workspace).await?.generation, head);
    assert_eq!(
        host.read_content(&edited, &read, 64).await?.as_ref(),
        b"after\n"
    );
    assert_eq!(
        host.read_content(&user, &read, 64).await?.as_ref(),
        b"user\n"
    );
    assert!(matches!(
        host.edit_text(
            &source,
            &read,
            &write,
            &replacement,
            64,
            &IdempotencyKey::new("new-stale-edit")?
        )
        .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(host.resolve(&workspace).await?.generation, head);
    Ok(())
}
