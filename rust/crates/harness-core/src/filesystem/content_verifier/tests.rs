use super::*;
use crate::core::Authority;
use crate::{
    Capabilities,
    core::{AggregateKind, AuthorityRoot},
};

#[tokio::test]
async fn issuer_layers_preserve_precedence_pins_and_original_read_authority() -> Result<()> {
    use crate::{
        AgentId,
        conversation::VolumeOwner,
        runtime::{Bindings, ContentBindings, RuntimeScope, ToolContext},
    };
    async fn read(
        task: Arc<crate::runtime::TaskContext>,
        path: &str,
    ) -> Result<crate::tool::ToolResult> {
        let operation = crate::OperationId::new();
        let context = ToolContext::new(task, operation, "configuration-read")?;
        crate::tool::files::read()?
            .executor
            .execute(
                crate::tool::ToolInvocation {
                    operation_id: operation,
                    call_id: "configuration-read".into(),
                    name: "read".into(),
                    arguments: serde_json::json!({"path": path}),
                },
                Some(context),
            )
            .await
    }
    let provider = ProviderRef::new("layers".into(), "filesystem".into(), "1".into(), &mut None)?;
    let fs = Fs::memory();
    let host =
        FilesystemHost::new(fs.clone(), &provider, None)?.with_changes(fs.stream_provider())?;
    let agent = AgentId::from_bytes([123; 16]);
    let mut volumes = Vec::new();
    let mut capabilities = vec!["tool:call:read".to_owned()];
    for name in crate::layers::ORDER {
        let volume = VolumeRef::new(
            provider.clone(),
            name.into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )?;
        host.create_volume(&volume).await?;
        capabilities.push(volume.directory_read_capability("", &mut None)?);
        capabilities.push(volume.capability(VolumeOperation::Write, &mut None)?);
        volumes.push(volume);
    }
    let issuer = AuthorityRoot::new(
        "layers",
        [123; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "layers".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "layers",
        &Capabilities::new(capabilities.clone()),
        Some(agent),
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let reader = FilesystemContentVerifier::new(host.clone(), &verifier, &scope, 4096, None)?;
    let access = crate::conversation::ContentAccess::default();
    let mut pins = Vec::new();
    for (index, volume) in volumes.iter().enumerate() {
        let grant =
            ContentGrant::verify(&verifier, &scope, volume, VolumeOperation::Write, &mut None)?;
        for visible in index..6 {
            let path = format!("selection-{visible}");
            host.put_content(
                volume,
                &grant,
                &path,
                crate::layers::ORDER[index].as_bytes(),
                "text/plain",
                &path,
                Some(4096),
                &IdempotencyKey::new(format!("{index}-{visible}"))?,
                None,
                access,
            )
            .await?;
        }
        if matches!(index, 0 | 4) {
            host.put_content(
                volume,
                &grant,
                "disabled",
                if index == 4 { b"off: true" } else { b"seed" },
                "text/plain",
                "disabled",
                Some(4096),
                &IdempotencyKey::new(format!("disabled-{index}"))?,
                None,
                access,
            )
            .await?;
        }
        let (generation, _hold) = reader
            .select_private_generation(volume, "", &format!("selection-{index}"), None, access)?
            .await?;
        pins.push(crate::layers::PinnedLayer {
            volume: volume.clone(),
            generation,
        });
    }
    let binding = ContentBindings {
        volume: volumes[5].clone(),
        reader: reader.clone(),
        writer: Some(FilesystemContentPublisher::new(
            host.clone(),
            &volumes[5],
            &verifier,
            &scope,
            4096,
            None,
        )?),
    };
    let compose = |grants| -> Result<_> {
        Bindings {
            content: Some(binding.clone()),
            seed: Some(pins[0].clone()),
            configuration: std::array::from_fn(|index| Some(pins[index + 1].clone())),
            scope: RuntimeScope::new(
                grants,
                crate::conversation::Limits {
                    file_bytes: Some(4096),
                    render_bytes: Some(4096),
                    ..Default::default()
                },
            )?,
            ..Bindings::local()
        }
        .build()
    };
    let harness = compose(Capabilities::new(capabilities.clone()))?;
    // Workspace rebinding must retain all of the original issuer sources.
    let harness = harness.with_workspace(None, binding.clone(), binding.clone())?;
    let task = Arc::new(harness.agent_context(&Capabilities::default(), binding.clone())?);
    for (index, expected) in crate::layers::ORDER.into_iter().enumerate() {
        let result = read(
            task.clone(),
            &format!("volume:///effective/selection-{index}"),
        )
        .await?;
        assert_eq!(result.value["text"], expected);
    }
    let suppressed = read(task.clone(), "volume:///effective/disabled").await;
    assert!(
        matches!(suppressed, Err(ref error) if error.code() == crate::wire::ErrorCode::NotFound)
    );
    let parent = &volumes[4];
    let grant = ContentGrant::verify(&verifier, &scope, parent, VolumeOperation::Write, &mut None)?;
    host.put_content(
        parent,
        &grant,
        "selection-4",
        b"later publication",
        "text/plain",
        "selection-4",
        Some(4096),
        &IdempotencyKey::new("replace-parent")?,
        None,
        access,
    )
    .await?;
    for path in [
        "volume:///effective/selection-4",
        "volume:///parent/selection-4",
    ] {
        assert_eq!(read(task.clone(), path).await?.value["text"], "parent");
    }
    use futures::FutureExt as _;
    let context = ToolContext::new(task, crate::OperationId::new(), "read-only-source")?;
    context
        .wait_tool_path("volume:///parent/selection-4", None)
        .now_or_never()
        .expect("an existing pinned file is ready")?;
    // A later publication must not make an absent file appear in this cut.
    host.put_content(
        parent,
        &grant,
        "later-only",
        b"later",
        "text/plain",
        "later-only",
        Some(4096),
        &IdempotencyKey::new("later-only")?,
        None,
        access,
    )
    .await?;
    for path in [
        "volume:///parent/later-only",
        "volume:///parent/never-published",
    ] {
        let absent = context
            .wait_tool_path(path, None)
            .now_or_never()
            .expect("an immutable absent file cannot wait for a future publication");
        assert!(
            matches!(absent, Err(ref error) if error.code() == crate::wire::ErrorCode::NotFound)
        );
    }
    let denied_write = context.tool_write_path_selection("volume:///parent/new", None);
    assert!(
        matches!(denied_write, Err(ref error) if error.code() == crate::wire::ErrorCode::Unauthorized)
    );
    let parent_read = parent.directory_read_capability("", &mut None)?;
    let denied = compose(Capabilities::new(
        capabilities.iter().filter(|grant| **grant != parent_read),
    ))?;
    let denied = Arc::new(denied.agent_context(&Capabilities::default(), binding)?);
    let denied_read = read(denied, "volume:///effective/selection-4").await;
    assert!(
        matches!(denied_read, Err(ref error) if error.code() == crate::wire::ErrorCode::Unauthorized)
    );
    Ok(())
}

#[tokio::test]
async fn path_wait_observes_publication_without_probing_or_accepting_other_files() -> Result<()> {
    use crate::{AgentId, conversation::VolumeOwner};
    use futures::FutureExt as _;
    let fs = Fs::memory();
    let provider = ProviderRef::new("wait".into(), "filesystem".into(), "1".into(), &mut None)?;
    let host =
        FilesystemHost::new(fs.clone(), &provider, None)?.with_changes(fs.stream_provider())?;
    let agent = AgentId::from_bytes([119; 16]);
    let volume = VolumeRef::new(
        provider.clone(),
        "wait".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let seed = VolumeRef::new(
        provider,
        "seed".into(),
        VolumeClass::Project,
        VolumeOwner::Project("seed".into()),
        &mut None,
    )?;
    host.create_volume(&seed).await?;
    let issuer = AuthorityRoot::new(
        "wait",
        [119; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "wait".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "wait",
        &Capabilities::new([
            volume.directory_read_capability("", &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
            volume.capability(VolumeOperation::Delete, &mut None)?,
            seed.directory_read_capability("", &mut None)?,
            seed.capability(VolumeOperation::Write, &mut None)?,
        ]),
        Some(agent),
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let reader = FilesystemContentVerifier::new(host.clone(), &verifier, &scope, 4096, None)?;
    let access = crate::conversation::ContentAccess::default();
    let absent = reader
        .read_private_range(
            &volume,
            "",
            "wanted",
            None,
            acyclic_fs::ByteRange {
                offset: 0,
                length: 0,
            },
            access,
        )?
        .await;
    assert!(matches!(absent, Err(ref error) if error.code() == crate::wire::ErrorCode::NotFound));
    let mut waiting = Box::pin(reader.wait_private_path(&volume, "", "wanted", None, access)?);
    assert!(waiting.as_mut().now_or_never().is_none());
    let grant = ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Write,
        &mut None,
    )?;
    for path in ["unrelated", "wanted"] {
        host.put_content(
            &volume,
            &grant,
            path,
            b"published",
            "text/plain",
            path,
            Some(4096),
            &IdempotencyKey::new(path)?,
            None,
            access,
        )
        .await?;
        if path == "unrelated" {
            assert!(
                waiting.as_mut().now_or_never().is_none(),
                "unrelated file completed the wait"
            );
        }
    }
    waiting.await?;

    // Removing a physical suppression file exposes the pinned seed. The
    // public waiting read must re-resolve that view on the deletion event.
    let seed_grant =
        ContentGrant::verify(&verifier, &scope, &seed, VolumeOperation::Write, &mut None)?;
    host.put_content(
        &seed,
        &seed_grant,
        "wanted",
        b"seed output",
        "text/plain",
        "seed",
        Some(4096),
        &IdempotencyKey::new("seed")?,
        None,
        access,
    )
    .await?;
    let (generation, _hold) = reader
        .select_private_generation(&seed, "", "wanted", None, access)?
        .await?;
    let suppressed = host
        .put_content(
            &volume,
            &grant,
            "wanted",
            b"off: true",
            "text/plain",
            "off",
            Some(4096),
            &IdempotencyKey::new("suppressed")?,
            None,
            access,
        )
        .await?;
    let binding = crate::runtime::ContentBindings {
        volume: volume.clone(),
        reader: reader.clone(),
        writer: Some(FilesystemContentPublisher::new(
            host.clone(),
            &volume,
            &verifier,
            &scope,
            4096,
            None,
        )?),
    };
    let task = crate::runtime::Bindings {
        content: Some(binding.clone()),
        seed: Some(crate::layers::PinnedLayer {
            volume: seed.clone(),
            generation: generation.clone(),
        }),
        scope: crate::runtime::RuntimeScope::new(
            Capabilities::new([
                volume.directory_read_capability("", &mut None)?,
                seed.directory_read_capability("", &mut None)?,
                "tool:call:read".into(),
            ]),
            crate::conversation::Limits {
                file_bytes: Some(4096),
                render_bytes: Some(4096),
                ..Default::default()
            },
        )?,
        ..crate::runtime::Bindings::local()
    }
    .build()?
    .agent_context(&Capabilities::default(), binding)?;
    let invocation = crate::tool::ToolInvocation {
        operation_id: crate::OperationId::new(),
        call_id: "deletion".into(),
        name: "read".into(),
        arguments: serde_json::json!({"path":"volume:///effective/wanted","wait":true}),
    };
    let context = crate::runtime::ToolContext::new(
        Arc::new(task),
        invocation.operation_id,
        &invocation.call_id,
    )?;
    host.put_content(
        &seed,
        &seed_grant,
        "later",
        b"new head",
        "text/plain",
        "later",
        Some(4096),
        &IdempotencyKey::new("later")?,
        None,
        access,
    )
    .await?;
    let request = crate::conversation::DirectoryRead {
        volume: &seed,
        granted_prefix: "",
        path: "",
        generation: None,
        after: None,
        maximum_entries: 1,
    };
    let (page, _hold) = context.list_tool_directory(request, None).await?;
    assert_eq!(page.generation, generation);
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        ["wanted"]
    );
    let (head, _hold) = reader
        .select_private_generation(&seed, "", "later", None, access)?
        .await?;
    assert_ne!(head, generation);
    let foreign = context
        .list_tool_directory(
            crate::conversation::DirectoryRead {
                generation: Some(&head),
                after: Some("later"),
                ..request
            },
            None,
        )
        .await;
    assert!(matches!(foreign, Err(ref error) if error.code() == crate::wire::ErrorCode::Conflict));
    let tool = crate::tool::files::read()?;
    let mut pending = tool.executor.execute(invocation, Some(context));
    assert!(pending.as_mut().now_or_never().is_none());
    host.put_changes(
        &volume,
        &grant,
        &[crate::conversation::ContentChange::DeleteExpected {
            source: &suppressed,
        }],
        &IdempotencyKey::new("remove-suppression")?,
        Some(4096),
        access,
        None,
    )?
    .await?;
    let result = pending
        .now_or_never()
        .expect("deletion kept the exposed seed read blocked")?;
    assert_eq!(result.value["text"], "seed output");
    // Negative control: an ordinary path wait still requires publication.
    let mut absent = Box::pin(reader.wait_private_path(&volume, "", "wanted", None, access)?);
    assert!(absent.as_mut().now_or_never().is_none());
    Ok(())
}

#[test]
fn native_reader_reuses_immutable_authentication_and_refuses_revocation() -> Result<()> {
    let provider = ProviderRef::new("reader".into(), "filesystem".into(), "1".into(), &mut None)?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let issuer = AuthorityRoot::new(
        "reader",
        [117; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "reader".into(),
        },
    );
    let scope = issuer.bootstrap_scope(
        "reader",
        &Capabilities::new(["event:append"]),
        None,
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 1_000_000),
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    let reader = FilesystemContentVerifier::new(host.clone(), &verifier, &scope, 4096, Some(&io))?;
    let before = work.receipt().bytes_hashed;
    let examined = work.receipt().items_examined;
    for _ in 0..4 {
        reader.require_native_work(&work)?;
    }
    assert_eq!(
        work.receipt().bytes_hashed,
        before,
        "immutable reader reauthenticated its workload"
    );
    assert_eq!(
        work.receipt().items_examined,
        examined,
        "immutable reader reexamined its workload"
    );
    // The former physical path compares even a retained, unmodified scope.
    verifier.verify(&scope, &mut Some(&mut io.history_io()))?;
    assert!(work.receipt().items_examined > examined);
    let mut forged =
        serde_json::to_value(&scope).map_err(|error| Error::Invalid(error.to_string()))?;
    forged["id"] = "other".into();
    let forged =
        serde_json::from_value(forged).map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(FilesystemContentVerifier::new(host, &verifier, &forged, 4096, Some(&io)).is_err());
    issuer.revoke_workload(&scope)?;
    assert!(
        reader.require_native_work(&work).is_err(),
        "immutable evidence ignored current revocation"
    );
    Ok(())
}
