use super::*;
use crate::core::Authority;
use crate::{
    Capabilities,
    core::{AggregateKind, AuthorityRoot},
};

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
    let reader = FilesystemContentVerifier::new(host.clone(), &verifier, &scope, Some(4096), None)?;
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
        u64::MAX,
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
    let seed_grant = ContentGrant::verify(
        &verifier,
        &scope,
        &seed,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
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
            Some(4096),
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
        arguments: serde_json::json!({"path":".volumes/effective/wanted","wait":true}),
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
    let reader =
        FilesystemContentVerifier::new(host.clone(), &verifier, &scope, Some(4096), Some(&io))?;
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
    assert!(
        FilesystemContentVerifier::new(host, &verifier, &forged, Some(4096), Some(&io)).is_err()
    );
    issuer.revoke_workload(&scope)?;
    assert!(
        reader.require_native_work(&work).is_err(),
        "immutable evidence ignored current revocation"
    );
    Ok(())
}
