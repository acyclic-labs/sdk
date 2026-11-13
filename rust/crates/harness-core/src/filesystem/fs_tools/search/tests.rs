use super::*;
use crate::{
    Capabilities, OperationId,
    conversation::Limits,
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::MemoryHarnessStorage,
    runtime::{Bindings, RuntimeScope},
};
use acyclic_stream::{AppendRequest, IdempotencyKey, MemoryStream, StreamProviderExt as _};

type Tools = FilesystemTools<MemoryStream, acyclic_fs::MemoryObjectBackend>;

struct Fixture {
    tools: Tools,
    context: ToolContext,
    decision: acyclic_stream::StreamPath,
    _issuer: AuthorityRoot,
    _content: MemoryHarnessStorage,
}

async fn fixture() -> Result<Fixture> {
    let owner = crate::AgentId::from_bytes([51; 16]);
    let path = crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes([51; 16]));
    let child = path
        .child(&crate::agent::AgentId::from_bytes([52; 16]))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let filesystem = acyclic_fs::Fs::memory();
    let provider = filesystem.stream_provider();
    let decision = acyclic_stream::StreamPath::new(format!("{child}/decisions"))?;
    provider
        .append(AppendRequest {
            path: decision.clone(),
            records: vec!["accepted patch".into()],
            if_tail: None,
            idempotency_key: IdempotencyKey::unique(),
        })
        .await?;
    let reference = crate::resources::ProviderRef::new(
        "search".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(filesystem, &reference, None)?;
    let issuer = AuthorityRoot::new(
        "search",
        [53; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "search".into(),
        },
    );
    let grants = Capabilities::new(["tool:call:fs"]);
    let scope = issuer.bootstrap_scope("search", &grants, Some(owner), &mut None)?;
    let tools = FilesystemTools::<MemoryStream, acyclic_fs::MemoryObjectBackend> {
        agent: path.clone(),
        host,
        store: StreamGitCompatStore::new(StreamAuthorityStore::new(provider)),
        volumes: Default::default(),
        primary: None,
        binding_identity: [0; 32],
        verifier: issuer.verifier(&mut None)?,
        scope,
    };
    let content = MemoryHarnessStorage::new(owner, 1_000_000).await?;
    let operation = OperationId::new();
    let task = Bindings {
        scope: RuntimeScope::new(grants, Limits::default())?,
        ..Bindings::local()
    }
    .build()?
    .agent_context(&Capabilities::default(), content.content_bindings())?
    .with_execution_journal(content.journal(), operation)?
    .for_agent(path.clone(), Vec::new());
    let context = ToolContext::new(Arc::new(task), operation, "search")?;
    Ok(Fixture {
        tools,
        context,
        decision,
        _issuer: issuer,
        _content: content,
    })
}

#[tokio::test]
async fn scope_grep_pages_original_child_decisions_and_refuses_foreign_cursors() -> Result<()> {
    let fixture = fixture().await?;
    let tools = &fixture.tools;
    let context = fixture.context.clone();
    let invoke = |command: String| ToolInvocation {
        operation_id: context.operation_id(),
        call_id: "search".into(),
        name: "fs".into(),
        arguments: serde_json::json!({"command": command}),
    };
    let mut command = "grep --scope . accepted --page-size 2".to_owned();
    let mut found = Vec::new();
    loop {
        let page = tools
            .execute(invoke(command), Some(context.clone()))
            .await?
            .value;
        let result = &page["result"];
        assert!(result["work"].as_u64().is_some_and(|work| work <= 2));
        found.extend(
            result["matches"]
                .as_array()
                .ok_or_else(|| Error::Invalid("missing matches".into()))?
                .iter()
                .cloned(),
        );
        let Some(next) = result["next"].as_str() else {
            break;
        };
        assert!(
            tools
                .execute(
                    invoke(format!("grep --scope . changed --cursor {next}")),
                    Some(context.clone())
                )
                .await?
                .value
                .to_string()
                .contains("search cursor differs from selection")
        );
        command = format!("grep --scope . accepted --page-size 2 --cursor {next}");
    }
    assert_eq!(found.len(), 1);
    assert_eq!(found[0]["path"], fixture.decision.as_str());
    assert_eq!(found[0]["sequence"], 0);
    let foreign = crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes([54; 16]));
    assert!(matches!(
        tools
            .execute(
                invoke(format!("grep --scope {foreign} accepted")),
                Some(context)
            )
            .await,
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}

#[tokio::test]
async fn native_scope_grep_preserves_cursor_selection_and_releases_original_custody() -> Result<()>
{
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let fixture = fixture().await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(error, _)| Error::from(error))?;
    let baseline = work.live_allocation_bytes();
    let invoke = |command: String| ToolInvocation {
        operation_id: fixture.context.operation_id(),
        call_id: "search".into(),
        name: "fs".into(),
        arguments: serde_json::json!({"command": command}),
    };
    let mut command = "grep --scope . accepted --page-size 2".to_owned();
    let mut found = Vec::new();
    loop {
        let invocation = invoke(command);
        let (result, custody) = fixture
            .tools
            .execute_with_native_context(&fixture.context, &invocation, &work)?
            .await?;
        assert!(custody.bytes() > 0);
        let page = &result.value["result"];
        assert!(page["work"].as_u64().is_some_and(|work| work <= 2));
        found.extend(
            page["matches"]
                .as_array()
                .expect("search matches")
                .iter()
                .cloned(),
        );
        let next = page["next"].as_str().map(str::to_owned);
        let io = PreparationIo::from_native_work(&work)?;
        let caller_value = io.value_json(&"caller-owned content", None)?;
        let caller_bytes = io.owned_bytes()?;
        fixture
            .tools
            .authorize_replay(Some(&fixture.context), &invocation, &result, Some(&io))?
            .expect("source replay authorization")
            .await?;
        if !page["matches"]
            .as_array()
            .expect("search matches")
            .is_empty()
        {
            let mut foreign = result.clone();
            foreign.value["result"]["matches"][0]["path"] = serde_json::json!("outside/history");
            let refused = fixture
                .tools
                .authorize_replay(Some(&fixture.context), &invocation, &foreign, Some(&io))?
                .expect("source replay authorization")
                .await;
            assert!(refused.is_err());
        }
        assert_eq!(
            io.owned_bytes()?,
            caller_bytes,
            "replay retired caller-owned storage"
        );
        let caller_hold = io.guard_owned(caller_bytes)?;
        drop(caller_value);
        drop(caller_hold);
        drop(io);
        drop(result);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), baseline);
        let Some(next) = next else {
            break;
        };
        let foreign = invoke(format!("grep --scope . changed --cursor {next}"));
        let refused = fixture
            .tools
            .execute_with_native_context(&fixture.context, &foreign, &work)?
            .await;
        assert!(matches!(
            refused,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                ..
            })
        ));
        assert_eq!(work.live_allocation_bytes(), baseline);
        command = format!("grep --scope . accepted --page-size 2 --cursor {next}");
    }
    assert_eq!(found.len(), 1);
    assert_eq!(found[0]["path"], fixture.decision.as_str());
    assert_eq!(found[0]["origin"], fixture.decision.as_str());
    assert_eq!(found[0]["sequence"], 0);
    let invocation = invoke("grep --scope . accepted".into());
    let saved = ToolResult {
        value: serde_json::json!({"result": {
            "kind": "grep", "matches": found, "work": 1, "next": null,
        }}),
    };
    fixture._issuer.revoke_workload(&fixture.tools.scope)?;
    let io = PreparationIo::from_native_work(&work)?;
    let refused = fixture
        .tools
        .authorize_replay(Some(&fixture.context), &invocation, &saved, Some(&io))?
        .expect("source replay authorization")
        .await;
    assert!(refused.is_err(), "revoked authority exposed a saved result");
    drop(io);
    assert_eq!(work.live_allocation_bytes(), baseline);
    cancellation.cancel();
    assert!(
        fixture
            .tools
            .execute_with_native_context(&fixture.context, &invocation, &work)
            .is_err()
    );
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn native_mounted_grep_pins_generation_and_reauthorizes_saved_files() -> Result<()> {
    use crate::conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let mut fixture = fixture().await?;
    let owner = crate::AgentId::from_bytes([51; 16]);
    let volume = VolumeRef::new(
        fixture.tools.host.provider.clone(),
        "files".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    fixture.tools.host.create_volume(&volume).await?;
    let grants = Capabilities::new([
        "tool:call:fs".to_owned(),
        volume.directory_read_capability("decisions", &mut None)?,
        volume.capability(VolumeOperation::Write, &mut None)?,
    ]);
    fixture.tools.scope =
        fixture
            ._issuer
            .bootstrap_scope("files", &grants, Some(owner), &mut None)?;
    let write = ContentGrant::verify(
        &fixture.tools.verifier,
        &fixture.tools.scope,
        &volume,
        VolumeOperation::Write,
        &mut None,
    )?;
    let original = fixture
        .tools
        .host
        .put_content(
            &volume,
            &write,
            "decisions/a.txt",
            b"accepted original",
            "text/plain",
            "decision",
            None,
            &IdempotencyKey::unique(),
            None,
            Default::default(),
        )
        .await?;
    let mut binding = crate::agent::WorkspaceEntry::initial_volume(
        crate::agent::WorkspaceRole::User,
        "files".into(),
        volume.clone(),
    );
    binding.root = "decisions".into();
    fixture.tools.volumes.insert("files".into(), binding);
    fixture.tools.primary = Some("files".into());
    let operation = OperationId::new();
    let task = Bindings {
        scope: RuntimeScope::new(grants, Limits::default())?,
        ..Bindings::local()
    }
    .build()?
    .agent_context(
        &Capabilities::default(),
        fixture._content.content_bindings(),
    )?
    .with_execution_journal(fixture._content.journal(), operation)?
    .for_agent(fixture.tools.agent.clone(), Vec::new());
    fixture.context = ToolContext::new(Arc::new(task), operation, "files")?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(error, _)| Error::from(error))?;
    let baseline = work.live_allocation_bytes();
    let invoke = |command: String| ToolInvocation {
        operation_id: operation,
        call_id: "files".into(),
        name: "fs".into(),
        arguments: serde_json::json!({"command": command}),
    };
    let mut command = "grep accepted --page-size 1".to_owned();
    let mut found = Vec::new();
    let mut opening = true;
    loop {
        let invocation = invoke(command);
        let (result, custody) = fixture
            .tools
            .execute_with_native_context(&fixture.context, &invocation, &work)?
            .await?;
        let page = &result.value["result"];
        assert!(page["work"].as_u64().is_some_and(|work| work <= 1));
        found.extend(
            page["matches"]
                .as_array()
                .expect("file matches")
                .iter()
                .cloned(),
        );
        let next = page["next"].as_str().map(str::to_owned);
        let io = PreparationIo::from_native_work(&work)?;
        fixture
            .tools
            .authorize_replay(Some(&fixture.context), &invocation, &result, Some(&io))?
            .expect("saved file authorization")
            .await?;
        drop(io);
        drop(result);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), baseline);
        if opening {
            opening = false;
            for (path, body) in [
                ("decisions/a.txt", b"replacement".as_slice()),
                ("decisions/later.txt", b"accepted later".as_slice()),
            ] {
                fixture
                    .tools
                    .host
                    .put_content(
                        &volume,
                        &write,
                        path,
                        body,
                        "text/plain",
                        "later",
                        None,
                        &IdempotencyKey::unique(),
                        None,
                        Default::default(),
                    )
                    .await?;
            }
            let foreign = invoke(format!(
                "grep changed --cursor {}",
                next.as_ref().expect("opening cut")
            ));
            assert!(
                fixture
                    .tools
                    .execute_with_native_context(&fixture.context, &foreign, &work)?
                    .await
                    .is_err()
            );
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        let Some(next) = next else {
            break;
        };
        command = format!("grep accepted --page-size 1 --cursor {next}");
    }
    assert_eq!(found.len(), 1, "later publication entered the opening cut");
    let address: crate::search::FileMatch = serde_json::from_value(found[0].clone())
        .map_err(|error| Error::Invalid(error.to_string()))?;
    assert!(address.file.matches_content(&original));
    let invocation = invoke("grep accepted".into());
    let saved = ToolResult {
        value: serde_json::json!({"result": {
            "kind": "grep", "matches": found, "work": 1, "next": null,
        }}),
    };
    let mut outside = saved.clone();
    outside.value["result"]["matches"][0]["file"]["path"] = serde_json::json!("outside/a.txt");
    let io = PreparationIo::from_native_work(&work)?;
    assert!(
        fixture
            .tools
            .authorize_replay(Some(&fixture.context), &invocation, &outside, Some(&io))?
            .expect("saved file authorization")
            .await
            .is_err()
    );
    fixture._issuer.revoke_workload(&fixture.tools.scope)?;
    assert!(
        fixture
            .tools
            .authorize_replay(Some(&fixture.context), &invocation, &saved, Some(&io))?
            .expect("saved file authorization")
            .await
            .is_err()
    );
    drop(io);
    cancellation.cancel();
    assert!(
        fixture
            .tools
            .execute_with_native_context(&fixture.context, &invocation, &work)
            .is_err()
    );
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}
