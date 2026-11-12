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

async fn fixture() -> Result<(Tools, ToolContext, acyclic_stream::StreamPath)> {
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
    let task = Bindings {
        scope: RuntimeScope::new(grants, Limits::default())?,
        ..Bindings::local()
    }
    .build()?
    .agent_context(&Capabilities::default(), content.content_bindings())?
    .for_agent(path.clone(), Vec::new());
    let context = ToolContext::new(Arc::new(task), OperationId::new(), "search")?;
    Ok((tools, context, decision))
}

#[tokio::test]
async fn scope_grep_pages_original_child_decisions_and_refuses_foreign_cursors() -> Result<()> {
    let (tools, context, decision) = fixture().await?;
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
    assert_eq!(found[0]["path"], decision.as_str());
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
