use super::*;
use crate::{
    agent::{
        InboxRecord, WorkspaceChanged, WorkspaceEntry, WorkspaceOp, WorkspaceRole, WorkspaceTable,
    },
    conversation::DirectoryTarget,
    filesystem::{WorkspaceMutation, workspace_ref},
};
use acyclic_harness_client::{AgentCommand, AgentPageRequest, AgentReply, ReadTarget};

async fn change(
    harness: &Arc<LocalHarness>,
    op: WorkspaceOp,
    entry: WorkspaceEntry,
    key: &str,
) -> Result<()> {
    let (agents, root) = harness.agents().await?;
    let streams = harness
        .agent_stream
        .as_ref()
        .ok_or_else(|| Error::NotFound("agent stream".into()))?;
    let record = InboxRecord::WorkspaceChanged(Box::new(WorkspaceChanged {
        version: 1,
        op,
        entry,
    }));
    let outcome = streams
        .commit(acyclic_stream::CommitRequest {
            idempotency_key: acyclic_stream::IdempotencyKey::new(key.as_bytes().to_vec())
                .map_err(agent)?,
            deadline_unix_millis: None,
            effects: vec![acyclic_stream::CommitEffect::hard(
                vec![acyclic_stream::CommitCondition::Absent {
                    path: root.closed().map_err(agent)?,
                }],
                vec![acyclic_stream::CommitMutation::Append {
                    path: root.inbox().map_err(agent)?,
                    records: vec![record.encode().map_err(agent)?.into()],
                }],
            )],
        })
        .await
        .map_err(agent)?;
    assert!(matches!(
        outcome,
        acyclic_stream::CommitOutcome::Committed(_)
    ));
    agents.drain(root).await.map_err(agent)?;
    Ok(())
}

#[tokio::test]
async fn workspace_directory_follows_commits_and_keeps_its_original_cut_and_scope() -> Result<()> {
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let provider = super::super::scripted([]);
    let harness = Arc::new(
        super::super::open_scripted(volume_config(
            directory.path(),
            project.clone(),
            provider.clone(),
        )?)
        .await?,
    );
    let (agents, root) = harness.agents().await?;
    let root = root.clone();
    let state = agents.state(&root).await.map_err(agent)?;
    let mut primary = WorkspaceTable::primary_in(&state.volumes)
        .ok_or_else(|| Error::NotFound("primary workspace".into()))?
        .clone();
    let connection = harness.connect().await?;
    let request = |target, generation, after_generation| AgentCommand::Directory {
        agent: root.clone(),
        target,
        path: "findings".into(),
        generation,
        after_generation,
        page: AgentPageRequest::new(1, 64 * 1024, None).expect("page credit"),
    };
    let AgentReply::Directory { page: absent } = connection
        .execute(&request(DirectoryTarget::Primary {}, None, None))
        .await?
    else {
        return Err(Error::Conflict("directory reply".into()));
    };
    assert!(absent.entries.is_empty() && !absent.has_more);
    let waiting_request = request(
        DirectoryTarget::Primary {},
        None,
        Some(absent.generation.clone()),
    );
    let waiting = connection.execute(&waiting_request);
    tokio::pin!(waiting);
    assert!(futures::poll!(&mut waiting).is_pending());
    let workspace = workspace_ref(
        project.provider().clone(),
        &project.storage_name(&mut None)?,
    )?;
    // Ordinary committed filesystem changes have no model-tool receipt or
    // report format. They still wake the same authorized directory query.
    harness
        .files
        .apply(
            &workspace,
            None,
            &[
                WorkspaceMutation::CreateDirectory {
                    path: "/findings".into(),
                },
                WorkspaceMutation::PutFile {
                    path: "/findings/opaque.data".into(),
                    bytes: b"original bytes".to_vec().into(),
                },
                WorkspaceMutation::PutFile {
                    path: "/outside".into(),
                    bytes: b"outside the later root".to_vec().into(),
                },
            ],
            &crate::IdempotencyKey::new("directory-create")?,
        )
        .await?;
    let AgentReply::Directory { page: first } = waiting.await? else {
        return Err(Error::Conflict("directory wait reply".into()));
    };
    assert_ne!(first.generation, absent.generation);
    assert_eq!(first.entries[0].name, "opaque.data");
    harness
        .files
        .apply(
            &workspace,
            None,
            &[WorkspaceMutation::PutFile {
                path: "/findings/opaque.data".into(),
                bytes: b"replacement".to_vec().into(),
            }],
            &crate::IdempotencyKey::new("directory-replace")?,
        )
        .await?;
    let read = |target, path: &str, generation| AgentCommand::Read {
        agent: root.clone(),
        offset: 0,
        target: ReadTarget::Path {
            target,
            path: path.into(),
            generation,
        },
    };
    let original = connection
        .execute(&read(
            DirectoryTarget::Primary {},
            "findings/opaque.data",
            first.generation.clone(),
        ))
        .await?;
    let (bytes, length) = original.content().map_err(agent)?;
    assert_eq!(bytes.as_slice(), b"original bytes");
    assert_eq!(length, 14);
    let changed_request = request(
        DirectoryTarget::Primary {},
        None,
        Some(first.generation.clone()),
    );
    let AgentReply::Directory { page: current } = connection.execute(&changed_request).await?
    else {
        return Err(Error::Conflict("changed directory reply".into()));
    };
    assert_ne!(current.generation, first.generation);
    assert!(
        acyclic_harness_client::encode_agent_reply(
            &Ok(AgentReply::Directory {
                page: first.clone()
            }),
            &changed_request,
        )
        .is_err(),
        "a replayed cut cannot satisfy a later-generation query"
    );
    assert!(
        connection
            .execute(&request(
                DirectoryTarget::Primary {},
                Some(first.generation.clone()),
                Some(first.generation.clone())
            ))
            .await
            .is_err()
    );

    primary.name = "renamed".into();
    change(
        &harness,
        WorkspaceOp::Rename,
        primary.clone(),
        "directory-rename",
    )
    .await?;
    primary.root = "findings".into();
    change(
        &harness,
        WorkspaceOp::Reroot,
        primary.clone(),
        "directory-reroot",
    )
    .await?;
    for target in [
        DirectoryTarget::Primary {},
        DirectoryTarget::Workspace { id: primary.id },
    ] {
        let result = connection
            .execute(&read(target, "opaque.data", first.generation.clone()))
            .await?;
        assert_eq!(
            result.content().map_err(agent)?.0.as_slice(),
            b"original bytes"
        );
        for path in ["outside", "../outside", "/outside", ".system/state"] {
            assert!(
                connection
                    .execute(&read(target, path, current.generation.clone()))
                    .await
                    .is_err()
            );
        }
    }
    let AgentReply::Directory { page: private } = connection
        .execute(&AgentCommand::Directory {
            agent: root.clone(),
            target: DirectoryTarget::Private {},
            path: String::new(),
            generation: None,
            after_generation: None,
            page: AgentPageRequest::default(),
        })
        .await?
    else {
        return Err(Error::Conflict("private directory reply".into()));
    };
    assert!(
        connection
            .execute(&read(
                DirectoryTarget::Primary {},
                "opaque.data",
                private.generation
            ))
            .await
            .is_err()
    );
    let internal = WorkspaceEntry::initial_volume(
        WorkspaceRole::Internal,
        "interaction".into(),
        harness.scratch.clone(),
    );
    let internal_target = DirectoryTarget::Workspace { id: internal.id };
    change(&harness, WorkspaceOp::Add, internal, "directory-internal").await?;
    assert!(
        connection
            .execute(&request(internal_target, None, None))
            .await
            .is_err()
    );
    assert!(
        connection
            .execute(&request(
                DirectoryTarget::Workspace {
                    id: crate::contract::EntryId::from_bytes([91; 16])
                },
                None,
                None
            ))
            .await
            .is_err()
    );
    assert_eq!(provider.request_count(), 0);
    Ok(())
}
