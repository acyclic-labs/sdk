//! Hosted session merges use real forked volumes without a local checkout.
use super::*;
use crate::agent::{WorkspaceRole, WorkspaceTable};
use crate::filesystem::{WorkspaceMutation, workspace_ref};

#[tokio::test]
async fn hosted_parent_merges_two_children_and_resolves_conflicts() -> Result {
    let bound = binding()?;
    let stream = bound.agents().provider().clone();
    let id = AgentId::from_bytes([17; 16]);
    let volume = |name: &str, class| -> Result<VolumeRef> {
        Ok(VolumeRef::new(
            provider()?,
            name.into(),
            class,
            VolumeOwner::Agent(crate::AgentId::from_bytes(id.to_bytes())),
            &mut None,
        )?)
    };
    let Serving::Served(server) = serve(
        bound,
        Arc::new(Owner::default()),
        &IntentId::try_from("merge-root")?,
        RootRequest {
            peer: false,
            agent: id.clone(),
            fresh: vec![
                WorkspaceEntry::initial_volume(
                    WorkspaceRole::Private,
                    PRIVATE_VOLUME.into(),
                    volume("merge-private", VolumeClass::AgentPrivate)?,
                ),
                WorkspaceEntry::initial_volume(
                    WorkspaceRole::User,
                    "project".into(),
                    volume("merge-project", VolumeClass::Project)?,
                ),
            ],
            retained: Vec::new(),
            brief: None,
            policy: None,
        },
    )
    .await?
    else {
        return Err("root publication uncertain".into());
    };
    let client = Client::in_process_with_products(
        stream,
        acyclic_stream::session::testing::authority(),
        None,
        server.clone(),
        Some(server.clone()),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, credential(server.resource()))?;
    let work = async {
        let project = |state: &AgentState| {
            let volume = WorkspaceTable::primary_in(&state.volumes)
                .ok_or("missing project")?
                .volume()?;
            Ok::<_, Box<dyn std::error::Error>>(workspace_ref(
                volume.provider().clone(),
                &volume.storage_name(&mut None)?,
            )?)
        };
        let root = server.root();
        let parent = project(&server.bound().agents().state(root).await?)?;
        // The initial file is shared ancestry, not an unrelated add/add case.
        server
            .bound()
            .volumes()
            .apply(
                &parent,
                None,
                &[WorkspaceMutation::PutFile {
                    path: "/shared.txt".into(),
                    bytes: acyclic_stream::OwnedBytes::from_static(b"base\n"),
                }],
                &crate::IdempotencyKey::new("merge-base")?,
            )
            .await?;
        let mut children = Vec::new();
        for name in ["first", "second"] {
            let Ok(AgentReply::Spawned { agent, .. }) = call(
                &client,
                "agents",
                &AgentCommand::Spawn {
                    parent: root.clone(),
                    id: IntentId::try_from(name)?,
                    brief: ModelContent::Text("edit the project".into()),
                    role: "worker".into(),
                },
            )
            .await?
            else {
                return Err("child not spawned".into());
            };
            children.push(agent);
        }
        for (child, name) in children.iter().zip(["first", "second"]) {
            let workspace = project(&server.bound().agents().state(child).await?)?;
            let text = acyclic_stream::OwnedBytes::from(format!("{name}\n"));
            server
                .bound()
                .volumes()
                .apply(
                    &workspace,
                    None,
                    &[
                        WorkspaceMutation::PutFile {
                            path: format!("/{name}.txt"),
                            bytes: text.clone(),
                        },
                        WorkspaceMutation::PutFile {
                            path: "/shared.txt".into(),
                            bytes: text,
                        },
                    ],
                    &crate::IdempotencyKey::new(name)?,
                )
                .await?;
        }
        let first = AgentCommand::MergeSession {
            agent: children[0].clone(),
            id: IntentId::try_from("merge-first")?,
        };
        let reply = call(&client, "agents", &first).await??;
        let AgentReply::Merged {
            commit: Some(commit),
        } = reply
        else {
            return Err(format!("first merge did not commit: {reply:?}").into());
        };
        assert!(
            matches!(call(&client, "agents", &first).await?, Ok(AgentReply::Merged { commit: Some(replay) }) if replay == commit)
        );
        let second = AgentCommand::MergeSession {
            agent: children[1].clone(),
            id: IntentId::try_from("merge-second")?,
        };
        assert!(call(&client, "agents", &second).await?.is_err());
        let conflict = server
            .bound()
            .volumes()
            .read(&parent, None, "/shared.txt", 4096, None)
            .await?;
        let conflict = std::str::from_utf8(&conflict)?;
        assert!(
            conflict.contains("<<<<<<<")
                && conflict.contains("first")
                && conflict.contains("second"),
            "{conflict}"
        );
        server
            .bound()
            .volumes()
            .apply(
                &parent,
                None,
                &[WorkspaceMutation::PutFile {
                    path: "/shared.txt".into(),
                    bytes: acyclic_stream::OwnedBytes::from_static(b"resolved\n"),
                }],
                &crate::IdempotencyKey::new("resolve-merge")?,
            )
            .await?;
        let Ok(AgentReply::Merged {
            commit: Some(commit),
        }) = call(&client, "agents", &second).await?
        else {
            return Err("resolved merge did not commit".into());
        };
        assert!(
            matches!(call(&client, "agents", &second).await?, Ok(AgentReply::Merged { commit: Some(replay) }) if replay == commit)
        );
        for (path, expected) in [
            ("/first.txt", "first\n"),
            ("/second.txt", "second\n"),
            ("/shared.txt", "resolved\n"),
        ] {
            assert_eq!(
                server
                    .bound()
                    .volumes()
                    .read(&parent, None, path, 4096, None)
                    .await?
                    .as_ref(),
                expected.as_bytes()
            );
        }
        Ok(())
    };
    tokio::select! {
        result = work => result,
        result = server.run(|_, _| {}) => Err(format!("server ended during merge: {result:?}").into()),
    }
}
