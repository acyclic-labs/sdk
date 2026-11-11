//! Inheritance is proved against real volume contents and one Streams decision.
use super::*;
use crate::{
    agent::{AgentId, ForkAdopted, WorkspaceKind, WorkspaceMode, WorkspaceRole},
    resources::ProviderRef,
};
use acyclic_stream::StreamProviderExt as _;

#[tokio::test]
async fn inheritance_and_tree_binding_follow_the_entry_in_one_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let host = FilesystemHost::new(
        acyclic_fs::Fs::from_memory_providers(
            stream.clone(),
            Arc::new(acyclic_objects::MemoryObjects::memory()),
        ),
        &ProviderRef::new(
            "inheritance".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?,
        None,
    )?;
    let parent = AgentPath::root(&AgentId::from_bytes([1; 16]));
    let child = parent.child(&AgentId::from_bytes([2; 16]))?;
    let mut entries = Vec::new();
    for (name, inherit) in [
        ("copy", WorkspaceInherit::Fork),
        ("board", WorkspaceInherit::Share),
        ("empty", WorkspaceInherit::Own),
    ] {
        let volume = VolumeRef::new(
            host.provider.clone(),
            name.into(),
            VolumeClass::Project,
            VolumeOwner::Project("owner".into()),
            &mut None,
        )?;
        host.create_volume(&volume).await?;
        let reference =
            super::super::workspace_ref(host.provider.clone(), &volume.storage_name(&mut None)?)?;
        host.apply(
            &reference,
            None,
            &[super::super::WorkspaceMutation::PutFile {
                path: "/parent.txt".into(),
                bytes: acyclic_stream::OwnedBytes::from_static(b"parent contents"),
            }],
            &crate::IdempotencyKey::new(name)?,
        )
        .await?;
        let mut entry = WorkspaceEntry::initial_volume(WorkspaceRole::User, name.into(), volume);
        entry.inherit = inherit;
        entries.push(entry);
    }
    let mut tree = entries[0].clone();
    tree.id = crate::agent::EntryId::from_bytes([4; 16]);
    tree.name = "hierarchy".into();
    tree.role = WorkspaceRole::Tree;
    tree.kind = WorkspaceKind::Tree;
    tree.source = WorkspaceSource::Subtree(parent.clone());
    tree.mode = WorkspaceMode::ReadOnly;
    tree.inherit = WorkspaceInherit::Own;
    entries.push(tree);
    let state = AgentState::adopted(
        parent,
        &ForkAdopted {
            agent: AgentId::from_bytes([1; 16]),
            parent: None,
            forked_at: 0,
            conversation_tail: 0,
            checkpoint: None,
            profile: None,
            volumes: entries.clone(),
            brief: None,
            policy: None,
        },
    )?;
    let bindings = fork_bindings(&child, &entries, None)?;
    let mut renamed = entries.clone();
    renamed[0].name = "renamed".into();
    let renamed = fork_bindings(&child, &renamed, None)?;
    assert_eq!(bindings[0].volume()?, renamed[0].volume()?);
    assert_ne!(bindings[0].volume()?, entries[0].volume()?);
    assert_eq!(bindings[1].volume()?, entries[1].volume()?);
    assert_ne!(bindings[2].volume()?, entries[2].volume()?);
    assert_eq!(bindings[3].source, WorkspaceSource::Subtree(child.clone()));
    let request = CommitRequest {
        idempotency_key: acyclic_stream::IdempotencyKey::new(b"inheritance".to_vec())?,
        deadline_unix_millis: None,
        effects: vec![acyclic_stream::CommitEffect::hard(
            vec![acyclic_stream::CommitCondition::Absent {
                path: child.inbox()?,
            }],
            vec![acyclic_stream::CommitMutation::Append {
                path: child.inbox()?,
                records: vec![acyclic_stream::OwnedBytes::from_static(b"adopted").into()],
            }],
        )],
    };
    let spawn = FilesystemSpawnVolumes::new(host.clone());
    let commit = spawn.root_volumes(&state, &child, None, request).await?;
    let reference = |entry: &WorkspaceEntry| -> crate::Result<_> {
        super::super::workspace_ref(
            host.provider.clone(),
            &entry.volume()?.storage_name(&mut None)?,
        )
    };
    for entry in [&bindings[0], &bindings[2]] {
        assert!(host.resolve(&reference(entry)?, None).await.is_err());
    }
    let cancellation = CancellationToken::new();
    assert!(matches!(
        host.filesystem
            .submit_commit(commit, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value,
        acyclic_fs::FilesystemCommitOutcome::Committed(_)
    ));
    for entry in [&bindings[0], &bindings[1]] {
        assert_eq!(
            host.read(&reference(entry)?, None, "/parent.txt", 64, None)
                .await?
                .as_ref(),
            b"parent contents"
        );
    }
    assert!(matches!(
        host.stat(&reference(&bindings[2])?, None, "/parent.txt")
            .await,
        Err(crate::Error::NotFound(_))
    ));
    assert_eq!(stream.tail(child.inbox()?).await?, 1);
    Ok(())
}
