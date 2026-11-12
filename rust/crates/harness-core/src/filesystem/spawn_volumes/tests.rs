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
    let board = VolumeRef::new(
        host.provider.clone(),
        "session-board".into(),
        VolumeClass::SessionShared,
        VolumeOwner::Session(parent.id().to_string()),
        &mut None,
    )?;
    host.create_volume(&board).await?;
    host.apply(
        &super::super::workspace_ref(host.provider.clone(), &board.storage_name(&mut None)?)?,
        None,
        &[super::super::WorkspaceMutation::PutFile {
            path: "/parent.txt".into(),
            bytes: acyclic_stream::OwnedBytes::from_static(b"parent contents"),
        }],
        &crate::IdempotencyKey::new("seed-board")?,
    )
    .await?;
    entries.push(WorkspaceEntry::initial_volume(
        WorkspaceRole::Shared,
        "bulletin".into(),
        board,
    ));
    let mut tree = entries[0].clone();
    tree.id = crate::agent::EntryId::from_bytes([4; 16]);
    tree.name = "hierarchy".into();
    tree.role = WorkspaceRole::Tree;
    tree.kind = WorkspaceKind::Tree;
    tree.source = WorkspaceSource::Subtree(parent.clone());
    tree.mode = WorkspaceMode::ReadOnly;
    tree.inherit = WorkspaceInherit::Own;
    entries.push(tree);
    let home = VolumeRef::new(
        host.provider.clone(),
        "configuration".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(crate::AgentId::from_bytes(parent.id_bytes())),
        &mut None,
    )?;
    host.create_volume(&home).await?;
    host.apply(
        &super::super::workspace_ref(host.provider.clone(), &home.storage_name(&mut None)?)?,
        None,
        &[super::super::WorkspaceMutation::PutFile {
            path: "/templates/worker/AGENT.md".into(),
            bytes: acyclic_stream::OwnedBytes::from_static(b"inherited configuration"),
        }],
        &crate::IdempotencyKey::new("seed-configuration")?,
    )
    .await?;
    entries.push(WorkspaceEntry::initial_volume(
        WorkspaceRole::Home,
        "configuration".into(),
        home,
    ));
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
    assert_eq!(bindings[3].volume()?, entries[3].volume()?);
    assert_eq!(bindings[4].source, WorkspaceSource::Subtree(child.clone()));
    let session = AgentPath::root(&AgentId::from_bytes([3; 16]));
    let session_bindings = fork_bindings(&session, &entries, None)?;
    assert_ne!(session_bindings[3].volume()?, entries[3].volume()?);
    // User Share remains explicit, including across independent roots.
    assert_eq!(session_bindings[1].volume()?, entries[1].volume()?);
    let session_child = session.child(&AgentId::from_bytes([6; 16]))?;
    assert_eq!(
        fork_bindings(&session_child, &session_bindings, None)?[3].volume()?,
        session_bindings[3].volume()?
    );
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
    let journal_ref = |path: &AgentPath| -> crate::Result<_> {
        let journal = crate::conversation::journal_volume(host.provider.clone(), &path.id())?;
        super::super::workspace_ref(host.provider.clone(), &journal.storage_name(&mut None)?)
    };
    let child_journal = journal_ref(&child)?;
    let mut refused = request.clone();
    refused.idempotency_key = acyclic_stream::IdempotencyKey::new(b"refused-spawn".to_vec())?;
    refused.effects[0]
        .conditions
        .push(acyclic_stream::CommitCondition::Tail {
            path: state.path.inbox()?,
            expected: 1,
        });
    let refused = spawn.root_volumes(&state, &child, None, refused).await?;
    let cancellation = CancellationToken::new();
    assert!(matches!(
        host.filesystem
            .submit_commit(refused, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value,
        acyclic_fs::FilesystemCommitOutcome::Rejected(_)
    ));
    assert!(matches!(
        stream.tail(child.inbox()?).await,
        Err(acyclic_stream::StreamError::NotFound)
    ));
    assert!(host.resolve(&child_journal, None).await.is_err());
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
    host.resolve(&child_journal, None).await?;
    host.apply(
        &child_journal,
        None,
        &[super::super::WorkspaceMutation::PutFile {
            path: "/retained.json".into(),
            bytes: acyclic_stream::OwnedBytes::from_static(b"child journal"),
        }],
        &crate::IdempotencyKey::new("journal-write")?,
    )
    .await?;
    let request = CommitRequest {
        idempotency_key: acyclic_stream::IdempotencyKey::new(b"independent-session".to_vec())?,
        deadline_unix_millis: None,
        effects: vec![acyclic_stream::CommitEffect::hard(
            vec![acyclic_stream::CommitCondition::Absent {
                path: session.inbox()?,
            }],
            vec![acyclic_stream::CommitMutation::Append {
                path: session.inbox()?,
                records: vec![acyclic_stream::OwnedBytes::from_static(b"session").into()],
            }],
        )],
    };
    let commit = spawn.root_volumes(&state, &session, None, request).await?;
    assert!(matches!(
        host.filesystem
            .submit_commit(commit, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value,
        acyclic_fs::FilesystemCommitOutcome::Committed(_)
    ));
    assert!(matches!(
        host.stat(&reference(&session_bindings[3])?, None, "/parent.txt")
            .await,
        Err(crate::Error::NotFound(_))
    ));
    assert_eq!(
        host.read(&reference(&entries[3])?, None, "/parent.txt", 64, None)
            .await?
            .as_ref(),
        b"parent contents"
    );
    assert_eq!(stream.tail(session.inbox()?).await?, 1);
    let session_journal = journal_ref(&session)?;
    host.resolve(&session_journal, None).await?;
    assert!(matches!(
        host.stat(&session_journal, None, "/retained.json").await,
        Err(crate::Error::NotFound(_))
    ));
    for path in [&child, &session] {
        let journal = crate::conversation::journal_volume(host.provider.clone(), &path.id())?;
        assert!(
            bindings
                .iter()
                .chain(&session_bindings)
                .all(|binding| binding.as_volume() != Some(&journal))
        );
    }
    for entry in [&bindings[5], &session_bindings[5]] {
        assert_ne!(entry.volume()?, entries[5].volume()?);
        assert_eq!(
            host.read(
                &reference(entry)?,
                None,
                "/templates/worker/AGENT.md",
                64,
                None,
            )
            .await?
            .as_ref(),
            b"inherited configuration"
        );
    }
    host.apply(
        &reference(&bindings[5])?,
        None,
        &[super::super::WorkspaceMutation::PutFile {
            path: "/templates/worker/AGENT.md".into(),
            bytes: acyclic_stream::OwnedBytes::from_static(b"child configuration"),
        }],
        &crate::IdempotencyKey::new("edit-configuration")?,
    )
    .await?;
    for entry in [&entries[5], &session_bindings[5]] {
        assert_eq!(
            host.read(
                &reference(entry)?,
                None,
                "/templates/worker/AGENT.md",
                64,
                None,
            )
            .await?
            .as_ref(),
            b"inherited configuration"
        );
    }
    Ok(())
}
