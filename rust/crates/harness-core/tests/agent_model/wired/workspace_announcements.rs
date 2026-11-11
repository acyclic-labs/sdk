//! C1 over real Streams, filesystem discovery, journal files and model requests.
use super::*;
use acyclic_harness_core::{
    agent::{WorkspaceUpdate, discover_workspace_instructions},
    conversation::{ContentAccess, ContentGrant},
    runtime::ContentBindings,
};
use std::collections::BTreeSet;

struct Exposed {
    turns: Arc<dyn TurnExecutors>,
    roster: Roster,
}

impl TurnExecutors for Exposed {
    fn executor<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a acyclic_harness_core::filesystem::TurnAuthority,
    ) -> BoxProviderFuture<
        'a,
        acyclic_harness_core::Result<Arc<dyn acyclic_harness_core::executor::Executor>>,
    > {
        self.turns.executor(agent, turn, input, authority)
    }

    fn roster(&self, _: &AgentState) -> Option<Arc<Roster>> {
        Some(Arc::new(self.roster.clone()))
    }

    fn workspaces<'a>(
        &'a self,
        agent: &'a AgentState,
        seen: &'a BTreeSet<EntryId>,
        content: &'a ContentBindings,
    ) -> BoxProviderFuture<'a, acyclic_harness_core::Result<Option<WorkspaceUpdate>>> {
        Box::pin(async move {
            let table = WorkspaceTable::new(agent.volumes.clone())
                .map_err(|error| acyclic_harness_core::Error::Invalid(error.to_string()))?;
            let exposures = table
                .entries()
                .iter()
                .filter(|entry| entry.role != WorkspaceRole::Internal)
                .map(|entry| WorkspaceExposure {
                    entry: entry.id,
                    paths: [("home".into(), None)].into(),
                })
                .collect();
            let document = WorkspaceAnnouncement::new(&table, &self.roster, exposures)
                .map_err(|error| acyclic_harness_core::Error::Invalid(error.to_string()))?;
            let mut instructions = Vec::new();
            for entry in table
                .entries()
                .iter()
                .filter(|entry| entry.role == WorkspaceRole::User && !seen.contains(&entry.id))
            {
                instructions.push(
                    discover_workspace_instructions(
                        entry,
                        content.reader.as_ref(),
                        &Default::default(),
                        &mut Default::default(),
                    )
                    .await?,
                );
            }
            Ok(Some(WorkspaceUpdate {
                document,
                instructions,
            }))
        })
    }
}

#[tokio::test]
async fn workspace_deltas_preserve_the_request_prefix_and_carry_added_instructions() -> Result {
    let path = AgentPath::root(&id(32));
    let stream = Arc::new(MemoryStream::default());
    let (files, private) = private_in(&stream, &path).await?;
    let issuer = AuthorityRoot::new(
        "agents",
        [7; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "agents".into(),
        },
    );
    let mut users = Vec::new();
    for name in ["first", "second", "replacement"] {
        let volume = VolumeRef::new(
            private.volume()?.provider().clone(),
            name.into(),
            VolumeClass::Project,
            VolumeOwner::Project("fixture".into()),
            &mut None,
        )?;
        files.create_volume(&volume).await?;
        let grants = Capabilities::new([volume.capability(VolumeOperation::Write, &mut None)?]);
        let scope = issuer.bootstrap_scope("seed", &grants, None, &mut None)?;
        let grant = ContentGrant::verify(
            &issuer.verifier(&mut None)?,
            &scope,
            &volume,
            VolumeOperation::Write,
            LIMITS.maximum_bytes,
            &mut None,
        )?;
        files
            .put_content(
                &volume,
                &grant,
                "AGENTS.md",
                format!("instructions for {name}").as_bytes(),
                "text/markdown",
                "AGENTS.md",
                LIMITS.maximum_bytes,
                &acyclic_harness_core::IdempotencyKey::new(name)?,
                None,
                ContentAccess::default(),
            )
            .await?;
        users.push(WorkspaceEntry::initial_volume(
            WorkspaceRole::User,
            name.into(),
            volume,
        ));
    }
    let model = Arc::new(
        ScriptedModelProvider::new((0..6).map(|n| ScriptedTurn::Text(format!("answer {n}"))))
            .reading_content(),
    );
    let turns = Exposed {
        turns: executors(model.clone())?,
        roster: Roster::home(TargetRecord {
            name: "home".into(),
            owner: "user".into(),
            class: TargetClass::Attached,
            os: "linux".into(),
            shell: "bash".into(),
            path_style: PathStyle::Posix,
            device_kind: DeviceKind::Desktop,
            state: TargetState::Online,
        })?,
    };
    let driver = ExecutorDriver::new(FilesystemTurnHost::new(
        stream.clone(),
        within(),
        files.clone(),
        issuer,
        TurnAdmission::LocalRoot,
        Arc::new(turns),
        LIMITS,
        Default::default(),
    ));
    let host = committing(&stream, &files, driver)?;
    let mut adoption = adopted(id(32));
    adoption.volumes = vec![private, users[0].clone()];
    host.create(&intent("root"), adoption).await?;
    let mut second = users[1].clone();
    let mut previous_document = None::<AnnouncedDocument>;
    for turn in 0..6 {
        let operation = match turn {
            2 => Some(WorkspaceOp::Add),
            3 => {
                second.name = "renamed".into();
                Some(WorkspaceOp::Rename)
            }
            4 => {
                second.source = users[2].source.clone();
                Some(WorkspaceOp::Reroot)
            }
            5 => Some(WorkspaceOp::Remove),
            _ => None,
        };
        if let Some(op) = operation {
            let request = acyclic_stream::CommitRequest {
                idempotency_key: IdempotencyKey::new(format!("workspace-{turn}").into_bytes())?,
                deadline_unix_millis: None,
                effects: vec![acyclic_stream::CommitEffect::hard(
                    vec![acyclic_stream::CommitCondition::Absent {
                        path: path.closed()?,
                    }],
                    vec![acyclic_stream::CommitMutation::Append {
                        path: path.inbox()?,
                        records: vec![
                            InboxRecord::WorkspaceChanged(Box::new(WorkspaceChanged {
                                version: 1,
                                op,
                                entry: second.clone(),
                            }))
                            .encode()?
                            .into(),
                        ],
                    }],
                )],
            };
            assert!(matches!(
                stream.commit(request).await?,
                CommitOutcome::Committed(_)
            ));
            host.drain(&path).await?;
        }
        let state = deliver(&host, &path, requested(&format!("turn-{turn}"), &path)).await?;
        assert_eq!(state.conversation_tail, turn + 1);
        let entries = super::super::values(stream.as_ref(), path.conversation()?).await?;
        let entry = ConversationEntry::decode(entries.last().ok_or("missing settled entry")?)?;
        let document = entry
            .announcements
            .iter()
            .find(|document| document.kind == AnnouncementKind::Workspaces);
        assert_eq!(document.is_some(), turn != 1);
        if let Some(document) = document {
            let requests = model.requests();
            let request = requests.last().ok_or("missing model request")?;
            assert_eq!(
                request
                    .messages
                    .iter()
                    .filter(|message| message
                        .content
                        .file_refs()
                        .any(|file| *file == document.file))
                    .count(),
                1,
                "exactly one appended announcement message"
            );
            if turn == 2 {
                assert_eq!(document.instructions.len(), 1);
                assert_eq!(document.instructions[0].entry, second.id);
                assert_eq!(document.instructions[0].files[0].path(), "AGENTS.md");
                let requests = model.requests();
                let request = requests.last().ok_or("missing model request")?;
                let delta = request
                    .messages
                    .iter()
                    .find(|message| {
                        message
                            .content
                            .file_refs()
                            .any(|file| *file == document.file)
                    })
                    .ok_or("workspace document not rendered")?;
                assert!(
                    delta
                        .content
                        .file_refs()
                        .any(|file| *file == document.instructions[0].files[0])
                );
            }
            if turn == 4 {
                let previous = previous_document.as_ref().ok_or("missing rename delta")?;
                assert_eq!(
                    previous.file.descriptor(),
                    document.file.descriptor(),
                    "reroot keeps displayed paths"
                );
                assert_ne!(
                    previous.digest, document.digest,
                    "source identity must still append a delta"
                );
            }
            previous_document = Some(document.clone());
        }
    }
    let requests = model.requests();
    assert_eq!(requests.len(), 6);
    for pair in requests.windows(2) {
        assert!(
            pair[1].messages.starts_with(&pair[0].messages),
            "an announcement rewrote the retained prefix"
        );
    }
    Ok(())
}
