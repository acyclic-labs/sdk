//! C1 over real Streams, filesystem discovery, journal files and model requests.
use super::*;
use acyclic_harness_core::{
    agent::{WorkspaceInstructions, WorkspaceUpdate, discover_workspace_instructions},
    conversation::{ContentAccess, ContentGrant},
    runtime::ContentBindings,
};
use std::collections::BTreeMap;

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
        seen: &'a BTreeMap<EntryId, [u8; 32]>,
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
                .filter(|entry| entry.role == WorkspaceRole::User)
            {
                let source = WorkspaceInstructions::source_for(entry)
                    .map_err(|error| acyclic_harness_core::Error::Invalid(error.to_string()))?;
                if seen.get(&entry.id) == Some(&source) {
                    continue;
                }
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

struct Fixture {
    path: AgentPath,
    stream: Arc<MemoryStream>,
    files: Host,
    private: WorkspaceEntry,
    issuer: AuthorityRoot,
    users: Vec<WorkspaceEntry>,
    instruction_files: Vec<FileRef>,
}

async fn fixture() -> Result<Fixture> {
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
    let mut instruction_files = Vec::new();
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
        let file = files
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
        instruction_files.push(file);
        users.push(WorkspaceEntry::initial_volume(
            WorkspaceRole::User,
            name.into(),
            volume,
        ));
    }
    Ok(Fixture {
        path,
        stream,
        files,
        private,
        issuer,
        users,
        instruction_files,
    })
}

fn exposed(turns: Arc<dyn TurnExecutors>) -> Result<Exposed> {
    Ok(Exposed {
        turns,
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
    })
}

async fn change(
    stream: &MemoryStream,
    path: &AgentPath,
    op: WorkspaceOp,
    entry: WorkspaceEntry,
    key: &str,
) -> Result {
    let request = acyclic_stream::CommitRequest {
        idempotency_key: IdempotencyKey::new(key.as_bytes().to_vec())?,
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
                        entry,
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
    Ok(())
}

#[tokio::test]
async fn workspace_deltas_preserve_the_request_prefix_and_restore_selected_instructions() -> Result
{
    let Fixture {
        path,
        stream,
        files,
        private,
        issuer,
        users,
        instruction_files,
    } = fixture().await?;
    let model = Arc::new(
        ScriptedModelProvider::new((0..18).map(|n| ScriptedTurn::Text(format!("answer {n}"))))
            .reading_content(),
    );
    let turns = exposed(executors(model.clone())?)?;
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
    for turn in 0..18 {
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
            9 => {
                second = users[2].clone();
                Some(WorkspaceOp::Add)
            }
            _ => None,
        };
        if let Some(op) = operation {
            change(
                &stream,
                &path,
                op,
                second.clone(),
                &format!("workspace-{turn}"),
            )
            .await?;
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
        assert_eq!(
            document.is_some(),
            matches!(turn, 0 | 2 | 3 | 4 | 5 | 9 | 16)
        );
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
                if turn == 16 { 2 } else { 1 },
                "one new delta; restoring instructions retains the earlier document message"
            );
            if matches!(turn, 2 | 4 | 16) {
                assert_eq!(document.instructions.len(), 1);
                let source = if turn == 16 {
                    0
                } else if turn == 4 {
                    2
                } else {
                    1
                };
                assert_eq!(
                    document.instructions[0].entry,
                    if turn == 16 { users[0].id } else { second.id }
                );
                assert_eq!(
                    document.instructions[0].files,
                    vec![instruction_files[source].clone()]
                );
                let requests = model.requests();
                let request = requests.last().ok_or("missing model request")?;
                let delta = request
                    .messages
                    .iter()
                    .rev()
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
            if turn == 16 {
                let previous = previous_document
                    .as_ref()
                    .ok_or("missing retained Add delta")?;
                assert_eq!(
                    previous.digest, document.digest,
                    "history cut retains the current table document"
                );
                assert_eq!(previous.instructions[0].entry, second.id);
            }
            previous_document = Some(document.clone());
        }
    }
    let requests = model.requests();
    assert_eq!(requests.len(), 18);
    for (at, pair) in requests.windows(2).enumerate() {
        if at + 1 == 16 {
            continue; // The history page changes its selected base at this cut.
        }
        assert!(
            pair[1].messages.starts_with(&pair[0].messages),
            "an announcement rewrote the retained prefix"
        );
    }
    Ok(())
}

#[tokio::test]
async fn workspace_instructions_remain_readable_when_an_open_turn_loses_their_source() -> Result {
    use acyclic_harness_core::{
        filesystem::{Grants, StockExecutors},
        model::{ModelEvent, ToolCall},
        test_support::ScriptedEvent,
    };

    for remove in [false, true] {
        let Fixture {
            path,
            stream,
            files,
            private,
            issuer,
            users,
            instruction_files,
        } = fixture().await?;
        let model = Arc::new(
            ScriptedModelProvider::new([
                ScriptedTurn::Events(vec![
                    ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                        "pause".into(),
                        "wait".into(),
                        serde_json::json!({"input":true,"timeout_ms":10}),
                    ))),
                    ScriptedEvent::Event(ModelEvent::Completed {
                        metadata: serde_json::Value::Null,
                    }),
                ]),
                ScriptedTurn::Text("resumed with the original instructions".into()),
            ])
            .reading_content(),
        );
        let turns = StockExecutors::new(
            (
                Model::new("test".into(), "text".into(), "1".into())?,
                model.clone(),
            ),
            ToolRegistry::default(),
            (Default::default(), CompactionPolicy::Disabled),
            Arc::new(Grants(vec!["tool:call:wait".into()])),
        )
        .with_agent_tools(&["wait".into()]);
        let driver = ExecutorDriver::new(FilesystemTurnHost::new(
            stream.clone(),
            within(),
            files.clone(),
            issuer,
            TurnAdmission::LocalRoot,
            Arc::new(exposed(Arc::new(turns))?),
            LIMITS,
            acyclic_harness_core::approvals::ApprovalPolicy::accept_all("owner"),
        ));
        let (clock, mut control) = acyclic_actors::local::ControlledClock::new(100);
        let host = AgentHost::with_spawn_volumes(
            stream.clone(),
            driver,
            within(),
            acyclic_actors::domain::ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            Arc::new(clock),
            Arc::new(acyclic_harness_core::filesystem::FilesystemSpawnVolumes::new(files)),
        )?;
        let mut adoption = adopted(path.id());
        adoption.volumes = vec![private, users[0].clone()];
        host.create(&intent("paused-root"), adoption).await?;
        let waiting = deliver(&host, &path, requested("pause-with-instructions", &path)).await?;
        assert_eq!(
            waiting.phase,
            Phase::Waiting {
                deadline_unix_ms: Some(110),
                input: true
            }
        );
        let pinned = turn_announcements(&waiting)?.ok_or("missing pinned delta")?;
        let mut replacement = users[0].clone();
        let op = if remove {
            WorkspaceOp::Remove
        } else {
            replacement.source = users[2].source.clone();
            WorkspaceOp::Reroot
        };
        change(&stream, &path, op, replacement, "replace-while-waiting").await?;
        host.drain(&path).await?;
        assert_eq!(
            model.requests().len(),
            1,
            "table changes cannot wake a parked turn"
        );
        control.advance_to(110)?;
        host.drain(&path).await?;
        let settled = host.state(&path).await?;
        assert_eq!(settled.phase, Phase::Idle);
        assert_eq!(settled.conversation_tail, 1);
        let entries = super::super::values(stream.as_ref(), path.conversation()?).await?;
        let entry = ConversationEntry::decode(entries.last().ok_or("missing resumed entry")?)?;
        assert_eq!(entry.outcome, TurnOutcome::Completed);
        assert_eq!(entry.announcements, pinned);
        let requests = model.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].messages.starts_with(&requests[0].messages));
        assert!(
            requests[1]
                .messages
                .iter()
                .flat_map(|message| message.content.file_refs())
                .any(|file| *file == instruction_files[0])
        );
    }
    Ok(())
}
