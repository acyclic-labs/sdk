//! C1 over real Streams, filesystem discovery, journal files and model requests.
use super::*;
use acyclic_harness_core::{
    agent::WorkspaceUpdate,
    conversation::{ContentAccess, ContentGrant},
    filesystem::FilesystemContentVerifier,
    runtime::ContentBindings,
};
use std::collections::BTreeMap;

struct Exposed {
    turns: Arc<dyn TurnExecutors>,
    roster: Roster,
    policy: acyclic_harness_core::context::ContextDiscoveryPolicy,
}

/// This host offers only wait and generation. Repository capabilities belong
/// to the turn's authenticated content reader, not its fixed tool configuration.
struct InstructionTools;

impl acyclic_harness_core::filesystem::TurnContext for InstructionTools {
    fn prepare<'a>(
        &'a self,
        (executor, limits): (
            acyclic_harness_core::executor::StockExecutor,
            acyclic_harness_core::conversation::Limits,
        ),
        agent: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        authority: &'a acyclic_harness_core::filesystem::TurnAuthority,
    ) -> BoxProviderFuture<
        'a,
        acyclic_harness_core::Result<acyclic_harness_core::executor::StockExecutor>,
    > {
        Box::pin(async move {
            let private = agent
                .volumes
                .iter()
                .find(|entry| entry.role == WorkspaceRole::Private)
                .ok_or_else(|| {
                    acyclic_harness_core::Error::Invalid("missing private entry".into())
                })?
                .volume()?;
            executor.with_tool_authority(
                acyclic_harness_core::runtime::RuntimeScope::new(
                    Capabilities::new([
                        "tool:call:wait".to_owned(),
                        acyclic_harness_core::contract::capability::MODEL_GENERATE.to_owned(),
                        private.capability(VolumeOperation::Read, &mut None)?,
                        private.capability(VolumeOperation::Write, &mut None)?,
                    ]),
                    limits,
                )?,
                authority.approval.clone(),
            )
        })
    }
}

impl TurnExecutors for Exposed {
    fn render_bytes(&self) -> Option<u64> {
        self.turns.render_bytes()
    }

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
        _: &'a acyclic_harness_core::core::RuntimeAuthority,
        _: &'a acyclic_harness_core::core::Scope,
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
            WorkspaceUpdate::discover(
                agent.volumes.clone(),
                Some(&self.roster),
                exposures,
                seen,
                content.reader.as_ref(),
                &self.policy,
                &Default::default(),
            )
            .await
            .map(Some)
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
        policy: Default::default(),
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

async fn selected_instructions(
    files: &Host,
    document: &AnnouncedDocument,
) -> Result<Vec<WorkspaceInstructions>> {
    let Some(selection) = &document.instructions else {
        return Ok(Vec::new());
    };
    let (_custody, reader) = instruction_reader(files, selection)?;
    Ok(selection.entries(reader.as_ref()).await?)
}

fn instruction_reader(
    files: &Host,
    selection: &AnnouncedInstructions,
) -> Result<(
    AuthorityRoot,
    Arc<dyn acyclic_harness_core::conversation::ContentResidencyVerifier>,
)> {
    let issuer = AuthorityRoot::new(
        "proof",
        [8; 32],
        Authority {
            kind: AggregateKind::Conversation,
            id: "proof".into(),
        },
    );
    let grants = Capabilities::new([
        selection.manifest.read_capability(&mut None)?,
        selection.content.read_capability(&mut None)?,
    ]);
    let scope = issuer.bootstrap_scope("proof", &grants, None, &mut None)?;
    let reader = FilesystemContentVerifier::new(
        files.clone(),
        &issuer.verifier(&mut None)?,
        &scope,
        acyclic_harness_core::conversation::MAX_EXACT_JS_INTEGER,
        None,
    )?;
    Ok((issuer, reader))
}

async fn large_instruction_selection(
    files: &Host,
    issuer: &AuthorityRoot,
    source: &WorkspaceEntry,
) -> Result<(WorkspaceEntry, Vec<String>)> {
    let volume = source.volume()?;
    let grants = Capabilities::new([volume.capability(VolumeOperation::Write, &mut None)?]);
    let scope = issuer.bootstrap_scope("large-instructions", &grants, None, &mut None)?;
    let grant = ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &scope,
        volume,
        VolumeOperation::Write,
        LIMITS.maximum_bytes,
        &mut None,
    )?;
    let prefix = vec!["r".repeat(240); 9].join("/");
    let root = format!("{prefix}/instructions");
    let mut names = Vec::new();
    for n in 0_u8..32 {
        let name = format!("INSTRUCTIONS-{n}.md");
        let body = if n < 2 {
            format!("body {n}: {}", "é🦀".repeat(6800))
        } else {
            format!("instruction body {n}")
        };
        files
            .put_content(
                volume,
                &grant,
                &format!("{root}/{name}"),
                body.as_bytes(),
                "text/markdown",
                "AGENTS.md",
                LIMITS.maximum_bytes,
                &acyclic_harness_core::IdempotencyKey::new(format!("large-{n}"))?,
                None,
                ContentAccess::default(),
            )
            .await?;
        names.push(name);
    }
    Ok((
        WorkspaceEntry {
            id: EntryId::from_bytes([64; 16]),
            name: "extra".into(),
            root,
            ..source.clone()
        },
        names,
    ))
}

fn tree(source: &WorkspaceEntry, path: &AgentPath) -> WorkspaceEntry {
    WorkspaceEntry {
        id: EntryId::from_bytes([33; 16]),
        name: "hierarchy".into(),
        role: WorkspaceRole::Tree,
        kind: WorkspaceKind::Tree,
        source: WorkspaceSource::Subtree(path.clone()),
        mode: WorkspaceMode::ReadOnly,
        inherit: WorkspaceInherit::Own,
        ..source.clone()
    }
}

#[tokio::test]
async fn growing_workspace_document_renders_without_a_whole_file_allowance() -> Result {
    let Fixture {
        files,
        issuer,
        users,
        ..
    } = fixture().await?;
    let entries = (0_u16..1024)
        .map(|n| {
            let mut id = [42; 16];
            id[..2].copy_from_slice(&n.to_le_bytes());
            WorkspaceEntry {
                id: EntryId::from_bytes(id),
                name: format!("repository-{n}"),
                ..users[0].clone()
            }
        })
        .collect::<Vec<_>>();
    let exposures = entries
        .iter()
        .map(|entry| WorkspaceExposure {
            entry: entry.id,
            paths: BTreeMap::new(),
        })
        .collect();
    let document = WorkspaceAnnouncement::new(
        &WorkspaceTable::new(entries.clone())?,
        None::<&Roster>,
        exposures,
    )?;
    let (bytes, digest) = document.canonical();
    let limits = acyclic_harness_core::conversation::Limits {
        render_bytes: 64 * 1024,
        file_bytes: 64 * 1024,
        attachments: std::num::NonZeroUsize::new(1),
        ..Default::default()
    };
    assert!(bytes.len() as u64 > limits.render_bytes);
    let volume = users[0].volume()?;
    let grants = Capabilities::new([
        volume.capability(VolumeOperation::Write, &mut None)?,
        volume.capability(VolumeOperation::Read, &mut None)?,
    ]);
    let scope = issuer.bootstrap_scope("large-document", &grants, None, &mut None)?;
    let verifier = issuer.verifier(&mut None)?;
    let grant = ContentGrant::verify(
        &verifier,
        &scope,
        volume,
        VolumeOperation::Write,
        bytes.len() as u64,
        &mut None,
    )?;
    let file = files
        .put_content(
            volume,
            &grant,
            "workspaces.json",
            bytes,
            "application/json",
            "workspaces.json",
            bytes.len() as u64,
            &acyclic_harness_core::IdempotencyKey::new("large-document")?,
            None,
            ContentAccess::default(),
        )
        .await?;
    let reader = FilesystemContentVerifier::new(
        files,
        &verifier,
        &scope,
        acyclic_harness_core::conversation::MAX_EXACT_JS_INTEGER,
        None,
    )?;
    let discovered = WorkspaceUpdate::discover(
        entries.clone(),
        None,
        entries
            .iter()
            .map(|entry| WorkspaceExposure {
                entry: entry.id,
                paths: BTreeMap::new(),
            })
            .collect(),
        &BTreeMap::new(),
        reader.as_ref(),
        &Default::default(),
        &Default::default(),
    )
    .await?;
    assert_eq!(discovered.instructions.len(), entries.len());
    assert!(
        discovered
            .instructions
            .iter()
            .all(|entry| entry.files.len() == 1)
    );
    let delta = announcement_messages(
        &[AnnouncedDocument {
            kind: AnnouncementKind::Workspaces,
            digest,
            file: file.clone(),
            instructions: None,
        }],
        reader.as_ref(),
        Some(limits.render_bytes),
    )
    .await?;
    for message in &delta {
        message.validate_limits(limits, &mut None)?;
        assert!(message.file_refs().all(|source| source == &file));
    }
    let rendered = delta
        .iter()
        .flat_map(ModelContent::parts)
        .filter_map(|part| match part {
            ModelContentPart::Text { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(rendered.as_bytes(), bytes);
    Ok(())
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
    adoption.volumes = vec![tree(&users[0], &path), private, users[0].clone()];
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
            // A removal can return to a document already in the prefix.
            let prior_occurrences = requests.len().checked_sub(2).map_or(0, |at| {
                requests[at]
                    .messages
                    .iter()
                    .filter(|message| {
                        message
                            .content
                            .file_refs()
                            .any(|file| *file == document.file)
                    })
                    .count()
            });
            assert_eq!(
                request
                    .messages
                    .iter()
                    .filter(|message| message
                        .content
                        .file_refs()
                        .any(|file| *file == document.file))
                    .count(),
                prior_occurrences + 1,
                "turn {turn} must append one delta and retain earlier uses of this document"
            );
            if matches!(turn, 2 | 4 | 16) {
                let instructions = selected_instructions(&files, document).await?;
                assert_eq!(instructions.len(), 1);
                let source = if turn == 16 {
                    0
                } else if turn == 4 {
                    2
                } else {
                    1
                };
                assert_eq!(
                    instructions[0].entry,
                    if turn == 16 { users[0].id } else { second.id }
                );
                assert_eq!(
                    instructions[0].files,
                    vec![instruction_files[source].clone()]
                );
                let requests = model.requests();
                let request = requests.last().ok_or("missing model request")?;
                request
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
                    request
                        .messages
                        .iter()
                        .flat_map(|message| message.content.file_refs())
                        .any(|file| *file == document.instructions.as_ref().unwrap().content)
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
                assert_eq!(
                    selected_instructions(&files, previous).await?[0].entry,
                    second.id
                );
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
        filesystem::StockExecutors,
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
            instruction_files: _,
        } = fixture().await?;
        let (extra, instruction_names) =
            large_instruction_selection(&files, &issuer, &users[0]).await?;
        let proof_files = files.clone();
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
        let projection_limits = acyclic_harness_core::conversation::Limits {
            render_bytes: 64 * 1024,
            file_bytes: 64 * 1024,
            attachments: std::num::NonZeroUsize::new(1),
            ..Default::default()
        };
        let turns = StockExecutors::new(
            (
                Model::new("test".into(), "text".into(), "1".into())?,
                model.clone(),
            ),
            ToolRegistry::default(),
            (projection_limits, CompactionPolicy::Disabled),
            Arc::new(InstructionTools),
        )
        .with_agent_tools(&["wait".into()]);
        let mut turns = exposed(Arc::new(turns))?;
        turns.policy.instruction_names.extend(instruction_names);
        let driver = ExecutorDriver::new(FilesystemTurnHost::new(
            stream.clone(),
            within(),
            files.clone(),
            issuer,
            TurnAdmission::LocalRoot,
            Arc::new(turns),
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
        adoption.volumes = vec![tree(&users[0], &path), private, users[0].clone()];
        host.create(&intent("paused-root"), adoption).await?;
        change(&stream, &path, WorkspaceOp::Add, extra.clone(), &extra.name).await?;
        host.drain(&path).await?;
        let waiting = deliver(&host, &path, requested("pause-with-instructions", &path)).await?;
        if waiting.phase == Phase::Idle {
            let entries = super::super::values(stream.as_ref(), path.conversation()?).await?;
            let entry = ConversationEntry::decode(entries.last().ok_or("missing initial turn")?)?;
            return Err(format!(
                "instruction turn failed before parking: {:?}",
                entry.outcome
            )
            .into());
        }
        assert_eq!(
            waiting.phase,
            Phase::Waiting {
                deadline_unix_ms: Some(110),
                input: true
            }
        );
        let pinned = turn_announcements(&waiting)?.ok_or("missing pinned delta")?;
        let document = pinned
            .iter()
            .find(|document| document.kind == AnnouncementKind::Workspaces)
            .ok_or("missing workspace announcement")?;
        let selection = document
            .instructions
            .as_ref()
            .ok_or("missing instruction selection")?;
        assert!(
            selection.manifest.descriptor().byte_length() > MAX_RECORD_BYTES as u64,
            "the reported instruction selection must exceed the record envelope"
        );
        let entries = selected_instructions(&proof_files, document).await?;
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries.iter().map(|entry| entry.files.len()).sum::<usize>(),
            33
        );
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
        let rendered = requests[0]
            .messages
            .iter()
            .filter(|message| {
                message
                    .content
                    .file_refs()
                    .any(|file| *file == selection.content)
            })
            .flat_map(|message| message.content.parts())
            .filter_map(|part| match part {
                ModelContentPart::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        let (_custody, reader) = instruction_reader(&proof_files, selection)?;
        let original = reader
            .read(
                &selection.content,
                selection.content.descriptor().byte_length(),
                None,
                None,
            )?
            .await?;
        assert!(original.len() as u64 > projection_limits.render_bytes);
        assert_eq!(
            rendered.as_bytes(),
            original.as_ref(),
            "part boundaries must preserve every UTF-8 byte"
        );
        for request in &requests {
            for message in &request.messages {
                message
                    .content
                    .validate_limits(projection_limits, &mut None)?;
            }
        }
        assert!(
            requests[1]
                .messages
                .iter()
                .flat_map(|message| message.content.file_refs())
                .any(|file| pinned
                    .iter()
                    .filter_map(|document| document.instructions.as_ref())
                    .any(|selection| *file == selection.content))
        );
    }
    Ok(())
}
