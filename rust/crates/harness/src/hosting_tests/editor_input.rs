use super::*;
use crate::{
    agent::{TurnInput, expand_attachments, model_input},
    conversation::{ContentAccess, ContentPublisher as _, ContentResidencyVerifier as _, FileRef},
    filesystem::{FilesystemContentPublisher, FilesystemContentVerifier, agent_authority},
    model::{FileProjectionPolicy, ModelContentPart},
};
use acyclic_harness_client::{
    AgentBinding, AgentControls, AgentSelection,
    acp::{AttachedSession, ContentBlock, PromptRequest},
};
use acyclic_stream::{ReadRequest, StreamProvider as _};
use futures::TryStreamExt as _;

#[tokio::test]
async fn editor_prompts_reach_authenticated_inbox_with_ordered_pinned_content() -> Result {
    let inherited = Arc::new(InheritedEditorInput::default());
    let bound = binding_with(inherited.clone(), Default::default())?;
    let agent = AgentId::from_bytes([6; 16]);
    let private = VolumeRef::new(
        provider()?,
        "editor-private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(crate::AgentId::from_bytes(agent.to_bytes())),
        &mut None,
    )?;
    let owner = Arc::new(Owner::default());
    let Serving::Served(server) = serve(
        bound,
        owner.clone(),
        &IntentId::try_from("editor-root")?,
        RootRequest {
            agent,
            fresh: vec![VolumeBinding {
                name: PRIVATE_VOLUME.into(),
                volume: private,
            }],
            retained: Vec::new(),
            brief: None,
            policy: None,
        },
    )
    .await?
    else {
        return Err("root publication was uncertain".into());
    };
    let bound = server.bound();
    let root = server.root();
    let state = bound.agents().state(root).await?;
    let authority =
        agent_authority(bound.issuer(), bound.admission(), root, &state.volumes, &[]).await?;
    let publisher = FilesystemContentPublisher::new(
        bound.volumes().clone(),
        &authority.private.volume,
        &authority.verifier,
        &authority.scope,
        64 * 1024,
        None,
    )?;
    let (file, _) = publisher
        .stage_once(
            OperationId::from_bytes([7; 16]),
            "editor/document",
            b"original revision",
            "text/plain",
            "document",
            ContentAccess::default(),
        )?
        .await?;
    let (sibling, _) = publisher
        .stage_once(
            OperationId::from_bytes([8; 16]),
            "editor/sibling",
            b"unshared private file",
            "text/plain",
            "sibling",
            ContentAccess::default(),
        )?
        .await?;
    *inherited
        .files
        .lock()
        .map_err(|_| "editor input control poisoned")? = Some((file.clone(), sibling));
    let reader = FilesystemContentVerifier::new(
        bound.volumes().clone(),
        &authority.verifier,
        &authority.scope,
        64 * 1024,
        None,
    )?;
    let client = Client::in_process_with_products(
        bound.agents().provider().clone(),
        acyclic_stream::session::testing::authority(),
        None,
        server.clone(),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, credential(server.resource()))?;
    let controls = AgentControls::new(
        client.clone(),
        0,
        AgentBinding::new(&server.binding().encode_to_vec())?,
    );
    let editor = AttachedSession::new(
        controls,
        AgentSelection {
            agent: root.clone(),
            conversation_from: 0,
            status_from: 0,
        },
    )?;
    for (intent, prompt, expected) in [
        (
            "editor-text",
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
            ModelContent::Parts(vec![ModelContentPart::Text {
                text: "hello".into(),
            }]),
        ),
        (
            "editor-file",
            vec![
                ContentBlock::Text {
                    text: "before".into(),
                },
                ContentBlock::ResourceLink {
                    uri: "editor://document".into(),
                    name: "document".into(),
                },
                ContentBlock::Text {
                    text: "after".into(),
                },
            ],
            ModelContent::Parts(vec![
                ModelContentPart::Text {
                    text: "before".into(),
                },
                ModelContentPart::File {
                    file: file.clone(),
                    policy: FileProjectionPolicy::for_attachment(&file),
                },
                ModelContentPart::Text {
                    text: "after".into(),
                },
            ]),
        ),
    ] {
        let id = IntentId::try_from(intent)?;
        let prepared = editor
            .prepare_prompt(
                id.clone(),
                PromptRequest {
                    session_id: root.to_string(),
                    prompt,
                },
                |_| std::future::ready(Ok::<_, std::convert::Infallible>(file.clone())),
            )
            .await?;
        let AgentReply::Delivered {
            delivery: Delivered::Committed { inbox },
        } = editor.controls().execute(&prepared).await?
        else {
            return Err("prompt did not commit".into());
        };
        assert!(matches!(editor.controls().execute(&prepared).await?,
            AgentReply::Delivered { delivery: Delivered::Committed { inbox: replay } } if replay == inbox));
        let mut records = bound
            .agents()
            .provider()
            .read(ReadRequest {
                key: None,
                path: root.inbox()?,
                from: inbox,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        let record = records
            .try_next()
            .await?
            .ok_or("committed prompt missing")?;
        assert_eq!(record.sequence, inbox);
        let InboxRecord::Message(message) = InboxRecord::decode(&record.value)? else {
            return Err("prompt did not retain a message".into());
        };
        assert_eq!(message.id, id);
        let input = model_input(&TurnInput::Message {
            from: message.from,
            id: message.id,
            body: message.body,
            attachments: message.attachments,
        })?;
        assert_eq!(expand_attachments(input, reader.as_ref()).await?, expected);
        assert!(
            call(
                &client,
                "agents",
                &AgentCommand::Prompt {
                    agent: root.clone(),
                    id,
                    body: ModelContent::Parts(vec![ModelContentPart::Text {
                        text: "changed".into()
                    }]),
                }
            )
            .await?
            .is_err(),
            "changed input reused a committed intent"
        );
    }
    assert_eq!(
        reader.read(&file, 64 * 1024, None, None)?.await?.as_ref(),
        b"original revision"
    );
    // Drive the original actors to completion without a model, timer or
    // readiness loop. The real namespace/volume fork inherits both settled
    // editor prompts, but grants only the file the structured prompt names.
    bound
        .agents()
        .runtime()
        .drain_actor(&bound.agents().actor(root)?)
        .await?;
    let by = Sender::Host {
        host: "editor-control".into(),
    };
    let id = IntentId::try_from("editor-fork")?;
    let brief = bound
        .stage_text(root, "brief", &by, &id, ("child", "child"))
        .await?;
    let Delivered::Committed { inbox } = bound
        .agents()
        .send(
            root,
            &by,
            &InboxRecord::Spawn(crate::agent::SpawnRequest {
                by: by.clone(),
                id,
                brief: Box::new(brief),
            }),
        )
        .await?
    else {
        return Err("spawn input did not commit".into());
    };
    bound
        .agents()
        .runtime()
        .drain_actor(&bound.agents().actor(root)?)
        .await?;
    let (intent, _) = crate::agent::requested_child(root, inbox)?;
    let crate::agent::SpawnOutcome::Spawned { child } =
        bound.agents().spawned(root, inbox, &intent).await?
    else {
        return Err("editor child was refused".into());
    };
    bound
        .agents()
        .runtime()
        .drain_actor(&bound.agents().actor(&child)?)
        .await?;
    assert!(
        inherited.observed.load(Ordering::SeqCst),
        "child never executed the inherited ordered editor input"
    );
    let foreign = FileRef::new(
        VolumeRef::new(
            provider()?,
            "foreign".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([9; 16])),
            &mut None,
        )?,
        file.path().into(),
        file.version().into(),
        file.descriptor().clone(),
        file.display_name().into(),
        &mut None,
    )?;
    let command = |id: &str, file| -> Result<_> {
        Ok(AgentCommand::Prompt {
            agent: root.clone(),
            id: IntentId::try_from(id)?,
            body: ModelContent::Part(ModelContentPart::File {
                file,
                policy: FileProjectionPolicy::BoundedFull,
            }),
        })
    };
    assert!(
        call(&client, "agents", &command("foreign-file", foreign)?)
            .await?
            .is_err()
    );
    owner.denied.store(true, Ordering::SeqCst);
    assert!(
        call(&client, "agents", &command("denied-file", file)?)
            .await
            .is_err()
    );
    Ok(())
}

#[derive(Default)]
struct InheritedEditorInput {
    files: std::sync::Mutex<Option<(FileRef, FileRef)>>,
    observed: Arc<AtomicBool>,
}

impl TurnExecutors for InheritedEditorInput {
    fn executor<'a>(
        &'a self,
        agent: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, crate::Result<Arc<dyn Executor>>> {
        Box::pin(async move {
            let expected = if agent.parent.is_some() {
                let (file, sibling) = self
                    .files
                    .lock()
                    .map_err(|_| crate::Error::Conflict("editor input control poisoned".into()))?
                    .clone()
                    .ok_or_else(|| crate::Error::Invalid("editor files absent".into()))?;
                let reader = &authority.content.reader;
                assert_eq!(
                    reader.read(&file, 64 * 1024, None, None)?.await?.as_ref(),
                    b"original revision"
                );
                assert!(
                    async { reader.read(&sibling, 64 * 1024, None, None)?.await }
                        .await
                        .is_err(),
                    "an exact inherited file grant exposed a sibling"
                );
                assert!(
                    crate::conversation::ContentGrant::verify(
                        &authority.issuer.verifier(&mut None)?,
                        &authority.scope,
                        file.volume(),
                        crate::conversation::VolumeOperation::Write,
                        u64::MAX,
                        &mut None,
                    )
                    .is_err(),
                    "an inherited read grant permitted a write"
                );
                Some(file)
            } else {
                None
            };
            Ok(Arc::new(EditorTurn {
                expected,
                observed: self.observed.clone(),
            }) as Arc<dyn Executor>)
        })
    }
}

struct EditorTurn {
    expected: Option<FileRef>,
    observed: Arc<AtomicBool>,
}

impl Executor for EditorTurn {
    fn execute<'a>(
        &'a self,
        input: crate::executor::TurnInput,
        _: &'a dyn crate::executor::ExecutionJournal,
    ) -> BoxProviderFuture<'a, crate::Result<crate::executor::TurnOutput>> {
        Box::pin(async move {
            if let Some(file) = &self.expected {
                let expected = ModelContent::Parts(vec![
                    ModelContentPart::Text {
                        text: "before".into(),
                    },
                    ModelContentPart::File {
                        file: file.clone(),
                        policy: FileProjectionPolicy::for_attachment(file),
                    },
                    ModelContentPart::Text {
                        text: "after".into(),
                    },
                ]);
                assert!(
                    input
                        .selected_context
                        .as_ref()
                        .is_some_and(|selected| selected
                            .messages
                            .iter()
                            .any(|message| message.content == expected)),
                    "child selection lost the inherited ordered prompt or pinned file identity"
                );
                self.observed.store(true, Ordering::SeqCst);
            }
            Ok(crate::executor::TurnOutput {
                text: "settled".into(),
                attachments: Vec::new(),
                metadata: serde_json::Value::Null,
                steps: 0,
            })
        })
    }
}
