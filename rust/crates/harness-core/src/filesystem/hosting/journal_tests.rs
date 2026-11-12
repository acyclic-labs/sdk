//! Exercise a retained inbox cut against actual actor commits, without a
//! timing hook: the next input settles before the original query resumes.
use super::*;
use crate::{
    agent::{AgentId, Control, InboxRecord, Phase},
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    filesystem::{PRIVATE_VOLUME, TurnAuthority},
};
use acyclic_stream::MemoryStream;

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
type Binding = Bound<MemoryStream, acyclic_fs::LogicalObjectStore<acyclic_objects::MemoryObjects>>;

struct NoTurns;
impl TurnExecutors for NoTurns {
    fn executor<'a>(
        &'a self,
        _: &'a crate::agent::AgentState,
        _: OperationId,
        _: &'a crate::model::ModelContent,
        _: &'a TurnAuthority,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Arc<dyn crate::executor::Executor>>>
    {
        Box::pin(async { Err(crate::Error::Unsupported("control-only fixture".into())) })
    }
}

async fn binding() -> TestResult<(Binding, AgentPath)> {
    let stream = Arc::new(MemoryStream::default());
    let provider = ProviderRef::new("fixture".into(), "filesystem".into(), "2".into(), &mut None)?;
    let bound = Bound::local_root(
        AuthorityRoot::new(
            "fixture",
            [7; 32],
            Authority {
                kind: crate::core::AggregateKind::Conversation,
                id: "local".into(),
            },
        ),
        Placement {
            filesystem: acyclic_fs::Fs::new(
                StreamAuthorityStore::new(stream),
                acyclic_fs::LogicalObjectStore::new(Arc::new(
                    acyclic_objects::MemoryObjects::memory(),
                )),
                acyclic_fs::EmbeddedCapabilities::MEMORY,
            ),
            provider: provider.clone(),
            within: ActorReference::new(b"account", b"agents", b"owner")?,
        },
        Turns {
            executors: Arc::new(NoTurns),
            limits: TurnLimits {
                maximum_bytes: 65_536,
                history_turns: 8,
            },
            actors: ActorLimits::new(10_000, 1 << 20, 1 << 16)?,
            clock: Arc::new(acyclic_actors::local::SystemClock),
            approval_policy: Default::default(),
        },
    )?;
    let id = AgentId::from_bytes([1; 16]);
    let RootSubmission::Created(root) = bound
        .create_root(
            &IntentId::try_from("root")?,
            RootRequest {
                peer: true,
                agent: id.clone(),
                fresh: vec![WorkspaceEntry::initial_volume(
                    crate::agent::WorkspaceRole::Private,
                    PRIVATE_VOLUME.into(),
                    VolumeRef::new(
                        provider,
                        "private".into(),
                        VolumeClass::AgentPrivate,
                        VolumeOwner::Agent(crate::AgentId::from_bytes(id.to_bytes())),
                        &mut None,
                    )?,
                )],
                retained: Vec::new(),
                brief: None,
                policy: None,
            },
        )
        .await?
    else {
        return Err("uncertain root".into());
    };
    bound.agents().drain(&root).await?;
    Ok((bound, root))
}

#[tokio::test]
async fn tool_result_reads_keep_the_settled_journal_cut() -> TestResult {
    use crate::{agent::TurnHost, executor::ExecutionJournal};
    let (bound, root) = binding().await?;
    let state = bound.agents().state(&root).await?;
    let journal = bound.turns().query_journal(&state).await?;
    let operation = OperationId::from_bytes([51; 16]);
    let result = journal
        .stage(
            operation,
            "tool:read:result",
            b"original",
            "text/plain",
            None,
        )?
        .await?;
    let output = bound
        .turns()
        .open_control(
            &state,
            &crate::agent::ContextAction::Clear,
            acyclic_actors::guest::CancellationToken::new(),
        )
        .await?
        .outputs
        .stage(operation, b"{}")
        .await?;
    assert_eq!(result.volume(), output.volume());
    assert_ne!(state.volumes[0].as_volume(), Some(output.volume()));
    let later = journal
        .stage(operation, "tool:later:result", b"later", "text/plain", None)?
        .await?;
    let (bytes, length) = bound
        .read_tool_output(&root, result.path(), Some(&output), 2, 3)
        .await?;
    assert_eq!(bytes, b"igi");
    assert_eq!(length, 8);
    assert!(
        bound
            .read_tool_output(&root, later.path(), Some(&output), 0, 32)
            .await
            .is_err()
    );
    assert!(
        bound
            .read_tool_output(&root, result.path(), None, 0, 32)
            .await
            .is_err()
    );
    assert_eq!(
        bound.tool_resource(&root).await?,
        bound.volumes().tool_resource(output.volume()).await?
    );
    Ok(())
}

#[tokio::test]
async fn input_previews_pin_pending_messages_without_reading_the_whole_body() -> TestResult {
    use crate::agent::{InputPreview, InputPreviewStep, Message};
    use futures::StreamExt as _;
    let (bound, root) = binding().await?;
    let by = Sender::Host {
        host: "preview".into(),
    };
    let first = IntentId::try_from("first")?;
    let text = format!("a🙂{}", "unread".repeat(4096));
    let file = bound
        .stage_text(&root, "message", &by, &first, ("preview", &text))
        .await?;
    bound
        .agents()
        .send(
            &root,
            &by,
            &InboxRecord::Message(Message {
                from: by.clone(),
                id: first,
                re: None,
                body: Box::new(file),
                attachments: None,
            }),
        )
        .await?;
    bound
        .agents()
        .send(
            &root,
            &by,
            &InboxRecord::Cancel(Control {
                by: by.clone(),
                id: IntentId::try_from("filtered")?,
            }),
        )
        .await?;
    let original = bound.input_preview_steps(root.clone(), 3);
    futures::pin_mut!(original);
    assert_eq!(
        original.next().await.transpose()?,
        Some(InputPreviewStep::Progress)
    );
    // A newer accepted prompt must not change the cut retained by the query.
    let later = IntentId::try_from("later")?;
    let file = bound
        .stage_text(&root, "message", &by, &later, ("preview", "newer"))
        .await?;
    bound
        .agents()
        .send(
            &root,
            &by,
            &InboxRecord::FollowUp(Message {
                from: by.clone(),
                id: later,
                re: None,
                body: Box::new(file),
                attachments: None,
            }),
        )
        .await?;
    assert_eq!(
        original.next().await.transpose()?,
        Some(InputPreviewStep::Selected(Some(InputPreview {
            text: "a".into(),
            bytes: text.len() as u64,
        })))
    );
    assert!(original.next().await.is_none());
    let fresh = bound.input_preview_steps(root, 5);
    futures::pin_mut!(fresh);
    assert_eq!(
        fresh.next().await.transpose()?,
        Some(InputPreviewStep::Selected(Some(InputPreview {
            text: "newer".into(),
            bytes: 5,
        })))
    );
    Ok(())
}

#[tokio::test]
async fn settled_arrivals_and_archive_do_not_end_a_retained_inbox_cut_early() -> TestResult {
    for archive in [false, true] {
        let (bound, root) = binding().await?;
        let agents = bound.agents();
        let observed = agents.state(&root).await?;
        let cut = crate::agent::inbox_tail(&**agents.provider(), &root).await?;
        let by = Sender::Host {
            host: "query-race".into(),
        };
        assert!(
            matches!(agents.send(&root, &by, &InboxRecord::Cancel(Control {
            by: by.clone(), id: IntentId::try_from("arrived")?,
        })).await?, Delivered::Committed { inbox } if inbox == cut)
        );
        agents.drain(&root).await?;
        let progress = agents.input_progress(&root, cut).await?;
        assert!(progress.settled);
        assert!(!progress.closed);
        assert!(matches!(agents.state(&root).await?.phase, Phase::Idle));
        if archive {
            agents
                .archive(&root, &by, &IntentId::try_from("archive")?)
                .await?;
            agents.drain(&root).await?;
        }
        // Resume the actual query after both the new input and its actor step.
        // The archive case must re-read the tail after observing closure.
        let (input, tail, _, closed, _) = bound
            .journal_from_input::<Usage>(&observed, cut, cut, 0, 0, (true, Some(0)))
            .await?;
        assert_eq!(input, cut);
        assert!(
            tail > cut,
            "the settled arrival was lost behind the captured tail"
        );
        assert!(
            !closed,
            "settlement was mistaken for an empty closed producer"
        );
        let (_, page, settled, _) = bound.usage_page(&root, cut, 0, false, None).await?;
        assert!(page.is_some() && settled);
        let (_, _, page, settled) = bound.event_page(&root, cut, 0, false).await?;
        assert!(page.is_some() && settled);
        if archive {
            let tail = crate::agent::inbox_tail(&**agents.provider(), &root).await?;
            let (_, page, closed, _) = bound.usage_page(&root, tail, 0, true, Some(0)).await?;
            assert!(page.is_none() && closed);
        }
    }
    Ok(())
}
