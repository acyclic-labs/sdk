//! Reproducers for aliases, live session updates and transcript checkpoint growth.
use super::*;
use acyclic_harness::{
    AgentId,
    client::{FileClientState, ViewUpdate},
    conversation::{
        ConversationMessage, FileDescriptor, FileRef, MessageKind, ReferencedAttachments,
        VolumeClass, VolumeOwner, VolumeRef,
    },
    model::ModelEvent,
    resources::ProviderRef,
};
use futures::{FutureExt, StreamExt};
use prost::Message;

#[cfg(unix)]
#[test]
fn journal_aliases_share_the_writer_lock_and_checkpoint_target() {
    use std::os::unix::fs::symlink;
    let directory = tempfile::tempdir().unwrap();
    let real = directory.path().join("real");
    std::fs::create_dir(&real).unwrap();
    let path = real.join("client.journal");
    let directory_alias = directory.path().join("directory-alias");
    symlink(&real, &directory_alias).unwrap();
    // New-file aliases must choose the same lock before either data file exists.
    let writer = FileClientState::open(&directory_alias.join("client.journal")).unwrap();
    assert!(matches!(
        FileClientState::open(&path),
        Err(Error::Conflict(_))
    ));
    drop(writer);
    let file_alias = directory.path().join("file-alias");
    symlink(&path, &file_alias).unwrap();
    for alias in [&file_alias, &directory_alias.join("client.journal"), &path] {
        let mut writer = ClientSession::open(FileClientState::open(alias).unwrap()).unwrap();
        writer.queue(command()).unwrap();
        for candidate in [&path, &file_alias, &directory_alias.join("client.journal")] {
            assert!(matches!(
                FileClientState::open(candidate),
                Err(Error::Conflict(_))
            ));
        }
        writer.checkpoint().unwrap();
        assert!(
            file_alias
                .symlink_metadata()
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(matches!(
            FileClientState::open(&file_alias),
            Err(Error::Conflict(_))
        ));
        drop(writer);
        let recovered = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
        assert_eq!(
            recovered.pending().cloned().collect::<Vec<_>>(),
            [command()]
        );
    }
    let dangling = directory.path().join("dangling");
    symlink(directory.path().join("absent"), &dangling).unwrap();
    assert!(FileClientState::open(&dangling).is_err());
    let hard_link = directory.path().join("hard-link");
    std::fs::hard_link(&path, &hard_link).unwrap();
    assert!(FileClientState::open(&path).is_err());
    assert!(FileClientState::open(&hard_link).is_err());
}

#[tokio::test]
async fn incoming_model_and_tool_streams_update_session_owned_views() {
    let provider = Arc::new(acyclic_stream::MemoryStream::default());
    let model = acyclic_stream::Stream::new(provider.clone(), "client/model").unwrap();
    let tools = acyclic_stream::Stream::new(provider, "client/tool-output").unwrap();
    // Establish the streams before following; later appends must wake the reader.
    model
        .append(
            serde_json::to_vec(&ModelEvent::Content {
                delta: "first".into(),
            })
            .unwrap(),
        )
        .await
        .unwrap();
    tools.append(b"stdout\n".to_vec()).await.unwrap();
    let mut model_source = model.follow(0).await.unwrap().map(|record| {
        let record = record?;
        Ok(ViewUpdate::Model {
            authority: owner(),
            event: serde_json::from_slice(&record.value)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        })
    });
    let mut tool_source = tools.follow(0).await.unwrap().map(|record| {
        let record = record?;
        Ok(ViewUpdate::ToolOutput {
            authority: owner(),
            call_id: "call".into(),
            delta: String::from_utf8(record.value.to_vec()).unwrap(),
        })
    });
    let mut client = HarnessClient::connect(Arc::new(Host::default()), session())
        .await
        .unwrap();
    assert!(client.next_update(&mut model_source).await.unwrap());
    assert!(
        client
            .next_update(&mut model_source)
            .now_or_never()
            .is_none()
    );
    for event in [
        ModelEvent::Content {
            delta: " second".into(),
        },
        ModelEvent::Reasoning {
            delta: "thinking".into(),
        },
        ModelEvent::ToolCall {
            call_id: "call".into(),
            name: "bash".into(),
            arguments: serde_json::json!({"command":"pwd"}),
        },
        ModelEvent::Completed {
            metadata: serde_json::json!({"tokens":2}),
        },
        ModelEvent::Completed {
            metadata: serde_json::json!({"tokens":3}),
        },
    ] {
        model
            .append(serde_json::to_vec(&event).unwrap())
            .await
            .unwrap();
        assert!(
            client
                .next_update(&mut model_source)
                .now_or_never()
                .expect("push must wake renderer")
                .unwrap()
        );
    }
    assert!(client.next_update(&mut tool_source).await.unwrap());
    tools.append(b"stderr\n".to_vec()).await.unwrap();
    assert!(
        client
            .next_update(&mut tool_source)
            .now_or_never()
            .unwrap()
            .unwrap()
    );
    let views = client.session().views();
    let key = "AGGREGATE_KIND_TASK:task";
    assert_eq!(views.transcripts[key].streaming_text, "first second");
    assert_eq!(views.transcripts[key].reasoning, "thinking");
    assert_eq!(views.tools[key]["call"].name, "bash");
    assert_eq!(views.tools[key]["call"].output, "stdout\nstderr\n");
    assert_eq!(views.usage[key], serde_json::json!({"tokens":5}));
    // Streaming and committed replay share one session, without journaling tails.
    client.replay().await.unwrap();
    client.next_delivery().await.unwrap();
    assert_eq!(
        client.session().views().transcripts[key].streaming_text,
        "first second"
    );
    assert_eq!(
        client.session().views().tasks[key].status,
        LifecycleState::Completed
    );
    let mut state = client.into_session();
    let views = state.views().clone();
    let mut untracked = owner();
    untracked.id = "untracked".into();
    assert!(
        state
            .model_event(
                &untracked,
                ModelEvent::Content {
                    delta: "bad".into()
                }
            )
            .is_err()
    );
    assert!(state.tool_output(&untracked, "call", "bad").is_err());
    assert_eq!(state.views(), &views);
    assert_eq!(state.into_store().load().unwrap().len(), 2);
}

fn message_delivery(revision: u64) -> wire::Delivery {
    let content = FileRef::new(
        VolumeRef::new(
            ProviderRef::new("test".into(), "memory".into(), "1".into(), &mut None).unwrap(),
            "conversation".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([7; 16])),
            &mut None,
        )
        .unwrap(),
        "message.txt".into(),
        "generation".into(),
        FileDescriptor::from_bytes(b"message", "text/plain", &mut None).unwrap(),
        "message.txt".into(),
        &mut None,
    )
    .unwrap();
    let message = ConversationMessage {
        id: uuid::Uuid::from_u128(u128::from(revision + 1)),
        sequence: revision + 1,
        kind: MessageKind::Assistant,
        content,
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: None,
        tool_call_id: None,
        extensions: Default::default(),
    };
    let mut delivery = delivery(revision, LifecycleState::Active);
    delivery.events[0].event_type = "conversation_message_appended".into();
    delivery.events[0].canonical_payload_json =
        serde_json::to_vec(&EventPayload::ConversationMessageAppended {
            message: Box::new(message),
        })
        .unwrap();
    delivery
}

#[derive(Default)]
struct MeasuredStore {
    inner: MemoryClientState,
    checkpoint_bytes: usize,
    checkpoint_count: usize,
}
impl ClientStateStore for MeasuredStore {
    fn load(&mut self) -> Result<Vec<SessionRecord>> {
        self.inner.load()
    }
    fn append(&mut self, record: &SessionRecord) -> Result<()> {
        self.inner.append(record)
    }
    fn replace(&mut self, record: &SessionRecord) -> Result<()> {
        self.checkpoint_bytes += record.encoded_len();
        self.checkpoint_count += 1;
        self.inner.replace(record)
    }
}

#[test]
fn checkpoint_work_is_linear_and_replay_rebuilds_disposable_transcripts() {
    let mut totals = Vec::new();
    for count in [256, 512] {
        let mut state = ClientSession::open(MeasuredStore::default()).unwrap();
        state.track(session().resume().cursors.remove(0)).unwrap();
        state.queue(command()).unwrap();
        for revision in 0..count {
            state.receive(frame(message_delivery(revision))).unwrap();
        }
        let expected = state.views().clone();
        assert_eq!(
            expected.transcripts["AGGREGATE_KIND_TASK:task"]
                .messages
                .len(),
            count as usize
        );
        let mut store = state.into_store();
        assert!(store.load().unwrap().len() <= 64);
        assert_eq!(store.checkpoint_count, count as usize / 64);
        totals.push(store.checkpoint_bytes);
        assert!(
            store.checkpoint_bytes < count as usize * 8,
            "checkpoint includes transcript bytes"
        );
        let mut recovered = ClientSession::open(store).unwrap();
        assert_eq!(recovered.resume().cursors[0].revision, count);
        assert_eq!(recovered.replay_request().cursors[0].revision, 0);
        assert_eq!(recovered.views(), &Default::default());
        for revision in 0..count {
            recovered
                .receive(frame(message_delivery(revision)))
                .unwrap();
            assert_eq!(recovered.resume().cursors[0].revision, count);
        }
        // An overlapping replay must not duplicate messages or re-journal history.
        recovered
            .receive(frame(message_delivery(count - 1)))
            .unwrap();
        assert_eq!(recovered.views(), &expected);
        assert_eq!(recovered.replay_request().cursors[0].revision, count);
        assert_eq!(
            recovered.pending().cloned().collect::<Vec<_>>(),
            [command()]
        );
        let mut store = recovered.into_store();
        assert!(store.load().unwrap().len() <= 64);
        assert_eq!(store.checkpoint_bytes, *totals.last().unwrap());
    }
    assert!(
        totals[1] <= totals[0] * 2 + 32,
        "checkpoint bytes: {totals:?}"
    );
}

#[test]
fn rehydration_accepts_a_page_crossing_the_durable_cursor_without_duplication() {
    let mut state = session();
    state.receive(frame(message_delivery(0))).unwrap();
    state.checkpoint().unwrap();
    let mut state = ClientSession::open(state.into_store()).unwrap();
    let mut page = message_delivery(0);
    page.through_revision = 2;
    page.events.extend(message_delivery(1).events);
    state.receive(frame(page.clone())).unwrap();
    assert_eq!(state.resume().cursors[0].revision, 2);
    assert_eq!(state.view_revision(&owner()), 2);
    state.receive(frame(page)).unwrap();
    assert_eq!(
        state.views().transcripts["AGGREGATE_KIND_TASK:task"]
            .messages
            .len(),
        2
    );
    let mut store = state.into_store();
    assert_eq!(store.load().unwrap().len(), 2);
    let mut state = ClientSession::open(store).unwrap();
    for revision in 0..2 {
        state.receive(frame(message_delivery(revision))).unwrap();
    }
    assert_eq!(
        state.views().transcripts["AGGREGATE_KIND_TASK:task"]
            .messages
            .len(),
        2
    );
}
