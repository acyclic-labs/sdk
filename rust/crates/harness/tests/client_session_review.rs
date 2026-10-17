//! Reproducers for aliases, live session updates and transcript checkpoint growth.
use super::*;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness::client::FileClientState;
use acyclic_harness::{
    AgentId,
    conversation::{
        ConversationMessage, FileDescriptor, FileRef, MessageKind, ReferencedAttachments,
        VolumeClass, VolumeOwner, VolumeRef,
    },
    resources::ProviderRef,
};
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
fn checkpoint_work_is_linear_and_replay_rebuilds_operation_progress() {
    let mut totals = Vec::new();
    for count in [256, 512] {
        let mut state = ClientSession::open(MeasuredStore::default()).unwrap();
        state.track(session().resume().cursors.remove(0)).unwrap();
        state.queue(command()).unwrap();
        for revision in 0..count {
            state.receive(frame(message_delivery(revision))).unwrap();
        }
        let expected = state.replay_revision(&owner());
        assert_eq!(expected, count);
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
        assert_eq!(recovered.replay_revision(&owner()), 0);
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
        assert_eq!(recovered.replay_revision(&owner()), expected);
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
    assert_eq!(state.replay_revision(&owner()), 2);
    state.receive(frame(page)).unwrap();
    assert_eq!(state.replay_revision(&owner()), 2);
    let mut store = state.into_store();
    assert_eq!(store.load().unwrap().len(), 2);
    let mut state = ClientSession::open(store).unwrap();
    for revision in 0..2 {
        state.receive(frame(message_delivery(revision))).unwrap();
    }
    assert_eq!(state.replay_revision(&owner()), 2);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn journal_sidecars_cannot_be_opened_as_journals_or_overwrite_existing_data() {
    for suffix in ["lock", "checkpoint"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state");
        let sidecar = directory.path().join(format!("state.{suffix}"));
        let mut writer = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
        writer.queue(command()).unwrap();
        assert!(matches!(
            FileClientState::open(&sidecar),
            Err(Error::Invalid(_))
        ));
        if suffix == "checkpoint" {
            // A caller-owned file at the former temp name must never be truncated.
            std::fs::write(&sidecar, b"another journal's committed command").unwrap();
        }
        let before = std::fs::read(&sidecar).unwrap();
        writer.checkpoint().unwrap();
        assert_eq!(std::fs::read(&sidecar).unwrap(), before);
        assert!(matches!(
            FileClientState::open(&path),
            Err(Error::Conflict(_))
        ));
        drop(writer);
        let recovered = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
        assert_eq!(
            recovered.pending().cloned().collect::<Vec<_>>(),
            [command()]
        );
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    let lock = directory.path().join("state.lock");
    std::fs::write(&lock, b"a pre-existing journal").unwrap();
    assert!(FileClientState::open(&path).is_err());
    assert_eq!(std::fs::read(lock).unwrap(), b"a pre-existing journal");
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn long_journal_names_survive_automatic_checkpoint_and_reopen() {
    for name in ["a".repeat(220), "b".repeat(250), "\u{e9}".repeat(110)] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(name);
        let mut writer = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
        writer
            .track(wire::ReplayCursor {
                authority: Some(owner()),
                generation: "generation".into(),
                revision: 0,
            })
            .unwrap();
        writer.queue(command()).unwrap();
        for revision in 0..64 {
            writer
                .receive(frame(delivery(revision, LifecycleState::Active)))
                .unwrap();
        }
        drop(writer);
        let mut store = FileClientState::open(&path).unwrap();
        assert!(
            store.load().unwrap().len() < 64,
            "automatic checkpoint was skipped"
        );
        let recovered = ClientSession::open(store).unwrap();
        assert_eq!(
            recovered.pending().cloned().collect::<Vec<_>>(),
            [command()]
        );
        assert_eq!(recovered.resume().cursors[0].revision, 64);
    }
}

proptest::proptest! {
    // Property: all portable spellings of reserved suffixes fail before creating files.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn journal_reserved_suffixes_are_case_and_alias_independent(
        name in "[a-zA-Z0-9_-]{1,24}", upper in proptest::collection::vec(proptest::bool::ANY, 11),
        checkpoint in proptest::bool::ANY, trailing in proptest::sample::select(vec!["", ".", " ", ". "]),
    ) {
        let suffix = if checkpoint { ".checkpoint" } else { ".lock" };
        let suffix: String = suffix.chars().zip(upper).map(|(ch, upper)| if upper { ch.to_ascii_uppercase() } else { ch }).collect();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(format!("{name}{suffix}{trailing}"));
        proptest::prop_assert!(matches!(FileClientState::open(&path), Err(Error::Invalid(_))));
        proptest::prop_assert!(!path.exists());
    }

    // Property: every admitted basename within NAME_MAX leaves room for checkpoint files.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn checkpoint_names_are_independent_of_journal_name_length(name in "[a-zA-Z0-9_-]{1,250}") {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join(name);
        let mut session = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
        session.queue(command()).unwrap();
        session.checkpoint().unwrap();
        drop(session);
        let recovered = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
        proptest::prop_assert_eq!(recovered.pending().cloned().collect::<Vec<_>>(), vec![command()]);
    }

    // Property: file framing and canonical protobuf JSON preserve every admitted field.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn client_frame_and_journal_record_roundtrip(
        id in ".{0,32}", generation in ".{0,32}", revision in proptest::num::u64::ANY,
        kind in 0i32..6,
    ) {
        let request = wire::ClientFrame { frame: Some(wire::client_frame::Frame::Acknowledge(wire::Acknowledge {
            authority: Some(wire::Authority { kind, id }), generation, through_revision: revision,
        })) };
        let encoded = request.encode_to_vec();
        proptest::prop_assert_eq!(wire::ClientFrame::decode(encoded.as_slice()).unwrap(), request.clone());
        let record = SessionRecord { request: Some(request), response: None, checkpoint: None };
        let directory = tempfile::tempdir().unwrap();
        let mut store = FileClientState::open(&directory.path().join("state")).unwrap();
        store.append(&record).unwrap();
        proptest::prop_assert_eq!(store.load().unwrap(), vec![record]);
    }
}
