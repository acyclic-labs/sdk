use super::*;
use acyclic_harness::client::FileClientState;
use std::fs::OpenOptions;

struct CheckpointStore {
    inner: MemoryClientState,
    fail: Arc<AtomicBool>,
    after_replace: bool,
}
impl ClientStateStore for CheckpointStore {
    fn load(&mut self) -> Result<Vec<SessionRecord>> {
        self.inner.load()
    }
    fn append(&mut self, record: &SessionRecord) -> Result<()> {
        self.inner.append(record)
    }
    fn replace(&mut self, record: &SessionRecord) -> Result<()> {
        if !self.fail.load(Ordering::Relaxed) {
            return self.inner.replace(record);
        }
        if self.after_replace {
            self.inner.replace(record)?;
        }
        Err(Error::Storage("checkpoint failed after append".into()))
    }
}

fn next_command() -> wire::CommandEnvelope {
    let mut next = command();
    next.operation = Some(wire::OperationIdentity {
        operation_id: "00000000-0000-0000-0000-000000000002".into(),
        idempotency_key: "submit-2".into(),
    });
    next
}

fn admission_boundary(padding: usize) -> ClientSession<MemoryClientState> {
    let mut state = session();
    state.queue(command()).unwrap();
    for index in 0..padding {
        let mut cursor = state.resume().cursors.remove(0);
        cursor.authority.as_mut().unwrap().id = format!("zz-padding-{index}");
        state.track(cursor).unwrap();
    }
    state
}

async fn stdin_recovers_both_admissions(first_id: u128, second_id: u128) -> Vec<wire::Admission> {
    let host = Arc::new(Host::default());
    host.requires_admission.store(true, Ordering::Relaxed);
    let mut first = command();
    first.operation.as_mut().unwrap().operation_id = uuid::Uuid::from_u128(first_id).to_string();
    let mut second = next_command();
    second.operation.as_mut().unwrap().operation_id = uuid::Uuid::from_u128(second_id).to_string();
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    let mut admissions = Vec::new();
    *host.admission_state.lock().unwrap() = Some(wire::AdmissionState::Indeterminate);
    client
        .submit(first.clone(), |response| {
            admissions.push(response);
            Ok(())
        })
        .await
        .unwrap();
    *host.admission_state.lock().unwrap() = Some(wire::AdmissionState::Accepted);
    client
        .submit(second.clone(), |response| {
            admissions.push(response);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(client.session().pending().count(), 0);
    assert_eq!(
        host.commands.lock().unwrap().as_slice(),
        &[first, command_with_id(first_id), second]
    );
    admissions
}

fn command_with_id(id: u128) -> wire::CommandEnvelope {
    let mut command = command();
    command.operation.as_mut().unwrap().operation_id = uuid::Uuid::from_u128(id).to_string();
    command
}

#[tokio::test]
async fn stdin_emits_a_recovered_predecessor_before_the_new_commands_admission() {
    let first = command_with_id(1);
    let second = next_command();
    assert_eq!(
        stdin_recovers_both_admissions(1, 2).await,
        [
            admission(&first, wire::AdmissionState::Indeterminate),
            admission(&first, wire::AdmissionState::Accepted),
            admission(&second, wire::AdmissionState::Accepted),
        ]
    );
}

#[tokio::test]
async fn admission_checkpoint_failure_keeps_intent_for_retry() {
    for after_replace in [false, true] {
        for reopen in [false, true] {
            let fail = Arc::new(AtomicBool::new(true));
            let state = ClientSession::open(CheckpointStore {
                inner: admission_boundary(61).into_store(),
                fail: fail.clone(),
                after_replace,
            })
            .unwrap();
            let host = Arc::new(Host::default());
            let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
            let mut admissions = Vec::new();
            assert!(
                client
                    .submit(command(), |response| {
                        admissions.push(response);
                        Ok(())
                    })
                    .await
                    .is_err()
            );
            assert_eq!(
                client.session().pending().cloned().collect::<Vec<_>>(),
                [command()]
            );
            assert!(admissions.is_empty());
            let state = client.into_session();
            let state = if reopen {
                ClientSession::open(state.into_store()).unwrap()
            } else {
                state
            };
            fail.store(false, Ordering::Relaxed);
            let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
            let mut admissions = Vec::new();
            client
                .submit(command(), |response| {
                    admissions.push(response);
                    Ok(())
                })
                .await
                .unwrap();
            assert_eq!(
                admissions,
                [admission(&command(), wire::AdmissionState::Accepted)]
            );
            assert_eq!(
                host.commands.lock().unwrap().as_slice(),
                &[command(), command()]
            );
            assert_eq!(client.session().pending().count(), 0);
        }
    }
}

proptest::proptest! {
    // Property: every produced admission is emitted once and in order, including
    // definitive recovery of an older indeterminate command during a later submit.
    #[test]
    fn stdin_admission_stream_preserves_both_operation_identities(first_id in 1u128..=u128::MAX - 1) {
        let responses = futures::executor::block_on(stdin_recovers_both_admissions(first_id, first_id + 1));
        let ids: Vec<_> = responses.iter().map(|response| response.operation.as_ref().unwrap().operation_id.as_str()).collect();
        let first = uuid::Uuid::from_u128(first_id).to_string();
        let second = uuid::Uuid::from_u128(first_id + 1).to_string();
        proptest::prop_assert_eq!(ids, [first.as_str(), first.as_str(), second.as_str()]);
        proptest::prop_assert_eq!(responses.iter().map(|response| response.state).collect::<Vec<_>>(), [wire::AdmissionState::Indeterminate as i32, wire::AdmissionState::Accepted as i32, wire::AdmissionState::Accepted as i32]);
    }

    // Property: admission failure leaves retry intent recoverable, regardless of
    // record position, decision, or a replacement committed before reporting failure.
    #[test]
    fn admission_maintenance_is_atomic_with_outbox_removal(
        padding in proptest::prop_oneof![proptest::strategy::Just(61usize), 0usize..61],
        after_replace in proptest::bool::ANY,
        rejected in proptest::bool::ANY,
    ) {
        let mut state = ClientSession::open(CheckpointStore {
            inner: admission_boundary(padding).into_store(),
            fail: Arc::new(AtomicBool::new(true)),
            after_replace,
        }).unwrap();
        let decision = if rejected { wire::AdmissionState::Rejected } else { wire::AdmissionState::Accepted };
        let response = admission(&command(), decision);
        let result = state.receive(wire::ServerFrame { frame: Some(wire::server_frame::Frame::Admission(response)) });
        let expected = if result.is_err() { vec![command()] } else { Vec::new() };
        proptest::prop_assert_eq!(state.pending().cloned().collect::<Vec<_>>(), expected.clone());
        let recovered = ClientSession::open(state.into_store()).unwrap();
        proptest::prop_assert_eq!(recovered.pending().cloned().collect::<Vec<_>>(), expected);
    }
}

#[test]
fn checkpoint_keeps_a_paused_opener_on_the_same_writer_lock() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.journal");
    let mut writer = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    writer.track(session().resume().cursors.remove(0)).unwrap();
    let lock_path = directory.path().join("client.journal.lock");
    // An opener has opened the stable lock inode but has not yet tried locking it.
    let paused = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    for revision in 0..2 {
        writer
            .receive(frame(delivery(revision, LifecycleState::Active)))
            .unwrap();
        writer.checkpoint().unwrap();
        assert!(paused.try_lock().is_err());
        assert!(FileClientState::open(&path).is_err());
    }
    drop(writer);
    paused.try_lock().unwrap();
    assert!(FileClientState::open(&path).is_err());
    paused.unlock().unwrap();
    drop(paused);
    let mut writer = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    assert_eq!(writer.resume().cursors[0].revision, 2);
    writer.queue(command()).unwrap();
    drop(writer);
    let recovered = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    assert_eq!(recovered.resume().cursors[0].revision, 2);
    assert_eq!(
        recovered.pending().cloned().collect::<Vec<_>>(),
        [command()]
    );
}

#[tokio::test]
async fn checkpoint_failure_keeps_the_committed_delivery_before_replay() {
    for after_replace in [false, true] {
        let mut base = session();
        for index in 0..62 {
            let mut pending = command();
            pending.operation = Some(wire::OperationIdentity {
                operation_id: format!("00000000-0000-0000-0001-{index:012}"),
                idempotency_key: format!("pending-{index}"),
            });
            base.queue(pending).unwrap();
        }
        let fail = Arc::new(AtomicBool::new(true));
        let state = ClientSession::open(CheckpointStore {
            inner: base.into_store(),
            fail: fail.clone(),
            after_replace,
        })
        .unwrap();
        let host = Arc::new(Host::default());
        let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
        client.replay().await.unwrap();
        assert!(client.next_delivery().await.is_err());
        assert_eq!(client.session().resume().cursors[0].revision, 1);
        assert!(host.acknowledgements.lock().unwrap().is_empty());
        fail.store(false, Ordering::Relaxed);
        client.replay().await.unwrap();
        assert_eq!(
            client.next_delivery().await.unwrap(),
            Some(delivery(0, LifecycleState::Completed))
        );
        assert_eq!(
            client.next_delivery().await.unwrap(),
            Some(delivery(1, LifecycleState::Completed))
        );
        assert!(client.next_delivery().await.unwrap().is_none());
        let recovered = ClientSession::open(client.into_session().into_store()).unwrap();
        assert_eq!(recovered.resume().cursors[0].revision, 2);
        assert_eq!(recovered.pending().count(), 62);
        assert_eq!(host.acknowledgements.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn new_submissions_drain_earlier_pending_commands_in_order() {
    let host = Arc::new(Host::default());
    host.lost_reply.store(true, Ordering::Relaxed);
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    assert!(submit(&mut client, command()).await.is_err());
    let next = next_command();
    assert_eq!(
        submit(&mut client, next.clone()).await.unwrap(),
        [
            admission(&command(), wire::AdmissionState::Accepted),
            admission(&next, wire::AdmissionState::Accepted)
        ]
    );
    assert_eq!(
        host.commands.lock().unwrap().as_slice(),
        &[command(), command(), next]
    );
    assert_eq!(client.session().pending().count(), 0);
}

#[tokio::test]
async fn indeterminate_admission_blocks_submission_and_outbox_drain() {
    let host = Arc::new(Host::default());
    host.lost_reply.store(true, Ordering::Relaxed);
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    assert!(submit(&mut client, command()).await.is_err());
    *host.admission_state.lock().unwrap() = Some(wire::AdmissionState::Indeterminate);
    let next = next_command();
    assert_eq!(
        submit(&mut client, next.clone()).await.unwrap(),
        [admission(&command(), wire::AdmissionState::Indeterminate)]
    );
    assert_eq!(
        host.commands.lock().unwrap().as_slice(),
        &[command(), command()]
    );
    let mut admissions = Vec::new();
    client
        .flush_outbox(|admission| {
            admissions.push(admission);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        admissions,
        [admission(&command(), wire::AdmissionState::Indeterminate)]
    );
    assert_eq!(
        client.session().pending().cloned().collect::<Vec<_>>(),
        [command(), next.clone()]
    );
    *host.admission_state.lock().unwrap() = Some(wire::AdmissionState::Accepted);
    client.flush_outbox(|_| Ok(())).await.unwrap();
    assert_eq!(
        host.commands.lock().unwrap().as_slice(),
        &[command(), command(), command(), command(), next]
    );
    assert_eq!(client.session().pending().count(), 0);
}

#[tokio::test]
async fn blocked_predecessor_is_reported_without_sending_the_target() {
    for queued in [false, true] {
        let host = Arc::new(Host::default());
        *host.admission_state.lock().unwrap() = Some(wire::AdmissionState::Indeterminate);
        let mut state = session();
        state.queue(command()).unwrap();
        let next = next_command();
        if queued {
            state.queue(next.clone()).unwrap();
        }
        let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
        let mut admissions = Vec::new();
        if queued {
            client
                .flush_outbox(|response| {
                    admissions.push(response);
                    Ok(())
                })
                .await
                .unwrap();
        } else {
            client
                .submit(next, |response| {
                    admissions.push(response);
                    Ok(())
                })
                .await
                .unwrap();
        }
        assert_eq!(host.commands.lock().unwrap().as_slice(), &[command()]);
        assert_eq!(client.session().pending().count(), 2);
        assert_eq!(
            admissions,
            [admission(&command(), wire::AdmissionState::Indeterminate)]
        );
    }
}

#[tokio::test]
async fn resubmitting_an_earlier_intent_leaves_later_intents_queued() {
    let host = Arc::new(Host::default());
    let mut state = session();
    state.queue(command()).unwrap();
    let next = next_command();
    state.queue(next.clone()).unwrap();
    let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
    assert_eq!(
        submit(&mut client, command()).await.unwrap(),
        [admission(&command(), wire::AdmissionState::Accepted)]
    );
    assert_eq!(host.commands.lock().unwrap().as_slice(), &[command()]);
    assert_eq!(
        client.session().pending().cloned().collect::<Vec<_>>(),
        [next.clone()]
    );
    assert_eq!(
        submit(&mut client, next.clone()).await.unwrap(),
        [admission(&next, wire::AdmissionState::Accepted)]
    );
    assert_eq!(host.commands.lock().unwrap().as_slice(), &[command(), next]);
    assert_eq!(client.session().pending().count(), 0);
}

#[cfg(unix)]
#[test]
fn opening_a_journal_requires_its_directory_entry_to_be_synced() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.journal");
    // File creation requires write+search, while opening the directory for sync
    // requires read permission. This fails the durability boundary deterministically.
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o300)).unwrap();
    let opened = FileClientState::open(&path);
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(matches!(opened, Err(Error::Storage(_))));
    let mut state = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    state.queue(command()).unwrap();
    drop(state);
    let recovered = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    assert_eq!(
        recovered.pending().cloned().collect::<Vec<_>>(),
        [command()]
    );
}

#[test]
fn fork_lineage_and_child_lifecycle_are_independent_of_cross_authority_order() {
    use acyclic_harness::{fork::ForkSeed, wire_codec::encode_authority};
    let seed: ForkSeed =
        serde_json::from_str(include_str!("../fixtures/v2/fork-seed.json")).unwrap();
    seed.validate(&mut None).unwrap();
    let parent = encode_authority(&seed.parent);
    let child = encode_authority(&seed.child);
    let event = |authority: &wire::Authority, revision, kind: &str, payload: EventPayload| {
        let mut value = delivery(revision, LifecycleState::Active);
        value.authority = Some(authority.clone());
        let event = value.events.first_mut().unwrap();
        event.authority = Some(authority.clone());
        event.event_type = kind.into();
        event.canonical_payload_json = serde_json::to_vec(&payload).unwrap();
        frame(value)
    };
    let fork = event(
        &parent,
        seed.parent_revision,
        "fork_published",
        EventPayload::ForkPublished {
            seed: Box::new(seed.clone()),
        },
    );
    let active = event(
        &child,
        0,
        "lifecycle_transitioned",
        EventPayload::LifecycleTransitioned {
            from: LifecycleState::Pending,
            to: LifecycleState::Active,
            reason: Some("working".into()),
        },
    );
    for fork_first in [false, true] {
        let mut state = session();
        for authority in [&parent, &child] {
            state
                .track(wire::ReplayCursor {
                    authority: Some(authority.clone()),
                    generation: "generation".into(),
                    revision: 0,
                })
                .unwrap();
        }
        let transitions = [
            (LifecycleState::Pending, LifecycleState::Active),
            (LifecycleState::Active, LifecycleState::Waiting),
            (LifecycleState::Waiting, LifecycleState::Active),
        ];
        assert_eq!(seed.parent_revision, transitions.len() as u64);
        for (revision, (from, to)) in transitions.into_iter().enumerate() {
            state
                .receive(event(
                    &parent,
                    revision as u64,
                    "lifecycle_transitioned",
                    EventPayload::LifecycleTransitioned {
                        from,
                        to,
                        reason: None,
                    },
                ))
                .unwrap();
        }
        let ordered = if fork_first {
            [fork.clone(), active.clone()]
        } else {
            [active.clone(), fork.clone()]
        };
        for frame in ordered {
            state.receive(frame).unwrap();
        }
        assert_eq!(state.replay_revision(&parent), seed.parent_revision + 1);
        assert_eq!(state.replay_revision(&child), 1);
        let mut store = state.into_store();
        let history = store.load().unwrap();
        let mut state = ClientSession::open(store).unwrap();
        state.checkpoint().unwrap();
        let mut recovered = ClientSession::open(state.into_store()).unwrap();
        for record in history {
            if let Some(response) = record.response {
                recovered.receive(response).unwrap();
            }
        }
        assert_eq!(recovered.replay_revision(&parent), seed.parent_revision + 1);
        assert_eq!(recovered.replay_revision(&child), 1);
    }
}
