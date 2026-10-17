use super::*;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_harness::client::FileClientState;
use acyclic_harness::client::{HeadlessOutcome, run_headless, run_jsonl};
use futures::{FutureExt, StreamExt};
use std::io::Write;
#[cfg(not(target_arch = "wasm32"))]
use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom},
};

pub(super) fn target() -> wire::ObserveRequest {
    wire::ObserveRequest {
        operation_id: command().operation.unwrap().operation_id,
        protocol: Some(current_protocol()),
        owner: Some(owner()),
        scope: Some(wire::Scope {
            id: "observe".into(),
            capabilities: vec!["operation:observe".into(), "operation:cancel".into()],
            proof: vec![0; 32],
            ..Default::default()
        }),
    }
}

fn trace(host: &Host, states: &[LifecycleState], completion: wire::CompletionState) {
    host.through.store(states.len(), Ordering::Relaxed);
    *host.state.lock().unwrap() = Some(completion);
    let frames = states
        .iter()
        .enumerate()
        .map(|(index, state)| Ok(delivery(index as u64, *state)))
        .collect::<Vec<_>>();
    *host.trace.lock().unwrap() = Some(Box::pin(futures::stream::iter(frames)));
}

fn frames(output: Vec<u8>) -> Vec<wire::ServerFrame> {
    String::from_utf8(output)
        .unwrap()
        .lines()
        .map(|line| jsonl::parse_server_frame(line).unwrap())
        .collect()
}

#[tokio::test]
async fn every_settled_outcome_has_its_documented_exit_code() {
    let cases = [
        (
            LifecycleState::Completed,
            wire::CompletionState::Succeeded,
            HeadlessOutcome::Completed,
            0,
        ),
        (
            LifecycleState::Failed,
            wire::CompletionState::Failed,
            HeadlessOutcome::Failed,
            1,
        ),
        (
            LifecycleState::Cancelled,
            wire::CompletionState::Cancelled,
            HeadlessOutcome::Cancelled,
            2,
        ),
        (
            LifecycleState::Waiting,
            wire::CompletionState::Running,
            HeadlessOutcome::Blocked,
            3,
        ),
        (
            LifecycleState::Failed,
            wire::CompletionState::Indeterminate,
            HeadlessOutcome::Indeterminate,
            4,
        ),
    ];
    for (state, completion, expected, exit_code) in cases {
        let host = Arc::new(Host::default());
        trace(&host, &[state], completion);
        let mut client = HarnessClient::connect(host.clone(), session())
            .await
            .unwrap();
        let mut output = Vec::new();
        let outcome = run_headless(&mut client, command(), target(), &mut output)
            .await
            .unwrap();
        assert_eq!(outcome, expected);
        assert_eq!(outcome.exit_code(), exit_code);
        let frames = frames(output);
        assert_eq!(frames.len(), 4);
        assert!(matches!(
            frames[0].frame,
            Some(wire::server_frame::Frame::Handshake(_))
        ));
        assert!(matches!(
            frames[1].frame,
            Some(wire::server_frame::Frame::Admission(_))
        ));
        assert!(matches!(
            frames[2].frame,
            Some(wire::server_frame::Frame::Status(_))
        ));
        assert!(matches!(
            frames[3].frame,
            Some(wire::server_frame::Frame::Delivery(_))
        ));
        assert_eq!(
            host.acknowledgements.lock().unwrap()[0],
            wire::ClientFrame {
                frame: Some(wire::client_frame::Frame::Acknowledge(wire::Acknowledge {
                    authority: Some(owner()),
                    generation: "generation".into(),
                    through_revision: 1,
                })),
            }
        );
    }
}

#[tokio::test]
async fn completed_status_does_not_hide_a_racing_final_delivery() {
    let host = Arc::new(Host::default());
    trace(
        &host,
        &[LifecycleState::Active, LifecycleState::Completed],
        wire::CompletionState::Succeeded,
    );
    let mut client = HarnessClient::connect(host, session()).await.unwrap();
    let mut output = Vec::new();
    assert_eq!(
        run_headless(&mut client, command(), target(), &mut output)
            .await
            .unwrap(),
        HeadlessOutcome::Completed
    );
    let deliveries = frames(output)
        .into_iter()
        .filter_map(|frame| match frame.frame {
            Some(wire::server_frame::Frame::Delivery(delivery)) => Some(delivery),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[1].through_revision, 2);
}

#[tokio::test]
async fn historical_waiting_is_drained_through_the_observed_settlement_cut() {
    let host = Arc::new(Host::default());
    *host.state.lock().unwrap() = Some(wire::CompletionState::Succeeded);
    host.through.store(3, Ordering::Relaxed);
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    *host.trace.lock().unwrap() = Some(Box::pin(receiver));
    sender
        .unbounded_send(Ok(delivery(0, LifecycleState::Waiting)))
        .unwrap();
    let mut client = HarnessClient::connect(host, session()).await.unwrap();
    let mut output = Vec::new();
    {
        let mut runner = Box::pin(run_headless(&mut client, command(), target(), &mut output));
        assert!(
            runner.as_mut().now_or_never().is_none(),
            "historical waiting is not settlement"
        );
        sender
            .unbounded_send(Ok(delivery(1, LifecycleState::Active)))
            .unwrap();
        sender
            .unbounded_send(Ok(delivery(2, LifecycleState::Completed)))
            .unwrap();
        assert_eq!(
            runner
                .as_mut()
                .now_or_never()
                .expect("settled live replay must return")
                .unwrap(),
            HeadlessOutcome::Completed
        );
    }
    let revisions = frames(output)
        .into_iter()
        .filter_map(|frame| match frame.frame {
            Some(wire::server_frame::Frame::Delivery(delivery)) => Some(delivery.through_revision),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(revisions, [1, 2, 3]);
}

#[tokio::test]
async fn successful_idle_exits_at_the_cut_without_a_terminal_event_or_eof() {
    for already_replayed in [false, true] {
        let host = Arc::new(Host::default());
        trace(
            &host,
            &[LifecycleState::Active],
            wire::CompletionState::Succeeded,
        );
        let mut state = session();
        if already_replayed {
            state
                .receive(frame(delivery(0, LifecycleState::Active)))
                .unwrap();
            *host.trace.lock().unwrap() = Some(Box::pin(futures::stream::pending()));
        } else {
            let replay = host.trace.lock().unwrap().take().unwrap();
            *host.trace.lock().unwrap() = Some(Box::pin(replay.chain(futures::stream::pending())));
        }
        let mut client = HarnessClient::connect(host, state).await.unwrap();
        assert_eq!(
            run_headless(&mut client, command(), target(), &mut Vec::new())
                .now_or_never()
                .expect("successful idle must not wait for another prompt")
                .unwrap(),
            HeadlessOutcome::Completed
        );
        assert_eq!(client.session().replay_revision(&owner()), 1);
    }
}

#[tokio::test]
async fn running_status_is_refreshed_when_replay_reaches_its_cut() {
    let host = Arc::new(Host::default());
    trace(
        &host,
        &[LifecycleState::Active, LifecycleState::Active],
        wire::CompletionState::Running,
    );
    host.complete_on_ack.store(true, Ordering::Relaxed);
    let replay = host.trace.lock().unwrap().take().unwrap();
    *host.trace.lock().unwrap() = Some(Box::pin(replay.chain(futures::stream::pending())));
    let mut client = HarnessClient::connect(host, session()).await.unwrap();
    assert_eq!(
        run_headless(&mut client, command(), target(), &mut Vec::new())
            .now_or_never()
            .expect("refresh the running snapshot at its cut")
            .unwrap(),
        HeadlessOutcome::Completed
    );
    assert_eq!(client.session().resume().cursors[0].revision, 2);
}

#[tokio::test]
async fn waiting_only_blocks_at_the_current_cut_and_eof_cannot_replace_it() {
    let host = Arc::new(Host::default());
    trace(
        &host,
        &[
            LifecycleState::Waiting,
            LifecycleState::Active,
            LifecycleState::Waiting,
        ],
        wire::CompletionState::Running,
    );
    let mut client = HarnessClient::connect(host, session()).await.unwrap();
    assert_eq!(
        run_headless(&mut client, command(), target(), &mut Vec::new())
            .await
            .unwrap(),
        HeadlessOutcome::Blocked
    );
    assert_eq!(client.session().resume().cursors[0].revision, 3);

    let host = Arc::new(Host::default());
    trace(
        &host,
        &[LifecycleState::Active],
        wire::CompletionState::Succeeded,
    );
    // This service cannot supply its advertised cut, even after a page refresh.
    *host.snapshot_history.lock().unwrap() = Some(vec![delivery(0, LifecycleState::Active)]);
    host.through.store(2, Ordering::Relaxed);
    let mut client = HarnessClient::connect(host, session()).await.unwrap();
    assert!(matches!(
        run_headless(&mut client, command(), target(), &mut Vec::new()).await,
        Err(Error::Indeterminate(_))
    ));
}

#[tokio::test]
async fn earlier_admission_is_streamed_before_a_later_outbox_failure() {
    let host = Arc::new(Host::default());
    host.fail_submit.store(2, Ordering::Relaxed);
    let mut client = HarnessClient::connect(host, session()).await.unwrap();
    client.queue(command()).unwrap();
    let mut second = command();
    second.operation = Some(wire::OperationIdentity {
        operation_id: "00000000-0000-0000-0000-000000000002".into(),
        idempotency_key: "submit-2".into(),
    });
    let mut target = target();
    target.operation_id = second.operation.as_ref().unwrap().operation_id.clone();
    let mut output = Vec::new();
    assert!(
        run_headless(&mut client, second, target, &mut output)
            .await
            .is_err()
    );
    let frames = frames(output);
    assert_eq!(frames.len(), 2);
    assert!(matches!(
        frames[1].frame,
        Some(wire::server_frame::Frame::Admission(_))
    ));
    assert_eq!(client.session().pending().count(), 1);
}

#[tokio::test]
async fn commit_failure_sends_no_ack_and_ack_failure_keeps_the_committed_frame() {
    let host = Arc::new(Host::default());
    let fail = Arc::new(AtomicBool::new(false));
    let state = ClientSession::open(FailingStore {
        inner: session().into_store(),
        fail: fail.clone(),
    })
    .unwrap();
    let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
    client.replay().await.unwrap();
    fail.store(true, Ordering::Relaxed);
    assert!(client.next_delivery().await.is_err());
    assert!(host.acknowledgements.lock().unwrap().is_empty());
    assert_eq!(client.session().resume().cursors[0].revision, 0);
    fail.store(false, Ordering::Relaxed);
    client.replay().await.unwrap();
    host.lost_ack.store(true, Ordering::Relaxed);
    assert!(client.next_delivery().await.is_err());
    assert_eq!(client.session().resume().cursors[0].revision, 1);
    let saved = client.next_delivery().await.unwrap().unwrap();
    assert_eq!(saved.through_revision, 1);
    assert_eq!(host.acknowledgements.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn cancelling_an_ack_wait_does_not_drop_the_committed_delivery() {
    let host = Arc::new(Host::default());
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    client.replay().await.unwrap();
    host.hold_ack.store(true, Ordering::Relaxed);
    assert!(client.next_delivery().now_or_never().is_none());
    assert_eq!(client.session().resume().cursors[0].revision, 1);
    host.hold_ack.store(false, Ordering::Relaxed);
    assert_eq!(
        client
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
            .through_revision,
        1
    );
    assert_eq!(client.into_session().into_store().load().unwrap().len(), 2);
}

#[tokio::test]
async fn jsonl_stdin_can_cancel_while_replay_is_waiting() {
    let host = Arc::new(Host::default());
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    *host.trace.lock().unwrap() = Some(Box::pin(receiver));
    *host.live_sender.lock().unwrap() = Some(sender);
    *host.state.lock().unwrap() = Some(wire::CompletionState::Running);
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    let cancel = wire::ClientFrame {
        frame: Some(wire::client_frame::Frame::Cancel(wire::CancelRequest {
            operation_id: target().operation_id,
            protocol: Some(current_protocol()),
            owner: Some(owner()),
            scope: target().scope,
            recursive: true,
            idempotency_key: "cancel-1".into(),
        })),
    };
    let input = futures::stream::iter([Ok(jsonl::client_frame(&cancel).unwrap())]);
    let mut output = Vec::new();
    assert_eq!(
        run_jsonl(&mut client, target(), input, &mut output)
            .await
            .unwrap(),
        HeadlessOutcome::Cancelled
    );
    assert!(frames(output).iter().any(|frame| matches!(
        frame.frame,
        Some(wire::server_frame::Frame::Cancellation(_))
    )));
}

#[tokio::test]
async fn jsonl_stdin_submits_wire_commands_and_eof_does_not_cancel() {
    let host = Arc::new(Host::default());
    *host.state.lock().unwrap() = Some(wire::CompletionState::Running);
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    *host.trace.lock().unwrap() = Some(Box::pin(receiver));
    *host.live_sender.lock().unwrap() = Some(sender);
    *host.submit_delivery.lock().unwrap() = Some(delivery(0, LifecycleState::Completed));
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    let input = futures::stream::iter([Ok(jsonl::client_frame(&wire::ClientFrame {
        frame: Some(wire::client_frame::Frame::Command(command())),
    })
    .unwrap())]);
    let mut output = Vec::new();
    assert_eq!(
        run_jsonl(&mut client, target(), input, &mut output)
            .await
            .unwrap(),
        HeadlessOutcome::Completed
    );
    assert_eq!(host.commands.lock().unwrap().as_slice(), &[command()]);
    assert!(
        frames(output)
            .iter()
            .any(|frame| matches!(frame.frame, Some(wire::server_frame::Frame::Admission(_))))
    );
}

#[tokio::test]
async fn malformed_stdin_is_rejected_without_a_submit() {
    let host = Arc::new(Host::default());
    *host.state.lock().unwrap() = Some(wire::CompletionState::Running);
    *host.trace.lock().unwrap() = Some(Box::pin(futures::stream::pending()));
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    let input = futures::stream::iter([Ok("{\"unknown\":{}}".into())]);
    assert!(
        run_jsonl(&mut client, target(), input, &mut Vec::new())
            .await
            .is_err()
    );
    assert!(host.commands.lock().unwrap().is_empty());
}

#[test]
fn checkpoint_bounds_recovery_after_ten_thousand_deliveries() {
    let mut session = session();
    session.queue(command()).unwrap();
    for revision in 0..10_000 {
        session
            .receive(frame(delivery(revision, LifecycleState::Active)))
            .unwrap();
    }
    let revision = session.replay_revision(&owner());
    let mut store = session.into_store();
    let records = store.load().unwrap();
    assert!(records.len() <= 64, "{} recovery records", records.len());
    assert!(records[0].checkpoint.is_some());
    let mut recovered = ClientSession::open(store).unwrap();
    assert_eq!(recovered.replay_revision(&owner()), 0);
    for revision in 0..10_000 {
        recovered
            .receive(frame(delivery(revision, LifecycleState::Active)))
            .unwrap();
    }
    assert_eq!(recovered.replay_revision(&owner()), revision);
    assert_eq!(recovered.resume().cursors[0].revision, 10_000);
    assert_eq!(
        recovered.pending().cloned().collect::<Vec<_>>(),
        vec![command()]
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_journal_recovers_a_torn_tail_and_checkpoint_replaces_the_prefix() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.journal");
    let mut native = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    native.track(session().resume().cursors.remove(0)).unwrap();
    native.queue(command()).unwrap();
    for revision in 0..128 {
        native
            .receive(frame(delivery(revision, LifecycleState::Active)))
            .unwrap();
    }
    assert!(FileClientState::open(&path).is_err());
    native.checkpoint().unwrap();
    let expected = native.replay_revision(&owner());
    drop(native);
    let length = std::fs::metadata(&path).unwrap().len();
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[17, 0, 0])
        .unwrap();
    let mut store = FileClientState::open(&path).unwrap();
    assert_eq!(store.load().unwrap().len(), 1);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), length);
    let mut recovered = ClientSession::open(store).unwrap();
    for revision in 0..128 {
        recovered
            .receive(frame(delivery(revision, LifecycleState::Active)))
            .unwrap();
    }
    assert_eq!(recovered.replay_revision(&owner()), expected);
    assert_eq!(recovered.resume().cursors[0].revision, 128);
    assert_eq!(recovered.pending().count(), 1);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_journal_rejects_corruption() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.journal");
    let mut native = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    native.track(session().resume().cursors.remove(0)).unwrap();
    drop(native);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(68)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(68)).unwrap();
    file.write_all(&byte).unwrap();
    drop(file);
    assert!(ClientSession::open(FileClientState::open(&path).unwrap()).is_err());
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn damaged_length_headers_never_truncate_committed_cursors_or_outbox() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.journal");
    let mut native = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    native.track(session().resume().cursors.remove(0)).unwrap();
    native.queue(command()).unwrap();
    native
        .receive(frame(delivery(0, LifecycleState::Active)))
        .unwrap();
    native.checkpoint().unwrap();
    drop(native);
    let committed = std::fs::read(&path).unwrap();
    for size in [u32::MAX, 0] {
        let mut damaged = committed.clone();
        damaged[..4].copy_from_slice(&size.to_le_bytes());
        std::fs::write(&path, &damaged).unwrap();
        for _ in 0..2 {
            assert!(ClientSession::open(FileClientState::open(&path).unwrap()).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), damaged);
        }
    }
    std::fs::write(&path, &committed).unwrap();
    let restored = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    assert_eq!(restored.resume().cursors[0].revision, 1);
    assert_eq!(restored.pending().cloned().collect::<Vec<_>>(), [command()]);
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn a_valid_header_with_an_incomplete_payload_is_only_a_torn_append() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("client.journal");
    let mut native = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    native.track(session().resume().cursors.remove(0)).unwrap();
    let committed_length = std::fs::metadata(&path).unwrap().len();
    native.queue(command()).unwrap();
    drop(native);
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(std::fs::metadata(&path).unwrap().len() - 1)
        .unwrap();
    drop(file);
    let restored = ClientSession::open(FileClientState::open(&path).unwrap()).unwrap();
    assert_eq!(restored.resume().cursors[0].revision, 0);
    assert_eq!(restored.pending().count(), 0);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), committed_length);
}

#[test]
fn large_checkpoint_bulk_restores_ordered_outbox_and_identity_indexes() {
    let mut base = session();
    base.checkpoint().unwrap();
    let mut store = base.into_store();
    let mut checkpoint = store.load().unwrap().remove(0);
    let pending = (1..=20_000)
        .map(|index| {
            let mut command = command();
            command.operation = Some(wire::OperationIdentity {
                operation_id: format!("00000000-0000-0000-0000-{index:012}"),
                idempotency_key: format!("submit-{index}"),
            });
            command
        })
        .collect::<Vec<_>>();
    checkpoint.checkpoint.as_mut().unwrap().pending = pending.clone();
    store.replace(&checkpoint).unwrap();
    let mut recovered = ClientSession::open(store).unwrap();
    assert!(recovered.pending().eq(pending.iter()));
    for index in [19_999, 0, 9_999] {
        recovered.queue(pending[index].clone()).unwrap();
        recovered
            .receive(wire::ServerFrame {
                frame: Some(wire::server_frame::Frame::Admission(admission(
                    &pending[index],
                    wire::AdmissionState::Accepted,
                ))),
            })
            .unwrap();
        // Both private indexes must release a removed identity.
        recovered.queue(pending[index].clone()).unwrap();
    }
    recovered.checkpoint().unwrap();
    let recovered = ClientSession::open(recovered.into_store()).unwrap();
    assert_eq!(recovered.pending().count(), pending.len());
    assert_eq!(recovered.pending().last(), Some(&pending[9_999]));
    for duplicate_operation in [false, true] {
        let mut invalid = checkpoint.clone();
        let commands = &mut invalid.checkpoint.as_mut().unwrap().pending;
        let mut collision = commands[0].clone();
        let identity = collision.operation.as_mut().unwrap();
        if duplicate_operation {
            identity.idempotency_key = "distinct-key".into();
        } else {
            identity.operation_id = "ffffffff-ffff-ffff-ffff-ffffffffffff".into();
        }
        commands.push(collision);
        let mut store = MemoryClientState::default();
        store.replace(&invalid).unwrap();
        assert!(ClientSession::open(store).is_err());
    }
}

#[tokio::test]
async fn late_settlement_wakes_without_another_history_delivery() {
    for live in [false, true] {
        let host = Arc::new(Host::default());
        trace(
            &host,
            &[LifecycleState::Active],
            wire::CompletionState::Running,
        );
        if live {
            let replay = host.trace.lock().unwrap().take().unwrap();
            *host.trace.lock().unwrap() = Some(Box::pin(replay.chain(futures::stream::pending())));
        }
        let mut client = HarnessClient::connect(host.clone(), session())
            .await
            .unwrap();
        let mut output = Vec::new();
        {
            let mut runner = Box::pin(run_headless(&mut client, command(), target(), &mut output));
            assert!(runner.as_mut().now_or_never().is_none());
            host.complete(wire::CompletionState::Succeeded);
            assert_eq!(
                runner
                    .as_mut()
                    .now_or_never()
                    .expect("status change wakes runner")
                    .unwrap(),
                HeadlessOutcome::Completed
            );
        }
        assert_eq!(
            frames(output)
                .iter()
                .filter(|frame| matches!(frame.frame, Some(wire::server_frame::Frame::Delivery(_))))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn stdin_eof_during_ack_cannot_overtake_jsonl_delivery() {
    let host = Arc::new(Host::default());
    trace(
        &host,
        &[LifecycleState::Completed],
        wire::CompletionState::Succeeded,
    );
    let (eof_sender, eof_receiver) = futures::channel::oneshot::channel();
    let (release_sender, release_receiver) = futures::channel::oneshot::channel();
    *host.ack_input_eof.lock().unwrap() = Some(eof_sender);
    *host.ack_release.lock().unwrap() = Some(release_receiver);
    let input = futures::stream::unfold(Some(eof_receiver), |mut receiver| async move {
        if let Some(receiver) = receiver.take() {
            receiver.await.unwrap();
        }
        None::<(
            Result<String>,
            Option<futures::channel::oneshot::Receiver<()>>,
        )>
    })
    .boxed();
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    let mut output = Vec::new();
    {
        let mut runner = Box::pin(run_jsonl(&mut client, target(), input, &mut output));
        assert!(
            runner.as_mut().now_or_never().is_none(),
            "ack is pending after EOF"
        );
        release_sender.send(()).unwrap();
        assert_eq!(
            runner
                .as_mut()
                .now_or_never()
                .expect("released acknowledgement")
                .unwrap(),
            HeadlessOutcome::Completed
        );
    }
    assert_eq!(client.session().resume().cursors[0].revision, 1);
    assert!(
        frames(output)
            .iter()
            .any(|frame| matches!(frame.frame, Some(wire::server_frame::Frame::Delivery(_))))
    );
    assert_eq!(host.acknowledgements.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn buffered_committed_delivery_is_drained_before_settlement() {
    let host = Arc::new(Host::default());
    trace(
        &host,
        &[LifecycleState::Completed],
        wire::CompletionState::Succeeded,
    );
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    client.replay().await.unwrap();
    host.hold_ack.store(true, Ordering::Relaxed);
    assert!(client.next_delivery().now_or_never().is_none());
    host.hold_ack.store(false, Ordering::Relaxed);
    let mut output = Vec::new();
    assert_eq!(
        run_jsonl(&mut client, target(), futures::stream::empty(), &mut output)
            .await
            .unwrap(),
        HeadlessOutcome::Completed
    );
    assert!(
        frames(output)
            .iter()
            .any(|frame| matches!(frame.frame, Some(wire::server_frame::Frame::Delivery(_))))
    );
}

#[tokio::test]
async fn fresh_jsonl_target_is_admitted_before_status_is_required() {
    let host = Arc::new(Host::default());
    host.requires_admission.store(true, Ordering::Relaxed);
    *host.state.lock().unwrap() = Some(wire::CompletionState::Running);
    let (sender, receiver) = futures::channel::mpsc::unbounded();
    *host.trace.lock().unwrap() = Some(Box::pin(receiver));
    *host.live_sender.lock().unwrap() = Some(sender);
    *host.submit_delivery.lock().unwrap() = Some(delivery(0, LifecycleState::Completed));
    assert!(matches!(
        host.observe(target()).await,
        Err(Error::NotFound(_))
    ));
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    let input = futures::stream::iter([Ok(jsonl::client_frame(&wire::ClientFrame {
        frame: Some(wire::client_frame::Frame::Command(command())),
    })
    .unwrap())]);
    let mut output = Vec::new();
    assert_eq!(
        run_jsonl(&mut client, target(), input, &mut output)
            .await
            .unwrap(),
        HeadlessOutcome::Completed
    );
    assert_eq!(host.commands.lock().unwrap().as_slice(), &[command()]);
    let output = frames(output);
    assert!(
        output
            .iter()
            .any(|frame| matches!(frame.frame, Some(wire::server_frame::Frame::Admission(_))))
    );
    assert!(
        output
            .iter()
            .any(|frame| matches!(frame.frame, Some(wire::server_frame::Frame::Delivery(_))))
    );
}

#[tokio::test]
async fn failed_jsonl_write_never_commits_or_acknowledges_the_delivery() {
    struct FailDelivery;
    impl Write for FailDelivery {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.starts_with(b"{\"delivery\"") {
                return Err(std::io::Error::other("stdout closed"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let host = Arc::new(Host::default());
    trace(
        &host,
        &[LifecycleState::Completed],
        wire::CompletionState::Succeeded,
    );
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    assert!(matches!(
        run_headless(&mut client, command(), target(), &mut FailDelivery).await,
        Err(Error::Storage(_))
    ));
    assert_eq!(client.session().resume().cursors[0].revision, 0);
    assert!(host.acknowledgements.lock().unwrap().is_empty());
}

#[tokio::test]
async fn fresh_jsonl_submission_replaces_the_pre_admission_finite_snapshot() {
    for already_replayed in [false, true] {
        let host = Arc::new(Host::default());
        host.requires_admission.store(true, Ordering::Relaxed);
        *host.state.lock().unwrap() = Some(wire::CompletionState::Running);
        let mut state = session();
        let history = if already_replayed {
            let previous = delivery(0, LifecycleState::Active);
            state.receive(frame(previous.clone())).unwrap();
            vec![previous]
        } else {
            Vec::new()
        };
        *host.snapshot_history.lock().unwrap() = Some(history);
        host.through
            .store(usize::from(already_replayed), Ordering::Relaxed);
        let final_delivery = delivery(u64::from(already_replayed), LifecycleState::Completed);
        *host.submit_delivery.lock().unwrap() = Some(final_delivery.clone());
        let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
        let input = futures::stream::iter([Ok(jsonl::client_frame(&wire::ClientFrame {
            frame: Some(wire::client_frame::Frame::Command(command())),
        })
        .unwrap())]);
        let mut output = Vec::new();
        let outcome = run_jsonl(&mut client, target(), input, &mut output)
            .now_or_never()
            .expect("finite replay must settle without another wakeup")
            .unwrap();
        assert_eq!(outcome, HeadlessOutcome::Completed);
        assert_eq!(outcome.exit_code(), 0);
        assert_eq!(
            client.session().resume().cursors[0].revision,
            u64::from(already_replayed) + 1
        );
        assert_eq!(host.commands.lock().unwrap().as_slice(), &[command()]);
        let deliveries = frames(output)
            .into_iter()
            .filter_map(|frame| match frame.frame {
                Some(wire::server_frame::Frame::Delivery(delivery)) => Some(delivery),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(deliveries, [final_delivery]);
    }
}

#[tokio::test]
async fn settlement_drains_ready_stdin_and_replay() {
    let host = Arc::new(Host::default());
    let final_delivery = delivery(0, LifecycleState::Completed);
    trace(
        &host,
        &[LifecycleState::Completed],
        wire::CompletionState::Succeeded,
    );
    let mut state = session();
    state.receive(frame(final_delivery.clone())).unwrap();
    let mut client = HarnessClient::connect(host, state).await.unwrap();
    let input = futures::stream::iter([Ok(jsonl::client_frame(&wire::ClientFrame {
        frame: Some(wire::client_frame::Frame::Observe(target())),
    })
    .unwrap())]);
    let mut output = Vec::new();
    assert_eq!(
        run_jsonl(&mut client, target(), input, &mut output)
            .now_or_never()
            .expect("ready sources drain without another wakeup")
            .unwrap(),
        HeadlessOutcome::Completed
    );
    let output = frames(output);
    assert_eq!(
        output
            .iter()
            .filter(|frame| matches!(frame.frame, Some(wire::server_frame::Frame::Status(_))))
            .count(),
        2
    );
    let deliveries = output
        .into_iter()
        .filter_map(|frame| match frame.frame {
            Some(wire::server_frame::Frame::Delivery(delivery)) => Some(delivery),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(deliveries, [final_delivery]);
    assert_eq!(client.session().resume().cursors[0].revision, 1);
}

#[tokio::test]
async fn finite_replay_pages_reach_the_settlement_boundary() {
    let host = Arc::new(Host::default());
    let history = [
        LifecycleState::Active,
        LifecycleState::Active,
        LifecycleState::Completed,
    ]
    .into_iter()
    .enumerate()
    .map(|(revision, state)| delivery(revision as u64, state))
    .collect::<Vec<_>>();
    *host.snapshot_history.lock().unwrap() = Some(history.clone());
    host.replay_page_size.store(1, Ordering::Relaxed);
    host.through.store(history.len(), Ordering::Relaxed);
    let mut client = HarnessClient::connect(host, session()).await.unwrap();
    let mut output = Vec::new();
    assert_eq!(
        run_headless(&mut client, command(), target(), &mut output)
            .now_or_never()
            .expect("finite pages need no external wakeup")
            .unwrap(),
        HeadlessOutcome::Completed
    );
    let delivered = frames(output)
        .into_iter()
        .filter_map(|frame| match frame.frame {
            Some(wire::server_frame::Frame::Delivery(delivery)) => Some(delivery),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(delivered, history);
    assert_eq!(client.session().resume().cursors[0].revision, 3);
}

#[tokio::test]
async fn finite_replay_refreshes_when_a_later_status_advances_its_cut() {
    for before_eof in [false, true] {
        let host = Arc::new(Host::default());
        *host.state.lock().unwrap() = Some(wire::CompletionState::Running);
        *host.snapshot_history.lock().unwrap() = Some(Vec::new());
        let final_delivery = delivery(0, LifecycleState::Completed);
        let input: BoxProviderStream<'static, Result<String>> = if before_eof {
            let host = host.clone();
            let final_delivery = final_delivery.clone();
            Box::pin(
                futures::stream::once(async move {
                host.snapshot_history
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .push(final_delivery);
                host.through.store(1, Ordering::Relaxed);
                host.complete(wire::CompletionState::Succeeded);
                Ok(jsonl::client_frame(&wire::ClientFrame {
                    frame: Some(wire::client_frame::Frame::Observe(target())),
                })
                .unwrap())
                })
                .chain(futures::stream::pending()),
            )
        } else {
            Box::pin(futures::stream::pending())
        };
        let mut client = HarnessClient::connect(host.clone(), session())
            .await
            .unwrap();
        let mut output = Vec::new();
        {
            let mut runner = Box::pin(run_jsonl(&mut client, target(), input, &mut output));
            if !before_eof {
                assert!(runner.as_mut().now_or_never().is_none());
                host.snapshot_history
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .push(final_delivery.clone());
                host.through.store(1, Ordering::Relaxed);
                host.complete(wire::CompletionState::Succeeded);
            }
            assert_eq!(
                runner
                    .as_mut()
                    .now_or_never()
                    .expect("a newer status must wake finite replay")
                    .unwrap(),
                HeadlessOutcome::Completed
            );
        }
        let delivered = frames(output)
            .into_iter()
            .filter_map(|frame| match frame.frame {
                Some(wire::server_frame::Frame::Delivery(delivery)) => Some(delivery),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(delivered, [final_delivery]);
        assert_eq!(client.session().resume().cursors[0].revision, 1);
    }
}
