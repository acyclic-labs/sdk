use super::headless::target;
use super::*;
use acyclic_harness::client::{HeadlessOutcome, run_headless};
use futures::FutureExt;

#[tokio::test]
async fn historical_waiting_cannot_settle_a_different_operation() {
    for recovered in [false, true] {
        let host = Arc::new(Host::default());
        *host.state.lock().unwrap() = Some(wire::CompletionState::Running);
        host.through.store(1, Ordering::Relaxed);
        let (sender, source) = futures::channel::mpsc::unbounded();
        *host.trace.lock().unwrap() = Some(Box::pin(source));
        let mut old = delivery(0, LifecycleState::Waiting);
        old.events[0].operation_id = "00000000-0000-0000-0000-000000000002".into();
        let mut saved = session();
        if recovered {
            saved.receive(frame(old.clone())).unwrap();
            saved.checkpoint().unwrap();
            saved = ClientSession::open(saved.into_store()).unwrap();
        }
        sender.unbounded_send(Ok(old)).unwrap();
        let mut client = HarnessClient::connect(host.clone(), saved).await.unwrap();
        let mut output = Vec::new();
        let mut run = Box::pin(run_headless(&mut client, command(), target(), &mut output));
        assert!(
            run.as_mut().now_or_never().is_none(),
            "recovered={recovered}"
        );
        sender
            .unbounded_send(Ok(delivery(1, LifecycleState::Active)))
            .unwrap();
        sender
            .unbounded_send(Ok(delivery(2, LifecycleState::Completed)))
            .unwrap();
        host.through.store(3, Ordering::Relaxed);
        host.complete(wire::CompletionState::Succeeded);
        assert_eq!(run.await.unwrap(), HeadlessOutcome::Completed);
        assert_eq!(client.session().resume().cursors[0].revision, 3);
        assert_eq!(client.session().replay_revision(&owner()), 3);
        let revisions = String::from_utf8(output)
            .unwrap()
            .lines()
            .map(|line| jsonl::parse_server_frame(line).unwrap())
            .filter_map(|frame| match frame.frame {
                Some(wire::server_frame::Frame::Delivery(delivery)) => {
                    Some(delivery.through_revision)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(revisions, vec![1, 2, 3]);
    }
}

#[tokio::test]
async fn owner_waiting_arrives_without_a_scheduler_status_change() {
    let host = Arc::new(Host::default());
    *host.state.lock().unwrap() = Some(wire::CompletionState::Running);
    host.through.store(1, Ordering::Relaxed);
    let (sender, source) = futures::channel::mpsc::unbounded();
    sender
        .unbounded_send(Ok(delivery(0, LifecycleState::Active)))
        .unwrap();
    *host.trace.lock().unwrap() = Some(Box::pin(source));
    let mut client = HarnessClient::connect(host.clone(), session())
        .await
        .unwrap();
    let mut output = Vec::new();
    let mut run = Box::pin(run_headless(&mut client, command(), target(), &mut output));
    assert!(run.as_mut().now_or_never().is_none());
    // The scheduler remains Running at cut 1. Only owner history changes.
    sender
        .unbounded_send(Ok(delivery(1, LifecycleState::Waiting)))
        .unwrap();
    assert_eq!(
        run.now_or_never()
            .expect("live history must wake headless")
            .unwrap(),
        HeadlessOutcome::Blocked
    );
    assert_eq!(client.session().resume().cursors[0].revision, 2);
}
