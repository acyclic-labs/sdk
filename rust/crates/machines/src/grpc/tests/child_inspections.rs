use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{mpsc, oneshot};

type Reply = Result<wire::MachineState, Status>;

pub(super) struct ChildInspections {
    replies: std::sync::Mutex<BTreeMap<MachineId, oneshot::Receiver<Reply>>>,
    started: mpsc::UnboundedSender<MachineId>,
    active: AtomicUsize,
    maximum: AtomicUsize,
}

impl ChildInspections {
    pub(super) async fn inspect(&self, request: wire::InspectMachineRequest) -> Reply {
        let machine = decode_machine(request.machine.as_ref())
            .map_err(|_| Status::invalid_argument("missing child"))?;
        let reply = self
            .replies
            .lock()
            .expect("child replies")
            .remove(&machine)
            .ok_or_else(|| Status::invalid_argument("unexpected child"))?;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        let _ = self.started.send(machine);
        let result = reply
            .await
            .unwrap_or_else(|_| Err(Status::cancelled("fixture closed")));
        self.active.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

fn state(machine: MachineId) -> wire::MachineState {
    wire::MachineState {
        machine: Some(encode_machine(machine)),
        contract: Some(codecs::wire_contract(&[])),
        status: wire::MachineStatus::Running as i32,
        created_at_unix_ms: 1,
        changed_at_unix_ms: 1,
        ..Default::default()
    }
}

struct Fixture {
    service: OperationService,
    children: Vec<MachineId>,
    replies: Vec<(MachineId, oneshot::Sender<Reply>)>,
    started: mpsc::UnboundedReceiver<MachineId>,
}

impl Fixture {
    fn new(count: usize) -> Self {
        let (mut service, _, _) = scripted_operation_service(vec![], vec![]);
        let (started, receiver) = mpsc::unbounded_channel();
        let mut pending = BTreeMap::new();
        let mut replies = Vec::new();
        let children: Vec<_> = (0..count).map(|_| MachineId::new()).collect();
        for &child in &children {
            let (sender, receiver) = oneshot::channel();
            pending.insert(child, receiver);
            replies.push((child, sender));
        }
        service.children = Some(Arc::new(ChildInspections {
            replies: std::sync::Mutex::new(pending),
            started,
            active: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        }));
        Self {
            service,
            children,
            replies,
            started: receiver,
        }
    }
}

#[tokio::test]
async fn child_inspections_overlap_with_a_bound_and_keep_admission_order()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut fixture = Fixture::new(CHILD_INSPECTION_CONCURRENCY + 2);
    let stats = fixture.service.children.clone().expect("child fixture");
    let (provider, shutdown, server) = serve_operation_service(fixture.service).await?;
    let expected = fixture.children;
    let children = expected.clone();
    let work = tokio::spawn(async move { inspect_children(&provider, children).await });
    let mut started = BTreeSet::new();
    for _ in 0..CHILD_INSPECTION_CONCURRENCY {
        started.insert(
            tokio::time::timeout(Duration::from_secs(5), fixture.started.recv())
                .await?
                .expect("child started"),
        );
    }
    assert_eq!(
        started,
        expected[..CHILD_INSPECTION_CONCURRENCY]
            .iter()
            .copied()
            .collect()
    );
    // Complete the initial requests in reverse order; returned observations
    // must still follow the admission, not completion order.
    for (child, reply) in fixture.replies.drain(..CHILD_INSPECTION_CONCURRENCY).rev() {
        assert!(reply.send(Ok(state(child))).is_ok());
    }
    for _ in 0..2 {
        assert!(
            tokio::time::timeout(Duration::from_secs(5), fixture.started.recv())
                .await?
                .is_some()
        );
    }
    for (child, reply) in fixture.replies {
        assert!(reply.send(Ok(state(child))).is_ok());
    }
    let observations = tokio::time::timeout(Duration::from_secs(5), work).await???;
    assert_eq!(
        observations
            .into_iter()
            .map(|value| value.id)
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(
        stats.maximum.load(Ordering::SeqCst),
        CHILD_INSPECTION_CONCURRENCY
    );
    let _ = shutdown.send(());
    server.await??;
    Ok(())
}

#[tokio::test]
async fn concurrent_child_inspections_reject_failure_and_substituted_identity()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for substituted in [false, true] {
        let mut fixture = Fixture::new(2);
        let (provider, shutdown, server) = serve_operation_service(fixture.service).await?;
        let children = fixture.children;
        let wrong_child = children[1];
        let work = tokio::spawn(async move { inspect_children(&provider, children).await });
        for _ in 0..2 {
            assert!(
                tokio::time::timeout(Duration::from_secs(5), fixture.started.recv())
                    .await?
                    .is_some()
            );
        }
        let (_, first) = fixture.replies.remove(0);
        let reply = if substituted {
            Ok(state(wrong_child))
        } else {
            Err(Status::permission_denied("revoked"))
        };
        assert!(first.send(reply).is_ok());
        let expected = if substituted {
            "machine identity was substituted"
        } else {
            "revoked"
        };
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), work).await??,
            Err(ProviderError::Rejected(expected.into()))
        );
        drop(fixture.replies);
        let _ = shutdown.send(());
        server.await??;
    }
    Ok(())
}
