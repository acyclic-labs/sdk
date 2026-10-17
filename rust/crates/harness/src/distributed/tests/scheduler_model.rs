use super::model_provider::{
    Pending, PublicationCut, block_on, declare_fixture, fixture_event, pause_before_publish,
    publish_until_cut, start_fixture,
};
use super::*;
use crate::{
    Outcome,
    conversation::{BoundedContentFuture, ContentIo},
};
use acyclic_stream::StreamPath;
use loom::sync::atomic::AtomicU8;

const JOIN: &[u8] = br#"[{"slot":"a","value":11},{"slot":"b","value":22}]"#;

struct Contents(Vec<(FileRef, OwnedBytes)>);

impl Contents {
    fn new() -> Self {
        Self(
            [b"null".as_slice(), b"11", b"22", JOIN]
                .into_iter()
                .enumerate()
                .map(|(index, bytes)| {
                    let file = FileRef::new(
                        state_ref().unwrap().volume().clone(),
                        format!("results/{index}.json"),
                        "model-generation".into(),
                        FileDescriptor::from_bytes(bytes, "application/json", &mut None).unwrap(),
                        "value.json".into(),
                        &mut None,
                    )
                    .unwrap();
                    (file, OwnedBytes::from_static(bytes))
                })
                .collect(),
        )
    }

    fn reference(&self, bytes: &[u8]) -> FileRef {
        self.0
            .iter()
            .find(|(_, stored)| stored.as_slice() == bytes)
            .unwrap()
            .0
            .clone()
    }
}

impl ContentResidencyVerifier for Contents {
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, OwnedBytes>> {
        let bytes = self
            .0
            .iter()
            .find(|(stored, _)| stored.descriptor() == file.descriptor())
            .ok_or_else(|| Error::NotFound("model content".into()))?
            .1
            .clone();
        assert!(bytes.len() as u64 <= maximum);
        BoundedContentFuture::new(io, || async move { Ok(bytes) })
    }
}

impl SchedulerPayloadStore for Contents {
    fn stage<'a>(
        &'a self,
        _: OperationId,
        _: &'a str,
        bytes: &'a [u8],
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, FileRef>> {
        BoundedContentFuture::new(io, || async move { Ok(self.reference(bytes)) })
    }
}

fn coordinator<P: StreamProvider>(
    client: &Arc<P>,
    contents: &Arc<Contents>,
) -> Box<DistributedCoordinator<P>> {
    Box::new(
        block_on(DistributedCoordinator::open(
            client,
            contents.clone(),
            Some(contents.clone()),
            &mut None,
        ))
        .unwrap(),
    )
}

fn initialized(client: &Arc<MemoryStream>, contents: &Arc<Contents>, kind: Orchestration) {
    let mut coordinator = coordinator(client, contents);
    let parent = OperationId::from_bytes([1; 16]);
    let mut root = spec(parent, 0).unwrap();
    root.orchestration = kind;
    declare_fixture(&mut coordinator, root, "parent").unwrap();
    let lease = start_fixture(&mut coordinator, parent, &worker("parent"), "parent-start").unwrap();
    fixture_event(
        &mut coordinator,
        SchedulerEvent::WaitingForChildren {
            operation_id: parent,
            fence: LeaseFence::from(&lease.reservation),
        },
        "parent-wait",
    )
    .unwrap();
    for (identity, slot, bytes) in [(2, "a", b"11".as_slice()), (3, "b", b"22")] {
        let operation = OperationId::from_bytes([identity; 16]);
        let mut child = spec(operation, 0).unwrap();
        child.parent = Some(ParentLink {
            operation_id: parent,
            slot: slot.into(),
        });
        declare_fixture(&mut coordinator, child, slot).unwrap();
        let lease = start_fixture(
            &mut coordinator,
            operation,
            &worker(slot),
            &format!("start-{slot}"),
        )
        .unwrap();
        fixture_event(
            &mut coordinator,
            SchedulerEvent::Completed {
                operation_id: operation,
                outcome: Outcome::Succeeded(contents.reference(bytes)),
                fence: Some(LeaseFence::from(&lease.reservation)),
                execution_duration_ns: None,
            },
            &format!("complete-{slot}"),
        )
        .unwrap();
    }
}

fn prepared(
    client: &Arc<MemoryStream>,
    contents: &Arc<Contents>,
    key: &'static str,
    broken: bool,
) -> (Pending<Result<bool>>, Arc<AtomicU8>) {
    let phase = Arc::new(AtomicU8::new(0));
    let provider = Arc::new(PublicationCut {
        inner: (**client).clone(),
        phase: phase.clone(),
        omit_condition: broken,
    });
    let mut coordinator = coordinator(&provider, contents);
    let future: Pending<Result<bool>> = Box::pin(async move {
        Box::pin(coordinator.orchestrate(
            OperationId::from_bytes([1; 16]),
            IdempotencyKey::new(key).unwrap(),
            None,
        ))
        .await
    });
    let future = pause_before_publish(future, &phase);
    (future, phase)
}

fn settlement_model(join: bool, broken: bool) {
    loom::model(move || {
        let client = Arc::new(MemoryStream::default());
        let contents = Arc::new(Contents::new());
        initialized(
            &client,
            &contents,
            if join {
                Orchestration::Join
            } else {
                Orchestration::Race
            },
        );
        let tail = block_on(client.tail(StreamPath::new(COORDINATOR_PATH).unwrap())).unwrap();
        let (one, first_phase) = prepared(&client, &contents, "settle-one", broken);
        let (two, second_phase) = prepared(&client, &contents, "settle-two", broken);
        let first = loom::thread::spawn(move || publish_until_cut(one, first_phase));
        let second = loom::thread::spawn(move || publish_until_cut(two, second_phase));
        let first = first.join().unwrap();
        let second = second.join().unwrap();
        // Check durable publication before sequentially finishing acknowledgement
        // and replay, which cannot influence either already-completed append.
        assert_eq!(
            block_on(client.tail(StreamPath::new(COORDINATOR_PATH).unwrap())).unwrap(),
            tail + 1,
            "settlement appended more than once"
        );
        let results = [block_on(first), block_on(second)];
        assert!(
            results
                .iter()
                .all(|result| matches!(result, Ok(_) | Err(Error::Conflict(_))))
        );
        assert_eq!(
            results
                .into_iter()
                .filter(|result| matches!(result, Ok(true)))
                .count(),
            1
        );
        let reopened = coordinator(&client, &contents);
        let parent = reopened
            .scheduler
            .operation(OperationId::from_bytes([1; 16]))
            .unwrap();
        assert_eq!(parent.phase, OperationPhase::Terminal);
        assert!(parent.reservation.is_none());
        assert_eq!(
            parent.outcome,
            Some(Outcome::Succeeded(contents.reference(if join {
                JOIN
            } else {
                b"11"
            })))
        );
    });
}

#[test]
fn concurrent_coordinators_publish_join_and_race_settlement_once() {
    for join in [false, true] {
        settlement_model(join, false);
        let error = std::panic::catch_unwind(|| settlement_model(join, true))
            .expect_err("removing publication CAS must fail");
        let message = error
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| error.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            message.contains("settlement appended more than once"),
            "{message}"
        );
    }
}
