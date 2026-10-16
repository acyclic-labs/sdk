use super::*;
use crate::sync::Mutex;
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitId, CommittedEnvelope,
    ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation, MemoryStream, ReadRequest,
    RecordStream, StreamBounds, StreamError, StreamProvider,
};
use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

const ID: AuthorityId = AuthorityId::from_bytes([61; 16]);
const BUDGET: WorkBudget = crate::performance::BOUNDARY_BUDGET;

// The provider contract makes each stream operation atomic. This adapter runs
// the real MemoryStream operations under the modeled boundary mutex; their
// futures must complete immediately. It models the authority algorithm, not
// the dependency's uninstrumented Tokio locks. No filesystem call holds this
// mutex; no other model lock is acquired while it is held.
struct ModelStream(Mutex<State>, CancellationToken);
struct State {
    inner: MemoryStream,
    retired: bool,
    retired_at_record_read: bool,
    cancelled_at_record_read: bool,
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => unreachable!("atomic memory-provider operation yielded"),
    }
}

#[async_trait::async_trait]
impl StreamProvider for ModelStream {
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        ready(self.0.lock().unwrap().inner.tail(path))
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        let mut state = self.0.lock().unwrap();
        if request.path.as_str().ends_with("/records") {
            state.retired_at_record_read |= state.retired;
            state.cancelled_at_record_read |= self.1.is_cancelled();
        }
        ready(state.inner.read(request))
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let mut state = self.0.lock().unwrap();
        let retirement = request.mutations.iter().any(|mutation| {
            matches!(mutation,
            CommitMutation::Append { path, .. } if path.as_str().ends_with("/retirement"))
        });
        let result = ready(state.inner.commit(request));
        if matches!(result, Ok(CommitOutcome::Committed(_))) {
            state.retired |= retirement;
        }
        result
    }
    async fn inspect_idempotency(
        &self,
        _: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn bounds(&self, _: StreamPath) -> Result<StreamBounds, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn append(&self, _: AppendRequest) -> Result<AppendOutcome, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn fork(&self, _: ForkRequest) -> Result<ForkReceipt, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn follow(&self, _: StreamPath, _: u64) -> Result<RecordStream, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn children(&self, _: ChildrenRequest) -> Result<ChildStream, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn read_commit(&self, _: CommitId) -> Result<CommittedEnvelope, StreamError> {
        Err(StreamError::Unsupported)
    }
}

#[derive(Clone, Copy)]
enum Change {
    Publish,
    Fence,
    Retire,
    Cancel,
}

fn durable(sequence: u64, previous_digest: Digest, body: &'static [u8]) -> DurableCommit {
    let mut record = DurableCommit {
        epoch: Epoch::GENESIS,
        sequence: Sequence::new(sequence),
        operation_id: OperationId::from_bytes([sequence as u8; 16]),
        fingerprint: Digest::ZERO,
        previous_digest,
        digest: Digest::ZERO,
        payload: Bytes::from_static(body),
        settled_at_micros: None,
    };
    record.digest = authority_commit_digest(
        ID,
        record.epoch,
        record.sequence,
        record.operation_id,
        record.fingerprint,
        previous_digest,
        body,
    );
    record
}

fn request(
    suffix: &str,
    records: Vec<acyclic_stream::OwnedBytes>,
    key: u8,
    tail: Option<u64>,
) -> CommitRequest {
    let path = stream_path(ID, suffix).unwrap();
    CommitRequest {
        conditions: vec![match tail {
            Some(expected) => CommitCondition::Tail {
                path: path.clone(),
                expected,
            },
            None => CommitCondition::Absent { path: path.clone() },
        }],
        mutations: vec![CommitMutation::Append { path, records }],
        idempotency_key: stream_key(b"head-model", &[key]).unwrap(),
    }
}

fn model(change: Change, recheck: bool) {
    loom::model(move || {
        let first = durable(1, Digest::ZERO, b"old");
        let second = durable(2, first.digest, b"new");
        let first_bytes = StreamsDurableRecord::encode(&first, STREAM_RECORD_LIMIT).unwrap();
        let second_bytes = StreamsDurableRecord::encode(&second, STREAM_RECORD_LIMIT).unwrap();
        let inner = MemoryStream::default();
        let epoch = Epoch::GENESIS.get().to_le_bytes();
        let mut initial = request(
            "/records",
            vec![[GENESIS_DOMAIN, &epoch].concat().into(), first_bytes.into()],
            0,
            None,
        );
        initial.conditions.push(CommitCondition::Absent {
            path: stream_path(ID, "/epochs").unwrap(),
        });
        initial.mutations.push(CommitMutation::Append {
            path: stream_path(ID, "/epochs").unwrap(),
            records: vec![[EPOCH_DOMAIN, &epoch].concat().into()],
        });
        assert!(matches!(
            ready(inner.commit(initial)).unwrap(),
            CommitOutcome::Committed(_)
        ));
        let cancellation = CancellationToken::new();
        let provider = Arc::new(ModelStream(
            Mutex::new(State {
                inner,
                retired: false,
                retired_at_record_read: false,
                cancelled_at_record_read: false,
            }),
            cancellation.clone(),
        ));
        let reader_provider = provider.clone();
        let reader_cancel = cancellation.clone();
        let reader = loom::thread::spawn(move || {
            let store = StreamAuthorityStore::new(reader_provider);
            let mut work = AuthorityWork::new(BUDGET, &reader_cancel);
            let result = if recheck {
                ready(store.head_payload(ID, 32, &mut work))
            } else {
                // Deliberate broken variant of the same production function.
                ready(store.head_payload_checked::<false>(ID, 32, &mut work))
            };
            work.spent().verify(BUDGET).unwrap();
            result
        });
        let writer_provider = provider.clone();
        let writer = loom::thread::spawn(move || {
            let request = match change {
                Change::Publish => request("/records", vec![second_bytes.into()], 1, Some(2)),
                Change::Fence => request(
                    "/epochs",
                    vec![
                        [EPOCH_DOMAIN, &Epoch::new(2).unwrap().get().to_le_bytes()]
                            .concat()
                            .into(),
                    ],
                    2,
                    Some(1),
                ),
                Change::Retire => request(
                    "/retirement",
                    vec![acyclic_stream::OwnedBytes::from_static(AUTHORITY_RETIRED)],
                    3,
                    None,
                ),
                Change::Cancel => {
                    cancellation.cancel();
                    return;
                }
            };
            assert!(matches!(
                ready(writer_provider.commit(request)).unwrap(),
                CommitOutcome::Committed(_)
            ));
        });
        let result = reader.join().unwrap();
        writer.join().unwrap();
        match result {
            Ok(((head, Some(payload)), allocation)) => {
                assert!(
                    !provider.0.lock().unwrap().retired_at_record_read,
                    "retired payload escaped the second retirement check"
                );
                assert!(
                    !provider.0.lock().unwrap().cancelled_at_record_read,
                    "cancelled payload escaped admission"
                );
                let expected = if head.sequence == first.sequence {
                    &first
                } else {
                    &second
                };
                assert_eq!(head.sequence, expected.sequence);
                assert_eq!(head.digest, expected.digest);
                assert_eq!(payload, expected.payload);
                assert!(
                    head.epoch == Epoch::GENESIS
                        || matches!(change, Change::Fence) && head.epoch == Epoch::new(2).unwrap()
                );
                drop(payload);
                drop(allocation);
            }
            Err(AuthorityStoreError::Retired) => assert!(matches!(change, Change::Retire)),
            Err(AuthorityStoreError::Cancelled) => assert!(matches!(change, Change::Cancel)),
            other => unreachable!("unexpected head read: {other:?}"),
        }
    });
}

#[test]
fn real_head_payload_explores_publication_fence_retirement_and_cancellation() {
    for change in [
        Change::Publish,
        Change::Fence,
        Change::Retire,
        Change::Cancel,
    ] {
        model(change, true);
    }
}

#[test]
#[should_panic(expected = "retired payload escaped the second retirement check")]
fn missing_second_retirement_check_is_detected() {
    model(Change::Retire, false);
}
