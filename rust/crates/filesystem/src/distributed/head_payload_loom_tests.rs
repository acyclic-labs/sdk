use super::*;
use crate::sync::atomic::{AtomicBool, Ordering};
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitId, CommittedEnvelope,
    ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation, MemoryStream, ReadRequest,
    RecordStream, StreamBounds, StreamError, StreamProvider,
};
use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::AtomicBool as Observation;

const ID: AuthorityId = AuthorityId::from_bytes([61; 16]);
const BUDGET: WorkBudget = crate::performance::BOUNDARY_BUDGET;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    ResamplePayload,
    IgnoreEpoch,
}
loom::thread_local! { static FAULT: Cell<Fault> = Cell::new(Fault::None); }

pub(super) fn observed_epoch(epoch: Epoch) -> Epoch {
    if FAULT.with(Cell::get) == Fault::IgnoreEpoch {
        Epoch::GENESIS
    } else {
        epoch
    }
}

pub(super) fn resample_payload() -> bool {
    FAULT.with(Cell::get) == Fault::ResamplePayload
}

pub(super) async fn resampled_payload<P: StreamProvider>(
    store: &StreamAuthorityStore<P>,
    authority: AuthorityId,
    work: &mut AuthorityWork<'_>,
) -> Result<Bytes, AuthorityStoreError> {
    let tail = store.tail_of(authority, "/records", work).await?;
    let path = work.path(authority, "/records", None)?;
    let path_bytes = path.as_str().len() as u64;
    let latest = store.record(path, tail - 1, work).await;
    work.release(path_bytes)?;
    let (record, allocation) = latest?;
    let decoded = work.decode_durable(&record.value)?;
    let bytes = work.bytes(decoded.payload)?;
    drop((record, allocation));
    Ok(bytes)
}

struct ModelStream {
    inner: MemoryStream,
    change: Change,
    completed: AtomicBool,
    // Written only by the reader and inspected after its Loom join. These
    // observations do not participate in the protocol or schedule.
    retired_at_record_read: Observation,
    cancelled_at_record_read: Observation,
    cancellation: CancellationToken,
}

fn ready<F: Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

#[async_trait::async_trait]
impl StreamProvider for ModelStream {
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.inner.tail(path).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        if request.path.as_str().ends_with("/records") {
            if matches!(self.change, Change::Retire) {
                self.retired_at_record_read
                    .fetch_or(self.completed.load(Ordering::SeqCst), Ordering::Relaxed);
            }
            if matches!(self.change, Change::Cancel) {
                self.cancelled_at_record_read
                    .fetch_or(self.cancellation.is_cancelled(), Ordering::Relaxed);
            }
        }
        self.inner.read(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let result = self.inner.commit(request).await;
        if matches!(self.change, Change::Fence | Change::Retire)
            && matches!(result, Ok(CommitOutcome::Committed(_)))
        {
            self.completed.store(true, Ordering::SeqCst);
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

fn model(change: Change, broken: bool) {
    // Immutable fixture bytes do not depend on any schedule. Build them once;
    // every permutation still gets a fresh provider and cancellation state.
    let first = durable(1, Digest::ZERO, b"old");
    let second = durable(2, first.digest, b"new");
    let first_bytes = StreamsDurableRecord::encode(&first, STREAM_RECORD_LIMIT).unwrap();
    let second_bytes = StreamsDurableRecord::encode(&second, STREAM_RECORD_LIMIT).unwrap();
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
    let change_request = match change {
        Change::Publish => Some(request("/records", vec![second_bytes.into()], 1, Some(2))),
        Change::Fence => Some(request(
            "/epochs",
            vec![
                [EPOCH_DOMAIN, &Epoch::new(2).unwrap().get().to_le_bytes()]
                    .concat()
                    .into(),
            ],
            2,
            Some(1),
        )),
        Change::Retire => Some(request(
            "/retirement",
            vec![acyclic_stream::OwnedBytes::from_static(AUTHORITY_RETIRED)],
            3,
            None,
        )),
        Change::Cancel => None,
    };
    loom::model(move || {
        let inner = MemoryStream::default();
        assert!(matches!(
            ready(inner.commit(initial.clone())).unwrap(),
            CommitOutcome::Committed(_)
        ));
        let cancellation = CancellationToken::new();
        let provider = Arc::new(ModelStream {
            inner,
            change,
            completed: AtomicBool::new(false),
            retired_at_record_read: Observation::new(false),
            cancelled_at_record_read: Observation::new(false),
            cancellation: cancellation.clone(),
        });
        let reader_provider = provider.clone();
        let reader_cancel = cancellation.clone();
        let reader = loom::thread::spawn(move || {
            let completed_at_entry = matches!(change, Change::Fence)
                && reader_provider.completed.load(Ordering::SeqCst);
            let store = StreamAuthorityStore::new(reader_provider);
            FAULT.with(|fault| {
                fault.set(if broken {
                    match change {
                        Change::Publish => Fault::ResamplePayload,
                        Change::Fence => Fault::IgnoreEpoch,
                        _ => Fault::None,
                    }
                } else {
                    Fault::None
                })
            });
            let detached = CancellationToken::new();
            let token = if broken && matches!(change, Change::Cancel) {
                &detached
            } else {
                &reader_cancel
            };
            let mut work = AuthorityWork::new(BUDGET, token);
            let result = if !broken || !matches!(change, Change::Retire) {
                ready(crate::heap_future::in_heap(|| {
                    store.head_payload(ID, 32, &mut work)
                }))
            } else {
                // Deliberate broken variant of the same production function.
                ready(crate::heap_future::in_heap(|| {
                    store.head_payload_checked::<false>(ID, 32, &mut work)
                }))
            };
            work.spent().verify(BUDGET).unwrap();
            (result, completed_at_entry)
        });
        // Two actors suffice: the reader and this writer. An idle third
        // thread joining both adds schedules without adding a race.
        if let Some(request) = &change_request {
            assert!(matches!(
                ready(provider.commit(request.clone())).unwrap(),
                CommitOutcome::Committed(_)
            ));
        } else {
            cancellation.cancel();
        }
        let (result, completed_at_entry) = reader.join().unwrap();
        match result {
            Ok(((head, Some(payload)), allocation)) => {
                assert!(
                    !provider.retired_at_record_read.load(Ordering::Relaxed),
                    "retired payload escaped the second retirement check"
                );
                assert!(
                    !provider.cancelled_at_record_read.load(Ordering::Relaxed),
                    "cancelled payload escaped admission"
                );
                let expected = if head.sequence == first.sequence {
                    &first
                } else {
                    &second
                };
                assert_eq!(head.sequence, expected.sequence);
                assert_eq!(head.digest, expected.digest);
                assert_eq!(
                    payload, expected.payload,
                    "head and payload came from different publications"
                );
                if completed_at_entry && matches!(change, Change::Fence) {
                    assert_eq!(
                        head.epoch,
                        Epoch::new(2).unwrap(),
                        "completed fence was lost"
                    );
                }
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
fn real_head_payload_explores_publication() {
    model(Change::Publish, false);
}

#[test]
fn real_head_payload_explores_fencing() {
    model(Change::Fence, false);
}

#[test]
fn real_head_payload_explores_retirement() {
    model(Change::Retire, false);
}

#[test]
fn real_head_payload_explores_cancellation() {
    model(Change::Cancel, false);
}

#[test]
#[should_panic(expected = "retired payload escaped the second retirement check")]
fn missing_second_retirement_check_is_detected() {
    model(Change::Retire, true);
}

#[test]
#[should_panic(expected = "head and payload came from different publications")]
fn resampling_the_latest_payload_is_detected() {
    model(Change::Publish, true);
}

#[test]
#[should_panic(expected = "completed fence was lost")]
fn ignoring_the_published_epoch_is_detected() {
    model(Change::Fence, true);
}

#[test]
#[should_panic(expected = "cancelled payload escaped admission")]
fn disconnecting_the_operation_cancellation_is_detected() {
    model(Change::Cancel, true);
}
