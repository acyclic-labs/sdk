//! One bounded journal implementing the provider contract beneath real volumes.
use acyclic_stream::*;
use loom::sync::Mutex;
use std::{
    sync::Arc,
    task::{Poll, Waker},
};

struct State {
    records: Vec<Record>,
    waiters: Vec<Waker>,
}

pub(crate) struct Journal {
    path: StreamPath,
    // Leaf lock: provider operations invoke no caller or volume code while held.
    state: Arc<Mutex<State>>,
}

impl Journal {
    /// Retain real encoded seed records before concurrent provider operations.
    pub(crate) async fn snapshot(provider: &impl StreamProvider, path: StreamPath) -> Self {
        use futures::TryStreamExt as _;
        let limit = u32::try_from(provider.tail(path.clone()).await.unwrap()).unwrap();
        let records = provider
            .read(ReadRequest {
                path: path.clone(),
                from: 0,
                limit,
            })
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        Self::new(path, records)
    }

    pub(crate) fn new(path: StreamPath, records: Vec<Record>) -> Self {
        Self {
            path,
            state: Arc::new(Mutex::new(State {
                records,
                waiters: Vec::new(),
            })),
        }
    }

    fn subscription(&self, mut from: u64) -> RecordStream {
        let state = self.state.clone();
        Box::pin(futures::stream::poll_fn(move |cx| {
            let mut state = state.lock().unwrap();
            if let Some(record) = state.records.get(from as usize).cloned() {
                from += 1;
                return Poll::Ready(Some(Ok(record)));
            }
            // Cursor observation and waiter registration share the leaf lock.
            // Commit drains waiters under that lock and wakes only after release.
            if !state
                .waiters
                .iter()
                .any(|waker| waker.will_wake(cx.waker()))
            {
                state.waiters.push(cx.waker().clone());
            }
            Poll::Pending
        }))
    }
}

#[async_trait::async_trait]
impl StreamProvider for Journal {
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        assert_eq!(path, self.path);
        Ok(self.state.lock().unwrap().records.len() as u64)
    }

    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        let accepted = self.tail(path).await?;
        Ok(StreamWatermarks {
            accepted,
            durable: accepted,
        })
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        assert_eq!(request.path, self.path);
        let records = self
            .state
            .lock()
            .unwrap()
            .records
            .iter()
            .skip(request.from as usize)
            .take(request.limit as usize)
            .cloned()
            .map(Ok)
            .collect::<Vec<_>>();
        Ok(Box::pin(futures::stream::iter(records)))
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        assert_eq!(request.conditions.len(), 1);
        let mut state = self.state.lock().unwrap();
        let retained = &mut state.records;
        for condition in request.conditions {
            let matches = match condition {
                CommitCondition::Head {
                    path,
                    expected,
                    commit_id,
                } => {
                    assert_eq!(path, self.path);
                    retained.len() as u64 == expected
                        && retained.last().unwrap().commit_id == commit_id
                }
                CommitCondition::Tail { path, expected } => {
                    assert_eq!(path, self.path);
                    retained.len() as u64 == expected
                }
                other => panic!("unexpected condition: {other:?}"),
            };
            if !matches {
                return Ok(CommitOutcome::Conflict(Vec::new()));
            }
        }
        let [
            CommitMutation::Append {
                path,
                records: bodies,
            },
        ] = request.mutations.as_slice()
        else {
            panic!("model requires one journal append")
        };
        assert_eq!(path, &self.path);
        assert!(!bodies.is_empty());
        let mut hash = blake3::Hasher::new();
        hash.update(retained.last().unwrap().commit_id.as_bytes());
        for body in bodies {
            hash.update(body);
        }
        let commit_id = CommitId::from_bytes(*hash.finalize().as_bytes());
        let start = retained.len() as u64;
        let records: Vec<_> = bodies
            .iter()
            .enumerate()
            .map(|(offset, body)| Record {
                sequence: start + offset as u64,
                value: body.clone(),
                commit_id,
                committed_at_micros: 1,
            })
            .collect();
        let end = start + records.len() as u64;
        retained.extend(records.iter().cloned());
        let waiters = std::mem::take(&mut state.waiters);
        drop(state);
        for waker in waiters {
            waker.wake();
        }
        Ok(CommitOutcome::Committed(CommittedEnvelope {
            commit_id,
            mutations: vec![CommittedMutation::Append(CommittedAppend {
                path: path.clone(),
                start,
                end,
                tail: end,
                records,
            })],
        }))
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
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        assert_eq!(path, self.path);
        Ok(self.subscription(from))
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
    ) -> Result<RecordStream, StreamError> {
        assert_eq!(path, self.path);
        let state = self.state.lock().unwrap();
        assert_eq!(
            from.checked_sub(1)
                .and_then(|sequence| state.records.get(sequence as usize))
                .map(|record| record.commit_id),
            Some(preceding)
        );
        // This model retains an immutable history: no compaction or replacement.
        // Subscription replays all appends since this atomically checked anchor.
        drop(state);
        Ok(self.subscription(from))
    }
    async fn children(&self, _: ChildrenRequest) -> Result<ChildStream, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn read_commit(&self, _: CommitId) -> Result<CommittedEnvelope, StreamError> {
        Err(StreamError::Unsupported)
    }
}
