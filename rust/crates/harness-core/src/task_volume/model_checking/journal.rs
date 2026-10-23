//! One bounded journal implementing the provider contract beneath real volumes.
use acyclic_stream::*;
use loom::sync::Mutex;

pub(crate) struct Journal {
    operation_claims: acyclic_stream::OperationClaims,
    path: StreamPath,
    // Leaf lock: provider operations invoke no caller or volume code while held.
    records: Mutex<Vec<Record>>,
}

impl Journal {
    pub(crate) fn new(path: StreamPath, records: Vec<Record>) -> Self {
        Self {
            path,
            operation_claims: Default::default(),
            records: Mutex::new(records),
        }
    }
}

#[async_trait::async_trait]
impl StreamProvider for Journal {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        &self.operation_claims
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        assert_eq!(path, self.path);
        Ok(self.records.lock().unwrap().len() as u64)
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
            .records
            .lock()
            .unwrap()
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
        let mut retained = self.records.lock().unwrap();
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
        assert_eq!(bodies.len(), 1);
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
    async fn follow(
        &self,
        _: StreamPath,
        _: u64,
        _filter: acyclic_stream::FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn children(&self, _: ChildrenRequest) -> Result<ChildStream, StreamError> {
        Err(StreamError::Unsupported)
    }
    async fn read_commit(&self, _: CommitId) -> Result<CommittedEnvelope, StreamError> {
        Err(StreamError::Unsupported)
    }
}
