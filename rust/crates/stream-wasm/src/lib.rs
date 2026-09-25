//! Browser binding for the canonical Rust memory stream provider.

#[cfg(target_arch = "wasm32")]
mod browser {
    use acyclic_stream::{
        AppendOutcome, AppendRequest, ChildrenPageRequest, CommitCondition, CommitConflict,
        CommitId, CommitMutation, CommitOutcome, CommitRequest, CommittedEnvelope,
        CommittedMutation, ForkRequest, IdempotencyKey, IdempotencyObservation, IdempotencyOutcome,
        MemoryStream, ReadRequest, Record, StreamError, StreamPath, StreamProvider, wire,
    };
    use bytes::Bytes;
    use futures::{
        StreamExt as _,
        future::{Either, select},
    };
    use prost::Message;
    use tokio::sync::{Mutex, watch};
    use wasm_bindgen::prelude::*;

    fn path(value: String) -> Result<StreamPath, StreamError> {
        StreamPath::new(value)
    }
    fn key(value: Bytes) -> Result<IdempotencyKey, StreamError> {
        IdempotencyKey::new(value)
    }
    fn optional_key(value: Option<Bytes>) -> Result<Option<IdempotencyKey>, StreamError> {
        value.map(key).transpose()
    }
    fn commit_id(value: &[u8]) -> Result<CommitId, StreamError> {
        Ok(CommitId::from_bytes(
            value.try_into().map_err(|_| StreamError::InvalidArgument)?,
        ))
    }
    fn bytes(value: CommitId) -> Bytes {
        Bytes::copy_from_slice(value.as_bytes())
    }
    fn decode<M: Message + Default>(value: &[u8]) -> Result<M, JsValue> {
        M::decode(value).map_err(|_| error(StreamError::InvalidArgument))
    }
    fn error(value: StreamError) -> JsValue {
        let code = match value {
            StreamError::InvalidPath => "invalid_path",
            StreamError::InvalidArgument => "invalid_argument",
            StreamError::LimitExceeded => "limit_exceeded",
            StreamError::NotFound => "stream_not_found",
            StreamError::AlreadyExists => "destination_exists",
            StreamError::Retired => "stream_retired",
            StreamError::PrefixNotRetained => "prefix_not_retained",
            StreamError::OutOfRange => "cursor_trimmed",
            StreamError::HierarchyChanged => "hierarchy_changed",
            StreamError::IdempotencyMismatch => "idempotency_mismatch",
            StreamError::Capacity => "capacity_exhausted",
            StreamError::AccessDenied => "access_denied",
            StreamError::Unavailable => "unavailable",
            StreamError::DeadlineElapsed => "deadline_elapsed",
            StreamError::Unsupported => "unsupported",
        };
        error_code(value, code)
    }
    fn error_code(value: StreamError, code: &str) -> JsValue {
        let message = js_sys::Error::new(&value.to_string());
        let _ = js_sys::Reflect::set(
            &message,
            &JsValue::from_str("code"),
            &JsValue::from_str(code),
        );
        message.into()
    }
    fn record(value: Record) -> wire::Record {
        wire::Record {
            sequence: value.sequence,
            value: value.value,
            commit_id: bytes(value.commit_id),
        }
    }
    fn envelope(value: CommittedEnvelope) -> wire::CommittedEnvelope {
        wire::CommittedEnvelope {
            commit_id: bytes(value.commit_id),
            mutations: value
                .mutations
                .into_iter()
                .map(|mutation| {
                    use wire::committed_mutation::Mutation;
                    let mutation = match mutation {
                        CommittedMutation::Append(value) => {
                            Mutation::Append(wire::CommittedAppend {
                                path: value.path.to_string(),
                                start: value.start,
                                end: value.end,
                                tail: value.tail,
                                records: value.records.into_iter().map(record).collect(),
                            })
                        }
                        CommittedMutation::Fork(value) => Mutation::Fork(wire::CommittedFork {
                            source: value.source.to_string(),
                            destination: value.destination.to_string(),
                            forked_at: value.forked_at,
                            tail: value.tail,
                        }),
                        CommittedMutation::Trim(value) => Mutation::Trim(wire::CommittedTrim {
                            path: value.path.to_string(),
                            trim_point: value.trim_point,
                        }),
                        CommittedMutation::Delete(value) => {
                            Mutation::Delete(wire::CommittedDelete {
                                path: value.path.to_string(),
                            })
                        }
                    };
                    wire::CommittedMutation {
                        mutation: Some(mutation),
                    }
                })
                .collect(),
        }
    }
    fn append_outcome(value: AppendOutcome) -> wire::AppendResponse {
        use wire::append_response::Outcome;
        let outcome = match value {
            AppendOutcome::Committed(value) => Outcome::Committed(wire::AppendReceipt {
                start: value.start,
                end: value.end,
                tail: value.tail,
                commit_id: bytes(value.commit_id),
            }),
            AppendOutcome::TailConflict { actual_tail } => {
                Outcome::Conflict(wire::TailConflict { actual_tail })
            }
        };
        wire::AppendResponse {
            outcome: Some(outcome),
        }
    }
    fn commit_outcome(value: CommitOutcome) -> wire::CommitResponse {
        use wire::commit_response::Outcome;
        let outcome = match value {
            CommitOutcome::Committed(value) => Outcome::Committed(envelope(value)),
            CommitOutcome::Conflict(conflicts) => Outcome::Conflict(wire::CommitConflicts {
                conflicts: conflicts
                    .into_iter()
                    .map(|value| {
                        use wire::commit_conflict::Conflict;
                        let conflict = match value {
                            CommitConflict::Tail {
                                path,
                                expected,
                                actual,
                            } => Conflict::Tail(wire::TailCommitConflict {
                                path: path.to_string(),
                                expected,
                                actual,
                            }),
                            CommitConflict::Exists { path } => {
                                Conflict::Exists(wire::ExistsCommitConflict {
                                    path: path.to_string(),
                                })
                            }
                            CommitConflict::Retired { path } => {
                                Conflict::Retired(wire::RetiredCommitConflict {
                                    path: path.to_string(),
                                })
                            }
                        };
                        wire::CommitConflict {
                            conflict: Some(conflict),
                        }
                    })
                    .collect(),
            }),
        };
        wire::CommitResponse {
            outcome: Some(outcome),
        }
    }
    fn observation(value: IdempotencyObservation) -> wire::IdempotencyObservation {
        use wire::idempotency_observation::Outcome;
        let outcome = match value.outcome {
            IdempotencyOutcome::Append(value) => Outcome::Append(append_outcome(value)),
            IdempotencyOutcome::Fork(value) => Outcome::Fork(wire::ForkReceipt {
                source: value.source.to_string(),
                destination: value.destination.to_string(),
                forked_at: value.forked_at,
                tail: value.tail,
                commit_id: bytes(value.commit_id),
            }),
            IdempotencyOutcome::Trim(value) => Outcome::Trim(wire::TrimReceipt {
                path: value.path.to_string(),
                trim_point: value.trim_point,
                commit_id: bytes(value.commit_id),
            }),
            IdempotencyOutcome::Delete(value) => Outcome::Delete(wire::DeleteReceipt {
                path: value.path.to_string(),
                commit_id: bytes(value.commit_id),
            }),
            IdempotencyOutcome::Commit(value) => Outcome::Commit(commit_outcome(value)),
        };
        wire::IdempotencyObservation {
            idempotency_key: Bytes::copy_from_slice(value.idempotency_key.as_bytes()),
            request_digest: Bytes::copy_from_slice(&value.request_digest),
            outcome: Some(outcome),
        }
    }
    fn condition(value: wire::CommitCondition) -> Result<CommitCondition, StreamError> {
        match value.condition.ok_or(StreamError::InvalidArgument)? {
            wire::commit_condition::Condition::Tail(value) => Ok(CommitCondition::Tail {
                path: path(value.path)?,
                expected: value.expected,
            }),
            wire::commit_condition::Condition::Absent(value) => Ok(CommitCondition::Absent {
                path: path(value.path)?,
            }),
        }
    }
    fn mutation(value: wire::CommitMutation) -> Result<CommitMutation, StreamError> {
        match value.mutation.ok_or(StreamError::InvalidArgument)? {
            wire::commit_mutation::Mutation::Append(value) => Ok(CommitMutation::Append {
                path: path(value.path)?,
                records: value.records,
            }),
            wire::commit_mutation::Mutation::Fork(value) => Ok(CommitMutation::Fork {
                source: path(value.source)?,
                destination: path(value.destination)?,
                at_tail: value.at_tail,
            }),
            wire::commit_mutation::Mutation::Trim(value) => Ok(CommitMutation::Trim {
                path: path(value.path)?,
                before: value.before,
            }),
            wire::commit_mutation::Mutation::Delete(value) => Ok(CommitMutation::Delete {
                path: path(value.path)?,
            }),
        }
    }

    /// One Rust-owned follow cursor. `close` interrupts a pending `next` without polling state in JS.
    #[wasm_bindgen]
    pub struct FollowBinding {
        records: Mutex<acyclic_stream::RecordStream>,
        closed: watch::Sender<bool>,
    }

    #[wasm_bindgen]
    impl FollowBinding {
        pub async fn next(&self) -> Result<Option<Vec<u8>>, JsValue> {
            if *self.closed.borrow() {
                return Ok(None);
            }
            let mut cancellation = self.closed.subscribe();
            if *cancellation.borrow() {
                return Ok(None);
            }
            let mut records = self.records.lock().await;
            let next = records.next();
            let cancelled = cancellation.changed();
            futures::pin_mut!(next, cancelled);
            match select(next, cancelled).await {
                Either::Left((Some(Ok(value)), _)) => Ok(Some(
                    wire::ReadResponse {
                        record: Some(record(value)),
                    }
                    .encode_to_vec(),
                )),
                Either::Left((Some(Err(value)), _)) => Err(error(value)),
                Either::Left((None, _)) | Either::Right((_, _)) => Ok(None),
            }
        }
        pub fn close(&self) {
            self.closed.send_replace(true);
        }
    }

    /// One independent bounded Rust state machine, exchanged as canonical Stream v2 protobuf bytes.
    #[wasm_bindgen]
    pub struct MemoryStreamBinding {
        inner: MemoryStream,
    }

    #[wasm_bindgen]
    impl MemoryStreamBinding {
        #[wasm_bindgen(constructor)]
        pub fn new() -> Self {
            Self {
                inner: MemoryStream::default(),
            }
        }

        pub async fn inspect_idempotency(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::InspectIdempotencyRequest = decode(&request)?;
            let value = self
                .inner
                .inspect_idempotency(key(request.idempotency_key).map_err(error)?)
                .await
                .map_err(error)?;
            Ok(wire::InspectIdempotencyResponse {
                observation: value.map(observation),
            }
            .encode_to_vec())
        }
        pub async fn tail(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::TailRequest = decode(&request)?;
            let bounds = self
                .inner
                .bounds(path(request.path).map_err(error)?)
                .await
                .map_err(error)?;
            Ok(wire::TailResponse {
                tail: bounds.tail,
                trim_point: Some(bounds.trim_point),
            }
            .encode_to_vec())
        }
        pub async fn append(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::AppendRequest = decode(&request)?;
            let value = self
                .inner
                .append(AppendRequest {
                    path: path(request.path).map_err(error)?,
                    records: request.records,
                    if_tail: request.if_tail,
                    idempotency_key: optional_key(request.idempotency_key).map_err(error)?,
                })
                .await
                .map_err(error)?;
            Ok(append_outcome(value).encode_to_vec())
        }
        pub async fn fork(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::ForkRequest = decode(&request)?;
            let value = self
                .inner
                .fork(ForkRequest {
                    source: path(request.source).map_err(error)?,
                    destination: path(request.destination).map_err(error)?,
                    at_tail: request.at_tail,
                    idempotency_key: optional_key(request.idempotency_key).map_err(error)?,
                })
                .await
                .map_err(error)?;
            Ok(wire::ForkReceipt {
                source: value.source.to_string(),
                destination: value.destination.to_string(),
                forked_at: value.forked_at,
                tail: value.tail,
                commit_id: bytes(value.commit_id),
            }
            .encode_to_vec())
        }
        pub async fn trim(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::TrimRequest = decode(&request)?;
            let value = self
                .inner
                .trim(
                    path(request.path).map_err(error)?,
                    request.before,
                    key(request
                        .idempotency_key
                        .ok_or(StreamError::InvalidArgument)
                        .map_err(error)?)
                    .map_err(error)?,
                )
                .await
                .map_err(error)?;
            Ok(wire::TrimReceipt {
                path: value.path.to_string(),
                trim_point: value.trim_point,
                commit_id: bytes(value.commit_id),
            }
            .encode_to_vec())
        }
        pub async fn delete(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::DeleteRequest = decode(&request)?;
            let value = self
                .inner
                .delete(
                    path(request.path).map_err(error)?,
                    key(request
                        .idempotency_key
                        .ok_or(StreamError::InvalidArgument)
                        .map_err(error)?)
                    .map_err(error)?,
                )
                .await
                .map_err(error)?;
            Ok(wire::DeleteReceipt {
                path: value.path.to_string(),
                commit_id: bytes(value.commit_id),
            }
            .encode_to_vec())
        }
        pub async fn read(&self, request: Vec<u8>) -> Result<JsValue, JsValue> {
            let request: wire::ReadRequest = decode(&request)?;
            let mut records = self
                .inner
                .read(ReadRequest {
                    path: path(request.path).map_err(error)?,
                    from: request.from,
                    limit: request.limit,
                })
                .await
                .map_err(error)?;
            let result = js_sys::Array::new();
            while let Some(item) = records.next().await {
                let frame = wire::ReadResponse {
                    record: Some(record(item.map_err(error)?)),
                }
                .encode_to_vec();
                result.push(&js_sys::Uint8Array::from(frame.as_slice()));
            }
            Ok(result.into())
        }
        pub async fn follow(&self, request: Vec<u8>) -> Result<FollowBinding, JsValue> {
            let request: wire::FollowRequest = decode(&request)?;
            let records = self
                .inner
                .follow(path(request.path).map_err(error)?, request.from)
                .await
                .map_err(error)?;
            let (closed, _) = watch::channel(false);
            Ok(FollowBinding {
                records: Mutex::new(records),
                closed,
            })
        }
        pub async fn children_page(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::ChildrenPageRequest = decode(&request)?;
            let value = self
                .inner
                .children_page(ChildrenPageRequest {
                    parent: request.parent.map(path).transpose().map_err(error)?,
                    after: request.after.map(path).transpose().map_err(error)?,
                    hierarchy_version: request
                        .hierarchy_version
                        .as_deref()
                        .map(commit_id)
                        .transpose()
                        .map_err(error)?,
                    limit: request.limit,
                })
                .await
                .map_err(error)?;
            Ok(wire::ChildrenPageResponse {
                hierarchy_version: bytes(value.hierarchy_version),
                children: value
                    .children
                    .into_iter()
                    .map(|item| wire::Child {
                        path: item.path.to_string(),
                    })
                    .collect(),
                next_after: value.next_after.map(|item| item.to_string()),
            }
            .encode_to_vec())
        }
        pub async fn commit(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::CommitRequest = decode(&request)?;
            let value = CommitRequest {
                conditions: request
                    .conditions
                    .into_iter()
                    .map(condition)
                    .collect::<Result<_, _>>()
                    .map_err(error)?,
                mutations: request
                    .mutations
                    .into_iter()
                    .map(mutation)
                    .collect::<Result<_, _>>()
                    .map_err(error)?,
                idempotency_key: key(request.idempotency_key).map_err(error)?,
            };
            let outcome = if let Some(deadline) = request.deadline_unix_millis {
                self.inner.commit_before(value, deadline).await
            } else {
                self.inner.commit(value).await
            }
            .map_err(error)?;
            Ok(commit_outcome(outcome).encode_to_vec())
        }
        pub async fn read_commit(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request: wire::ReadCommitRequest = decode(&request)?;
            let value = self
                .inner
                .read_commit(commit_id(&request.commit_id).map_err(error)?)
                .await
                .map_err(|value| {
                    if value == StreamError::NotFound {
                        error_code(value, "commit_not_found")
                    } else {
                        error(value)
                    }
                })?;
            Ok(envelope(value).encode_to_vec())
        }
    }
}
