//! Canonical protobuf provider port. SQL, DO RPC and guest code share no Rust
//! pointers; this adapter only projects calls and owns cursor cancellation.
use crate::*;
use futures::StreamExt;
use js_sys::{Promise, Reflect, Uint8Array};
use prost::Message;
use std::rc::Rc;
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen]
extern "C" {
    #[derive(Clone)]
    type Port;
    #[wasm_bindgen(method, catch)]
    fn dispatch(this: &Port, operation: &str, input: &[u8]) -> Result<Promise, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn open(
        this: &Port,
        operation: &str,
        input: &[u8],
        preceding: &[u8],
    ) -> Result<Promise, JsValue>;
    type Cursor;
    #[wasm_bindgen(method, catch)]
    fn next(this: &Cursor) -> Result<Promise, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn close(this: &Cursor) -> Result<(), JsValue>;
}

fn failed(_: JsValue) -> StreamError {
    StreamError::Unavailable
}
async fn reply(promise: Result<Promise, JsValue>) -> Result<JsValue, StreamError> {
    let reply = JsFuture::from(promise.map_err(failed)?)
        .await
        .map_err(failed)?;
    let code = Reflect::get(&reply, &"code".into()).map_err(failed)?;
    if let Some(code) = code.as_string() {
        return Err(StreamError::from_code(&code).unwrap_or(StreamError::InvalidResponse));
    }
    if !code.is_undefined() {
        return Err(StreamError::InvalidResponse);
    }
    Reflect::get(&reply, &"value".into()).map_err(failed)
}
fn snapshot(value: JsValue) -> Result<Vec<u8>, StreamError> {
    let bytes: Uint8Array = value.dyn_into().map_err(|_| StreamError::InvalidResponse)?;
    if usize::try_from(bytes.length()).map_err(|_| StreamError::LimitExceeded)? > MAX_COMMAND_BYTES
    {
        return Err(StreamError::LimitExceeded);
    }
    Ok(bytes.to_vec())
}
struct OwnedCursor(Cursor);
impl Drop for OwnedCursor {
    fn drop(&mut self) {
        let _closed = self.0.close();
    }
}

/// A trusted host port over the public canonical provider protobufs.
pub(crate) struct InjectedStream {
    port: Port,
    claims: OperationClaims,
}
impl InjectedStream {
    pub(crate) fn new(port: JsValue) -> Result<Self, StreamError> {
        if !port.is_object() {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self {
            port: port.unchecked_into(),
            claims: OperationClaims::default(),
        })
    }
    async fn call<R: Message + Default>(
        &self,
        operation: &str,
        input: impl Message,
    ) -> Result<R, StreamError> {
        let input = input.encode_to_vec();
        if input.len() > MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        let bytes = snapshot(reply(self.port.dispatch(operation, &input)).await?)?;
        R::decode(bytes.as_slice()).map_err(|_| StreamError::InvalidResponse)
    }
    async fn cursor(
        &self,
        operation: &str,
        input: impl Message,
        preceding: Option<CommitId>,
        from: u64,
    ) -> Result<FollowStream, StreamError> {
        let input = input.encode_to_vec();
        if input.len() > MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        let anchor = preceding.map_or_else(Vec::new, |id| id.as_bytes().to_vec());
        let cursor = reply(self.port.open(operation, &input, &anchor)).await?;
        if !cursor.is_object() {
            return Err(StreamError::InvalidResponse);
        }
        let cursor = OwnedCursor(cursor.unchecked_into());
        let read = operation == "read";
        Ok(
            futures::stream::try_unfold((cursor, from), move |(cursor, next)| async move {
                let value = reply(cursor.0.next()).await?;
                if value.is_null() {
                    return Ok(None);
                }
                let bytes = snapshot(value)?;
                let frame = wire::SessionFrame::decode(bytes.as_slice())
                    .map_err(|_| StreamError::InvalidResponse)?;
                if frame.channel_id != 1 || frame.generation != 1 {
                    return Err(StreamError::InvalidResponse);
                }
                let (update, after) = match frame.body {
                    Some(wire::session_frame::Body::Data(data)) => {
                        let Some(wire::session_data::Item::Record(record)) = data.item else {
                            return Err(StreamError::InvalidResponse);
                        };
                        let record = wire_codec::record(record)?;
                        if record.sequence < next || (read && record.sequence != next) {
                            return Err(StreamError::InvalidResponse);
                        }
                        let after = record
                            .sequence
                            .checked_add(1)
                            .ok_or(StreamError::OutOfRange)?;
                        if data
                            .after
                            .as_ref()
                            .is_none_or(|cursor| cursor.next != after || !cursor.opaque.is_empty())
                        {
                            return Err(StreamError::InvalidResponse);
                        }
                        (FollowUpdate::Record(record), after)
                    }
                    Some(wire::session_frame::Body::Watermark(progress)) if !read => {
                        let after = progress.after.ok_or(StreamError::InvalidResponse)?;
                        if after.next <= next || !after.opaque.is_empty() {
                            return Err(StreamError::InvalidResponse);
                        }
                        (
                            FollowUpdate::Progress {
                                next: after.next,
                                preceding: wire_codec::commit_id(&progress.preceding_commit_id)?,
                            },
                            after.next,
                        )
                    }
                    _ => return Err(StreamError::InvalidResponse),
                };
                Ok(Some((update, (cursor, after))))
            })
            .boxed_local(),
        )
    }
    async fn commit_deadline(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
    ) -> Result<CommitOutcome, StreamError> {
        let input = wire::CommitRequest {
            deadline_unix_millis: deadline.into_iter().chain(request.deadline_unix_millis).min(),
            ..wire_codec::commit_to_wire(&request)
        };
        wire_codec::commit_outcome_from_wire(self.call("commit", input).await?)
    }
}
#[async_trait::async_trait(?Send)]
impl StreamProvider for InjectedStream {
    fn operation_claims(&self) -> &OperationClaims {
        &self.claims
    }
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        let result: wire::InspectIdempotencyResponse = self
            .call(
                "inspect_idempotency",
                wire::InspectIdempotencyRequest {
                    idempotency_key: bytes::Bytes::copy_from_slice(key.as_bytes()),
                },
            )
            .await?;
        let observed = result
            .observation
            .map(wire_codec::observation_from_wire)
            .transpose()?;
        if observed.as_ref().is_some_and(|o| o.idempotency_key != key) {
            return Err(StreamError::InvalidResponse);
        }
        Ok(observed)
    }
    async fn head(&self, path: StreamPath) -> Result<crate::PathHead, StreamError> {
        wire_codec::head_from_wire(self.call("head", wire::HeadRequest {
            path: path.to_string(),
        }).await?)
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        Ok(self.watermarks(path).await?.accepted)
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        Ok(StreamBounds {
            tail: self.tail(path).await?,
        })
    }
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        wire_codec::watermarks_from_wire(
            self.call(
                "tail",
                wire::TailRequest {
                    path: path.to_string(),
                    accepted: true,
                    durable_through: None,
                },
            )
            .await?,
        )
    }
    async fn durable_receipt(
        &self,
        path: StreamPath,
        through: u64,
    ) -> Result<Option<DurableReceipt>, StreamError> {
        let marks = wire_codec::watermarks_from_wire(
            self.call(
                "tail",
                wire::TailRequest {
                    path: path.to_string(),
                    accepted: true,
                    durable_through: Some(through),
                },
            )
            .await?,
        )?;
        Ok(DurableReceipt::covering(path, &marks, through))
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        let request = crate::journal::Command::Append(request).validated()?;
        let crate::journal::Command::Append(request) = request else {
            return Err(StreamError::InvalidArgument);
        };
        let input = wire::AppendRequest {
            path: request.path.to_string(),
            records: request
                .records
                .into_iter()
                .map(wire_codec::append_record_wire)
                .collect(),
            if_tail: request.if_tail,
            idempotency_key: request
                .idempotency_key
                .map(|key| bytes::Bytes::copy_from_slice(key.as_bytes())),
        };
        wire_codec::append_outcome_from_wire(self.call("append", input).await?)
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let input = wire::ForkRequest {
            source: request.source.to_string(),
            destination: request.destination.to_string(),
            at_tail: request.at_tail,
            idempotency_key: request
                .idempotency_key
                .map(|key| bytes::Bytes::copy_from_slice(key.as_bytes())),
            exclude: request.exclude.into_iter().map(|p| p.to_string()).collect(),
        };
        let result: wire::ForkReceipt = self.call("fork", input).await?;
        Ok(ForkReceipt {
            source: wire_codec::path(result.source)?,
            destination: wire_codec::path(result.destination)?,
            forked_at: result.forked_at,
            tail: result.tail,
            commit_id: wire_codec::commit_id(&result.commit_id)?,
        })
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.commit_deadline(request, None).await
    }
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.commit_deadline(request, Some(deadline)).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        crate::memory::validate_limit(request.limit)?;
        let from = request.from;
        Ok(Box::pin(crate::records_only(
            self.cursor(
                "read",
                wire::ReadRequest {
                    path: request.path.to_string(),
                    from,
                    limit: request.limit,
                },
                None,
                from,
            )
            .await?,
        )))
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        let input = wire::ChildrenPageRequest {
            parent: request.parent.map(|p| p.to_string()),
            after: request.after.map(|p| p.to_string()),
            hierarchy_version: request
                .hierarchy_version
                .map(|id| bytes::Bytes::copy_from_slice(id.as_bytes())),
            tail_stream: request.tail_stream.map(|p| p.to_string()),
            limit: request.limit,
        };
        let result: wire::ChildrenPageResponse = self.call("children_page", input).await?;
        Ok(ChildrenPage {
            hierarchy_version: wire_codec::commit_id(&result.hierarchy_version)?,
            children: result
                .children
                .into_iter()
                .map(|child| {
                    Ok(Child {
                        origin: child.origin.map(wire_codec::fork_origin_from_wire).transpose()?,
                        path: wire_codec::path(child.path)?,
                        tail_record: child.tail_record.map(wire_codec::record).transpose()?,
                    })
                })
                .collect::<Result<_, StreamError>>()?,
            next_after: result.next_after.map(wire_codec::path).transpose()?,
        })
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        wire_codec::envelope_from_wire(
            self.call(
                "read_commit",
                wire::ReadCommitRequest {
                    commit_id: bytes::Bytes::copy_from_slice(id.as_bytes()),
                },
            )
            .await?,
        )
    }
    async fn follow(&self, request: crate::FollowRequest) -> Result<crate::FollowStream, StreamError> {
        self.cursor(
            "follow",
            wire::FollowRequest {
                path: request.path.to_string(),
                from: request.from,
                filter: Some(request.filter.transport_filter()?),
                progress: request.progress,
            },
            request.preceding,
            request.from,
        ).await
    }
}

impl super::WasmStream {
    pub(super) async fn provider_cursor(
        &self,
        operation: &str,
        input: &[u8],
        preceding: &[u8],
    ) -> Result<super::WasmFollow, JsValue> {
        super::check_command_size(input)?;
        let provider = self.provider();
        let updates: FollowStream = match operation {
            "read" if preceding.is_empty() => {
                let request =
                    wire_codec::read_from_wire(super::decode::<wire::ReadRequest>(input)?)
                        .map_err(super::js_error)?;
                provider
                    .read(request)
                    .await
                    .map_err(super::js_error)?
                    .map(|item| item.map(FollowUpdate::Record))
                    .boxed_local()
            }
            "follow" => {
                let input = super::decode::<wire::FollowRequest>(input)?;
                let progress = input.progress;
                let (path, from, filter) = wire_codec::follow_from_wire(input)
                    .map_err(super::js_error)?;
                let anchor = if preceding.is_empty() {
                    None
                } else {
                    Some(wire_codec::commit_id(preceding).map_err(super::js_error)?)
                };
                provider
                    .follow(crate::FollowRequest { path, from, preceding: anchor, filter, progress })
                    .await
                    .map_err(super::js_error)?
            }
            _ => return Err(super::js_error(StreamError::InvalidArgument)),
        };
        let stream = updates
            .map(|item| {
                item.map(|update| {
                    let body = match update {
                        FollowUpdate::Record(record) => {
                            let after = record.sequence + 1;
                            wire::session_frame::Body::Data(wire::SessionData {
                                after: Some(wire::SessionCursor {
                                    next: after,
                                    opaque: bytes::Bytes::new(),
                                }),
                                accepted: after,
                                durable: 0,
                                item: Some(wire::session_data::Item::Record(
                                    wire_codec::record_wire(record),
                                )),
                            })
                        }
                        FollowUpdate::Progress { next, preceding } => {
                            wire::session_frame::Body::Watermark(wire::SessionWatermark {
                                accepted: next,
                                durable: 0,
                                after: Some(wire::SessionCursor {
                                    next,
                                    opaque: bytes::Bytes::new(),
                                }),
                                preceding_commit_id: bytes::Bytes::copy_from_slice(
                                    preceding.as_bytes(),
                                ),
                            })
                        }
                    };
                    wire::SessionFrame {
                        channel_id: 1,
                        generation: 1,
                        body: Some(body),
                    }
                    .encode_to_vec()
                })
            })
            .boxed_local();
        Ok(super::WasmFollow {
            state: Rc::new(super::FollowState::new(stream)),
        })
    }
}
