//! Canonical protobuf provider port. SQL, DO RPC and guest code share no Rust
//! pointers; this adapter only projects calls and owns cursor cancellation.
//! Every byte argument is the port's own copy, so an asynchronous port may
//! read it after WebAssembly memory grows.
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
    fn dispatch(this: &Port, operation: &str, input: &Uint8Array) -> Result<Promise, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn open(
        this: &Port,
        operation: &str,
        input: &Uint8Array,
        preceding: &Uint8Array,
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

struct CursorFrames {
    cursor: OwnedCursor,
    decoder: wire_codec::session::FrameDecoder,
    chunk: Vec<u8>,
    offset: usize,
}
impl CursorFrames {
    async fn next(&mut self) -> Result<Option<wire::SessionFrame>, StreamError> {
        loop {
            let mut remaining = &self.chunk[self.offset..];
            let frame = self.decoder.read(&mut remaining)?;
            self.offset = self.chunk.len() - remaining.len();
            if frame.is_some() {
                return Ok(frame);
            }
            let value = reply(self.cursor.0.next()).await?;
            if value.is_null() {
                self.decoder.finish()?;
                return Ok(None);
            }
            let bytes: Uint8Array = value.dyn_into().map_err(|_| StreamError::InvalidResponse)?;
            // Chunk boundaries and sizes belong to the transport. The shared
            // decoder admits each logical frame, even when a chunk has several.
            self.chunk = bytes.to_vec();
            self.offset = 0;
        }
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
        let bytes =
            snapshot(reply(self.port.dispatch(operation, &Uint8Array::from(&input[..]))).await?)?;
        R::decode(bytes.as_slice()).map_err(|_| StreamError::InvalidResponse)
    }
    async fn cursor(
        &self,
        operation: &str,
        path: &crate::StreamPath,
        input: impl Message,
        preceding: Option<CommitId>,
        from: u64,
    ) -> Result<FollowStream, StreamError> {
        let input = input.encode_to_vec();
        if input.len() > MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        // The original request carries its minimum. Registration observes it
        // atomically in the canonical provider, outside the host's write order.
        let anchor = preceding.map_or_else(Vec::new, |id| id.as_bytes().to_vec());
        let cursor = reply(self.port.open(
            operation,
            &Uint8Array::from(&input[..]),
            &Uint8Array::from(&anchor[..]),
        ))
        .await?;
        if !cursor.is_object() {
            return Err(StreamError::InvalidResponse);
        }
        let cursor = CursorFrames {
            cursor: OwnedCursor(cursor.unchecked_into()),
            decoder: wire_codec::session::FrameDecoder::default(),
            chunk: Vec::new(),
            offset: 0,
        };
        let read = operation == "read";
        let seek = if read {
            wire_codec::read_from_wire(
                wire::ReadRequest::decode(input.as_slice())
                    .map_err(|_| StreamError::InvalidArgument)?,
            )?
            .key
        } else {
            None
        };
        let path = path.clone();
        Ok(
            futures::stream::try_unfold((cursor, from), move |(mut cursor, next)| {
                let (seek, path) = (seek.clone(), path.clone());
                async move {
                    let Some(frame) = cursor.next().await? else {
                        return if read {
                            Ok(None)
                        } else {
                            Err(StreamError::Unavailable)
                        };
                    };
                    if frame.channel_id != 1 || frame.generation != 1 {
                        return Err(StreamError::InvalidResponse);
                    }
                    let (update, after) = match frame.body {
                        Some(wire::session_frame::Body::Data(data)) => {
                            let Some(wire::session_data::Item::Record(record)) = data.item else {
                                return Err(StreamError::InvalidResponse);
                            };
                            let record = wire_codec::record(record, Some(&path))?;
                            if record.sequence < next
                                || (read && seek.is_none() && record.sequence != next)
                                || seek.as_ref().is_some_and(|key| {
                                    record.sequence >= key.before
                                        || !record
                                            .headers
                                            .keys()
                                            .iter()
                                            .any(|value| value == &key.key)
                                })
                            {
                                return Err(StreamError::InvalidResponse);
                            }
                            let after = record
                                .sequence
                                .checked_add(1)
                                .ok_or(StreamError::OutOfRange)?;
                            if data.after.as_ref().is_none_or(|cursor| {
                                cursor.next != after || !cursor.opaque.is_empty()
                            }) {
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
                                    preceding: wire_codec::commit_id(
                                        &progress.preceding_commit_id,
                                    )?,
                                },
                                after.next,
                            )
                        }
                        _ => return Err(StreamError::InvalidResponse),
                    };
                    Ok(Some((update, (cursor, after))))
                }
            })
            .boxed_local(),
        )
    }
}
#[async_trait::async_trait(?Send)]
impl StreamProvider for InjectedStream {
    fn context(&self) -> crate::ProviderContext<'_> {
        crate::ProviderContext::new(&self.claims)
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
    async fn heads(&self, request: crate::HeadsRequest) -> Result<crate::Heads, StreamError> {
        let view = wire_codec::heads_from_response(
            self.call("heads", wire_codec::heads_request_to_wire(request.clone()))
                .await?,
        )?;
        view.validate_for(&request)?;
        Ok(view)
    }

    fn write(
        &self,
        request: crate::WriteRequest,
    ) -> crate::BoxProviderFuture<'_, std::result::Result<crate::WriteOutcome, crate::StreamError>>
    {
        match request {
            crate::WriteRequest::Append(request) => Box::pin(futures::TryFutureExt::map_ok(
                async move {
                    let request = crate::WriteRequest::Append(request).validated()?;
                    let crate::WriteRequest::Append(request) = request else {
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
                        idempotency_key: bytes::Bytes::copy_from_slice(
                            request.idempotency_key.as_bytes(),
                        ),
                    };
                    wire_codec::append_outcome_from_wire(self.call("append", input).await?)
                },
                crate::WriteOutcome::Append,
            )),
            crate::WriteRequest::Fork(request) => Box::pin(futures::TryFutureExt::map_ok(
                async move {
                    let input = wire::ForkRequest {
                        source: request.source.to_string(),
                        destination: request.destination.to_string(),
                        at_tail: request.at_tail,
                        idempotency_key: bytes::Bytes::copy_from_slice(
                            request.idempotency_key.as_bytes(),
                        ),
                        exclude: request.exclude.into_iter().map(|p| p.to_string()).collect(),
                    };
                    let result: wire::ForkReceipt = self.call("fork", input).await?;
                    wire_codec::fork_receipt_from_wire(result)
                },
                crate::WriteOutcome::Fork,
            )),
            crate::WriteRequest::Commit(request) => Box::pin(futures::TryFutureExt::map_ok(
                async move {
                    let input = wire_codec::commit_to_wire(&request);
                    wire_codec::commit_outcome_from_wire(self.call("commit", input).await?)
                },
                crate::WriteOutcome::Commit,
            )),
        }
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        crate::memory::validate_limit(request.limit)?;
        let from = request.from;
        Ok(crate::records_only(
            self.cursor(
                "read",
                &request.path,
                wire::ReadRequest {
                    key: wire_codec::read_key_to_wire(&request.key),
                    path: request.path.to_string(),
                    from,
                    limit: request.limit,
                    at_least: wire_codec::cut_to_wire(&request.at_least),
                },
                None,
                from,
            )
            .await?,
        ))
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
    async fn follow(
        &self,
        request: crate::FollowRequest,
    ) -> Result<crate::FollowStream, StreamError> {
        self.cursor(
            "follow",
            &request.path,
            wire::FollowRequest {
                when_absent: request.when_absent.to_wire(),
                path: request.path.to_string(),
                from: request.from,
                filter: Some(request.filter.transport_filter()?),
                progress: request.progress,
                at_least: wire_codec::cut_to_wire(&request.at_least),
            },
            request.preceding,
            request.from,
        )
        .await
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
        let (path, updates): (_, FollowStream) = match operation {
            "read" if preceding.is_empty() => {
                let request =
                    wire_codec::read_from_wire(super::decode::<wire::ReadRequest>(input)?)
                        .map_err(super::js_error)?;
                let path = request.path.clone();
                let records = provider
                    .read(request)
                    .await
                    .map_err(super::js_error)?
                    .map(|item| item.map(FollowUpdate::Record))
                    .boxed_local();
                (path, records)
            }
            "follow" => {
                let mut request =
                    wire_codec::follow_from_wire(super::decode::<wire::FollowRequest>(input)?)
                        .map_err(super::js_error)?;
                let anchor = if preceding.is_empty() {
                    None
                } else {
                    Some(wire_codec::commit_id(preceding).map_err(super::js_error)?)
                };
                request.preceding = anchor;
                let path = request.path.clone();
                (
                    path,
                    provider.follow(request).await.map_err(super::js_error)?,
                )
            }
            _ => return Err(super::js_error(StreamError::InvalidArgument)),
        };
        let stream = updates
            .map(move |item| {
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
                                    wire_codec::record_wire(record, Some(&path)),
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
                    .encode_length_delimited_to_vec()
                })
            })
            .boxed_local();
        Ok(super::WasmFollow {
            state: Rc::new(super::FollowState::new(stream, StreamError::Unavailable)),
        })
    }
}
