//! One serving projection for the injected provider port's cursors, shared by
//! every host that serves that port: the browser's `WasmStream` and native
//! hosts relaying a remote provider to a guest.
use futures::StreamExt as _;
use prost::Message as _;

use acyclic_stream_contract_core::{FollowStream, FollowUpdate, StreamError, StreamProvider};
use acyclic_stream_wire::wire;

fn decode<T: prost::Message + Default>(input: &[u8]) -> Result<T, StreamError> {
    T::decode(input).map_err(|_| StreamError::InvalidArgument)
}

/// Opens the port's `read` or `follow` operation on `provider`. `input` is the
/// canonical `ReadRequest` or `FollowRequest`; `preceding` is empty or a
/// follow's 32-byte checked anchor. The cursor is length-delimited
/// `SessionFrame`s on channel 1, generation 1: `Opened`, then each record and
/// opening or progress watermark, in order. A byte transport may split or
/// coalesce its chunks without changing a frame.
pub async fn provider_cursor<P: StreamProvider + ?Sized>(
    provider: &P,
    operation: &str,
    input: &[u8],
    preceding: &[u8],
) -> Result<
    acyclic_stream_contract_core::BoxProviderStream<'static, Result<Vec<u8>, StreamError>>,
    StreamError,
> {
    if input.len() > acyclic_stream_contract_core::MAX_COMMAND_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    let (path, mut updates, indexed): (_, FollowStream, bool) = match operation {
        "read" if preceding.is_empty() => {
            let request = super::read_from_wire(decode::<wire::ReadRequest>(input)?)?;
            let path = request.path.clone();
            let indexed = request.key.is_some();
            let records = provider
                .read(request)
                .await?
                .map_cursor(|cursor| Box::pin(cursor.map(|item| item.map(FollowUpdate::Record))));
            (path, records, indexed)
        }
        "follow" => {
            let mut request = super::follow_from_wire(decode::<wire::FollowRequest>(input)?)?;
            request.preceding = if preceding.is_empty() {
                None
            } else {
                Some(super::commit_id(preceding)?)
            };
            let path = request.path.clone();
            let filter = request.filter.clone();
            let mut updates = provider.follow(request).await?;
            updates.selection.bootstrap = updates
                .selection
                .bootstrap
                .take()
                .map(|record| filter.project(record))
                .transpose()?
                .flatten();
            (path, updates, false)
        }
        _ => return Err(StreamError::InvalidArgument),
    };
    let frame = |body| {
        wire::SessionFrame {
            channel_id: 1,
            generation: 1,
            body: Some(body),
        }
        .encode_length_delimited_to_vec()
    };
    let opened = frame(wire::session_frame::Body::Opened(super::opened_to_wire(
        &updates,
    )));
    let bootstrap = updates
        .selection
        .bootstrap
        .take()
        .map(|record| Ok(FollowUpdate::Record(record)));
    let mut next = updates.selection.from;
    let updates = futures::stream::iter(bootstrap)
        .chain(updates)
        .map(move |item| {
            let body = match item? {
                FollowUpdate::Opening { tail } => {
                    wire::session_frame::Body::Watermark(wire::SessionWatermark {
                        opening_tail: Some(tail),
                        accepted: tail,
                        ..Default::default()
                    })
                }
                FollowUpdate::Record(record) => {
                    let accepted = record
                        .sequence
                        .checked_add(1)
                        .ok_or(StreamError::OutOfRange)?;
                    // An indexed read's cursor counts its results.
                    let after = if indexed {
                        next = next.checked_add(1).ok_or(StreamError::OutOfRange)?;
                        next
                    } else {
                        accepted
                    };
                    wire::session_frame::Body::Data(wire::SessionData {
                        after: Some(wire::SessionCursor {
                            next: after,
                            opaque: bytes::Bytes::new(),
                        }),
                        accepted,
                        durable: 0,
                        item: Some(wire::session_data::Item::Record(super::record_wire(
                            record,
                            Some(&path),
                        ))),
                    })
                }
                FollowUpdate::Progress { next, preceding } => {
                    wire::session_frame::Body::Watermark(wire::SessionWatermark {
                        opening_tail: None,
                        accepted: next,
                        durable: 0,
                        after: Some(wire::SessionCursor {
                            next,
                            opaque: bytes::Bytes::new(),
                        }),
                        preceding_commit_id: bytes::Bytes::copy_from_slice(preceding.as_bytes()),
                    })
                }
            };
            Ok(frame(body))
        });
    Ok(Box::pin(
        futures::stream::once(async move { Ok(opened) }).chain(updates),
    ))
}
