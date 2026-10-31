//! Original JavaScript request ownership over the canonical binary client.
use super::{Reply, Transport};
use crate::{Availability, Blobs, Digest, Error, wire};
use bytes::Bytes;
use futures::{StreamExt as _, TryStreamExt as _};
use js_sys::{Function, Promise, Reflect, Uint8Array};
use prost::Message as _;
use std::{rc::Rc, sync::Arc};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen(inline_js = "
export function objectsErrorCode(error) {
    const code = error?.code;
    return typeof code === 'number' && (code | 0) === code ? code : 0;
}
")]
extern "C" {
    #[wasm_bindgen(catch, js_name = objectsErrorCode)]
    fn error_code(error: &JsValue) -> Result<i32, JsValue>;
}

#[wasm_bindgen(typescript_custom_section)]
const REPLY_TYPE: &str = "
export interface ObjectsV1BinaryReply {
    body: Uint8Array;
    /** Original canonical Stream Revision bytes, when the producer supplies it. */
    revision?: Uint8Array;
}
";

/// Project an original account result without discarding its serving revision.
pub fn js_reply(
    body: &[u8],
    revision: Option<&acyclic_stream::Revision>,
) -> Result<JsValue, JsValue> {
    let reply = js_sys::Object::new();
    Reflect::set(&reply, &"body".into(), &Uint8Array::from(body))?;
    if let Some(revision) = revision {
        let bytes = acyclic_stream::wire_codec::revision_to_wire(revision).encode_to_vec();
        Reflect::set(
            &reply,
            &"revision".into(),
            &Uint8Array::from(bytes.as_slice()),
        )?;
    }
    Ok(reply.into())
}

fn unavailable() -> Error {
    wire::ErrorCode::Unavailable.into()
}

fn original_error(value: JsValue) -> Error {
    error_code(&value)
        .ok()
        .and_then(|code| wire::ErrorCode::try_from(code).ok())
        .filter(|code| *code != wire::ErrorCode::Unspecified)
        .map_or_else(unavailable, Into::into)
}

/// Preserve an original Objects failure at a JavaScript boundary.
#[must_use]
pub fn js_error(value: Error) -> JsValue {
    let result = js_sys::Error::new(&value.to_string());
    result.set_name("ObjectsV1Error");
    // The fresh error is extensible and has no caller-defined properties.
    let _ = Reflect::set(&result, &"code".into(), &JsValue::from(value.code as i32));
    result.into()
}

fn method(owner: &JsValue, name: &str) -> Result<Function, Error> {
    Reflect::get(owner, &JsValue::from_str(name))
        .map_err(original_error)?
        .dyn_into()
        .map_err(|_| unavailable())
}

/// Retains an already authenticated account's original `open` request producer.
///
/// Its `open(route, input)` returns the original owner with `next()` and `close()`.
/// `next` returns a promise for one `ObjectsV1BinaryReply`, or null after closure.
/// `close` must synchronously cancel that same request and settle a pending pull.
/// No endpoint, credential, timer or observation is synthesized here. A supplied
/// serving revision uses the shared Stream codec; absence remains absence.
#[derive(Clone)]
pub struct JavaScriptTransport {
    owner: JsValue,
    open: Function,
}

impl JavaScriptTransport {
    /// Capture the admitted producer once without starting an operation.
    pub fn new(owner: JsValue) -> Result<Self, Error> {
        let open = method(&owner, "open")?;
        Ok(Self { owner, open })
    }

    fn request(&self, route: &str, input: &[u8]) -> Result<Request, Error> {
        let input = Uint8Array::from(input);
        let owner = self
            .open
            .call2(&self.owner, &JsValue::from_str(route), &input)
            .map_err(original_error)?;
        Ok(Request {
            close: method(&owner, "close")?,
            owner,
        })
    }
}

struct Request {
    owner: JsValue,
    close: Function,
}

impl Request {
    fn opened(&self) -> Result<(), Error> {
        let opened = Reflect::get(&self.owner, &"opened".into()).map_err(original_error)?;
        if !opened.is_undefined() {
            opened
                .dyn_into::<Function>()
                .map_err(|_| unavailable())?
                .call0(&self.owner)
                .map_err(original_error)?;
        }
        Ok(())
    }

    async fn next(&self) -> Result<Option<JsValue>, Error> {
        let promise: Promise = method(&self.owner, "next")?
            .call0(&self.owner)
            .map_err(original_error)?
            .dyn_into()
            .map_err(|_| unavailable())?;
        let reply = JsFuture::from(promise).await.map_err(original_error)?;
        Ok((!reply.is_null()).then_some(reply))
    }
}

fn reply_body(reply: &JsValue) -> Result<Bytes, Error> {
    let body: Uint8Array = Reflect::get(reply, &"body".into())
        .map_err(original_error)?
        .dyn_into()
        .map_err(|_| unavailable())?;
    let (body, ()) =
        acyclic_stream::javascript_bytes::snapshot(&body, |_| Ok(())).map_err(original_error)?;
    Ok(body.into())
}

impl Drop for Request {
    fn drop(&mut self) {
        // The original producer owns cancellation; dropping the future invokes it
        // even while its next promise is parked. Destructors cannot throw.
        let _ = self.close.call0(&self.owner);
    }
}

impl Transport for JavaScriptTransport {
    async fn invoke(&self, route: &str, input: Bytes) -> Result<Reply, Error> {
        // JS receives owned request bytes, never a view into movable Rust memory.
        let request = self.request(route, &input)?;
        let reply = request.next().await?.ok_or_else(unavailable)?;
        let body = reply_body(&reply)?;
        let revision = Reflect::get(&reply, &"revision".into()).map_err(original_error)?;
        let revision = if revision.is_undefined() {
            None
        } else {
            let revision: Uint8Array = revision.dyn_into().map_err(|_| unavailable())?;
            let (revision, ()) = acyclic_stream::javascript_bytes::snapshot(&revision, |_| Ok(()))
                .map_err(original_error)?;
            let revision = acyclic_stream::wire::Revision::decode(Bytes::from(revision))
                .map_err(|_| unavailable())?;
            Some(
                acyclic_stream::wire_codec::revision_from_wire(revision)
                    .map_err(|_| unavailable())?,
            )
        };
        Ok((body, revision))
    }

    async fn availability(&self, digest: Digest) -> Result<Availability, Error> {
        use acyclic_stream::wire_codec::session::FrameDecoder;
        let request = self.request(
            "blobs/availability",
            &wire::AvailabilityRequest {
                digest: Some(digest.into()),
            }
            .encode_to_vec(),
        )?;
        let decoder = FrameDecoder::with_decoder(acyclic_stream::MAX_COMMAND_BYTES, |bytes| {
            wire::AvailabilityCursor::decode(bytes)
                .map_err(|_| acyclic_stream::StreamError::InvalidResponse)
        });
        let mut frames = futures::stream::try_unfold(
            (request, decoder, Bytes::new(), true),
            |(request, mut decoder, mut chunk, mut opening)| async move {
                loop {
                    let mut input = chunk.as_ref();
                    let frame = decoder.read(&mut input).map_err(|_| unavailable())?;
                    chunk = chunk.slice(chunk.len() - input.len()..);
                    if let Some(frame) = frame {
                        let frame = frame.try_into()?;
                        if opening {
                            request.opened()?;
                            opening = false;
                        }
                        return Ok::<_, Error>(Some((frame, (request, decoder, chunk, opening))));
                    }
                    let reply = request.next().await?.ok_or_else(unavailable)?;
                    chunk = reply_body(&reply)?;
                }
            },
        )
        .boxed_local();
        let initial = frames.next().await.ok_or_else(unavailable)??;
        Ok(Availability::new(initial, frames))
    }
}

/// One original account operation served by Rust blobs to JavaScript, through
/// the shared pull and cancellation owner: every Rust-backed account's
/// `open(route, input)` returns one, so it is an `ObjectsV1BinaryPort`.
#[wasm_bindgen]
pub struct ObjectsV1Request {
    state: Rc<acyclic_stream::pull::PullCursor<crate::route::Served<Vec<u8>>, Error>>,
}

impl ObjectsV1Request {
    /// Observes the original Rust account directly, without a JavaScript port
    /// round trip or framing trusted in-process cursors.
    pub fn observe_blobs(blobs: Arc<dyn Blobs>, input: &Uint8Array) -> Result<Self, JsValue> {
        let input = availability_input(input)?;
        Ok(Self::observe_source(async move {
            crate::route::availability(blobs.as_ref(), input).await
        }))
    }

    fn observe_source(open: impl Future<Output = Result<Availability, Error>> + 'static) -> Self {
        let request = futures::stream::once(async move {
            let observation = open.await?;
            Ok::<_, Error>(
                crate::route::availability_frames(observation)
                    .map(|frame| Ok((frame?.encode_to_vec(), None)))
                    .boxed_local(),
            )
        })
        .try_flatten()
        .boxed_local();
        Self {
            state: Rc::new(acyclic_stream::pull::PullCursor::new(
                request,
                unavailable(),
            )),
        }
    }

    /// Own one lazy canonical request against `blobs`. The input is copied
    /// before this returns; the operation starts on the first `next`.
    ///
    /// # Errors
    /// When the input bytes cannot be snapshotted.
    pub fn serve(
        blobs: Arc<dyn Blobs>,
        route: String,
        input: &Uint8Array,
    ) -> Result<Self, JsValue> {
        let (input, ()) = acyclic_stream::javascript_bytes::snapshot(input, |_| Ok(()))?;
        let request = if route == "blobs/availability" {
            futures::stream::once(async move {
                Ok::<_, Error>(
                    crate::route::invoke_stream(blobs.as_ref(), &route, input.into())
                        .await?
                        .map(|frame| Ok((frame?, None)))
                        .boxed_local(),
                )
            })
            .try_flatten()
            .boxed_local()
        } else {
            futures::stream::once(async move {
                crate::route::invoke_at(blobs.as_ref(), &route, input.into()).await
            })
            .boxed_local()
        };
        Ok(Self {
            state: Rc::new(acyclic_stream::pull::PullCursor::new(
                request,
                unavailable(),
            )),
        })
    }

    /// The single reply body, for a caller that needs no revision.
    ///
    /// # Errors
    /// The operation's own refusal, or `UNAVAILABLE` once closed.
    pub async fn body(&self) -> Result<Vec<u8>, Error> {
        Ok(self.state.next().await?.ok_or_else(unavailable)?.0)
    }
}

#[wasm_bindgen]
impl ObjectsV1Request {
    /// Validates an original binary port's framed availability with the same
    /// cursor and framing rules used by native clients. Closing cancels opening
    /// as well as a parked change; no second observation source is created.
    pub fn observe(owner: JsValue, input: &Uint8Array) -> Result<Self, JsValue> {
        let input = availability_input(input)?;
        let transport = JavaScriptTransport::new(owner).map_err(js_error)?;
        let client = super::BinaryObjects::new(transport);
        Ok(Self::observe_source(async move {
            crate::route::availability(&client, input).await
        }))
    }

    /// Frames a generated gRPC cursor for the shared binary stream decoder.
    pub fn frame(input: &Uint8Array) -> Result<Vec<u8>, JsValue> {
        let (input, ()) = acyclic_stream::javascript_bytes::snapshot(input, |_| Ok(()))?;
        let frame = wire::AvailabilityCursor::decode(input.as_slice())
            .map_err(|_| js_error(unavailable()))?;
        Ok(frame.encode_length_delimited_to_vec())
    }

    /// Resolve one original reply, or null when closed or consumed.
    #[wasm_bindgen(unchecked_return_type = "ObjectsV1BinaryReply | null")]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        match self.state.next().await.map_err(js_error)? {
            Some((body, revision)) => js_reply(&body, revision.as_ref()),
            None => Ok(JsValue::NULL),
        }
    }

    /// Cancel the original operation and settle its outstanding pull.
    pub fn close(&self) {
        self.state.close();
    }
}

fn availability_input(input: &Uint8Array) -> Result<wire::AvailabilityRequest, JsValue> {
    let (input, ()) = acyclic_stream::javascript_bytes::snapshot(input, |_| Ok(()))?;
    wire::AvailabilityRequest::decode(input.as_slice())
        .map_err(|_| js_error(wire::ErrorCode::InvalidArgument.into()))
}
