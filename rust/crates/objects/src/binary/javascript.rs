//! Original JavaScript request ownership over the canonical binary client.
use super::{Reply, Transport};
use crate::{Availability, Blobs, Digest, Error, wire};
use bytes::Bytes;
use futures::StreamExt as _;
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
}

struct Request {
    owner: JsValue,
    close: Function,
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
        let input = Uint8Array::from(input.as_ref());
        let owner = self
            .open
            .call2(&self.owner, &JsValue::from_str(route), &input)
            .map_err(original_error)?;
        let request = Request {
            close: method(&owner, "close")?,
            owner,
        };
        let promise: Promise = method(&request.owner, "next")?
            .call0(&request.owner)
            .map_err(original_error)?
            .dyn_into()
            .map_err(|_| unavailable())?;
        let reply = JsFuture::from(promise).await.map_err(original_error)?;
        let body: Uint8Array = Reflect::get(&reply, &"body".into())
            .map_err(original_error)?
            .dyn_into()
            .map_err(|_| unavailable())?;
        let (body, ()) = acyclic_stream::javascript_bytes::snapshot(&body, |_| Ok(()))
            .map_err(original_error)?;
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
        Ok((body.into(), revision))
    }

    async fn availability(&self, _digest: Digest) -> Result<Availability, Error> {
        Err(wire::ErrorCode::Unsupported.into())
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
        let request = futures::stream::once(async move {
            crate::route::invoke_at(blobs.as_ref(), &route, input.into()).await
        })
        .boxed_local();
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
