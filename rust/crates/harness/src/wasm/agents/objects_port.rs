//! A host's Objects v1 storage behind the one blob engine and client: either
//! an admitted account's original binary port, or the host's durable keys
//! under [`Store`]. The harness keeps no second object store.
use acyclic_objects::binary::javascript::{JavaScriptTransport, ObjectsV1Request, js_error};
use acyclic_objects::{Blobs, Medium, MediumError, Store, binary};
use acyclic_stream::OwnedBytes;
use js_sys::{Array, Promise, Reflect, Uint8Array};
use std::{ops::Range, sync::Arc};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen(typescript_custom_section)]
const OBJECTS: &str = r#"
/** An admitted Objects v1 account's original transport, the public
 * `ObjectsV1BinaryPort`: `open(route, input)` owns one canonical binary
 * request whose `next()` answers its reply and whose `close()` cancels it. A
 * rejection carrying a numeric Objects `code` keeps it; any other rejection is
 * `UNAVAILABLE`. A put resolves only once durable. Every `ObjectsV1Provider`
 * is one. */
export interface WasmObjectsPort {
    open(route: string, input: Uint8Array): { next(): Promise<{ body: Uint8Array; revision?: Uint8Array } | null>; close(): void };
}
/** The host's own keys beneath the one blob engine. Every write is atomic and
 * visible once it returns; `flush` makes every earlier write durable, and is
 * the only durability wait. `replace` and `removeIf` are atomic against every
 * other write of the key. A ranged read past the value's end throws. Results
 * may be returned directly or as promises. */
export interface WasmObjectsMedium {
    create(key: string, bytes: Uint8Array): boolean | Promise<boolean>;
    read(key: string, start: number | null, end: number | null): Uint8Array | null | Promise<Uint8Array | null>;
    remove(key: string): boolean | Promise<boolean>;
    replace(key: string, expected: Uint8Array, bytes: Uint8Array): boolean | Promise<boolean>;
    removeIf(key: string, expected: Uint8Array): boolean | Promise<boolean>;
    /** Up to `limit` `[key, size]` pairs under `prefix`, ascending by UTF-8 bytes, strictly after `after`. */
    list(prefix: string, after: string | null, limit: number): [string, number][] | Promise<[string, number][]>;
    flush(): void | Promise<void>;
}
/** What a host's objects are: an account port, or its own durable keys
 * holding the named account. */
export type WasmObjectsSource =
    | { readonly port: WasmObjectsPort }
    | { readonly medium: WasmObjectsMedium; readonly account: Uint8Array };
"#;

#[wasm_bindgen]
extern "C" {
    type ObjectsMedium;
    #[wasm_bindgen(method, catch)]
    fn create(this: &ObjectsMedium, key: &str, bytes: &[u8]) -> Result<JsValue, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn read(
        this: &ObjectsMedium,
        key: &str,
        start: Option<f64>,
        end: Option<f64>,
    ) -> Result<JsValue, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn remove(this: &ObjectsMedium, key: &str) -> Result<JsValue, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn replace(
        this: &ObjectsMedium,
        key: &str,
        expected: &[u8],
        bytes: &[u8],
    ) -> Result<JsValue, JsValue>;
    #[wasm_bindgen(method, catch, js_name = removeIf)]
    fn remove_if(this: &ObjectsMedium, key: &str, expected: &[u8]) -> Result<JsValue, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn list(
        this: &ObjectsMedium,
        prefix: &str,
        after: Option<String>,
        limit: f64,
    ) -> Result<JsValue, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn flush(this: &ObjectsMedium) -> Result<JsValue, JsValue>;
}

/// One host's Objects v1 blobs, opened once and shared by every agent it
/// serves: one engine owns a medium's unflushed writes and locks.
#[wasm_bindgen]
pub struct WasmObjects {
    blobs: Arc<dyn Blobs>,
}

#[wasm_bindgen]
impl WasmObjects {
    /// Opens `source`: an admitted account's port or the host's durable keys.
    ///
    /// # Errors
    /// When `source` names neither or both, or the account is empty.
    #[wasm_bindgen(constructor)]
    pub fn new(
        #[wasm_bindgen(unchecked_param_type = "WasmObjectsSource")] source: JsValue,
    ) -> Result<WasmObjects, JsValue> {
        Ok(Self {
            blobs: open(&source)?,
        })
    }

    /// Owns one route's canonical binary request against these blobs, so
    /// they are also a `WasmObjectsPort` a host can hand on. A refusal is an
    /// `ObjectsV1Error` carrying its numeric `code`.
    ///
    /// # Errors
    /// When the input bytes cannot be copied.
    pub fn open(&self, route: String, input: &Uint8Array) -> Result<ObjectsV1Request, JsValue> {
        ObjectsV1Request::serve(Arc::clone(&self.blobs), route, input)
    }
}

impl WasmObjects {
    pub(super) fn blobs(&self) -> Arc<dyn Blobs> {
        Arc::clone(&self.blobs)
    }
}

/// A fixed snapshot of a caller's bytes, through the one admitted copy.
fn copied(view: &Uint8Array) -> Result<Vec<u8>, JsValue> {
    acyclic_stream::javascript_bytes::snapshot(view, |_| Ok(())).map(|(bytes, ())| bytes)
}

fn refused(message: &str) -> JsValue {
    JsValue::from_str(&format!(
        "{message}; recovery: inject {{ port }} or {{ medium, account }} from the admitted Objects owner"
    ))
}

fn open(objects: &JsValue) -> Result<Arc<dyn Blobs>, JsValue> {
    if !objects.is_object() {
        return Err(refused("objects must be an object"));
    }
    let field = |name: &str| {
        Reflect::get(objects, &JsValue::from_str(name))
            .ok()
            .filter(JsValue::is_object)
    };
    match (field("port"), field("medium")) {
        (Some(port), None) => Ok(Arc::new(binary::BinaryObjects::new(
            JavaScriptTransport::new(port).map_err(js_error)?,
        ))),
        (None, Some(medium)) => {
            let account: Uint8Array = field("account")
                .and_then(|account| account.dyn_into().ok())
                .ok_or_else(|| refused("objects account must be bytes"))?;
            let account = Store::new(HostMedium(medium.unchecked_into()))
                .account(&copied(&account)?)
                .map_err(|error| refused(&error.to_string()))?;
            Ok(Arc::new(account))
        }
        _ => Err(refused("objects must name exactly one of port or medium")),
    }
}

/// The host's durable keys beneath the one blob engine.
struct HostMedium(ObjectsMedium);

fn failed(error: &JsValue) -> MediumError {
    MediumError::Failed(std::io::Error::other(format!("{error:?}")))
}

fn invalid(message: &str) -> MediumError {
    MediumError::Failed(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

/// A direct result, or the promise of one; a synchronous host pays no task hop.
async fn settle(result: Result<JsValue, JsValue>) -> Result<JsValue, MediumError> {
    let value = result.map_err(|error| failed(&error))?;
    match value.dyn_into::<Promise>() {
        Ok(pending) => JsFuture::from(pending)
            .await
            .map_err(|error| failed(&error)),
        Err(value) => Ok(value),
    }
}

async fn flag(result: Result<JsValue, JsValue>) -> Result<bool, MediumError> {
    settle(result)
        .await?
        .as_bool()
        .ok_or_else(|| invalid("medium returned a non-boolean"))
}

/// JavaScript integers are exact through 2^53.
const EXACT: u64 = 1 << 53;

fn position(value: u64) -> Result<f64, MediumError> {
    (value <= EXACT)
        .then_some(value as f64)
        .ok_or_else(|| invalid("position beyond a JavaScript integer"))
}

#[async_trait::async_trait(?Send)]
impl Medium for HostMedium {
    async fn create(&self, key: &str, value: OwnedBytes) -> Result<bool, MediumError> {
        flag(self.0.create(key, &value)).await
    }

    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        let (start, end) = match &range {
            Some(range) => (Some(position(range.start)?), Some(position(range.end)?)),
            None => (None, None),
        };
        let value = settle(self.0.read(key, start, end)).await?;
        if value.is_null() {
            return Ok(None);
        }
        let value: Uint8Array = value
            .dyn_into()
            .map_err(|_| invalid("medium returned non-bytes"))?;
        if range.is_some_and(|range| u64::from(value.length()) != range.end - range.start) {
            return Err(invalid("medium returned a short range"));
        }
        Ok(Some(copied(&value).map_err(|error| failed(&error))?.into()))
    }

    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        flag(self.0.remove(key)).await
    }

    async fn replace(
        &self,
        key: &str,
        expected: &[u8],
        value: OwnedBytes,
    ) -> Result<bool, MediumError> {
        flag(self.0.replace(key, expected, &value)).await
    }

    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        flag(self.0.remove_if(key, expected)).await
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError> {
        let page: Array = settle(self.0.list(prefix, after.map(str::to_owned), limit as f64))
            .await?
            .dyn_into()
            .map_err(|_| invalid("medium list is not an array"))?;
        if page.length() as usize > limit {
            return Err(invalid("medium list exceeds its limit"));
        }
        let mut previous = after.map(str::to_owned);
        page.iter()
            .map(|entry| {
                let entry: Array = entry
                    .dyn_into()
                    .map_err(|_| invalid("medium list entry is not a pair"))?;
                let key = entry.get(0).as_string();
                let size = entry
                    .get(1)
                    .as_f64()
                    .filter(|size| size.fract() == 0.0 && *size >= 0.0 && *size <= EXACT as f64);
                match (key, size) {
                    (Some(key), Some(size))
                        if key.starts_with(prefix)
                            && previous
                                .as_deref()
                                .is_none_or(|previous| key.as_str() > previous) =>
                    {
                        previous = Some(key.clone());
                        Ok((key, size as u64))
                    }
                    _ => Err(invalid(
                        "medium list entry out of order or outside its prefix",
                    )),
                }
            })
            .collect()
    }

    async fn flush(&self, _keys: &[String]) -> Result<(), MediumError> {
        settle(self.0.flush()).await.map(drop)
    }

    fn volatile(&self) -> bool {
        // A write survives a crash once flushed.
        true
    }

    fn recovered(&self) -> bool {
        // Each write lands whole or not at all, so a crash leaves no damaged key.
        false
    }

    fn settle(&self) {}
}
