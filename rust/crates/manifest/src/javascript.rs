//! Browser projection of the shared selected-entry admission and publication.
use crate::{ManifestError, PreparedEntry, entry, parse, verify_artifact};
use acyclic_objects::{
    RootId,
    binary::{
        BinaryObjects,
        javascript::{JavaScriptTransport, js_error, js_reply},
    },
    wire,
};
use futures::StreamExt as _;
use js_sys::{Reflect, Uint8Array};
use prost::Message as _;
use std::rc::Rc;
use wasm_bindgen::JsCast as _;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(typescript_custom_section)]
const PUBLICATION_TYPE: &str = "
export interface PublishedDeployment {
    /** Original canonical Actors DeploymentRequest bytes. */
    request: Uint8Array;
    /** Canonical Objects Verified and RetainResponse, with original revisions. */
    object: ObjectsV1BinaryReply;
    retention: ObjectsV1BinaryReply;
}
export type EntryResult = PublishedDeployment | ObjectsV1BinaryReply;
";

fn field(value: &JsValue, name: &str) -> Result<JsValue, JsValue> {
    Reflect::get(value, &name.into())
}

fn text_field(value: &JsValue, name: &str) -> Result<String, JsValue> {
    field(value, name)?
        .as_string()
        .ok_or_else(|| js_sys::TypeError::new(&format!("{name} must be text")).into())
}

/// The field's bytes, copied once before any await.
fn bytes_field(value: &JsValue, name: &str) -> Result<Vec<u8>, JsValue> {
    let bytes: Uint8Array = field(value, name)?
        .dyn_into()
        .map_err(|_| js_sys::TypeError::new(&format!("{name} must be Uint8Array bytes")))?;
    let (bytes, ()) = acyclic_stream::javascript_bytes::snapshot(&bytes, |_| Ok(()))?;
    Ok(bytes)
}

fn entry_error(error: entry::Error) -> JsValue {
    match error {
        entry::Error::Objects(error) => js_error(&error),
        entry::Error::Manifest(error) => {
            let value = js_sys::Error::new(&error.to_string());
            value.set_name("ManifestError");
            let _ = Reflect::set(&value, &"path".into(), &error.path.into());
            let _ = Reflect::set(&value, &"rule".into(), &error.rule.into());
            value.into()
        }
    }
}

fn unavailable() -> entry::Error {
    acyclic_objects::Error::from(wire::ErrorCode::Unavailable).into()
}

fn root(bytes: &Uint8Array) -> Result<RootId, JsValue> {
    let (bytes, ()) = acyclic_stream::javascript_bytes::snapshot(bytes, |_| Ok(()))?;
    RootId::new(bytes).map_err(|error| js_error(&error))
}

/// Selected-entry publication over the caller's original admitted Objects owner.
#[wasm_bindgen(js_name = EntryStore)]
pub struct JavaScriptEntryStore {
    inner: Rc<entry::EntryStore<BinaryObjects<JavaScriptTransport>>>,
}

#[wasm_bindgen(js_class = EntryStore)]
impl JavaScriptEntryStore {
    /// Capture the original account without opening a connection or operation.
    #[wasm_bindgen(constructor)]
    pub fn new(objects: JsValue) -> Result<Self, JsValue> {
        Ok(Self {
            inner: Rc::new(entry::EntryStore::new(BinaryObjects::new(
                JavaScriptTransport::new(objects).map_err(|error| js_error(&error))?,
            ))),
        })
    }

    /// Admit all selected bytes before the original publication can be polled.
    /// `selection` is the caller's `EntrySelection`: `manifest`, `environment`
    /// and `entry` text, the `artifact` bytes, and `files` of `{ path, bytes }`.
    #[wasm_bindgen(js_name = openPublication)]
    pub fn open_publication(
        &self,
        intent: &Uint8Array,
        selection: &JsValue,
        idempotency_key: &str,
    ) -> Result<EntryRequest, JsValue> {
        let root = root(intent)?;
        let artifact = bytes_field(selection, "artifact")?;
        let files = field(selection, "files")?
            .dyn_into::<js_sys::Array>()
            .map_err(|_| js_sys::TypeError::new("files must be an array"))?
            .iter()
            .map(|file| {
                Ok((
                    text_field(&file, "path")?,
                    bytes_field(&file, "bytes")?.into(),
                ))
            })
            .collect::<Result<_, JsValue>>()?;
        let prepared = PreparedEntry::from_json(
            text_field(selection, "manifest")?.as_bytes(),
            &text_field(selection, "environment")?,
            &text_field(selection, "entry")?,
            artifact.into(),
            files,
        )
        .map_err(|error| entry_error(error.into()))?;
        // Lowering admits the original deployment intent before any request.
        let prepared = prepared
            .into_deployment(idempotency_key)
            .map_err(|error| entry_error(error.into()))?;
        let store = self.inner.clone();
        Ok(EntryRequest::new(async move {
            let published = store.publish_prepared(&root, prepared).await?;
            let request: acyclic_actors::wire::DeploymentRequest = published.request.into();
            let object: wire::Verified = (&published.publication.object).into();
            let retention = wire::RetainResponse {
                references: published.publication.retention.references,
            };
            let value = js_sys::Object::new();
            Reflect::set(
                &value,
                &"request".into(),
                &Uint8Array::from(request.encode_to_vec().as_slice()),
            )
            .map_err(|_| unavailable())?;
            Reflect::set(
                &value,
                &"object".into(),
                &js_reply(
                    &object.encode_to_vec(),
                    published.publication.object.revision.as_ref(),
                )
                .map_err(|_| unavailable())?,
            )
            .map_err(|_| unavailable())?;
            Reflect::set(
                &value,
                &"retention".into(),
                &js_reply(
                    &retention.encode_to_vec(),
                    published.publication.retention.revision.as_ref(),
                )
                .map_err(|_| unavailable())?,
            )
            .map_err(|_| unavailable())?;
            Ok(value.into())
        }))
    }

    /// Release this deployment's stable intent root through the same account.
    #[wasm_bindgen(js_name = openRelease)]
    pub fn open_release(&self, intent: &Uint8Array) -> Result<EntryRequest, JsValue> {
        let root = root(intent)?;
        let store = self.inner.clone();
        Ok(EntryRequest::new(async move {
            let released = store.release(&root).await?;
            js_reply(
                &wire::ReleaseResponse {
                    existed: released.existed,
                }
                .encode_to_vec(),
                released.revision.as_ref(),
            )
            .map_err(|_| unavailable())
        }))
    }
}

/// Owns one original publication or release through the shared pull primitive.
#[wasm_bindgen]
pub struct EntryRequest {
    state: Rc<acyclic_stream::pull::PullCursor<JsValue, entry::Error>>,
}

impl EntryRequest {
    fn new(operation: impl Future<Output = Result<JsValue, entry::Error>> + 'static) -> Self {
        Self {
            state: Rc::new(acyclic_stream::pull::PullCursor::new(
                futures::stream::once(operation).boxed_local(),
                unavailable(),
            )),
        }
    }
}

#[wasm_bindgen]
impl EntryRequest {
    /// Resolve the original result; closing also settles a parked pull.
    #[wasm_bindgen(unchecked_return_type = "EntryResult | null")]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        self.state
            .next()
            .await
            .map_err(entry_error)?
            .ok_or_else(|| entry_error(unavailable()))
    }

    /// Drop the original source and its pending Objects request.
    pub fn close(&self) {
        self.state.close();
    }
}

/// Node/browser artifact admission: the entry bytes and every supplied file are
/// checked against the parsed manifest's declared entry and assets.
#[wasm_bindgen::prelude::wasm_bindgen(js_name = verifyArtifact)]
pub fn verify_artifact_json(
    input: &str,
    environment: &str,
    entry: &str,
    artifact: &js_sys::Uint8Array,
    paths: Vec<String>,
    files: Vec<js_sys::Uint8Array>,
) -> Result<(), wasm_bindgen::JsValue> {
    let manifest = parse(input.as_bytes())
        .map_err(|error| wasm_bindgen::JsValue::from_str(&error.to_string()))?;
    // Every supplied file is checked: unpaired paths or files are refused, not dropped.
    if paths.len() != files.len() {
        return Err(wasm_bindgen::JsValue::from_str(
            &ManifestError::invalid("assets", "undeclared-asset").to_string(),
        ));
    }
    let (artifact, ()) = acyclic_stream::javascript_bytes::snapshot(artifact, |_| Ok(()))?;
    let files: Vec<(String, Vec<u8>)> = paths
        .into_iter()
        .zip(files)
        .map(|(path, file)| {
            let (bytes, ()) = acyclic_stream::javascript_bytes::snapshot(&file, |_| Ok(()))?;
            Ok((path, bytes))
        })
        .collect::<Result<_, wasm_bindgen::JsValue>>()?;
    verify_artifact(
        &manifest,
        environment,
        entry,
        &artifact,
        files
            .iter()
            .map(|(path, bytes)| (path.as_str(), bytes.as_slice())),
    )
    .map_err(|error| wasm_bindgen::JsValue::from_str(&error.to_string()))
}

/// Node/browser admission export; executes the same parser and validator as Rust.
#[wasm_bindgen::prelude::wasm_bindgen(js_name = validate)]
pub fn validate_json(input: &str) -> Result<(), wasm_bindgen::JsValue> {
    parse(input.as_bytes())
        .map(|_| ())
        .map_err(|error| wasm_bindgen::JsValue::from_str(&error.to_string()))
}

/// Admit a JSON SecretRef through the same Rust contract as deployment
/// admission, returning its canonical JSON with a decimal-string version.
/// No credential lookup or value resolution runs.
#[wasm_bindgen(js_name = parseSecretReference)]
pub fn parse_secret_reference(input: &str) -> Result<String, JsValue> {
    let reference: acyclic_auth::secrets::SecretRef = serde_json::from_str(input)
        .map_err(|_| entry_error(ManifestError::invalid("reference", "secret-reference").into()))?;
    serde_json::to_string(&reference)
        .map_err(|_| entry_error(ManifestError::invalid("reference", "secret-reference").into()))
}
