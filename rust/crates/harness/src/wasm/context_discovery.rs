//! Browser platform reads for the same native discovery and pinned-body code.

use super::{from_js, js_error, to_js, to_js_admitted};
use crate::{
    Error, Result,
    context::{
        ContextDiscovery, ContextReloadPolicy, ContextRoot, DiscoveredContext, PinnedContextPath,
    },
    conversation::{
        ContentFuture, ContentResidencyVerifier, FileRef, PrivateDirectoryEntry,
        PrivateDirectoryPage, VolumeRef,
    },
    resources::GenerationRef,
};
use js_sys::{Array, Function, Promise, Reflect, Uint8Array};
use serde::Serialize;
use tsify::Tsify;
use wasm_bindgen::{JsCast as _, prelude::*};
use wasm_bindgen_futures::JsFuture;

#[derive(Serialize, Tsify)]
struct ContextDirectoryQuery {
    root: ContextRoot,
    path: String,
    #[tsify(type = "(WasmResourceRefWire & { kind: 'generation' }) | null")]
    expected_generation: Option<GenerationRef>,
    after: Option<String>,
    maximum_entries: u32,
}

#[derive(Serialize, Tsify)]
#[tsify(large_number_types_as_bigints)]
struct ContextReadQuery {
    source: PinnedContextPath,
    maximum_bytes: u64,
}

#[derive(Serialize, Tsify)]
struct ContextPathResult {
    #[tsify(type = "WasmFileRefWire")]
    file: FileRef,
    #[tsify(type = "Uint8Array")]
    bytes: Vec<u8>,
}

#[wasm_bindgen(typescript_custom_section)]
const READER: &'static str = r#"
/** Bound owner authenticates each read; declarations confer no authority.
 * Functions are captured for one call and never retained in a process catalog. */
export interface ContextDiscoveryReader {
    list(query: ContextDirectoryQuery): Promise<PrivateDirectoryPage>;
    prefix(query: ContextReadQuery): Promise<Uint8Array>;
    read(query: ContextReadQuery): Promise<ContextPathResult>;
}
"#;

struct BrowserReader {
    receiver: JsValue,
    list: Function,
    prefix: Function,
    read: Function,
    maximum_bytes: u64,
}

fn platform_error(error: JsValue) -> Error {
    Error::Storage(
        error
            .as_string()
            .unwrap_or_else(|| format!("browser context read failed: {error:?}")),
    )
}

impl BrowserReader {
    fn new(receiver: JsValue, maximum_bytes: u64) -> Result<Self> {
        if maximum_bytes == 0 {
            return Err(Error::Invalid("browser read bound is invalid".into()));
        }
        let function = |name: &str| -> Result<Function> {
            Reflect::get(&receiver, &JsValue::from_str(name))
                .map_err(platform_error)?
                .dyn_into()
                .map_err(|_| Error::Invalid(format!("context reader requires {name}")))
        };
        Ok(Self {
            list: function("list")?,
            prefix: function("prefix")?,
            read: function("read")?,
            receiver,
            maximum_bytes,
        })
    }

    async fn invoke<T: Serialize>(&self, function: &Function, query: &T) -> Result<JsValue> {
        let query = to_js(query).map_err(platform_error)?;
        let promise = function
            .call1(&self.receiver, &query)
            .map_err(platform_error)?;
        JsFuture::from(Promise::resolve(&promise))
            .await
            .map_err(platform_error)
    }

    fn bytes(value: JsValue, maximum_bytes: u64) -> Result<Vec<u8>> {
        let bytes: Uint8Array = value
            .dyn_into()
            .map_err(|_| Error::Storage("context reader requires Uint8Array".into()))?;
        if u64::from(bytes.length()) > maximum_bytes {
            return Err(Error::Storage("context reader exceeded byte bound".into()));
        }
        Ok(bytes.to_vec())
    }
}

impl ContentResidencyVerifier for BrowserReader {
    fn verify<'a>(&'a self, _: &'a FileRef) -> ContentFuture<'a, Result<()>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "bind the owner file verifier for direct refs".into(),
            ))
        })
    }

    fn list_private_directory<'a>(
        &'a self,
        volume: &'a VolumeRef,
        prefix: &'a str,
        path: &'a str,
        generation: Option<&'a GenerationRef>,
        after: Option<&'a str>,
        maximum_entries: u32,
    ) -> ContentFuture<'a, Result<PrivateDirectoryPage>> {
        Box::pin(async move {
            let query = ContextDirectoryQuery {
                root: ContextRoot {
                    volume: volume.clone(),
                    directory: prefix.into(),
                },
                path: path.into(),
                expected_generation: generation.cloned(),
                after: after.map(str::to_owned),
                maximum_entries,
            };
            let result = self.invoke(&self.list, &query).await?;
            let values: Array = Reflect::get(&result, &JsValue::from_str("entries"))
                .map_err(platform_error)?
                .dyn_into()
                .map_err(|_| Error::Storage("context page requires an entry array".into()))?;
            if values.length() > maximum_entries {
                return Err(Error::Storage("context page exceeded entry bound".into()));
            }
            let entries: Vec<PrivateDirectoryEntry> = (0..values.length())
                .map(|index| from_js(values.get(index)).map_err(platform_error))
                .collect::<Result<_>>()?;
            let generation = from_js(
                Reflect::get(&result, &JsValue::from_str("generation")).map_err(platform_error)?,
            )
            .map_err(platform_error)?;
            let has_more = Reflect::get(&result, &JsValue::from_str("hasMore"))
                .map_err(platform_error)?
                .as_bool()
                .ok_or_else(|| Error::Storage("context page requires hasMore".into()))?;
            Ok(PrivateDirectoryPage {
                generation,
                entries,
                has_more,
            })
        })
    }

    fn read_private_prefix<'a>(
        &'a self,
        volume: &'a VolumeRef,
        prefix: &'a str,
        path: &'a str,
        generation: &'a GenerationRef,
        maximum_bytes: u64,
    ) -> ContentFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            let query = ContextReadQuery {
                source: PinnedContextPath {
                    root: ContextRoot {
                        volume: volume.clone(),
                        directory: prefix.into(),
                    },
                    path: path.into(),
                    generation: generation.clone(),
                },
                maximum_bytes,
            };
            Self::bytes(self.invoke(&self.prefix, &query).await?, maximum_bytes)
        })
    }

    fn read_private_path<'a>(
        &'a self,
        volume: &'a VolumeRef,
        prefix: &'a str,
        path: &'a str,
        generation: Option<&'a GenerationRef>,
    ) -> ContentFuture<'a, Result<(FileRef, Vec<u8>)>> {
        Box::pin(async move {
            let generation = generation.ok_or_else(|| {
                Error::Invalid("context path requires a pinned generation".into())
            })?;
            let query = ContextReadQuery {
                source: PinnedContextPath {
                    root: ContextRoot {
                        volume: volume.clone(),
                        directory: prefix.into(),
                    },
                    path: path.into(),
                    generation: generation.clone(),
                },
                maximum_bytes: self.maximum_bytes,
            };
            let result = self.invoke(&self.read, &query).await?;
            let bytes = Self::bytes(
                Reflect::get(&result, &JsValue::from_str("bytes")).map_err(platform_error)?,
                self.maximum_bytes,
            )?;
            let file =
                from_js(Reflect::get(&result, &JsValue::from_str("file")).map_err(platform_error)?)
                    .map_err(platform_error)?;
            Ok((file, bytes))
        })
    }
}

/// Caller admits refresh through its ordinary workflow; this function starts no watcher.
#[wasm_bindgen(js_name = captureDiscoveredContext, unchecked_return_type = "DiscoveredContext")]
pub async fn capture_discovered_context(
    #[wasm_bindgen(unchecked_param_type = "ContextDiscovery")] declaration: JsValue,
    #[wasm_bindgen(unchecked_param_type = "ContextDiscoveryReader")] reader: JsValue,
) -> std::result::Result<JsValue, JsValue> {
    let declaration: ContextDiscovery = from_js(declaration)?;
    let reader =
        BrowserReader::new(reader, declaration.limits.instruction_bytes).map_err(js_error)?;
    let snapshot = declaration.capture(&reader).await.map_err(js_error)?;
    snapshot_to_js(&snapshot)
}

fn snapshot_to_js(snapshot: &DiscoveredContext) -> std::result::Result<JsValue, JsValue> {
    let result = to_js(snapshot)?;
    let files = Array::new();
    for file in &snapshot.instructions {
        files.push(&to_js_admitted(file)?);
    }
    Reflect::set(&result, &JsValue::from_str("instructions"), &files)?;
    Ok(result)
}

/// Select an immutable revision using Rust's explicit or next-request refresh policy.
#[wasm_bindgen(js_name = contextForRequest, unchecked_return_type = "DiscoveredContext")]
pub async fn context_for_request(
    #[wasm_bindgen(unchecked_param_type = "ContextDiscovery")] declaration: JsValue,
    #[wasm_bindgen(unchecked_param_type = "ContextDiscoveryReader")] reader: JsValue,
    #[wasm_bindgen(unchecked_param_type = "DiscoveredContext")] current: JsValue,
    #[wasm_bindgen(unchecked_param_type = "ContextReloadPolicy")] reload: JsValue,
) -> std::result::Result<JsValue, JsValue> {
    let declaration: ContextDiscovery = from_js(declaration)?;
    let current: DiscoveredContext = from_js(current)?;
    let reload: ContextReloadPolicy = from_js(reload)?;
    if reload == ContextReloadPolicy::Explicit {
        current.validate().map_err(js_error)?;
        return snapshot_to_js(&current);
    }
    let reader =
        BrowserReader::new(reader, declaration.limits.instruction_bytes).map_err(js_error)?;
    let snapshot = declaration
        .for_request(&reader, &current, reload)
        .await
        .map_err(js_error)?;
    snapshot_to_js(&snapshot)
}

/// Ordinary bounded owner read of a discovered body; no model projection or execution is implied.
#[wasm_bindgen(js_name = readPinnedContextPath, unchecked_return_type = "ContextPathResult")]
pub async fn read_pinned_context_path(
    #[wasm_bindgen(unchecked_param_type = "PinnedContextPath")] source: JsValue,
    #[wasm_bindgen(unchecked_param_type = "ContextDiscoveryReader")] reader: JsValue,
    maximum_bytes: u32,
) -> std::result::Result<JsValue, JsValue> {
    let source: PinnedContextPath = from_js(source)?;
    let reader = BrowserReader::new(reader, u64::from(maximum_bytes)).map_err(js_error)?;
    let (file, bytes) = source.read(&reader).await.map_err(js_error)?;
    let body = ContextPathResult { file, bytes };
    let result = js_sys::Object::new();
    Reflect::set(
        &result,
        &JsValue::from_str("file"),
        &to_js_admitted(&body.file)?,
    )?;
    Reflect::set(
        &result,
        &JsValue::from_str("bytes"),
        &Uint8Array::from(body.bytes.as_slice()),
    )?;
    Ok(result.into())
}
