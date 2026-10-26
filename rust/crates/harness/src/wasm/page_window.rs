//! Browser ownership adapter for the one provider-page window.

use crate::client::{
    PageDirection, PageRequest, PageWindow, PageWindowError, PageWindowPolicy, WindowPage,
};
use js_sys::{Array, Uint8Array};
use serde::Deserialize;
use std::{cell::RefCell, sync::Arc};
use wasm_bindgen::prelude::*;

const MAXIMUM_PAGE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Deserialize, tsify::Tsify)]
#[serde(deny_unknown_fields)]
struct WasmPageEntry {
    key: String,
    #[tsify(type = "unknown")]
    value: Box<serde_json::value::RawValue>,
}

#[derive(Deserialize, tsify::Tsify)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WasmPageData {
    generation: String,
    items: Vec<WasmPageEntry>,
    #[tsify(type = "string")]
    before: Arc<String>,
    #[tsify(type = "string")]
    after: Arc<String>,
    has_more_before: bool,
    has_more_after: bool,
}

impl WindowPage for WasmPageData {
    type Item = WasmPageEntry;
    type Key = str;
    type Cursor = String;
    type Generation = String;

    fn items(&self) -> &[WasmPageEntry] { &self.items }
    fn key(item: &WasmPageEntry) -> &str { &item.key }
    fn before(&self) -> &Arc<String> { &self.before }
    fn after(&self) -> &Arc<String> { &self.after }
    fn generation(&self) -> &String { &self.generation }
    fn has_more_before(&self) -> bool { self.has_more_before }
    fn has_more_after(&self) -> bool { self.has_more_after }
    fn retained_bytes(&self) -> u64 {
        let arc_header = 2 * std::mem::size_of::<usize>();
        let metadata = std::mem::size_of::<Self>()
            + self.items.capacity() * std::mem::size_of::<WasmPageEntry>()
            + self.generation.capacity()
            + 2 * (std::mem::size_of::<String>() + arc_header)
            + self.before.capacity() + self.after.capacity();
        self.items.iter().fold(metadata as u64, |bytes, item| {
            bytes + item.key.capacity() as u64 + item.value.get().len() as u64
        })
    }
}

fn error(name: &str, cause: impl std::fmt::Display) -> JsValue {
    let error = js_sys::Error::new(&cause.to_string());
    error.set_name(name);
    error.into()
}

fn refusal(cause: PageWindowError) -> JsValue {
    let name = match cause {
        PageWindowError::Capacity => "page_capacity",
        PageWindowError::Busy => "page_busy",
        PageWindowError::ForeignRequest => "page_foreign_request",
        PageWindowError::SettledRequest => "page_settled_request",
        PageWindowError::StaleResponse => "page_stale_response",
        PageWindowError::GenerationChanged => "page_generation_changed",
        PageWindowError::DuplicateKey => "page_duplicate_key",
        PageWindowError::NoProgress => "page_no_progress",
        PageWindowError::Exhausted => "page_exhausted",
    };
    error(name, cause)
}

fn direction(value: &str) -> Result<PageDirection, JsValue> {
    match value {
        "before" => Ok(PageDirection::Before),
        "after" => Ok(PageDirection::After),
        _ => Err(error("page_direction", "expected before or after")),
    }
}

fn decode(encoded: &Uint8Array, maximum_bytes: u64) -> Result<WasmPageData, JsValue> {
    let (bytes, ()) = super::bytes::snapshot(encoded, |length| {
        if length > maximum_bytes {
            return Err(refusal(PageWindowError::Capacity));
        }
        Ok(())
    })?;
    // The byte admission precedes copying/parsing. Raw values retain exactly
    // their bounded JSON bytes; the core also charges keys, cursors and layout.
    super::json::reject_out_of_range_integer_tokens(&bytes)
        .map_err(|cause| error("page_invalid", format!("{cause:?}")))?;
    serde_json::from_slice(&bytes).map_err(|cause| error("page_invalid", cause))
}

/// Linear physical-fetch custody. Reset fences its response without allowing
/// another fetch. Finish it after the provider has settled, even on failure.
#[wasm_bindgen]
pub struct WasmPageRequest {
    request: Option<PageRequest<String>>,
}

#[wasm_bindgen]
impl WasmPageRequest {
    /// Exact opaque exclusive provider anchor, absent for initial discovery.
    pub fn cursor(&self) -> Result<Option<String>, JsValue> {
        self.request.as_ref().map(|request| request.cursor().cloned())
            .ok_or_else(|| refusal(PageWindowError::SettledRequest))
    }
}

/// The same PageWindow used natively; no browser paging state or entry map.
#[wasm_bindgen]
pub struct WasmPageWindow {
    window: PageWindow<WasmPageData>,
    cache: RefCell<Option<(u64, Array)>>,
    maximum_encoded_bytes: u64,
}

#[wasm_bindgen]
impl WasmPageWindow {
    /// Finite complete-page, entry and retained-backing-byte budgets.
    #[wasm_bindgen(constructor)]
    pub fn new(pages: u32, items: u32, bytes: u32) -> Result<Self, JsValue> {
        let policy = PageWindowPolicy::new(pages as usize, items as usize, u64::from(bytes))
            .map_err(refusal)?;
        Ok(Self {
            window: PageWindow::new(policy),
            cache: RefCell::new(None),
            maximum_encoded_bytes: u64::from(bytes).min(MAXIMUM_PAGE_BYTES),
        })
    }

    /// Admit one complete provider page atomically, fencing older responses.
    pub fn reset(&mut self, encoded: &Uint8Array) -> Result<(), JsValue> {
        let page = decode(encoded, self.maximum_encoded_bytes)?;
        self.window.reset(page).map_err(refusal)?;
        self.cache.get_mut().take();
        Ok(())
    }

    /// The caller owns I/O; this grants the sole response capability.
    pub fn begin(&mut self, edge: &str) -> Result<Option<WasmPageRequest>, JsValue> {
        Ok(self.window.begin(direction(edge)?).map_err(refusal)?
            .map(|request| WasmPageRequest { request: Some(request) }))
    }

    /// Consume a settled fetch. Null means provider failure/drained cancellation.
    /// Invalid bytes settle custody too, without changing the admitted window.
    pub fn finish(&mut self, request: &mut WasmPageRequest, encoded: Option<Uint8Array>) -> Result<(), JsValue> {
        let page = match encoded.as_ref()
            .map(|bytes| decode(bytes, self.maximum_encoded_bytes)).transpose() {
            Ok(page) => page,
            Err(cause) => {
                self.window.finish(&mut request.request, None).map_err(refusal)?;
                return Err(cause);
            }
        };
        self.window.finish(&mut request.request, page).map_err(refusal)?;
        self.cache.get_mut().take();
        Ok(())
    }

    /// One exported array per changed revision, reused by unchanged reads.
    /// Individual values use the existing exact-integer JSON boundary.
    pub fn items(&self) -> Result<Array, JsValue> {
        let revision = self.window.revision();
        let mut cache = self.cache.borrow_mut();
        if let Some((cached, values)) = cache.as_ref() {
            if *cached == revision { return Ok(values.clone()); }
        }
        let values = Array::new();
        for item in self.window.items() {
            let value = super::json::parse_json_bytes(item.value.get().as_bytes())?;
            values.push(&super::json::to_js_page_value(&value)?);
        }
        js_sys::Object::freeze(values.unchecked_ref::<js_sys::Object>());
        *cache = Some((revision, values.clone()));
        Ok(values)
    }

    #[wasm_bindgen(js_name = hasMore)]
    pub fn has_more(&self, edge: &str) -> Result<bool, JsValue> {
        Ok(self.window.has_more(direction(edge)?))
    }

    #[wasm_bindgen(js_name = retainedBytes)]
    /// Charged Rust pages only; the one cached JS projection is separate.
    pub fn retained_bytes(&self) -> u64 { self.window.retained_page_bytes() }
    #[wasm_bindgen(js_name = pageCount)]
    pub fn page_count(&self) -> usize { self.window.page_count() }
    pub fn revision(&self) -> u64 { self.window.revision() }
}
