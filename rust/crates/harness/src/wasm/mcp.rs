//! Generated portable MCP contract facade; all semantics remain in Rust.

use super::{WasmModelToolDefinitionWire, from_js, js_error};
use crate::mcp::McpCatalog;
use serde::Serialize as _;
use wasm_bindgen::prelude::*;

/// Validates the complete bounded replacement before the host selects it.
#[wasm_bindgen(js_name=validateMcpCatalog)]
pub fn validate_mcp_catalog(
    #[wasm_bindgen(unchecked_param_type = "McpCatalog")] catalog: JsValue,
    maximum_tools: u32,
    maximum_bytes: u32,
) -> Result<(), JsValue> {
    from_js::<McpCatalog>(catalog)?
        .validate(maximum_tools, maximum_bytes)
        .map_err(js_error)
}

mod browser;

/// Searches a pinned catalog without network I/O or authority changes.
#[wasm_bindgen(js_name = searchMcpCatalog, unchecked_return_type = "WasmModelToolDefinitionWire[]")]
pub fn search_mcp_catalog(
    #[wasm_bindgen(unchecked_param_type = "McpCatalog")] catalog: JsValue,
    query: String,
    after: Option<String>,
    maximum_results: u32,
    maximum_tools: u32,
    maximum_bytes: u32,
) -> Result<JsValue, JsValue> {
    let catalog: McpCatalog = from_js(catalog)?;
    let definitions = catalog
        .search(
            &query,
            after.as_deref(),
            maximum_results,
            maximum_tools,
            maximum_bytes,
        )
        .map_err(js_error)?;
    let wire = definitions
        .into_iter()
        .map(|definition| WasmModelToolDefinitionWire {
            name: definition.name,
            revision: definition.revision,
            description: definition.description,
            input_schema: definition.input_schema,
            output_schema: definition.output_schema,
        })
        .collect::<Vec<_>>();
    // Definitions contain only admitted JavaScript JSON and added string/bool
    // envelope fields. Preserve its number/object representation instead of
    // converting JSON schema objects to Maps or integer literals to BigInts.
    wire.serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(|error| JsValue::from_str(&error.to_string()))
}
