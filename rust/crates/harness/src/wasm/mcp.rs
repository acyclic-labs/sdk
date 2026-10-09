//! Generated portable MCP contract facade; all semantics remain in Rust.

use super::{WasmModelToolDefinitionWire, from_js, js_error};
use crate::mcp::McpCatalog;
use serde::Serialize as _;
use wasm_bindgen::prelude::*;

/// Validates an exact stdio descriptor without selecting a native provider.
#[wasm_bindgen(js_name=validateMcpStdioRequest)]
pub fn validate_mcp_stdio_request(
    #[wasm_bindgen(unchecked_param_type = "McpStdioRequest")] request: JsValue,
) -> Result<(), JsValue> {
    from_js::<crate::mcp::stdio::McpStdioRequest>(request)?
        .validate()
        .map_err(js_error)
}

/// Validates the complete bounded replacement before the host selects it.
#[wasm_bindgen(js_name=validateMcpCatalog)]
pub fn validate_mcp_catalog(
    #[wasm_bindgen(unchecked_param_type = "McpCatalog")] catalog: JsValue,
    #[wasm_bindgen(unchecked_param_type = "number")] maximum_tools: JsValue,
    #[wasm_bindgen(unchecked_param_type = "number")] maximum_bytes: JsValue,
) -> Result<(), JsValue> {
    from_js::<McpCatalog>(catalog)?
        .validate(from_js(maximum_tools)?, from_js(maximum_bytes)?)
        .map_err(js_error)
}

mod browser;

/// Projects the host-selected schema exposure through the ordinary model wire.
#[wasm_bindgen(js_name = mcpModelDefinitions, unchecked_return_type = "WasmModelToolDefinitionWire[]")]
pub fn mcp_model_definitions(
    #[wasm_bindgen(unchecked_param_type = "McpCatalog")] catalog: JsValue,
    #[wasm_bindgen(unchecked_param_type = "number")] maximum_tools: JsValue,
    #[wasm_bindgen(unchecked_param_type = "number")] maximum_bytes: JsValue,
) -> Result<JsValue, JsValue> {
    let catalog: McpCatalog = from_js(catalog)?;
    definitions_to_js(
        catalog
            .model_definitions(from_js(maximum_tools)?, from_js(maximum_bytes)?)
            .map_err(js_error)?,
    )
}

/// Searches a pinned catalog without network I/O or authority changes.
#[wasm_bindgen(js_name = searchMcpCatalog, unchecked_return_type = "WasmModelToolDefinitionWire[]")]
pub fn search_mcp_catalog(
    #[wasm_bindgen(unchecked_param_type = "McpCatalog")] catalog: JsValue,
    query: String,
    after: Option<String>,
    #[wasm_bindgen(unchecked_param_type = "number")] maximum_results: JsValue,
    #[wasm_bindgen(unchecked_param_type = "number")] maximum_tools: JsValue,
    #[wasm_bindgen(unchecked_param_type = "number")] maximum_bytes: JsValue,
) -> Result<JsValue, JsValue> {
    let catalog: McpCatalog = from_js(catalog)?;
    let definitions = catalog
        .search(
            &query,
            after.as_deref(),
            from_js(maximum_results)?,
            from_js(maximum_tools)?,
            from_js(maximum_bytes)?,
        )
        .map_err(js_error)?;
    definitions_to_js(definitions)
}

fn definitions_to_js(definitions: Vec<crate::tool::ToolDefinition>) -> Result<JsValue, JsValue> {
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
