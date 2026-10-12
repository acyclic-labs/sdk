use super::*;

/// JavaScript-facing tool definition shape. The public TypeScript facade uses
/// camelCase names while the native definition remains `snake_case`.
#[derive(Deserialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub(super) struct WasmToolDefinitionInput {
    name: String,
    revision: String,
    description: String,
    #[tsify(type = "WasmToolJsonSchema")]
    input_schema: serde_json::Value,
    #[tsify(type = "WasmToolJsonSchema")]
    output_schema: serde_json::Value,
    #[tsify(type = "WasmToolJsonSchema")]
    projection_schema: serde_json::Value,
}

impl From<WasmToolDefinitionInput> for ToolDefinition {
    fn from(value: WasmToolDefinitionInput) -> Self {
        Self {
            name: value.name,
            revision: value.revision,
            description: value.description,
            input_schema: value.input_schema,
            output_schema: value.output_schema,
            projection_schema: value.projection_schema,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct WasmToolInvocationInput {
    call_id: String,
    name: String,
    arguments: serde_json::Value,
}

#[derive(Deserialize)]
pub(super) struct WasmToolResultInput {
    value: serde_json::Value,
}

/// Validates a model-visible tool definition using the native contract.
#[wasm_bindgen(js_name = validateToolDefinition)]
pub fn validate_tool_definition(definition: JsValue) -> Result<(), JsValue> {
    let definition: ToolDefinition = from_js::<WasmToolDefinitionInput>(definition)?.into();
    definition.validate(&mut None).map_err(js_error)
}

/// Validates one tool invocation against its registered definition.
#[wasm_bindgen(js_name = validateToolInvocation)]
pub fn validate_tool_invocation(definition: JsValue, invocation: JsValue) -> Result<(), JsValue> {
    let definition: ToolDefinition = from_js::<WasmToolDefinitionInput>(definition)?.into();
    definition.validate(&mut None).map_err(js_error)?;
    let invocation: WasmToolInvocationInput = from_js(invocation)?;
    if invocation.call_id.is_empty() || invocation.name != definition.name {
        return Err(JsValue::from_str(
            "tool invocation identity does not match definition",
        ));
    }
    crate::contract::validate_json_schema_value(
        &definition.input_schema,
        &invocation.arguments,
        "tool input",
        &mut None,
    )
    .map_err(js_error)
}

/// Validates one successful tool result against its registered definition.
#[wasm_bindgen(js_name = validateToolResult)]
pub fn validate_tool_result(definition: JsValue, result: JsValue) -> Result<(), JsValue> {
    let definition: ToolDefinition = from_js::<WasmToolDefinitionInput>(definition)?.into();
    definition.validate(&mut None).map_err(js_error)?;
    let result: WasmToolResultInput = from_js(result)?;
    crate::contract::validate_json_schema_value(
        &definition.output_schema,
        &result.value,
        "tool output",
        &mut None,
    )
    .map_err(js_error)
}

/// Validates the explicit result envelope without granting file or option authority.
#[wasm_bindgen(js_name = validateToolProjection)]
pub fn validate_tool_projection(
    #[wasm_bindgen(unchecked_param_type = "WasmToolDefinitionInput")] definition: JsValue,
    #[wasm_bindgen(unchecked_param_type = "WasmModelToolResultContentInput")] projection: JsValue,
    #[wasm_bindgen(unchecked_param_type = "WasmModelLimitsInput")] limits: JsValue,
) -> Result<(), JsValue> {
    let definition: ToolDefinition = from_js::<WasmToolDefinitionInput>(definition)?.into();
    definition.validate(&mut None).map_err(js_error)?;
    let projection: serde_json::Value = from_js(projection)?;
    let limits: Limits = from_js(limits)?;
    definition
        .validate_projection(&projection, limits, None)
        .map(drop)
        .map_err(js_error)
}

/// Wraps a consumer's JSON value schema in the complete typed projection envelope.
#[wasm_bindgen(js_name = jsonToolProjectionSchema, unchecked_return_type = "WasmToolJsonSchema")]
pub fn json_tool_projection_schema(
    #[wasm_bindgen(unchecked_param_type = "WasmToolJsonSchema")] value_schema: JsValue,
) -> Result<JsValue, JsValue> {
    let schema = crate::tool::json_projection_schema(from_js(value_schema)?);
    crate::contract::compile_json_schema(&schema, "tool projection", &mut None)
        .map_err(js_error)?;
    schema
        .serialize(&serde_wasm_bindgen::Serializer::json_compatible())
        .map_err(|error| JsValue::from_str(&error.to_string()))
}
