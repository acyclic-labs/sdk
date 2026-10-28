use super::*;

#[derive(Deserialize)]
struct SchemaDefinition {
    name: String,
    version: u32,
    schema: serde_json::Value,
    implementation_digest: [u8; 32],
    fork_policy: ExtensionForkPolicy,
}

pub(super) fn schema_registry(value: JsValue) -> Result<SchemaRegistry, JsValue> {
    let definitions: Vec<SchemaDefinition> = from_js(value)?;
    let mut registry = SchemaRegistry::new();
    for definition in definitions {
        registry
            .register(
                definition.name,
                definition.version,
                definition.schema,
                definition.implementation_digest,
                definition.fork_policy,
                [],
                None,
            )
            .map_err(js_error)?;
    }
    Ok(registry)
}
