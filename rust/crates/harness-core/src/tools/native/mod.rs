//! Pinned public tool contracts for each model family's native tool surface.
//!
//! The family profile layer (`profile/native/*.md`) pins each declaration's
//! capability, variant name and revision; these files pin the exact public
//! names, descriptions and input schemas a model of that family was trained on.
use crate::{Error, Result, model::NativeToolProfile, tool::ToolDefinition};
use serde_json::Value;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// Exact source declarations in the profile's registration order.
pub const fn declarations(profile: NativeToolProfile) -> &'static str {
    match profile {
        NativeToolProfile::Anthropic => include_str!("anthropic.json"),
        NativeToolProfile::Openai => include_str!("openai.json"),
        NativeToolProfile::Google => include_str!("google.json"),
    }
}

/// Decodes one profile's public declarations into model-visible definitions,
/// in the profile layer's frozen order. The revision is the profile layer's
/// pinned revision; output and projection schemas belong to the capability
/// executor that binds each declaration, so they are left empty here.
pub fn definitions(profile: NativeToolProfile) -> Result<Vec<ToolDefinition>> {
    let mut source: Vec<Value> = serde_json::from_str(declarations(profile))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let revisions = profile.tools()?;
    if revisions.len() != source.len() {
        return Err(Error::Invalid(
            "native profile layer and public declarations differ".into(),
        ));
    }
    revisions
        .iter()
        .zip(source.iter_mut())
        .map(|(revision, entry)| {
            let name = take_string(entry, "name")?;
            if name != revision.variant {
                return Err(Error::Invalid(
                    "native declaration order differs from its profile layer".into(),
                ));
            }
            Ok(ToolDefinition {
                name,
                revision: revision.revision.clone(),
                description: take_string(entry, "description")?,
                input_schema: entry
                    .get_mut("parametersJsonSchema")
                    .map(Value::take)
                    .ok_or_else(|| Error::Invalid("native input contract is absent".into()))?,
                output_schema: Value::Bool(true),
                projection_schema: Value::Bool(true),
            })
        })
        .collect()
}

fn take_string(value: &mut Value, key: &str) -> Result<String> {
    match value.get_mut(key).map(Value::take) {
        Some(Value::String(text)) => Ok(text),
        _ => Err(Error::Invalid("native declaration string is absent".into())),
    }
}
