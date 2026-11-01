//! Pinned public tool contracts for each model family's native tool surface.
//!
//! The family profile layer (`profile/native/*.md`) pins each declaration's
//! capability, variant name and revision; these files pin the exact public
//! names, descriptions and input schemas a model of that family was trained on.
use crate::{
    Error, Result,
    model::NativeToolProfile,
    tool::{Tool, ToolDefinition, ToolRegistry},
};
use serde_json::Value;
use std::sync::Arc;

mod adapter;
mod arguments;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod arguments_tests;
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

/// Offers each capability in `tools` that `profile` maps as that family's
/// native declarations, in the layer's order. Each declaration runs the
/// canonical tool through its layer template; its grant, policy and approval
/// are the capability's. Unmapped declarations, and capabilities `tools` does
/// not select, stay as they are.
pub fn bind(tools: &mut ToolRegistry, profile: NativeToolProfile) -> Result<()> {
    let mut presented: Vec<(String, Vec<String>)> = Vec::new();
    for (revision, mut definition) in profile.tools()?.into_iter().zip(definitions(profile)?) {
        let Some(template) = revision.arguments else {
            continue;
        };
        let Some(original) = tools.get(&revision.capability, None, None)? else {
            continue;
        };
        let original = Arc::new(original.clone());
        // Results are the capability's, so their contracts are too.
        definition
            .output_schema
            .clone_from(&original.definition.output_schema);
        definition
            .projection_schema
            .clone_from(&original.definition.projection_schema);
        let name = definition.name.clone();
        let adapter = Arc::new(adapter::Adapter {
            arguments: arguments::Arguments::new(template, revision.refuse)?,
            original,
        });
        tools.register(
            (
                Tool {
                    hook_names: adapter.original.hook_names,
                    definition,
                    executor: adapter.clone(),
                    projection: adapter,
                },
                None,
            ),
            &mut None,
            None,
        )?;
        match presented
            .iter_mut()
            .find(|(capability, _)| *capability == revision.capability)
        {
            Some((_, natives)) => natives.push(name),
            None => presented.push((revision.capability, vec![name])),
        }
    }
    presented
        .into_iter()
        .try_for_each(|(capability, natives)| tools.present_natively(&capability, natives))
}

fn take_string(value: &mut Value, key: &str) -> Result<String> {
    match value.get_mut(key).map(Value::take) {
        Some(Value::String(text)) => Ok(text),
        _ => Err(Error::Invalid("native declaration string is absent".into())),
    }
}
