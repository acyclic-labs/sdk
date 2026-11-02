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

/// One profile's mapped declarations, decoded and checked once.
pub struct NativeTools(Vec<(String, ToolDefinition, Arc<arguments::Arguments>)>);

impl NativeTools {
    /// The declarations `profile`'s layer maps, in the layer's order.
    /// Unmapped declarations are left out.
    ///
    /// # Errors
    /// [`Error::Invalid`] when the layer, its declarations or a template is malformed.
    pub fn new(profile: NativeToolProfile) -> Result<Self> {
        profile
            .tools()?
            .into_iter()
            .zip(definitions(profile)?)
            .filter_map(|(revision, definition)| {
                let template = revision.arguments?;
                Some(
                    arguments::Arguments::new(template, revision.refuse)
                        .map(|arguments| (revision.capability, definition, Arc::new(arguments))),
                )
            })
            .collect::<Result<_>>()
            .map(Self)
    }

    /// Offers each capability `tools` selects as these declarations. Each runs
    /// the capability's selected revision through its template; its grant,
    /// policy and approval are the capability's. A manifest pins `tools`
    /// first, so the declarations run the revisions it pins.
    ///
    /// # Errors
    /// A registry refusal of a declaration.
    pub fn bind(&self, tools: &mut ToolRegistry) -> Result<()> {
        let mut presented: Vec<(&str, Vec<String>)> = Vec::new();
        for (capability, definition, arguments) in &self.0 {
            let Some(original) = tools.get(capability, None, None)? else {
                continue;
            };
            let original = Arc::new(original.clone());
            let mut definition = definition.clone();
            // Results are the capability's, so their contracts are too.
            definition
                .output_schema
                .clone_from(&original.definition.output_schema);
            definition
                .projection_schema
                .clone_from(&original.definition.projection_schema);
            let name = definition.name.clone();
            let adapter = Arc::new(adapter::Adapter {
                arguments: Arc::clone(arguments),
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
                .find(|(offered, _)| offered == capability)
            {
                Some((_, natives)) => natives.push(name),
                None => presented.push((capability, vec![name])),
            }
        }
        presented
            .into_iter()
            .try_for_each(|(capability, natives)| tools.present_natively(capability, natives))
    }
}

/// Binds `profile`'s mapped declarations into `tools`; see [`NativeTools::bind`].
///
/// # Errors
/// As [`NativeTools::new`] and [`NativeTools::bind`].
pub fn bind(tools: &mut ToolRegistry, profile: NativeToolProfile) -> Result<()> {
    NativeTools::new(profile)?.bind(tools)
}

fn take_string(value: &mut Value, key: &str) -> Result<String> {
    match value.get_mut(key).map(Value::take) {
        Some(Value::String(text)) => Ok(text),
        _ => Err(Error::Invalid("native declaration string is absent".into())),
    }
}
