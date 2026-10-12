//! Explicit bounded text tools using the original task's authenticated reader.

use super::schema::ProjectionMode;
use super::text::{ReadOptions, SearchMatches, SearchOptions, TextRange, TextSelection};
use super::{
    Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolResult, decode,
};
use crate::{
    Error, Result,
    conversation::FileRef,
    runtime::{RuntimeScope, ToolContext},
};
use acyclic_stream::BoxProviderFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

/// Selects an explicit partial-read variant without changing the default reader.
pub fn read_file_range(
    options: ReadOptions,
    maximum_result_bytes: u64,
    mode: ProjectionMode,
) -> Result<Tool> {
    options.validate()?;
    finite_result_bound(maximum_result_bytes)?;
    let adapter = Arc::new(TextTool {
        kind: TextKind::Read(options),
        maximum_result_bytes,
    });
    let projection = ReadProjection(mode);
    assemble(
        "acyclic.read_file_range",
        "Read one exact bounded UTF-8 byte interval from an authorized immutable source",
        adapter,
        super::schema::input::<ReadInput>()?,
        super::schema::output::<ReadResult>()?,
        projection.schema()?,
        Arc::new(projection),
        json!({"read":options,"maximum_result_bytes":maximum_result_bytes,"projection":mode}),
    )
}

/// Selects bounded literal search over one pinned source; consumers compose files.
pub fn search_file(
    options: SearchOptions,
    maximum_result_bytes: u64,
    mode: ProjectionMode,
) -> Result<Tool> {
    options.validate()?;
    finite_result_bound(maximum_result_bytes)?;
    let adapter = Arc::new(TextTool {
        kind: TextKind::Search(options),
        maximum_result_bytes,
    });
    let projection = SearchProjection(mode);
    assemble(
        "acyclic.search_file",
        "Search one authorized immutable UTF-8 source with finite byte comparisons and explicit omitted-match accounting",
        adapter,
        super::schema::input::<SearchInput>()?,
        super::schema::output::<SearchResult>()?,
        projection.schema()?,
        Arc::new(projection),
        json!({"search":options,"maximum_result_bytes":maximum_result_bytes,"projection":mode}),
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "one tool assembly binds its independent definition, executor and projector"
)]
fn assemble(
    name: &str,
    description: &str,
    adapter: Arc<TextTool>,
    mut input_schema: Value,
    output_schema: Value,
    projection_schema: Value,
    projection: Arc<dyn ToolProjection>,
    configuration: Value,
) -> Result<Tool> {
    input_schema
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("generated text-tool schema is not an object".into()))?
        .insert("x-harness-text-configuration".into(), configuration);
    let definition = ToolDefinition {
        name: name.into(),
        revision: "portable-text-1".into(),
        description: description.into(),
        input_schema,
        output_schema,
        projection_schema,
    };
    definition.validate(&mut None)?;
    Ok(Tool {
        definition,
        executor: adapter,
        projection,
    })
}

fn finite_result_bound(maximum: u64) -> Result<()> {
    if maximum == 0 || maximum > crate::conversation::MAX_EXACT_JS_INTEGER {
        return Err(Error::Invalid(
            "text result bound must be positive and exactly representable".into(),
        ));
    }
    Ok(())
}

mod inputs;
pub use inputs::{ReadInput, ReadResult, SearchInput, SearchResult};
mod projection;
pub use projection::{ReadProjection, SearchProjection};
mod executor;
use executor::{TextKind, TextTool};
#[cfg(test)]
mod tests;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use super::value;
use super::{edit, files, schema, text};
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod records;
