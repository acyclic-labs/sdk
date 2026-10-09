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

/// Exact immutable source and byte interval supplied by the caller.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadInput {
    /// Owner-authorized immutable source.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// Exact UTF-8 byte interval; no implicit rounding or truncation.
    pub range: TextRange,
}

/// Literal search arguments; there is no implicit regex, directory or shell search.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchInput {
    /// Owner-authorized immutable source.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// Nonempty case-sensitive UTF-8 literal, including overlapping matches.
    #[schemars(length(min = 1))]
    pub query: String,
}

/// Durable read result retaining exact source identity and explicit omissions.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReadResult {
    /// Original reference, including provider, volume, generation and content identity.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// Exact selected bytes and omitted source intervals.
    pub selection: TextSelection,
}

/// Durable literal-search result, independent of its model projection.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchResult {
    /// Original reference, including provider, volume, generation and content identity.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmFileRefWire"))]
    pub file: FileRef,
    /// Exact literal searched in that source.
    pub query: String,
    /// Complete match accounting with explicitly bounded retained positions.
    pub matches: SearchMatches,
}

/// Independently selectable read projector and its complete generated contract.
#[derive(Clone, Copy)]
pub struct ReadProjection(pub ProjectionMode);

/// Independently selectable search projector and its complete generated contract.
#[derive(Clone, Copy)]
pub struct SearchProjection(pub ProjectionMode);

fn encode<T: Serialize>(value: T) -> Result<Value> {
    serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))
}

impl ReadProjection {
    /// Complete projection envelope schema, owning all nested definitions.
    pub fn schema(self) -> Result<Value> {
        match self.0 {
            ProjectionMode::Full => super::schema::json_projection::<ReadResult>(),
            ProjectionMode::Reference => super::schema::reference_projection(),
        }
    }
}

impl ToolProjection for ReadProjection {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        let value: ReadResult = decode(&result.value)?;
        match self.0 {
            ProjectionMode::Full => super::schema::project_json(value),
            ProjectionMode::Reference => super::schema::project_reference(
                value.file,
                format!(
                    "Read bytes {}..{}; {} source bytes omitted before and {} after. Selected text is retained in the canonical tool result and omitted from this model projection.",
                    value.selection.range.start,
                    value.selection.range.end,
                    value.selection.omitted_before,
                    value.selection.omitted_after,
                ),
            ),
        }
    }
}

impl SearchProjection {
    /// Complete projection envelope schema, owning all nested definitions.
    pub fn schema(self) -> Result<Value> {
        match self.0 {
            ProjectionMode::Full => super::schema::json_projection::<SearchResult>(),
            ProjectionMode::Reference => super::schema::reference_projection(),
        }
    }
}

impl ToolProjection for SearchProjection {
    fn project(&self, _: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        let value: SearchResult = decode(&result.value)?;
        match self.0 {
            ProjectionMode::Full => super::schema::project_json(value),
            ProjectionMode::Reference => super::schema::project_reference(
                value.file,
                format!(
                    "Literal search completed: {} total matches, {} retained positions, {} positions omitted from the canonical result, {} byte comparisons. Query and retained positions are retained in the canonical tool result and omitted from this model projection.",
                    value.matches.total_matches,
                    value.matches.matches.len(),
                    value.matches.omitted_matches,
                    value.matches.work,
                ),
            ),
        }
    }
}

/// Selects an explicit partial-read variant without changing the default reader.
/// The result-byte ceiling and options are pinned in the definition digest.
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
    definition.validate()?;
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

#[derive(Clone, Copy)]
enum TextKind {
    Read(ReadOptions),
    Search(SearchOptions),
}

struct TextTool {
    kind: TextKind,
    maximum_result_bytes: u64,
}

impl TextTool {
    // This preflight is also used when the executor authorizes durable replay.
    // It does not read content or manufacture a replacement task authority.
    fn preflight(&self, scope: &RuntimeScope, invocation: &ToolInvocation) -> Result<FileRef> {
        invocation.validate()?;
        crate::contract::validate_json_byte_bound(
            &invocation.arguments,
            scope.limits().file_bytes,
        )?;
        let result_bound = self.maximum_result_bytes.min(scope.limits().file_bytes);
        let file = match self.kind {
            TextKind::Read(options) => {
                options.validate()?;
                let input: ReadInput = decode(&invocation.arguments)?;
                let length = input
                    .range
                    .end
                    .checked_sub(input.range.start)
                    .ok_or_else(|| Error::Invalid("read range is reversed".into()))?;
                if input.file.descriptor().byte_length() > options.maximum_input_bytes
                    || input.range.end > input.file.descriptor().byte_length()
                    || length > options.maximum_text_bytes.min(result_bound)
                {
                    return Err(Error::Invalid(
                        "read exceeds its admitted byte bounds".into(),
                    ));
                }
                input.file
            }
            TextKind::Search(options) => {
                options.validate()?;
                let input: SearchInput = decode(&invocation.arguments)?;
                if input.file.descriptor().byte_length() > options.maximum_input_bytes
                    || input.query.is_empty()
                    || input.query.len() as u64 > options.maximum_query_bytes
                    || u64::from(options.maximum_matches) * std::mem::size_of::<TextRange>() as u64
                        > result_bound
                {
                    return Err(Error::Invalid(
                        "search exceeds its admitted input or retained-position bounds".into(),
                    ));
                }
                input.file
            }
        };
        crate::conversation::validate_content_path(file.path())?;
        if crate::conversation::is_internal_path(file.path())
            || !crate::runtime::read_granted(scope.grants(), &file)?
        {
            return Err(Error::Unauthorized(
                "text tool requires a public owner-authorized file".into(),
            ));
        }
        if file.descriptor().byte_length() > scope.limits().file_bytes {
            return Err(Error::Invalid(
                "text source exceeds original task file bound".into(),
            ));
        }
        Ok(file)
    }

    fn preflight_with_context(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
    ) -> Result<FileRef> {
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
        {
            return Err(Error::Unauthorized(
                "text tool requires its exact admitted call context".into(),
            ));
        }
        self.preflight(context.task().scope(), invocation)
    }

    async fn run(&self, context: ToolContext, invocation: ToolInvocation) -> Result<ToolResult> {
        let file = self.preflight_with_context(&context, &invocation)?;
        let task = context.task();
        let bytes = task.read_file(&file).await?;
        let source = std::str::from_utf8(&bytes)
            .map_err(|_| Error::Invalid("text tool requires UTF-8 content".into()))?;
        let maximum = self
            .maximum_result_bytes
            .min(task.scope().limits().file_bytes);
        let value = match self.kind {
            TextKind::Read(options) => {
                let input: ReadInput = decode(&invocation.arguments)?;
                let value = ReadResult {
                    file,
                    selection: super::text::read_range(source, input.range, options)?,
                };
                crate::contract::validate_json_byte_bound(&value, maximum)?;
                encode(value)?
            }
            TextKind::Search(options) => {
                let input: SearchInput = decode(&invocation.arguments)?;
                let matches = super::text::literal_search(source, &input.query, options)?;
                let value = SearchResult {
                    file,
                    query: input.query,
                    matches,
                };
                crate::contract::validate_json_byte_bound(&value, maximum)?;
                encode(value)?
            }
        };
        Ok(ToolResult { value })
    }
}

impl ToolExecutor for TextTool {
    fn authorize(&self, scope: Option<&RuntimeScope>, invocation: &ToolInvocation) -> Result<()> {
        let scope = scope.ok_or_else(|| {
            Error::Unauthorized("text tool requires original runtime scope".into())
        })?;
        self.preflight(scope, invocation)?;
        Err(Error::Unauthorized(
            "portable text tool requires original task context".into(),
        ))
    }

    fn authorize_with_context(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        self.preflight_with_context(context, invocation).map(|_| ())
    }

    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "text tool requires owner-bound task context".into(),
            ))
        })
    }

    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(self.run(context, invocation))
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "text reconciliation requires owner-bound task context".into(),
            ))
        })
    }

    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        // Immutable reads have no publication effect; retry uses the same pinned source.
        Box::pin(async move { self.run(context, invocation).await.map(Some) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentId, Capabilities, OperationId,
        conversation::{FileDescriptor, Limits, VolumeClass, VolumeOwner, VolumeRef},
        model::{FileProjectionPolicy, ModelDataPart, ToolResultContent},
        resources::ProviderRef,
    };

    fn file() -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("text", "filesystem", "1")?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([12; 16])),
            )?,
            "text.txt",
            "generation-1",
            FileDescriptor::from_bytes(b"ababa", "text/plain")?,
            "text.txt",
        )
    }

    fn invocation(arguments: Value) -> ToolInvocation {
        ToolInvocation {
            operation_id: OperationId::from_bytes([13; 16]),
            call_id: "read-1".into(),
            name: "acyclic.read_file_range".into(),
            arguments,
        }
    }

    const READ: ReadOptions = ReadOptions {
        maximum_input_bytes: 4096,
        maximum_text_bytes: 32,
    };
    const SEARCH: SearchOptions = SearchOptions {
        maximum_input_bytes: 4096,
        maximum_query_bytes: 32,
        maximum_work: 4096,
        maximum_matches: 2,
    };

    fn assert_reference(projection: &Value, expected: &FileRef) -> Result<()> {
        let wire: ToolResultContent = decode(projection)?;
        let ToolResultContent::Parts { parts } = wire else {
            panic!("reference envelope")
        };
        assert_eq!(parts.len(), 2);
        assert!(matches!(&parts[0], ModelDataPart::Text { text } if text.contains("omitted")));
        assert!(
            matches!(&parts[1], ModelDataPart::File { file, policy: FileProjectionPolicy::Reference } if file == expected)
        );
        Ok(())
    }

    #[test]
    fn replaceable_projections_preserve_canonical_results_and_real_model_wire() -> Result<()> {
        let file = file()?;
        let input = invocation(json!({"file":file,"range":{"start":1,"end":4}}));
        let read = ReadResult {
            file: file.clone(),
            selection: super::super::text::read_range(
                "ababa",
                TextRange { start: 1, end: 4 },
                READ,
            )?,
        };
        let result = ToolResult {
            value: encode(&read)?,
        };
        let unchanged = result.clone();
        for mode in [ProjectionMode::Full, ProjectionMode::Reference] {
            let projector = ReadProjection(mode);
            let projected = projector.project(&input, &result)?;
            super::super::validate_value(&projector.schema()?, &projected, "read projection")?;
            if mode == ProjectionMode::Full {
                assert_eq!(projected, json!({"kind":"json","value":result.value}));
            } else {
                assert_reference(&projected, &file)?;
            }
            assert_eq!(result, unchanged);
        }
        let search = SearchResult {
            file: file.clone(),
            query: "a".into(),
            matches: super::super::text::literal_search("ababa", "a", SEARCH)?,
        };
        assert_eq!(search.matches.total_matches, 3);
        assert_eq!(search.matches.omitted_matches, 1);
        let result = ToolResult {
            value: encode(&search)?,
        };
        let unchanged = result.clone();
        for mode in [ProjectionMode::Full, ProjectionMode::Reference] {
            let projector = SearchProjection(mode);
            let projected = projector.project(&input, &result)?;
            super::super::validate_value(&projector.schema()?, &projected, "search projection")?;
            if mode == ProjectionMode::Full {
                assert_eq!(projected, json!({"kind":"json","value":result.value}));
            } else {
                assert_reference(&projected, &file)?;
            }
            assert_eq!(result, unchanged);
        }
        Ok(())
    }

    #[test]
    fn factory_contract_binds_mode_and_limits_and_rejects_wrong_envelopes() -> Result<()> {
        let full = read_file_range(READ, 4096, ProjectionMode::Full)?;
        let reference = read_file_range(READ, 4096, ProjectionMode::Reference)?;
        assert_ne!(full.definition.digest()?, reference.definition.digest()?);
        assert_eq!(
            full.definition.output_schema,
            reference.definition.output_schema
        );
        let changed = read_file_range(
            ReadOptions {
                maximum_text_bytes: 31,
                ..READ
            },
            4096,
            ProjectionMode::Full,
        )?;
        assert_ne!(full.definition.digest()?, changed.definition.digest()?);
        for tool in [
            full,
            reference,
            search_file(SEARCH, 4096, ProjectionMode::Full)?,
            search_file(SEARCH, 4096, ProjectionMode::Reference)?,
        ] {
            for schema in [
                &tool.definition.input_schema,
                &tool.definition.output_schema,
                &tool.definition.projection_schema,
            ] {
                crate::contract::compile_json_schema(schema, "typed text tool")?;
            }
            assert!(
                super::super::validate_value(
                    &tool.definition.projection_schema,
                    &json!({"file":file()?}),
                    "bare result"
                )
                .is_err()
            );
        }
        assert!(read_file_range(READ, 0, ProjectionMode::Full).is_err());
        assert!(
            search_file(
                SearchOptions {
                    maximum_matches: 0,
                    ..SEARCH
                },
                4096,
                ProjectionMode::Full
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn original_scope_preflight_rejects_missing_grants_and_oversize_inputs() -> Result<()> {
        let file = file()?;
        let adapter = TextTool {
            kind: TextKind::Read(READ),
            maximum_result_bytes: 4096,
        };
        let input = invocation(json!({"file":file,"range":{"start":1,"end":4}}));
        let empty = RuntimeScope::new(Capabilities::default(), Limits::default())?;
        assert!(adapter.authorize(None, &input).is_err());
        assert!(adapter.authorize(Some(&empty), &input).is_err());
        let grants = Capabilities::new([file.read_capability()?]);
        let scope = RuntimeScope::new(grants, Limits::default())?;
        assert_eq!(adapter.preflight(&scope, &input)?, file);
        for range in [json!({"start":4,"end":1}), json!({"start":0,"end":6})] {
            assert!(
                adapter
                    .preflight(&scope, &invocation(json!({"file":file,"range":range})))
                    .is_err()
            );
        }
        let bounded = TextTool {
            kind: TextKind::Read(ReadOptions {
                maximum_input_bytes: 4,
                ..READ
            }),
            maximum_result_bytes: 4096,
        };
        assert!(bounded.preflight(&scope, &input).is_err());
        let bounded = TextTool {
            kind: TextKind::Search(SEARCH),
            maximum_result_bytes: 1,
        };
        assert!(
            bounded
                .preflight(&scope, &invocation(json!({"file":file,"query":"a"})))
                .is_err()
        );
        let search = TextTool {
            kind: TextKind::Search(SEARCH),
            maximum_result_bytes: 4096,
        };
        assert!(
            search
                .preflight(&scope, &invocation(json!({"file":file,"query":""})))
                .is_err()
        );
        Ok(())
    }
}
