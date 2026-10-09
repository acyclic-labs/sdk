//! Native definitions and serializer fixtures for current file-tool consumers.
//! This emits actual public contracts; it does not execute a filesystem effect.
#![allow(missing_docs, reason = "contract example binary, not public API")]

use acyclic_harness::{
    AgentId, OperationId, Result,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
    tool::{
        Tool, ToolInvocation, ToolProjection, ToolResult,
        files::{self, FileResult, FileResultProjection, ReadFileInput, ReadFileProjection},
        schema::ProjectionMode,
        text::{self, ReadOptions, SearchOptions, TextRange},
        text_files::{
            self, ReadInput, ReadProjection, ReadResult, SearchInput, SearchProjection,
            SearchResult,
        },
    },
};
use serde::Serialize;
use serde_json::{Value, json};

const READ: ReadOptions = ReadOptions {
    maximum_input_bytes: 4096,
    maximum_text_bytes: 32,
};
const SEARCH: SearchOptions = SearchOptions {
    maximum_input_bytes: 4096,
    maximum_query_bytes: 16,
    maximum_work: 4096,
    maximum_matches: 1,
};
const MAXIMUM_RESULT_BYTES: u64 = 4096;

fn value<T: Serialize>(input: T) -> Result<Value> {
    serde_json::to_value(input).map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))
}

fn definition(variant: &str, tool: Tool) -> Result<Value> {
    tool.definition.validate()?;
    let digest = tool.definition.digest()?;
    Ok(json!({"variant":variant,"definition":tool.definition,"definition_digest":digest}))
}

fn invocation(name: &str, arguments: Value) -> ToolInvocation {
    ToolInvocation {
        operation_id: OperationId::from_bytes([52; 16]),
        call_id: "contract-1".into(),
        name: name.into(),
        arguments,
    }
}

fn definitions() -> Result<Vec<Value>> {
    let mut reference_read = files::read_file()?;
    let read_projector = ReadFileProjection(ProjectionMode::Reference);
    reference_read.definition.projection_schema = read_projector.schema()?;
    reference_read.projection = std::sync::Arc::new(read_projector);
    let mut reference_write = files::write_file()?;
    let reference_projector = FileResultProjection(ProjectionMode::Reference);
    reference_write.definition.projection_schema = reference_projector.schema()?;
    reference_write.projection = std::sync::Arc::new(reference_projector);
    Ok(vec![
        definition("exact_read_full", files::read_file()?)?,
        definition("exact_read_reference", reference_read)?,
        definition("write_full", files::write_file()?)?,
        definition("write_reference", reference_write)?,
        definition("exact_edit_full", files::edit_file()?)?,
        definition("patch_edit_full", files::patch_file(4096, 16)?)?,
        definition(
            "range_read_full",
            text_files::read_file_range(READ, MAXIMUM_RESULT_BYTES, ProjectionMode::Full)?,
        )?,
        definition(
            "range_read_reference",
            text_files::read_file_range(READ, MAXIMUM_RESULT_BYTES, ProjectionMode::Reference)?,
        )?,
        definition(
            "literal_search_full",
            text_files::search_file(SEARCH, MAXIMUM_RESULT_BYTES, ProjectionMode::Full)?,
        )?,
        definition(
            "literal_search_reference",
            text_files::search_file(SEARCH, MAXIMUM_RESULT_BYTES, ProjectionMode::Reference)?,
        )?,
    ])
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let source = "one\r\n🦀 abaaba\r\n";
    let file = FileRef::new(
        VolumeRef::new(
            ProviderRef::new("file-contract", "filesystem", "1")?,
            "private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([51; 16])),
        )?,
        "source.txt",
        "generation-1",
        FileDescriptor::from_bytes(source.as_bytes(), "text/plain")?,
        "source.txt",
    )?;
    let read_input = ReadInput {
        file: file.clone(),
        range: TextRange { start: 5, end: 9 },
    };
    let read = ReadResult {
        file: file.clone(),
        selection: text::read_range(source, read_input.range, READ)?,
    };
    let read_invocation = invocation("acyclic.read_file_range", value(&read_input)?);
    let read_result = ToolResult {
        value: value(&read)?,
    };
    let search_input = SearchInput {
        file: file.clone(),
        query: "aba".into(),
    };
    let search = SearchResult {
        file: file.clone(),
        query: search_input.query.clone(),
        matches: text::literal_search(source, &search_input.query, SEARCH)?,
    };
    let search_invocation = invocation("acyclic.search_file", value(&search_input)?);
    let search_result = ToolResult {
        value: value(&search)?,
    };
    let exact_input = ReadFileInput { file: file.clone() };
    let exact_invocation = invocation("acyclic.read_file", value(&exact_input)?);
    let exact_result = ToolResult {
        value: value(source)?,
    };
    // A valid serialized FileResult shape, not a claim that this example published it.
    let file_result = ToolResult {
        value: value(FileResult { file: file.clone() })?,
    };
    let file_invocation = invocation("acyclic.write_file", json!({}));
    let definitions = definitions()?;
    println!(
        "{}",
        json!({
            "tool_definitions":definitions,
            "source":{"file":file,"text":source},
            "range_read":{
                "input":read_input,"canonical":read,
                "full":ReadProjection(ProjectionMode::Full).project(&read_invocation,&read_result)?,
                "reference":ReadProjection(ProjectionMode::Reference).project(&read_invocation,&read_result)?,
            },
            "literal_search":{
                "input":search_input,"canonical":search,
                "full":SearchProjection(ProjectionMode::Full).project(&search_invocation,&search_result)?,
                "reference":SearchProjection(ProjectionMode::Reference).project(&search_invocation,&search_result)?,
            },
            "exact_read":{
                "input":exact_input,"canonical":exact_result.value,
                "full":ReadFileProjection(ProjectionMode::Full).project(&exact_invocation,&exact_result)?,
                "reference":ReadFileProjection(ProjectionMode::Reference).project(&exact_invocation,&exact_result)?,
            },
            "file_result":{
                "canonical":file_result.value,
                "full":FileResultProjection(ProjectionMode::Full).project(&file_invocation,&file_result)?,
                "reference":FileResultProjection(ProjectionMode::Reference).project(&file_invocation,&file_result)?,
            },
        })
    );
    Ok(())
}
