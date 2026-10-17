//! Native definitions and serializer fixtures for current file-tool consumers.
#![allow(missing_docs, reason = "contract example binary, not public API")]

use acyclic_harness::{
    AgentId, OperationId, Result,
    conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
    tool::{
        Tool, ToolInvocation, ToolProjection, ToolResult,
        files::{self, FileProjection, FileResult, ReadFileInput, ReadFileResult},
        schema::ProjectionMode,
        text::{self, SearchOptions, TextRange},
        text_files::{self, SearchInput, SearchResult},
    },
};
use serde::Serialize;
use serde_json::{Value, json};

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
    let definition = tool.definition;
    definition.validate(&mut None)?;
    let digest = definition.digest()?;
    Ok(json!({"variant":variant,"definition":definition,"definition_digest":digest}))
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
    let mut reference_read = files::read()?;
    let read_projector = FileProjection::Read(ProjectionMode::Reference);
    reference_read.definition.projection_schema = ProjectionMode::Reference.text_schema()?;
    reference_read.projection = std::sync::Arc::new(read_projector);
    let mut reference_write = files::write()?;
    let reference_projector = FileProjection::Publication(ProjectionMode::Reference);
    reference_write.definition.projection_schema = ProjectionMode::Reference.text_schema()?;
    reference_write.projection = std::sync::Arc::new(reference_projector);
    Ok(vec![
        definition("exact_read_full", files::read()?)?,
        definition("exact_read_reference", reference_read.clone())?,
        definition("write_full", files::write()?)?,
        definition("write_reference", reference_write)?,
        definition("exact_edit_full", files::edit()?)?,
        definition("patch_edit_full", files::patch_file(4096, 16)?)?,
        definition("range_read_full", files::read()?)?,
        definition("range_read_reference", reference_read)?,
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
            ProviderRef::new(
                "file-contract".into(),
                "filesystem".into(),
                "1".into(),
                &mut None,
            )?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([51; 16])),
            &mut None,
        )?,
        "source.txt".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(source.as_bytes(), "text/plain", &mut None)?,
        "source.txt".into(),
        &mut None,
    )?;
    let read_input = ReadFileInput {
        path: "source.txt".into(),
        offset: Some(2),
        limit: Some(1),
    };
    let read = ReadFileResult {
        file: file.clone(),
        offset: 2,
        text: "🦀 abaaba\r\n".into(),
    };
    let read_invocation = invocation("read", value(&read_input)?);
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
        matches: text::SearchMatches {
            matches: vec![TextRange { start: 10, end: 13 }],
            total_matches: 2,
            omitted_matches: 1,
            work: 19,
        },
    };
    let search_invocation = invocation("acyclic.search_file", value(&search_input)?);
    let search_result = ToolResult {
        value: value(&search)?,
    };
    let exact_input = ReadFileInput {
        path: "source.txt".into(),
        offset: None,
        limit: None,
    };
    let exact_invocation = invocation("read", value(&exact_input)?);
    let exact_result = ToolResult {
        value: value(ReadFileResult {
            file: file.clone(),
            offset: 1,
            text: source.into(),
        })?,
    };
    let file_result = ToolResult {
        value: value(FileResult { file: file.clone() })?,
    };
    let file_invocation = invocation("write", json!({}));
    let definitions = definitions()?;
    println!(
        "{}",
        json!({
            "tool_definitions":definitions,
            "source":{"file":file,"text":source},
            "range_read":{
                "input":read_input,"canonical":read,
                "full":FileProjection::Read(ProjectionMode::Full).project(&read_invocation,&read_result,None)?.0,
                "reference":FileProjection::Read(ProjectionMode::Reference).project(&read_invocation,&read_result,None)?.0,
            },
            "literal_search":{
                "input":search_input,"canonical":search,
                "full":FileProjection::Search(ProjectionMode::Full).project(&search_invocation,&search_result,None)?.0,
                "reference":FileProjection::Search(ProjectionMode::Reference).project(&search_invocation,&search_result,None)?.0,
            },
            "exact_read":{
                "input":exact_input,"canonical":exact_result.value,
                "full":FileProjection::Read(ProjectionMode::Full).project(&exact_invocation,&exact_result,None)?.0,
                "reference":FileProjection::Read(ProjectionMode::Reference).project(&exact_invocation,&exact_result,None)?.0,
            },
            "file_result":{
                "canonical":file_result.value,
                "full":FileProjection::Publication(ProjectionMode::Full).project(&file_invocation,&file_result,None)?.0,
                "reference":FileProjection::Publication(ProjectionMode::Reference).project(&file_invocation,&file_result,None)?.0,
            },
        })
    );
    Ok(())
}
