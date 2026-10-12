use super::*;

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
            ProviderRef::new("text".into(), "filesystem".into(), "1".into(), &mut None)?,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([12; 16])),
            &mut None,
        )?,
        "text.txt".into(),
        "generation-1".into(),
        FileDescriptor::from_bytes(b"ababa", "text/plain", &mut None)?,
        "text.txt".into(),
        &mut None,
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
    let wire: ToolResultContent = decode(projection, None)?.0;
    let ToolResultContent::Parts { parts } = wire else {
        panic!("reference envelope")
    };
    assert_eq!(parts.len(), 2);
    assert!(matches!(&parts[0], ModelDataPart::Text { text } if !text.is_empty()));
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
            &mut super::super::edit::AlgorithmWork::ordinary(),
        )?,
    };
    let result = ToolResult {
        value: serde_json::to_value(&read).map_err(|error| Error::Invalid(error.to_string()))?,
    };
    for mode in [ProjectionMode::Full, ProjectionMode::Reference] {
        let projector = ReadProjection(mode);
        let projected = projector.project(&input, &result, None)?.0;
        crate::contract::validate_json_schema_value(&mode.schema::<ReadResult>()?, &projected, "read projection", &mut None)?;
        if mode == ProjectionMode::Full {
            assert_eq!(projected, json!({"kind":"json","value":result.value}));
        } else {
            assert_reference(&projected, &file)?;
        }
    }
    let search = SearchResult {
        file: file.clone(),
        query: "a".into(),
        matches: super::super::text::literal_search(
            "ababa",
            "a",
            SEARCH,
            &mut super::super::edit::AlgorithmWork::ordinary(),
        )?,
    };
    assert_eq!(search.matches.total_matches, 3);
    assert_eq!(search.matches.omitted_matches, 1);
    let result = ToolResult {
        value: serde_json::to_value(&search).map_err(|error| Error::Invalid(error.to_string()))?,
    };
    for mode in [ProjectionMode::Full, ProjectionMode::Reference] {
        let projector = SearchProjection(mode);
        let projected = projector.project(&input, &result, None)?.0;
        crate::contract::validate_json_schema_value(&mode.schema::<SearchResult>()?, &projected, "search projection", &mut None)?;
        if mode == ProjectionMode::Full {
            assert_eq!(projected, json!({"kind":"json","value":result.value}));
        } else {
            assert_reference(&projected, &file)?;
        }
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
            crate::contract::compile_json_schema(schema, "typed text tool", &mut None)?;
        }
        assert!(
            crate::contract::validate_json_schema_value(&tool.definition.projection_schema, &json!({"file":file()?}), "bare result", &mut None)
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
