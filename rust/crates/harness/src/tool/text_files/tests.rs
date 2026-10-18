use super::super::files::{FileProjection, FileTool};
use super::super::{ToolExecutor, ToolInvocation, ToolProjection, ToolResult, decode};
use super::*;
use crate::runtime::RuntimeScope;
use crate::{
    Capabilities, OperationId,
    conversation::Limits,
    model::{FileProjectionPolicy, ModelDataPart, ToolResultContent},
};
use serde_json::{Value, json};

fn invocation(arguments: Value) -> ToolInvocation {
    ToolInvocation {
        operation_id: OperationId::from_bytes([13; 16]),
        call_id: "read-1".into(),
        name: "read".into(),
        arguments,
    }
}

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
    let mut work = super::super::edit::allocation(None, 0)?;
    let file = crate::tool::tests::file(b"ababa")?;
    let input = invocation(json!({"path":file.path(),"offset":2}));
    let result = ToolResult {
        value: json!({"file":file,"offset":2,"text":"bab"}),
    };
    for mode in [ProjectionMode::Full, ProjectionMode::Reference] {
        let projector = FileProjection::Read(mode);
        let projected = projector.project(&input, &result, None)?.0;
        crate::executor::validate_json_schema_value(
            &mode.text_schema()?,
            &projected,
            "read projection",
            &mut None,
        )?;
        if mode == ProjectionMode::Full {
            assert_eq!(
                projected,
                json!({"kind":"parts","parts":[{"kind":"text","text":"     2\tbab"}]})
            );
        } else {
            assert_reference(&projected, &file)?;
        }
    }
    // Negative controls: a result that cannot come from its source file, and
    // a zero line number, never reach the model.
    for value in [
        json!({"file":file,"offset":1,"text":"ababab"}),
        json!({"file":file,"offset":0,"text":"bab"}),
    ] {
        let projector = FileProjection::Read(ProjectionMode::Full);
        assert!(
            projector
                .project(&input, &ToolResult { value }, None)
                .is_err()
        );
    }
    let search = SearchResult {
        file: file.clone(),
        query: "a".into(),
        matches: super::super::text::literal_search("ababa", "a", SEARCH, &mut work)?,
    };
    assert_eq!(search.matches.total_matches, 3);
    assert_eq!(search.matches.omitted_matches, 1);
    let result = ToolResult {
        value: serde_json::to_value(&search).map_err(|error| Error::Invalid(error.to_string()))?,
    };
    for mode in [ProjectionMode::Full, ProjectionMode::Reference] {
        let projector = FileProjection::Search(mode);
        let projected = projector.project(&input, &result, None)?.0;
        crate::executor::validate_json_schema_value(
            &mode.schema::<SearchResult>()?,
            &projected,
            "search projection",
            &mut None,
        )?;
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
    let full = FileTool::Read.assemble(ProjectionMode::Full)?;
    let reference = FileTool::Read.assemble(ProjectionMode::Reference)?;
    assert_ne!(full.definition.digest()?, reference.definition.digest()?);
    assert_eq!(
        full.definition.output_schema,
        reference.definition.output_schema
    );
    let search = search_file(SEARCH, 4096, ProjectionMode::Full)?;
    let changed = search_file(
        SearchOptions {
            maximum_matches: 1,
            ..SEARCH
        },
        4096,
        ProjectionMode::Full,
    )?;
    assert_ne!(search.definition.digest()?, changed.definition.digest()?);
    for tool in [
        full,
        reference,
        search_file(SEARCH, 4096, ProjectionMode::Full)?,
        search_file(SEARCH, 4096, ProjectionMode::Reference)?,
    ] {
        assert!(
            crate::executor::validate_json_schema_value(
                &tool.definition.projection_schema,
                &json!({"file":crate::tool::tests::file(b"ababa")?}),
                "bare result",
                &mut None
            )
            .is_err()
        );
    }
    assert!(search_file(SEARCH, 0, ProjectionMode::Full).is_err());
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
    let file = crate::tool::tests::file(b"ababa")?;
    let adapter = FileTool::Read;
    let input = invocation(json!({"path":file.path(),"offset":2,"limit":1}));
    assert!(adapter.authorize(None, None, &input, None).is_err());
    let grants = Capabilities::new([file.read_capability(&mut None)?]);
    let scope = RuntimeScope::new(grants, Limits::default())?;
    // A path read resolves its file later, through the task view.
    assert!(adapter.preflight(&scope, &input, None)?.0.is_none());
    for arguments in [
        json!({"path":file.path(),"offset":0}),
        json!({"path":file.path(),"limit":0}),
        json!({"path":"../escape.txt"}),
        json!({"file":file,"range":{"start":1,"end":4}}),
    ] {
        assert!(
            adapter
                .preflight(&scope, &invocation(arguments), None)
                .is_err()
        );
    }
    let bounded = FileTool::Search {
        options: SEARCH,
        maximum_result_bytes: 1,
    };
    assert!(
        bounded
            .preflight(&scope, &invocation(json!({"file":file,"query":"a"})), None)
            .is_err()
    );
    let search = FileTool::Search {
        options: SEARCH,
        maximum_result_bytes: 4096,
    };
    assert!(
        search
            .preflight(&scope, &invocation(json!({"file":file,"query":""})), None)
            .is_err()
    );
    Ok(())
}
