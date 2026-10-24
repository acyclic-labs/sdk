use super::*;
use crate::{executor::compile_json_schema, tool::ToolProjection as _};
use acyclic_stream::MemoryStream;
use serde_json::json;

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn registered_contract_accepts_every_retained_outcome(title in ".{0,40}", snippet in ".{0,60}") {
        // Exercise the same construction and validation as GraphCoder startup.
        let tool = bind::<MemoryStream>(|| Ok(None)).unwrap();
        let canonical = compile_json_schema(&tool.definition.output_schema, "search output", &mut None).unwrap();
        let projected = compile_json_schema(&tool.definition.projection_schema, "search projection", &mut None).unwrap();
        let invocation = ToolInvocation {
            operation_id: crate::OperationId::new(),
            call_id: "search-contract".into(),
            name: "web_search".into(),
            arguments: json!({"query":"fixture"}),
        };
        for outcome in [
            files::SearchOutcome::Succeeded { response: SearchResponse { results: vec![SearchResult {
                url: "https://example.invalid".into(), title: title.clone(), snippet: snippet.clone(),
            }] } },
            files::SearchOutcome::Rejected,
            files::SearchOutcome::Failed,
            files::SearchOutcome::Indeterminate,
        ] {
            let value = serde_json::to_value(outcome).unwrap();
            let result = ToolResult { value: value.clone() };
            let envelope = JsonToolProjection.project(&invocation, &result, None).unwrap().0;
            proptest::prop_assert!(canonical.is_valid(&value));
            proptest::prop_assert!(projected.is_valid(&envelope));
            proptest::prop_assert!(!canonical.is_valid(&envelope));
            proptest::prop_assert!(!projected.is_valid(&value));
        }
        let malformed = json!({"kind":"json","value":{"status":"succeeded","response":{"results":[{"url":1,"title":title,"snippet":snippet}]}}});
        proptest::prop_assert!(!projected.is_valid(&malformed));
    }
}

#[test]
fn nesting_a_root_schema_loses_its_reference_scope_negative_control() {
    let schema =
        crate::tool::json_projection_schema(schema::output::<files::SearchOutcome>().unwrap());
    assert!(compile_json_schema(&schema, "broken search projection", &mut None).is_err());
}
