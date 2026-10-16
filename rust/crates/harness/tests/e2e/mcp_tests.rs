use super::{TestResult, drivers, mcp_script::McpScript, scenario};
use proptest::prelude::*;
use serde_json::{Value, json};
use std::path::Path;

fn baseline() -> TestResult<Value> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    Ok(scenario::load_value(&root, &root.join("mcp/text.json"))?)
}

#[tokio::test]
async fn mcp_oracles_require_installation_authority_arguments_and_projected_results() -> TestResult
{
    let baseline = baseline()?;
    drivers::run(baseline.clone()).await?;
    for field in [
        "installation",
        "authority",
        "arguments",
        "result",
        "tool",
        "payload",
        "schema",
        "required",
        "property",
        "additional",
        "completion",
    ] {
        let mut case = baseline.clone();
        match field {
            "installation" => case["mcp"] = Value::Null,
            "authority" => case["mcp"]["grants"] = json!([]),
            "arguments" => case["mcp"]["replies"][0]["arguments"] = json!({"text":"other"}),
            "result" => {
                case["mcp"]["replies"][0]["result"]["content"][0]["text"] = json!("corrupted")
            }
            "tool" => case["mcp"]["catalog"]["tools"][0]["name"] = json!("other"),
            "payload" => {
                case["expect"]["events"][0]["event"]["arguments"] = json!({"text":"corrupted"})
            }
            "schema" => {
                case["mcp"]["catalog"]["tools"][0]["inputSchema"] = json!({"type":"object"})
            }
            "required" => {
                case["expect"]["requests"][0]["tools"][0]["input_schema"]["required"] =
                    json!(["wrong"])
            }
            "property" => {
                case["expect"]["requests"][0]["tools"][0]["input_schema"]["properties"]["text"]["type"] =
                    json!("integer")
            }
            "additional" => {
                case["expect"]["requests"][0]["tools"][0]["input_schema"]["additionalProperties"] =
                    json!(true)
            }
            "completion" => {
                case["expect"]["events"]
                    .as_array_mut()
                    .ok_or("missing expected events")?
                    .last_mut()
                    .ok_or("missing completion")?["step"] = json!(2);
            }
            _ => return Err("unknown MCP mutation".into()),
        }
        assert!(
            drivers::run(case).await.is_err(),
            "accepted missing or corrupted {field}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn mcp_oracles_reject_exposing_unselected_tools_and_mixing_call_results() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let selected = scenario::load_value(&root, &root.join("mcp/selected.json"))?;
    drivers::run(selected.clone()).await?;
    let mut exposed = selected;
    exposed["mcp"]["catalog"]["schema_exposure"] = json!({"kind":"eager"});
    assert!(
        drivers::run(exposed).await.is_err(),
        "accepted the unselected tool"
    );

    let multiple = scenario::load_value(&root, &root.join("mcp/multiple-calls.json"))?;
    drivers::run(multiple.clone()).await?;
    let mut arguments = multiple.clone();
    arguments["turns"][0][1]["arguments"] = arguments["turns"][0][0]["arguments"].clone();
    assert!(
        drivers::run(arguments).await.is_err(),
        "accepted reused call arguments"
    );
    let mut results = multiple;
    results["mcp"]["replies"][1]["result"] = results["mcp"]["replies"][0]["result"].clone();
    assert!(
        drivers::run(results).await.is_err(),
        "accepted reused call results"
    );
    Ok(())
}

#[test]
fn mcp_scripts_reject_unknown_fields_and_wrong_types() -> TestResult {
    let baseline = baseline()?["mcp"].clone();
    for (path, value) in [
        (vec!["typo"], json!(true)),
        (vec!["catalog", "typo"], json!(true)),
        (vec!["catalog", "schema_exposure", "kind"], json!("unknown")),
        (vec!["grants"], json!("all")),
        (vec!["replies"], json!({})),
    ] {
        let mut input = baseline.clone();
        let mut slot = &mut input;
        for component in path {
            slot = &mut slot[component];
        }
        *slot = value;
        assert!(serde_json::from_value::<McpScript>(input).is_err());
    }
    let mut input = baseline;
    input["replies"][0]["typo"] = json!(true);
    assert!(serde_json::from_value::<McpScript>(input).is_err());
    Ok(())
}

fn json_values() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|number| json!(number)),
        proptest::collection::vec(any::<char>(), 0..24)
            .prop_map(|chars| Value::String(chars.into_iter().collect())),
    ]
    .prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            proptest::collection::btree_map("[a-z]{0,6}", inner, 0..4)
                .prop_map(|entries| Value::Object(entries.into_iter().collect())),
        ]
    })
}

// The case codec preserves every generated JSON argument/result, including
// nested values, integer boundaries, Unicode and empty containers.
#[test]
fn mcp_scripts_round_trip_without_changing_arguments_or_results() -> TestResult {
    let baseline = baseline()?["mcp"].clone();
    proptest::test_runner::TestRunner::default()
        .run(&(json_values(), json_values()), |(arguments, result)| {
            let mut value = baseline.clone();
            value["replies"][0]["arguments"] = arguments;
            value["replies"][0]["result"] = result;
            let script: McpScript = serde_json::from_value(value.clone()).unwrap();
            let bytes = serde_json::to_vec(&script).unwrap();
            let decoded: McpScript = serde_json::from_slice(&bytes).unwrap();
            prop_assert_eq!(serde_json::to_value(decoded).unwrap(), value);
            Ok(())
        })
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[test]
fn mcp_fuzz_corpus_replays_the_case_codec() -> TestResult {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus/mcp_scenario");
    let mut accepted = 0;
    let mut rejected = 0;
    for entry in std::fs::read_dir(root)? {
        let bytes = std::fs::read(entry?.path())?;
        match serde_json::from_slice::<McpScript>(&bytes) {
            Ok(script) => {
                let encoded = serde_json::to_vec(&script)?;
                let decoded: McpScript = serde_json::from_slice(&encoded)?;
                assert_eq!(
                    serde_json::to_value(script)?,
                    serde_json::to_value(decoded)?
                );
                accepted += 1;
            }
            Err(_) => rejected += 1,
        }
    }
    assert!(
        accepted > 0 && rejected > 0,
        "missing valid or invalid corpus seeds"
    );
    Ok(())
}
