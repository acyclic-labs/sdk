use super::{TestResult, execution};
use acyclic_harness::{
    OperationId,
    executor::{ExecutionEvent, ExecutionJournal},
    model::{ModelContentPart, ToolResultContent},
    search::SearchResponse,
    test_support::{ScriptedModelProvider, ScriptedSearch, ScriptedSearchProvider},
};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Search {
    pub configured: bool,
    pub granted: bool,
    pub steps: Vec<ScriptedSearch>,
    pub response: Option<SearchResponse>,
    pub events: Vec<String>,
}

pub(super) async fn check(
    expected: &Search,
    search: &ScriptedSearchProvider,
    provider: &ScriptedModelProvider,
    journal: &dyn ExecutionJournal,
    operation: OperationId,
) -> TestResult {
    if search.remaining() != 0 {
        return Err("unconsumed search steps".into());
    }
    let requests = provider.requests();
    for request in &requests {
        if request.tools.iter().any(|tool| tool.name == "web_search")
            != (expected.configured && expected.granted)
        {
            return Err("search tool visibility differs from configured capability".into());
        }
    }
    let records = execution::records(journal, operation).await?;
    let completed: Vec<_> = records
        .iter()
        .filter_map(|record| match &record.event {
            ExecutionEvent::ToolCompleted {
                result, projection, ..
            } => Some((result, projection)),
            _ => None,
        })
        .collect();
    let lifecycle: Vec<_> = records
        .iter()
        .filter_map(|record| match record.event {
            ExecutionEvent::ToolStarted { .. } => Some("started"),
            ExecutionEvent::ToolCompleted { .. } => Some("completed"),
            ExecutionEvent::ToolFailed { .. } => Some("failed"),
            _ => None,
        })
        .collect();
    if lifecycle != expected.events {
        return Err(format!(
            "search events: expected {:?}, got {lifecycle:?}",
            expected.events
        )
        .into());
    }
    for record in &records {
        if serde_json::to_string(record)?.contains("scripted search unavailable") {
            return Err("search diagnostic leaked into durable records".into());
        }
    }
    if let Some(response) = &expected.response {
        if completed.len() != 1 {
            return Err("expected exactly one completed search".into());
        }
        let (result, projection) = completed.first().ok_or("missing completed search")?;
        let response = serde_json::to_value(response)?;
        let result: Value = serde_json::from_slice(&journal.load(result, None)?.await?)?;
        if result != serde_json::json!({"value": response}) {
            return Err("retained search result differs from expected response".into());
        }
        let projection: Value = serde_json::from_slice(&journal.load(projection, None)?.await?)?;
        if projection != serde_json::json!({"kind":"json", "value":response}) {
            return Err("retained search projection differs from expected response".into());
        }
        let results: Vec<_> = requests
            .iter()
            .flat_map(|request| &request.messages)
            .flat_map(|message| message.content.parts())
            .filter_map(|part| match part {
                ModelContentPart::ToolResult {
                    name,
                    content: ToolResultContent::Json { value },
                    ..
                } if name == "web_search" => Some(value),
                _ => None,
            })
            .collect();
        if results != vec![&response] {
            return Err("second model turn must see the exact search output".into());
        }
    } else if !completed.is_empty() {
        return Err("unexpected completed search".into());
    }
    Ok(())
}

#[tokio::test]
async fn search_oracles_reject_changed_responses_events_scripts_and_visibility() -> TestResult {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/scenarios");
    let baseline = super::scenario::load_value(&root, &root.join("search/ranked-sources.json"))?;
    super::drivers::run(baseline.clone()).await?;
    for field in [
        "response",
        "events",
        "script",
        "unused",
        "visibility",
        "completion",
    ] {
        let mut case = baseline.clone();
        match field {
            "response" => {
                case["search"]["response"]["results"][0]["url"] = serde_json::json!("wrong")
            }
            "events" => case["search"]["events"] = serde_json::json!(["started"]),
            "script" => case["search"]["steps"][0]["request"]["query"] = serde_json::json!("wrong"),
            "unused" => {
                let extra = case["search"]["steps"][0].clone();
                case["search"]["steps"]
                    .as_array_mut()
                    .ok_or("missing script")?
                    .push(extra);
            }
            "visibility" => case["search"]["configured"] = serde_json::json!(false),
            "completion" => case["search"]["response"] = Value::Null,
            _ => return Err("unknown search mutation".into()),
        }
        if super::drivers::run(case).await.is_ok() {
            return Err(format!("accepted wrong search {field}").into());
        }
    }
    Ok(())
}
