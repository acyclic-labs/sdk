use super::TestResult;
use acyclic_harness::{
    AgentId, OperationId,
    client::parse_task_list,
    context::CompactionPolicy,
    conversation::VolumeClass,
    filesystem::MemoryHarnessStorage,
    model::{Model, ModelEvent},
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
    tool::files,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plans {
    prompt: String,
    writes: Vec<Write>,
    expect: BTreeMap<String, Expected>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Write {
    path: String,
    text: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    markdown: String,
    tasks: Option<Vec<ExpectedTask>>,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct ExpectedTask {
    state: String,
    text: String,
}

impl Plans {
    pub(super) fn parse(value: Value) -> Result<Self, String> {
        let case: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        let written: BTreeSet<_> = case
            .writes
            .iter()
            .map(|write| write.path.as_str())
            .collect();
        let expected: BTreeSet<_> = case.expect.keys().map(String::as_str).collect();
        if written.is_empty()
            || written != expected
            || written.iter().any(|path| !path.starts_with("plans/"))
        {
            return Err("every written plans/ file requires an expectation".into());
        }
        Ok(case)
    }

    pub(super) async fn run(self) -> TestResult {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
        let goal_markdown = b"---\nstatus: blocked\n---\n# Ship it\n";
        let goal = storage
            .stage(
                OperationId::new(),
                "GOAL.md",
                goal_markdown,
                "text/markdown",
                "GOAL.md",
            )
            .await?;
        let mut turns: Vec<_> = self.writes.iter().enumerate().map(|(index, write)| {
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall {
                    call_id: format!("write-{index}"),
                    name: "acyclic.write_file".into(),
                    arguments: json!({"path":write.path,"text":write.text,"media_type":"text/markdown","display_name":write.path.rsplit('/').next().unwrap_or_default()}),
                }),
                ScriptedEvent::Event(ModelEvent::Completed { metadata: Value::Null }),
            ])
        }).collect();
        turns.push(ScriptedTurn::Text("Plans saved".into()));
        let provider = Arc::new(ScriptedModelProvider::new(turns));
        let bundle = storage
            .builder()
            .model(
                Model::new("scripted".into(), "plans".into(), "1".into(), Value::Null)?,
                provider.clone(),
            )
            .grant("model:generate")
            .tool(files::write_file()?)?
            .grant("tool:call:acyclic.write_file")
            .compaction(CompactionPolicy::Disabled)
            .build()?;
        let output = storage
            .run_prompt(&bundle, &self.prompt, Vec::new())
            .await?;
        if output.text != "Plans saved"
            || provider.remaining() != 0
            || provider.requests().len() != self.writes.len() + 1
        {
            return Err("plan writer did not consume its complete model script".into());
        }
        for (path, expected) in self.expect {
            let (file, bytes) = storage
                .read_delegated_path(storage.owner_scope(), "", &path, None)
                .await?;
            if file.volume().class() != VolumeClass::AgentPrivate
                || file.volume() != goal.volume()
                || file.path() != path
                || bytes.as_ref() != expected.markdown.as_bytes()
            {
                return Err(format!("plan source or contents differ: {path}").into());
            }
            let markdown = std::str::from_utf8(&bytes)?;
            let tasks = parse_task_list(markdown).map(|list| {
                list.items()
                    .map(|task| ExpectedTask {
                        state: task.state.marker().into(),
                        text: task.text.into(),
                    })
                    .collect::<Vec<_>>()
            });
            if tasks != expected.tasks {
                return Err(format!("plan task rendering differs: {path}").into());
            }
        }
        let (_, goal_bytes) = storage
            .read_delegated_path(storage.owner_scope(), "", "GOAL.md", None)
            .await?;
        if goal_bytes.as_ref() != goal_markdown {
            return Err("plan writes changed GOAL.md".into());
        }
        Ok(())
    }
}
