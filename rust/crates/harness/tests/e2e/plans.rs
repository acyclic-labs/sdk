use super::TestResult;
use acyclic_harness::{
    AgentId, OperationId,
    client::parse_task_list,
    context::CompactionPolicy,
    conversation::VolumeClass,
    filesystem::MemoryHarnessStorage,
    model::{Model, ModelEvent},
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
};
use serde_json::json;
use std::sync::Arc;

include!("../support/schema/plans.rs");

impl Plans {
    pub(super) async fn run(self) -> TestResult {
        self.check(&self.capture(true).await?)
    }

    fn check(&self, actual: &BTreeMap<String, Expected>) -> TestResult {
        if actual != &self.expect {
            return Err("plan files or task rendering differ".into());
        }
        Ok(())
    }

    pub(super) async fn negative_oracles(mut self) -> TestResult {
        let actual = self.capture(false).await?;
        self.check(&actual)?;
        let path = self.expect.keys().next().ok_or("no expectations")?.clone();
        let good = self.expect.clone();
        for field in ["bytes", "tasks", "missing"] {
            self.expect = good.clone();
            match field {
                "bytes" => {
                    self.expect.get_mut(&path).ok_or("missing file")?.markdown =
                        "wrong bytes".into()
                }
                "tasks" => {
                    self.expect.get_mut(&path).ok_or("missing file")?.tasks =
                        Some(vec![ExpectedTask {
                            state: "[ ]".into(),
                            text: "wrong task".into(),
                        }])
                }
                _ => {
                    self.expect.remove(&path);
                }
            }
            assert!(self.check(&actual).is_err(), "accepted wrong {field}");
        }
        Ok(())
    }

    async fn capture(&self, agent: bool) -> TestResult<BTreeMap<String, Expected>> {
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
                ScriptedEvent::Event(ModelEvent::ToolCall(acyclic_harness::model::ToolCall::new(
                    format!("write-{index}"),
                    "write".into(),
                    json!({"path":write.path,"text":write.text,"media_type":"text/markdown","display_name":write.path.rsplit('/').next().unwrap_or_default()}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed { metadata: Value::Null }),
            ])
        }).collect();
        turns.push(ScriptedTurn::Text("Plans saved".into()));
        let provider = Arc::new(ScriptedModelProvider::new(turns));
        if agent {
            let bundle = storage
                .builder()
                .model(
                    Model::new("scripted".into(), "plans".into(), "1".into(), Value::Null)?,
                    provider.clone(),
                )
                .grant("model:generate")
                .grant("tool:call:write")
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
        } else {
            for write in &self.writes {
                storage
                    .stage(
                        OperationId::new(),
                        &write.path,
                        write.text.as_bytes(),
                        "text/markdown",
                        "plan.md",
                    )
                    .await?;
            }
        }
        let mut actual = BTreeMap::new();
        let paths: BTreeSet<_> = self.writes.iter().map(|write| &write.path).collect();
        for path in paths {
            let (file, bytes) = storage
                .read_delegated_path(storage.owner_scope(), "", &path, None)
                .await?;
            if file.volume().class() != VolumeClass::AgentPrivate
                || file.volume() != goal.volume()
                || file.path() != path
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
            actual.insert(
                path.clone(),
                Expected {
                    markdown: markdown.into(),
                    tasks,
                },
            );
        }
        let (_, goal_bytes) = storage
            .read_delegated_path(storage.owner_scope(), "", "GOAL.md", None)
            .await?;
        if goal_bytes.as_ref() != goal_markdown {
            return Err("plan writes changed GOAL.md".into());
        }
        Ok(actual)
    }
}
