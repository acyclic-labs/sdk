use super::TestResult;
use acyclic_harness::{
    AgentId, OperationId,
    client::{ClientSession, MemoryClientState},
    context::CompactionPolicy,
    filesystem::MemoryHarnessStorage,
    model::{Model, ModelEvent},
    task_volume::{FileQuerySource, Query},
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
    tool::files,
};
use futures::StreamExt as _;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PlanQuery {
    writes: Vec<Revision>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Revision {
    path: String,
    text: String,
    files: BTreeMap<String, String>,
    panel: Vec<String>,
}

impl PlanQuery {
    pub(super) fn parse(value: Value) -> Result<Self, String> {
        let case: Self = serde_json::from_value(value).map_err(|error| error.to_string())?;
        if case.writes.is_empty()
            || case
                .writes
                .iter()
                .any(|revision| revision.files.is_empty() || revision.panel.is_empty())
        {
            return Err("every plan revision requires file and panel expectations".into());
        }
        Ok(case)
    }

    pub(super) async fn run(self) -> TestResult {
        let storage = MemoryHarnessStorage::new(AgentId::new(), 65_536).await?;
        let goal = storage
            .stage(
                OperationId::new(),
                "GOAL.md",
                b"---\nstatus: blocked\n---\nShip\n",
                "text/markdown",
                "GOAL.md",
            )
            .await?;
        let volume = storage.file_query(acyclic_fs::performance::BOUNDARY_BUDGET)?;
        let turns: Vec<_> = self.writes.iter().enumerate().flat_map(|(index, revision)| [
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall {
                    call_id: format!("write-{index}"), name: "acyclic.write_file".into(),
                    arguments: json!({"path":revision.path,"text":revision.text,"media_type":"text/markdown","display_name":revision.path.rsplit('/').next().unwrap_or_default()}),
                }),
                ScriptedEvent::Event(ModelEvent::Completed { metadata: Value::Null }),
            ]),
            ScriptedTurn::Text("Plans saved".into()),
        ]).collect();
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
        let observation = volume.observe_files(Query::Plans, None, 32, 65_536).await?;
        let snapshot = observation.snapshot.ok_or("missing initial snapshot")?;
        let identity = (snapshot.cursor.volume, snapshot.cursor.query);
        let mut changes = observation.changes;
        let mut client = ClientSession::open(MemoryClientState::default())?;
        client.query_snapshot(Query::Plans, snapshot, 32, 65_536)?;
        for revision in self.writes {
            let output = storage
                .run_prompt(&bundle, "Update the private plan files", Vec::new())
                .await?;
            if output.text != "Plans saved" {
                return Err("agent did not settle the plan write".into());
            }
            let (file, contents) = storage
                .read_delegated_path(storage.owner_scope(), "", &revision.path, None)
                .await?;
            if file.volume() != goal.volume() || contents.as_ref() != revision.text.as_bytes() {
                return Err("agent write escaped the private volume or changed bytes".into());
            }
            let snapshot = volume
                .observe_files(Query::Plans, None, 32, 65_536)
                .await?
                .snapshot
                .ok_or("snapshot missing")?;
            while client.views().queries[&identity].cursor().position < snapshot.cursor.position {
                let change = changes.next().await.ok_or("follow ended")??;
                let cursor = change.cursor.clone();
                let modified = client.query_change(change)?;
                let view = &client.views().queries[&identity];
                if view.cursor() != &cursor {
                    return Err("client cursor did not advance".into());
                }
                if modified {
                    check_panel(view, &revision.panel)?;
                }
            }
            let view = client
                .views()
                .queries
                .get(&identity)
                .ok_or("file query missing")?;
            let files: BTreeMap<_, _> = view
                .files()
                .iter()
                .map(|(key, value)| {
                    (
                        key.path(),
                        String::from_utf8_lossy(&value.bytes).into_owned(),
                    )
                })
                .collect();
            if files != revision.files {
                return Err(format!("wrong file revision after {}", revision.path).into());
            }
            check_panel(view, &revision.panel)?;

            // A new client reconstructs exactly the accepted file view at each cut.
            let mut reopened = ClientSession::open(client.into_store())?;
            if !reopened.views().queries.is_empty() {
                return Err("file payloads leaked into recovery metadata".into());
            }
            reopened.query_snapshot(Query::Plans, snapshot, 32, 65_536)?;
            check_panel(
                reopened
                    .views()
                    .queries
                    .get(&identity)
                    .ok_or("snapshot missing")?,
                &revision.panel,
            )?;
            client = reopened;
        }
        if provider.remaining() != 0 {
            return Err("unused plan writer script".into());
        }
        Ok(())
    }
}

#[cfg(feature = "tui")]
fn check_panel(view: &acyclic_harness::client::FileQueryView, expected: &[String]) -> TestResult {
    use acyclic_harness::client::PlanPanel;
    use acyclic_harness_tui::ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
    let area = Rect::new(0, 0, 80, 20);
    let mut buffer = Buffer::empty(area);
    PlanPanel { files: view }.render(area, &mut buffer);
    let lines: Vec<String> = (1..19)
        .map(|y| {
            (1..79)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
                .trim_end()
                .to_owned()
        })
        .filter(|line| !line.is_empty())
        .collect();
    if lines != expected {
        return Err(format!("wrong plan panel: {lines:?}, expected {expected:?}").into());
    }
    Ok(())
}

#[cfg(not(feature = "tui"))]
fn check_panel(view: &acyclic_harness::client::FileQueryView, expected: &[String]) -> TestResult {
    if view.plan_lines() != expected {
        return Err("wrong plan panel rows".into());
    }
    Ok(())
}
