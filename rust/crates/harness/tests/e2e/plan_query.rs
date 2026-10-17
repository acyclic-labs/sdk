use super::TestResult;
use acyclic_harness::{
    AgentId, OperationId,
    client::FileQueryView,
    context::CompactionPolicy,
    filesystem::MemoryHarnessStorage,
    model::{Model, ModelEvent},
    task_volume::{FileQuerySource, Query},
    test_support::{ScriptedEvent, ScriptedModelProvider, ScriptedTurn},
};
use futures::StreamExt as _;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

type CapturedRevision = (BTreeMap<String, String>, Vec<Vec<String>>);

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
        self.check(&self.capture(true).await?)
    }

    fn check(&self, captured: &[CapturedRevision]) -> TestResult {
        if captured.len() != self.writes.len() {
            return Err("missing plan revisions".into());
        }
        for (revision, (files, panels)) in self.writes.iter().zip(captured) {
            if files != &revision.files {
                return Err(format!("wrong file revision after {}", revision.path).into());
            }
            let expected: Vec<_> = revision
                .panel
                .iter()
                .map(|line| line.trim_end().to_owned())
                .collect();
            if panels.is_empty() || panels.iter().any(|rows| rows != &expected) {
                return Err(format!("wrong plan panel: {panels:?}, expected {expected:?}").into());
            }
        }
        Ok(())
    }

    pub(super) async fn negative_oracles(mut self) -> TestResult {
        let captured = self.capture(false).await?;
        self.check(&captured)?;
        for index in 0..self.writes.len() {
            let good = std::mem::replace(
                &mut self.writes[index].files,
                BTreeMap::from([("plans/wrong.md".into(), "wrong revision".into())]),
            );
            assert!(
                self.check(&captured).is_err(),
                "accepted revision {index} files"
            );
            self.writes[index].files = good;
            let good = std::mem::replace(&mut self.writes[index].panel, vec!["wrong panel".into()]);
            assert!(
                self.check(&captured).is_err(),
                "accepted revision {index} panel"
            );
            self.writes[index].panel = good;
        }
        Ok(())
    }

    async fn capture(&self, agent: bool) -> TestResult<Vec<CapturedRevision>> {
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
                ScriptedEvent::Event(ModelEvent::ToolCall(acyclic_harness::model::ToolCall::new(
                    format!("write-{index}"), "write".into(),
                    json!({"path":revision.path,"text":revision.text,"media_type":"text/markdown","display_name":revision.path.rsplit('/').next().unwrap_or_default()}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed { metadata: Value::Null }),
            ]),
            ScriptedTurn::Text("Plans saved".into()),
        ]).collect();
        let provider = Arc::new(ScriptedModelProvider::new(turns));
        let bundle = if agent {
            Some(
                storage
                    .builder()
                    .model(
                        Model::new("scripted".into(), "plans".into(), "1".into(), Value::Null)?,
                        provider.clone(),
                    )
                    .grant("model:generate")
                    .grant("tool:call:write")
                    .compaction(CompactionPolicy::Disabled)
                    .build()?,
            )
        } else {
            None
        };
        let observation = volume.observe_files(Query::Plans, None, 32, 65_536).await?;
        let snapshot = observation.snapshot.ok_or("missing initial snapshot")?;
        let identity = (snapshot.cursor.volume, snapshot.cursor.query);
        let mut changes = observation.changes;
        let mut client = new_views()?;
        client.query_snapshot(Query::Plans, snapshot, 32, 65_536)?;
        let mut captured = Vec::new();
        for revision in &self.writes {
            let mut views = Vec::new();
            if let Some(bundle) = &bundle {
                let output = storage
                    .run_prompt(bundle, "Update the private plan files", Vec::new())
                    .await?;
                if output.text != "Plans saved" {
                    return Err("agent did not settle the plan write".into());
                }
            } else {
                // Oracle mutation tests need observed file/render outputs, not another
                // model turn. Full scenarios above retain the real agent-write path.
                storage
                    .stage(
                        OperationId::new(),
                        &revision.path,
                        revision.text.as_bytes(),
                        "text/markdown",
                        "plan.md",
                    )
                    .await?;
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
            while client.selection(&identity).unwrap().cursor().position < snapshot.cursor.position
            {
                super::stall::waiting("plan selection follow");
                let change = changes.next().await.ok_or("follow ended")??;
                let cursor = change.cursor.clone();
                let modified = client.query_change(change)?;
                let view = client.selection(&identity).unwrap();
                if view.cursor() != &cursor {
                    return Err("client cursor did not advance".into());
                }
                if modified {
                    views.push(view.clone());
                }
            }
            let view = client.selection(&identity).ok_or("file query missing")?;
            views.push(view.clone());

            // A new client reconstructs exactly the accepted file view at each cut.
            let mut reopened = new_views()?;
            if !reopened.selections().next().is_none() {
                return Err("file payloads leaked into recovery metadata".into());
            }
            reopened.query_snapshot(Query::Plans, snapshot, 32, 65_536)?;
            views.push(
                reopened
                    .selection(&identity)
                    .ok_or("snapshot missing")?
                    .clone(),
            );
            let files = views
                .last()
                .ok_or("missing final view")?
                .files()
                .iter()
                .map(|(key, value)| {
                    (
                        key.path(),
                        String::from_utf8_lossy(&value.segments[0]).into_owned(),
                    )
                })
                .collect();
            let panels = views.iter().map(panel_rows).collect();
            captured.push((files, panels));
            client = reopened;
        }
        if agent && provider.remaining() != 0 {
            return Err("unused plan writer script".into());
        }
        Ok(captured)
    }
}

#[cfg(feature = "tui")]
fn panel_rows(view: &FileQueryView) -> Vec<String> {
    use acyclic_harness::client::PlanPanel;
    use acyclic_harness_tui::ratatui::{buffer::Buffer, layout::Rect, text::Span, widgets::Widget};
    let area = Rect::new(0, 0, 80, 20);
    let mut buffer = Buffer::empty(area);
    PlanPanel { files: view }.render(area, &mut buffer);
    let mut lines: Vec<String> = (1..19)
        .map(|y| {
            let mut line = String::new();
            let mut x = 1;
            while x < 79 {
                let symbol = buffer[(x, y)].symbol();
                line.push_str(symbol);
                x += Span::raw(symbol).width().max(1) as u16;
            }
            line.trim_end().to_owned()
        })
        .collect();
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    lines
}

#[cfg(not(feature = "tui"))]
fn panel_rows(view: &FileQueryView) -> Vec<String> {
    view.plan_rows()
        .map(|line| line.trim_end().to_owned())
        .collect()
}

fn new_views() -> acyclic_harness::Result<acyclic_harness::client::ClientViews> {
    use acyclic_harness::client::{ClientViewLimits, ClientViews, QueryViewLimits};
    ClientViews::new(
        acyclic_stream::StreamPath::new("swarm").unwrap(),
        ClientViewLimits {
            agents: 0,
            selected: 0,
            overview_values: 0,
            overview_bytes: 0,
            query: QueryViewLimits {
                files: 0,
                segments: 0,
                bytes: 0,
            },
        },
    )
}
