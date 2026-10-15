use super::TestResult;
use acyclic_harness::{
    client::{ClientSession, MemoryClientState},
    task_volume::{FileKey, Query, TaskVolume, Write},
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::StreamExt as _;
use serde::Deserialize;
use serde_json::Value;
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
        let provider = Arc::new(MemoryStream::default());
        let volume = TaskVolume::open(provider, StreamPath::new("private")?).await?;
        let observation = volume.observe(Query::Plans, None, 32).await?;
        let snapshot = observation.snapshot.ok_or("missing initial snapshot")?;
        let mut generation = snapshot.generation;
        let identity = (snapshot.cursor.volume, snapshot.cursor.query);
        let mut changes = observation.changes;
        let mut client = ClientSession::open(MemoryClientState::default())?;
        client.query_snapshot(Query::Plans, snapshot, 32, 65_536)?;
        for revision in self.writes {
            let write = Write::new(
                FileKey::parse(&revision.path)?,
                revision.text.as_bytes().to_vec(),
            );
            generation = volume.append(generation, &write).await?;
            let change = changes.next().await.ok_or("follow ended")??;
            let cursor = change.cursor.clone();
            client.query_change(change)?;
            let view = client
                .views()
                .queries
                .get(&identity)
                .ok_or("file query missing")?;
            if view.cursor() != &cursor {
                return Err("client cursor did not advance".into());
            }
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
            let snapshot = volume.snapshot(&Query::Plans, 32).await?;
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
