use super::*;
use crate::{
    client::{ClientViews, GoalPanel},
    filesystem::FilesystemContentVerifier,
    task_volume::{FileKey, FileQuerySource, Query},
};
use acyclic_fs::performance::BOUNDARY_BUDGET;
use acyclic_harness_tui::ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
use futures::StreamExt as _;

#[tokio::test]
async fn local_goal_publications_render_status_reason_and_retained_count() -> Result<()> {
    let cases: Vec<Case> = serde_json::from_str(include_str!("../goals.json"))
        .map_err(super::super::super::storage)?;
    futures::future::try_join_all(cases.into_iter().map(render_case)).await?;
    Ok(())
}

async fn render_case(case: Case) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted(script(&case));
    let mut config = volume_config(directory.path(), project, provider.clone())?;
    config.goal_continuations = if case.enabled == Some(false) {
        None
    } else {
        Some(GoalContinuationPolicy {
            max_continuations: case.cap.unwrap_or(32),
        })
    };
    let harness = LocalHarness::open(config).await?;
    if let Some(initial) = &case.initial_goal {
        write_goal(&harness, initial).await?;
    }
    let reader = FilesystemContentVerifier::new(
        harness.files.clone(),
        &harness.verifier,
        &harness.signed,
        Limits::default().file_bytes,
        None,
    )?;
    let goal_source = reader.file_query(harness.scratch(), BOUNDARY_BUDGET)?;
    let head_source = reader.file_query(harness.journal_volume(), BOUNDARY_BUDGET)?;
    let goal_query = Query::File(FileKey::Goal);
    let head_query = Query::File(FileKey::RequestHead);
    let goal = goal_source
        .observe_files(goal_query.clone(), None, 1, 65536)
        .await
        .map_err(super::super::super::storage)?;
    let head = head_source
        .observe_files(head_query.clone(), None, 1, 65536)
        .await
        .map_err(super::super::super::storage)?;
    let goal_snapshot = goal.snapshot.unwrap();
    let head_snapshot = head.snapshot.unwrap();
    let goal_id = (goal_snapshot.cursor.volume, goal_snapshot.cursor.query);
    let head_id = (head_snapshot.cursor.volume, head_snapshot.cursor.query);
    assert_ne!(goal_id.0, head_id.0);
    let mut views = ClientViews::default();
    views.query_snapshot(goal_query.clone(), goal_snapshot, 1, 65536)?;
    views.query_snapshot(head_query.clone(), head_snapshot, 1, 65536)?;
    views.bind_goal_sources(goal_id, head_id)?;
    let mut omitted_head = views.clone();
    check_output(&case, harness.run("Start the objective").await)?;
    let goal_final = goal_source
        .observe_files(goal_query, None, 1, 65536)
        .await
        .map_err(super::super::super::storage)?
        .snapshot
        .unwrap()
        .cursor;
    let head_final = head_source
        .observe_files(head_query, None, 1, 65536)
        .await
        .map_err(super::super::super::storage)?
        .snapshot
        .unwrap()
        .cursor;
    let mut changes = futures::stream::select(goal.changes, head.changes);
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        while views.queries[&goal_id].cursor().position != goal_final.position
            || views.queries[&head_id].cursor().position != head_final.position
        {
            let change = changes
                .next()
                .await
                .expect("live follows ended")
                .map_err(super::super::super::storage)?;
            if change.cursor.volume == goal_id.0 {
                omitted_head.query_change(crate::task_volume::Change {
                    cursor: change.cursor.clone(),
                    file: change.file.clone(),
                    files: change.files.clone(),
                    removed: change.removed.clone(),
                })?;
            }
            views.query_change(change)?;
        }
        Ok::<_, Error>(())
    })
    .await
    .expect("publication follows stalled")?;
    assert_eq!(views.queries[&goal_id].cursor().commit, goal_final.commit);
    assert_eq!(views.queries[&head_id].cursor().commit, head_final.commit);
    let view = views.goal_view(goal_id.0, goal_id.1).unwrap();
    assert_eq!(
        view.progress.unwrap().continuations as usize,
        case.tasks - 1,
        "{}",
        case.name
    );
    assert_eq!(rows(view), case.panel, "{}", case.name);
    // Negative control: ignoring the real head follow leaves its count unknown.
    assert_ne!(
        rows(omitted_head.goal_view(goal_id.0, goal_id.1).unwrap()),
        case.panel,
        "{}",
        case.name
    );
    assert_eq!(provider.requests().len(), case.calls, "{}", case.name);
    assert_eq!(provider.remaining(), 0);
    Ok(())
}

fn rows(view: crate::client::GoalQueryView<'_>) -> Vec<String> {
    let area = Rect::new(0, 0, 100, 12);
    let mut buffer = Buffer::empty(area);
    GoalPanel { view }.render(area, &mut buffer);
    let mut rows: Vec<_> = (1..area.height - 1)
        .map(|y| {
            (1..area.width - 1)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
                .trim_end()
                .to_owned()
        })
        .collect();
    while rows.last().is_some_and(String::is_empty) {
        rows.pop();
    }
    rows
}
