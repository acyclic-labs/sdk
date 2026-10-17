use super::*;
use crate::{
    client::{ClientViewLimits, ClientViews, GoalPanel, QueryViewLimits},
    filesystem::FilesystemContentVerifier,
    task_volume::{FileKey, FileQuerySource, Query},
};
use acyclic_fs::performance::BOUNDARY_BUDGET;
use acyclic_harness_tui::ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
use futures::StreamExt as _;

#[tokio::test]
async fn local_goal_publications_render_status_reason_and_retained_count() -> Result<()> {
    futures::future::try_join_all(cases()?.into_iter().map(|case| render_case(case, false)))
        .await?;
    Ok(())
}

#[tokio::test]
async fn identical_goal_bytes_follow_a_new_publication() -> Result<()> {
    let case = cases()?
        .into_iter()
        .find(|case| case.name == "agent-blocked")
        .unwrap();
    render_case(case, true).await
}

async fn render_case(case: Case, repeat: bool) -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::super::storage)?;
    let project = volume(directory.path()).await?;
    let provider = scripted(script(&case));
    let config = goal_config(&case, directory.path(), project, provider.clone())?;
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
    let mut client = ClientViews::new(
        harness.task_files().inner.path().clone(),
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
    )?;
    let mut changes = client
        .select_goal_view(&goal_source, &head_source, 65536)
        .await?;
    let goal_cursor = client
        .selections()
        .map(|(_, files)| files)
        .find(|view| view.query() == &goal_query)
        .unwrap()
        .cursor();
    let goal_id = (goal_cursor.volume, goal_cursor.query);
    let head_cursor = client
        .selections()
        .map(|(_, files)| files)
        .find(|view| view.query() == &head_query)
        .unwrap()
        .cursor();
    let head_id = (head_cursor.volume, head_cursor.query);
    assert_ne!(goal_id.0, head_id.0);
    let mut omitted_head = client.clone();
    check_output(&case, harness.run("Start the objective").await)?;
    if repeat {
        let before = goal_source
            .observe_files(goal_query.clone(), None, 1, 65536)
            .await?
            .snapshot
            .unwrap()
            .files
            .remove(0);
        write_goal(&harness, case.final_goal.as_deref().unwrap()).await?;
        let after = goal_source
            .observe_files(goal_query.clone(), None, 1, 65536)
            .await?
            .snapshot
            .unwrap()
            .files
            .remove(0);
        assert_eq!(before.bytes, after.bytes);
        assert!(before.publication.is_some() && after.publication.is_some());
        assert_ne!(
            before.publication, after.publication,
            "identical bytes must have distinct write identities"
        );
        let snapshot = head_source
            .observe_files(head_query.clone(), None, 1, 65536)
            .await?
            .snapshot
            .unwrap();
        let turn = crate::goal::request_head::decode(&snapshot.files[0].bytes)
            .map_err(super::super::super::storage)?;
        check_output(
            &case,
            harness
                .resume(crate::runtime::task_id_for_admission(turn.root))
                .await,
        )?;
    }
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
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        while client.selection(&goal_id).unwrap().cursor().position != goal_final.position
            || client.selection(&head_id).unwrap().cursor().position != head_final.position
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
            client.query_change(change)?;
        }
        Ok::<_, Error>(())
    })
    .await
    .expect("publication follows stalled")?;
    assert_eq!(
        client.selection(&goal_id).unwrap().cursor().commit,
        goal_final.commit
    );
    assert_eq!(
        client.selection(&head_id).unwrap().cursor().commit,
        head_final.commit
    );
    let view = client.goal_view(goal_id.0, goal_id.1).unwrap();
    let observed_count = |views: &crate::client::ClientViews| {
        views
            .selection(&head_id)
            .unwrap()
            .files()
            .get(&FileKey::RequestHead)
            .and_then(|file| crate::goal::request_head::decode(&file.segments[0]).ok())
            .map(|turn| turn.automatic_turns)
    };
    assert_eq!(
        observed_count(&client),
        Some((case.tasks - 1) as u32),
        "{}",
        case.name
    );
    assert_eq!(
        view.progress.unwrap().continuations,
        (case.tasks - 1) as u32,
        "caught-up combined count: {}",
        case.name
    );
    assert_eq!(rows(view), case.panel, "{}", case.name);
    // Negative control: ignoring the real head follow loses the retained count,
    // including when the combined blocked rendering correctly keeps it pending.
    assert_ne!(
        (
            rows(omitted_head.goal_view(goal_id.0, goal_id.1).unwrap()),
            observed_count(&omitted_head)
        ),
        (case.panel.clone(), Some((case.tasks - 1) as u32)),
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
