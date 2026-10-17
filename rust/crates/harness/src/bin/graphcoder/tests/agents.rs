//! The agent tree is composed exactly as in `view::run`: status files in
//! through the shared ClientViews, selection out through `layout.json`.
use super::*;
use acyclic_harness::{
    TaskId,
    client::{
        CommandFileRoot, CommandSession, PromptFiles, TaskFileClient, file_tui_input,
        task_overview_files,
    },
    task_volume::{Component, FileKey, FileValue, Query, TaskFile, TaskVolume},
    tui::{
        AgentPanel, InputEffect,
        key_event::{KeyCode, KeyEvent, KeyModifiers},
    },
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::StreamExt as _;
use std::sync::Arc;

struct NoFiles;
impl acyclic_harness::conversation::ContentResidencyVerifier for NoFiles {}

fn task(byte: u8) -> TaskId {
    TaskId::from_bytes([byte; 16])
}

fn short(byte: u8) -> String {
    task(byte).to_string()[..8].to_owned()
}

async fn status(
    volume: &TaskVolume<MemoryStream>,
    id: TaskId,
    parent: Option<TaskId>,
    state: &str,
    attention: bool,
) {
    let value = FileValue::new(
        FileKey::Task {
            task: Component::new(id.to_string()).unwrap(),
            file: TaskFile::Status,
        },
        serde_json::to_vec(&serde_json::json!({
            "task": id, "parent": parent, "state": state, "attention": attention, "owner_agent": null
        }))
        .unwrap(),
    );
    volume
        .append(volume.head().await.unwrap(), &value)
        .await
        .unwrap();
}

fn paint(
    renderer: &mut ClientRenderer,
    agents: &AgentPanel,
    files: &CommandSession<MemoryStream>,
    input: &PromptInput,
) -> String {
    let screen = renderer.screen_for(files).unwrap();
    let mut terminal = Terminal::new(TestBackend::new(90, 16)).unwrap();
    terminal
        .draw(|frame| {
            let areas = screen.render(frame, input, &mut Viewport::default(), 0, 0);
            agents.render(frame, areas.transcript, &input.theme);
        })
        .unwrap();
    screen_text(&terminal)
}

/// One terminal key through the production prompt, the agent panel and the
/// ordinary file input path, in `view::run` order.
async fn press(
    input: &mut PromptInput,
    agents: &AgentPanel,
    files: &mut CommandSession<MemoryStream>,
    code: KeyCode,
    modifiers: KeyModifiers,
) {
    for effect in input.key(KeyEvent::new(code, modifiers), 1, 80) {
        if let Some(effect) = agents.input(files, effect).await.unwrap() {
            file_tui_input(files, &NoFiles, effect, 10, 0)
                .await
                .unwrap();
        }
    }
}

fn selected(files: &CommandSession<MemoryStream>) -> TaskId {
    files.selected_task().unwrap().unwrap()
}

#[tokio::test]
async fn followed_status_files_render_the_tree_and_keys_select_by_file() {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("agent-tree").unwrap(),
        )
        .await
        .unwrap(),
    );
    status(&volume, task(1), None, "completed", false).await;
    status(&volume, task(2), Some(task(1)), "completed", true).await;
    status(&volume, task(3), Some(task(2)), "pending", false).await;
    status(&volume, task(4), Some(task(2)), "cancelled", false).await;
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task(1).to_string(),
    };
    let mut files = CommandSession::open(
        volume.clone(),
        OperationId::from_bytes([9; 16])
            .to_string()
            .parse()
            .unwrap(),
        65_536,
        128,
    )
    .await
    .unwrap();
    files
        .write_state(
            CommandFileRoot::Config,
            "settings.json",
            &serde_json::json!({"renderer":"ratatui"}),
        )
        .await
        .unwrap();
    files
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &serde_json::json!({"selected_task": task(3)}),
        )
        .await
        .unwrap();
    let mut client = TaskFileClient::observe_task(volume.as_ref(), &owner, task(1), 128, 65_536)
        .await
        .unwrap();
    let mut overviews = task_overview_files(volume.as_ref(), 64, 65_536)
        .await
        .unwrap();
    client
        .apply_update(overviews.next().await.unwrap().unwrap())
        .unwrap();
    let mut input = PromptInput::default();
    PromptFiles::default().refresh(&files, &mut input).unwrap();
    let mut renderer = ClientRenderer::default();
    let mut agents = AgentPanel::default();
    agents.update(client.views(), &files).unwrap();
    assert!(!paint(&mut renderer, &agents, &files, &input).contains("Agents"));

    // Ctrl+T runs the catalog's /tree selection, which writes layout.json.
    press(
        &mut input,
        &agents,
        &mut files,
        KeyCode::Char('t'),
        KeyModifiers::CONTROL,
    )
    .await;
    assert_eq!(
        files
            .state(CommandFileRoot::Session, "layout.json")
            .unwrap()
            .unwrap()["tree"],
        true
    );
    agents.update(client.views(), &files).unwrap();
    let visible = paint(&mut renderer, &agents, &files, &input);
    assert!(visible.contains("Agents"));
    assert!(visible.contains(&format!("○ {} !", short(2))), "{visible}");
    assert!(visible.contains(&format!("├ ● {}", short(3))), "{visible}");
    assert!(
        visible.contains(&format!("└ ■ {} STOPPED", short(4))),
        "{visible}"
    );

    // Structural navigation is a layout.json write; the panel follows it.
    press(
        &mut input,
        &agents,
        &mut files,
        KeyCode::Right,
        KeyModifiers::ALT | KeyModifiers::SHIFT,
    )
    .await;
    assert_eq!(selected(&files), task(4));
    press(
        &mut input,
        &agents,
        &mut files,
        KeyCode::Up,
        KeyModifiers::ALT,
    )
    .await;
    assert_eq!(selected(&files), task(2));
    press(
        &mut input,
        &agents,
        &mut files,
        KeyCode::Up,
        KeyModifiers::ALT,
    )
    .await;
    press(
        &mut input,
        &agents,
        &mut files,
        KeyCode::Up,
        KeyModifiers::ALT,
    )
    .await;
    assert_eq!(selected(&files), task(1), "the root has no parent");
    press(
        &mut input,
        &agents,
        &mut files,
        KeyCode::Down,
        KeyModifiers::ALT,
    )
    .await;
    press(
        &mut input,
        &agents,
        &mut files,
        KeyCode::Down,
        KeyModifiers::ALT,
    )
    .await;
    assert_eq!(selected(&files), task(3));
    press(
        &mut input,
        &agents,
        &mut files,
        KeyCode::Left,
        KeyModifiers::ALT | KeyModifiers::SHIFT,
    )
    .await;
    assert_eq!(selected(&files), task(4));
    assert!(
        files.take_request().is_none(),
        "navigation dispatches no model work"
    );

    // A task switch replaces only the followed tail; status changes still
    // reach the same overview fold and appear without reopening anything.
    client
        .follow_task(volume.as_ref(), &owner, task(4))
        .await
        .unwrap();
    status(&volume, task(5), Some(task(4)), "pending", false).await;
    // Layout writes are cursor-only checkpoints for this query; they are
    // applied in order before the status publication.
    while client
        .views()
        .queries
        .values()
        .find(|view| matches!(view.query(), Query::TaskOverviews))
        .unwrap()
        .files()
        .len()
        < 5
    {
        client
            .apply_update(overviews.next().await.unwrap().unwrap())
            .unwrap();
    }
    agents.update(client.views(), &files).unwrap();
    assert!(paint(&mut renderer, &agents, &files, &input).contains(&format!("  ● {}", short(5))));
    press(
        &mut input,
        &agents,
        &mut files,
        KeyCode::Down,
        KeyModifiers::ALT,
    )
    .await;
    assert_eq!(selected(&files), task(5));

    // The selection is the production target: the next prompt continues it.
    file_tui_input(
        &mut files,
        &NoFiles,
        InputEffect::Submit {
            text: "continue the child".into(),
            queued: false,
            edit: None,
        },
        10,
        0,
    )
    .await
    .unwrap();
    let request = files.take_request().unwrap();
    assert!(
        matches!(&request.key, FileKey::Task { task: target, file: TaskFile::Request(_) } if target.as_str() == task(5).to_string()),
        "{:?}",
        request.key
    );

    // Replaying the same files into a fresh panel paints the same cells.
    agents.update(client.views(), &files).unwrap();
    let mut replay = AgentPanel::default();
    replay.update(client.views(), &files).unwrap();
    let live = paint(&mut renderer, &agents, &files, &input);
    assert_eq!(paint(&mut renderer, &replay, &files, &input), live);
    let snapshot = volume.snapshot(&Query::TaskOverviews, 64).await.unwrap();
    assert_eq!(snapshot.files.len(), 5);

    // /tree is the same file toggle; closing it hides the panel.
    file_tui_input(
        &mut files,
        &NoFiles,
        InputEffect::Submit {
            text: "/tree".into(),
            queued: false,
            edit: None,
        },
        10,
        0,
    )
    .await
    .unwrap();
    agents.update(client.views(), &files).unwrap();
    assert!(!agents.is_open());
    assert!(!paint(&mut renderer, &agents, &files, &input).contains("Agents"));
}
