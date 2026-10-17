use acyclic_harness::{
    OperationId,
    client::{TaskFileViews, CommandFileRoot, CommandSession, handle_tui_input},
    task_volume::{FileKey, Query, TaskFile, TaskVolume},
    tui::{
        ClientRenderer, InputEffect, PromptInput, Viewport,
        ratatui::{Terminal, backend::TestBackend},
    },
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

struct NoFiles;
impl acyclic_harness::conversation::ContentResidencyVerifier for NoFiles {}

async fn volume() -> Arc<TaskVolume<MemoryStream>> {
    Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("feedback").unwrap(),
        )
        .await
        .unwrap(),
    )
}
async fn session(volume: Arc<TaskVolume<MemoryStream>>, id: u8) -> CommandSession<MemoryStream> {
    CommandSession::open(
        volume,
        OperationId::from_bytes([id; 16])
            .to_string()
            .parse()
            .unwrap(),
        65_536,
        64,
    )
    .await
    .unwrap()
}
fn submit(text: &str) -> InputEffect {
    InputEffect::Submit {
        text: text.into(),
        queued: false,
        edit: None,
    }
}
fn painted(renderer: &mut ClientRenderer, files: &CommandSession<MemoryStream>) -> String {
    let screen = renderer.screen_for(files).unwrap();
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    terminal
        .draw(|frame| {
            screen.render(
                frame,
                &PromptInput::default(),
                &mut Viewport::default(),
                0,
                0,
            );
        })
        .unwrap();
    super::screen_text(&terminal)
}

#[tokio::test]
async fn rejected_commands_show_feedback_and_next_valid_action_continues() {
    let volume = volume().await;
    let mut files = session(volume.clone(), 4).await;
    handle_tui_input(&mut files, &NoFiles, submit("/not-a-command"))
        .await
        .unwrap();
    let mut renderer = ClientRenderer::default();
    let owner = acyclic_harness::wire::Authority {
        kind: acyclic_harness::wire::AggregateKind::Task as i32,
        id: "task".into(),
    };
    let mut views = TaskFileViews::default();
    views.tasks.insert(
        format!(
            "{}:{}",
            acyclic_harness::wire::AggregateKind::Task.as_str_name(),
            owner.id
        ),
        acyclic_harness::client::TaskView {
            status: acyclic_harness::core::LifecycleState::Active,
            lifecycle_operation_id: None,
            reason: Some("owner feedback".into()),
            parent: None,
        },
    );
    renderer.update(&views, &owner);
    assert!(painted(&mut renderer, &files).contains("owner feedback"));
    assert!(painted(&mut renderer, &files).contains("Unknown command: /not-a-command"));
    let before = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
    handle_tui_input(&mut files, &NoFiles, submit("/not-a-command"))
        .await
        .unwrap();
    let snapshot = volume.snapshot(&Query::Root, 64).await.unwrap();
    assert_eq!(snapshot.generation, before);
    assert!(snapshot.files.iter().all(|file| !matches!(
        file.key,
        FileKey::Session {
            file: TaskFile::Request(_),
            ..
        } | FileKey::Task {
            file: TaskFile::Request(_),
            ..
        }
    )));
    assert!(files.take_request().is_none());
    handle_tui_input(&mut files, &NoFiles, submit("/diff extra"))
        .await
        .unwrap();
    assert!(painted(&mut renderer, &files).contains("/diff accepts no arguments"));
    handle_tui_input(
        &mut files,
        &NoFiles,
        InputEffect::Submit {
            text: "replacement".into(),
            queued: false,
            edit: Some(OperationId::new().to_string()),
        },
    )
    .await
    .unwrap();
    assert!(painted(&mut renderer, &files).contains("editable prompt request"));
    assert!(files.take_request().is_none());
    handle_tui_input(&mut files, &NoFiles, submit("ordinary chat"))
        .await
        .unwrap();
    assert!(files.take_request().is_some());
    assert!(files.take_request().is_none());
    assert!(!painted(&mut renderer, &files).contains("Unknown command"));
    assert!(painted(&mut renderer, &files).contains("owner feedback"));
    assert_eq!(
        files
            .state(CommandFileRoot::Session, "notifications.json")
            .unwrap(),
        Some(serde_json::json!([]))
    );
    drop(files);
    let files = session(volume, 4).await;
    assert!(
        ClientRenderer::default()
            .screen_for(&files)
            .unwrap()
            .notifications
            .is_empty()
    );
}

#[tokio::test]
async fn unrelated_publication_is_retried_without_ending_the_input_session() {
    let volume = volume().await;
    let mut stale = session(volume.clone(), 4).await;
    let _other = session(volume.clone(), 5).await;
    let before = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
    handle_tui_input(&mut stale, &NoFiles, submit("/diff"))
        .await
        .unwrap();
    assert!(
        volume
            .snapshot(&Query::Root, 64)
            .await
            .unwrap()
            .generation
            .position
            > before.position
    );
    assert_eq!(
        stale
            .state(CommandFileRoot::Config, "settings.json")
            .unwrap()
            .unwrap()["diff_mode"],
        "unified"
    );
    let _later = session(volume.clone(), 6).await;
    handle_tui_input(&mut stale, &NoFiles, InputEffect::Quit)
        .await
        .unwrap();
    assert_eq!(
        stale
            .state(CommandFileRoot::Session, "layout.json")
            .unwrap()
            .unwrap()["exit_requested"],
        true
    );
    assert!(
        stale
            .state(CommandFileRoot::Session, "notifications.json")
            .unwrap()
            .is_none()
    );
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: notification-file round trips retain literal Unicode text while
    // emitting no terminal controls; malformed replacements preserve the cache.
    #[test]
    fn followed_feedback_round_trips_and_rejects_partial_updates(chars in proptest::collection::vec(proptest::char::any(), 0..32)) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let mut files = session(volume().await, 4).await;
            let message: String = chars.into_iter().collect();
            files.write_state(CommandFileRoot::Session, "notifications.json", &serde_json::json!([message])).await.unwrap();
            let mut renderer = ClientRenderer::default();
            let rows: Vec<String> = renderer.screen_for(&files).unwrap().notifications.iter()
                .map(|row| row.spans.iter().map(|span| span.content.as_ref()).collect()).collect();
            proptest::prop_assert_eq!(rows.len(), 1);
            let text = &rows[0];
            proptest::prop_assert_eq!(text, &message.escape_debug().to_string());
            proptest::prop_assert!(!text.chars().any(char::is_control));
            let first = painted(&mut renderer, &files);
            proptest::prop_assert_eq!(painted(&mut ClientRenderer::default(), &files), first);
            files.write_state(CommandFileRoot::Session, "notifications.json", &serde_json::json!(false)).await.unwrap();
            proptest::prop_assert!(renderer.screen_for(&files).is_err());
            let retained: Vec<String> = renderer.screen().notifications.iter()
                .map(|row| row.spans.iter().map(|span| span.content.as_ref()).collect()).collect();
            proptest::prop_assert_eq!(retained, rows);
            files.write_state(CommandFileRoot::Session, "notifications.json", &serde_json::json!([])).await.unwrap();
            proptest::prop_assert!(renderer.screen_for(&files).unwrap().notifications.is_empty());
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn unsupported_views_are_unadvertised_and_cannot_swallow_the_next_prompt() {
    let volume = volume().await;
    let mut files = session(volume, 44).await;
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
            &serde_json::json!({"open_view":"find","view_input":"find"}),
        )
        .await
        .unwrap();
    handle_tui_input(&mut files, &NoFiles, submit("/find"))
        .await
        .unwrap();
    for name in [
        "find", "theme", "feedback", "bug", "report", "tree", "terminal",
    ] {
        assert!(
            !files
                .catalog()
                .commands()
                .any(|spec| spec.name == name || spec.aliases.iter().any(|alias| alias == name)),
            "{name}"
        );
    }
    assert!(files.take_request().is_none());
    assert!(
        files
            .state(CommandFileRoot::Session, "layout.json")
            .unwrap()
            .unwrap()["view_input"]
            .is_null()
    );
    handle_tui_input(&mut files, &NoFiles, submit("visible next prompt"))
        .await
        .unwrap();
    let request = files.take_request().expect("ordinary next submission");
    let resolution: acyclic_harness::client::CommandResolution =
        serde_json::from_slice(&request.bytes).unwrap();
    assert!(
        matches!(resolution, acyclic_harness::client::CommandResolution::Prompt { text, .. } if text == "visible next prompt")
    );
    // A closed view may retain an old form pointer from another frontend.
    files
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &serde_json::json!({"view_input":"find"}),
        )
        .await
        .unwrap();
    handle_tui_input(&mut files, &NoFiles, submit("another visible prompt"))
        .await
        .unwrap();
    let request = files
        .take_request()
        .expect("stale form cannot intercept chat");
    assert!(
        matches!(serde_json::from_slice::<acyclic_harness::client::CommandResolution>(&request.bytes).unwrap(),
        acyclic_harness::client::CommandResolution::Prompt { text, .. } if text == "another visible prompt")
    );
    assert!(
        files
            .state(CommandFileRoot::Session, "search.json")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn quit_merges_the_refreshed_layout_without_losing_a_peer_edit() {
    let volume = volume().await;
    let mut stale = session(volume.clone(), 4).await;
    let mut peer = session(volume, 4).await;
    peer.write_state(
        CommandFileRoot::Session,
        "layout.json",
        &serde_json::json!({"peer":true}),
    )
    .await
    .unwrap();
    handle_tui_input(&mut stale, &NoFiles, InputEffect::Quit)
        .await
        .unwrap();
    let layout = stale
        .state(CommandFileRoot::Session, "layout.json")
        .unwrap()
        .unwrap();
    assert_eq!(layout["peer"], true);
    assert_eq!(layout["exit_requested"], true);
    assert!(
        stale
            .state(CommandFileRoot::Session, "notifications.json")
            .unwrap()
            .is_none()
    );
}
