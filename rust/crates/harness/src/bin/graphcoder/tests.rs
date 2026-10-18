//! Executable composition uses the real shared ClientViews and Markdown renderer.
#[path = "../../../tests/support/file_views.rs"]
mod file_views;
use acyclic_harness as harness;
use acyclic_harness::{
    OperationId,
    tui::{
        ClientRenderer, PromptInput, Viewport,
        ratatui::{Terminal, backend::TestBackend},
    },
    wire,
};

fn painted(renderer: &ClientRenderer) -> Terminal<TestBackend> {
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    terminal
        .draw(|frame| {
            renderer.screen().render(
                frame,
                &PromptInput::default(),
                &mut Viewport::default(),
                0,
                1000,
            );
        })
        .unwrap();
    terminal
}

fn picture(renderer: &ClientRenderer) -> String {
    screen_text(&painted(renderer))
}

fn screen_text(terminal: &Terminal<TestBackend>) -> String {
    use acyclic_harness::tui::ratatui::text::Line;
    let buffer = terminal.backend().buffer();
    let mut text = String::new();
    for row in buffer.content.chunks(usize::from(buffer.area.width).max(1)) {
        let mut column = 0;
        while let Some(cell) = row.get(column) {
            text.push_str(cell.symbol());
            column += Line::raw(cell.symbol()).width().max(1);
        }
        text.push('\n');
    }
    text
}

#[test]
fn shared_markdown_is_visible_and_snapshot_reset_removes_old_text() {
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: "task".into(),
    };
    let mut views = file_views::empty();
    let mut renderer = ClientRenderer::default();
    file_views::append_text(
        &mut views,
        &owner.id,
        false,
        "# Heading\n**streamed answer**",
    );
    renderer.update(&views, &owner);
    let visible = picture(&renderer);
    assert!(visible.contains("Heading"));
    assert!(visible.contains("streamed answer"));
    assert!(!visible.contains("**streamed"));
    renderer.update(&file_views::empty(), &owner);
    assert!(!picture(&renderer).contains("streamed answer"));
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // Property: the composed cells after arbitrary Unicode delta boundaries
    // equal the cells obtained by projecting the same complete ClientViews text.
    #[test]
    fn composed_markdown_is_independent_of_chunking(chunks in proptest::collection::vec(".{0,12}", 0..6)) {
        let owner = wire::Authority { kind: wire::AggregateKind::Task as i32, id: "task".into() };
        let mut views = file_views::empty();
        let mut live = ClientRenderer::default();
        live.update(&views, &owner);
        for chunk in &chunks {
            file_views::append_text(&mut views,&owner.id,false,chunk);
            live.update(&views, &owner);
        }
        let mut replayed = ClientRenderer::default();
        replayed.update(&views, &owner);
        let live = painted(&live);
        let replayed = painted(&replayed);
        proptest::prop_assert_eq!(live.backend().buffer(), replayed.backend().buffer());
    }
}

#[test]
fn file_completions_render_in_the_composer_and_enter_only_accepts() {
    use acyclic_harness::tui::{
        Screen,
        key_event::{KeyCode, KeyEvent, KeyModifiers},
    };
    let mut input = PromptInput::default();
    input
        .completion
        .set_file(
            "commands/tools.json",
            Some(br#"[{"name":"read","description":"Read a file","tool":true}]"#),
        )
        .unwrap();
    input.committed_text("/re", 0);
    input.refresh_completions();
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    terminal
        .draw(|frame| {
            Screen::default().render(frame, &input, &mut Viewport::default(), 0, 0);
        })
        .unwrap();
    let visible: String = screen_text(&terminal);
    assert!(visible.contains("Completions"));
    assert!(visible.contains("/read [tool]"));
    assert!(visible.contains("Read a file"));
    assert!(
        input
            .key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), 1, 78)
            .is_empty()
    );
    assert_eq!(input.editor.text(), "/read ");
    input.refresh_completions();
    assert!(!input.completion.active(&input.editor));
    input.committed_text("@rea", 2);
    input
        .completion
        .set_file("workspace/read me.rs", Some(b"source"))
        .unwrap();
    input.refresh_completions();
    assert!(
        input
            .key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE), 3, 78)
            .is_empty()
    );
    assert_eq!(input.editor.text(), "/read @\"read me.rs\" ");
    input.editor.undo();
    assert_eq!(input.editor.text(), "/read @rea");
}

#[test]
fn followed_documents_and_owner_feedback_render_and_reset() {
    use acyclic_harness::task_volume::{FileKey, PlanName};
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: "task".into(),
    };
    let mut views = file_views::empty();
    file_views::document(&mut views, FileKey::Goal, "# Finish rewrite");
    file_views::document(
        &mut views,
        FileKey::Plan(PlanName::new("layout").unwrap()),
        "**Compose widgets**",
    );
    file_views::status(&mut views, &owner.id, "active", Some("owner feedback"));
    let mut renderer = ClientRenderer::default();
    renderer.update(&views, &owner);
    assert!(renderer.is_busy());
    let visible = picture(&renderer);
    for text in [
        "Finish rewrite",
        "Compose widgets",
        "owner feedback",
        "GOAL.md",
        "plans/",
    ] {
        assert!(visible.contains(text), "missing {text}");
    }
    renderer.update(&file_views::empty(), &owner);
    let visible = picture(&renderer);
    for text in ["Finish rewrite", "Compose widgets", "owner feedback"] {
        assert!(!visible.contains(text), "stale {text}");
    }
}

#[tokio::test]
async fn tool_output_deltas_repaint_without_a_new_committed_message() {
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: "task".into(),
    };
    let mut views = file_views::empty();
    file_views::tool(&mut views, &owner.id, "read", "first chunk");
    let volume = std::sync::Arc::new(
        acyclic_harness::task_volume::TaskVolume::open(
            std::sync::Arc::new(acyclic_stream::MemoryStream::default()),
            acyclic_stream::StreamPath::new("tool-details").unwrap(),
        )
        .await
        .unwrap(),
    );
    let mut files = acyclic_harness::client::CommandSession::open(
        volume,
        OperationId::from_bytes([1; 16])
            .to_string()
            .parse()
            .unwrap(),
        65_536,
        64,
    )
    .await
    .unwrap();
    files
        .write_state(
            acyclic_harness::client::CommandFileRoot::Config,
            "settings.json",
            &serde_json::json!({"tool_details":true}),
        )
        .await
        .unwrap();
    let mut renderer = ClientRenderer::default();
    renderer.configure(&views, &owner, &files).unwrap();
    assert!(picture(&renderer).contains("first chunk"));
    file_views::put(
        &mut views,
        acyclic_harness::task_volume::FileKey::parse(".tasks/task/turns/0/tools/call/stdout/1")
            .unwrap(),
        b" second chunk".to_vec(),
        None,
    );
    renderer.update(&views, &owner);
    let visible = picture(&renderer);
    assert!(visible.contains("first chunk second chunk"));
    assert!(!visible.contains("Parts {"));
}

#[test]
fn follow_tail_keeps_new_output_visible_after_the_screen_fills() {
    use acyclic_harness::tui::{Screen, ratatui::text::Line};
    let rows: Vec<_> = (0..100)
        .map(|n| Line::from(format!("message {n}")))
        .collect();
    let screen = Screen {
        transcript: &rows,
        ..Screen::default()
    };
    let input = PromptInput::default();
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    terminal
        .draw(|frame| {
            let scroll = screen.tail_scroll(frame.area(), &input);
            screen.render(frame, &input, &mut Viewport::default(), scroll, 0);
        })
        .unwrap();
    let visible: String = screen_text(&terminal);
    assert!(visible.contains("message 99"));
    assert!(!visible.contains("message 0"));
}

#[test]
fn composed_reverse_search_restores_draft_and_ime_stays_multiline() {
    use acyclic_harness::tui::{
        Editor, PromptHistory, Screen,
        key_event::{KeyCode, KeyEvent, KeyModifiers},
    };
    let mut input = PromptInput::new(Editor::new("draft"));
    let mut history = PromptHistory::default();
    history.push("first 日本語".into());
    history.push("second prompt".into());
    input.replace_history(history);
    input.key(
        KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL),
        0,
        78,
    );
    input.committed_text("日本語", 1);
    assert_eq!(input.editor.text(), "first 日本語");
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    let mut viewport = Viewport::default();
    terminal
        .draw(|frame| {
            Screen::default().render(frame, &input, &mut viewport, 0, 1);
        })
        .unwrap();
    let visible: String = screen_text(&terminal);
    assert!(visible.contains("first 日本語"));
    input.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), 2, 78);
    assert_eq!(input.editor.text(), "draft");
    input.editor.select_all();
    input.committed_text("日本語\n👩‍💻", 3);
    terminal
        .draw(|frame| {
            Screen::default().render(frame, &input, &mut viewport, 0, 3);
        })
        .unwrap();
    let visible: String = screen_text(&terminal);
    assert!(visible.contains("日本語"));
    assert!(visible.contains("👩‍💻"));
    assert!(!visible.contains("[Pasted"));
    assert!(viewport.cursor.is_some());
}

#[path = "tests/diff.rs"]
mod diff;

#[tokio::test]
async fn completion_popup_follows_real_task_volume_publications() {
    use acyclic_harness::{
        task_volume::{FileKey, Query, TaskVolume, Write},
        tui::{Editor, Screen},
        tui_files::CompletionFiles,
    };
    use acyclic_stream::{MemoryStream, StreamPath};
    let volume = TaskVolume::open(
        std::sync::Arc::new(MemoryStream::default()),
        StreamPath::new("composer").unwrap(),
    )
    .await
    .unwrap();
    let mut input = PromptInput::new(Editor::new("@新"));
    let mut files = CompletionFiles::observe(&volume, &mut input, None, 8)
        .await
        .unwrap();
    assert!(!input.completion.active(&input.editor));
    let cut = volume.snapshot(&Query::Inputs, 8).await.unwrap().generation;
    volume
        .append(
            cut,
            &Write::new(
                FileKey::parse("workspace/新しい.rs").unwrap(),
                b"source".to_vec(),
            ),
        )
        .await
        .unwrap();
    let cursor = files.cursor.clone();
    let change = files.read_change().await.unwrap();
    assert_eq!(files.cursor, cursor);
    assert!(!input.completion.active(&input.editor));
    files.apply_change(change, &mut input).unwrap();
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    terminal
        .draw(|frame| {
            Screen::default().render(frame, &input, &mut Viewport::default(), 0, 0);
        })
        .unwrap();
    assert!(screen_text(&terminal).contains("新しい.rs"));
    assert!(input.completion.accept(&mut input.editor, 1));
    assert_eq!(input.editor.text(), "@新しい.rs ");
}

#[tokio::test]
async fn submitted_commands_quit_and_drafts_use_real_query_files() {
    use acyclic_harness::{
        client::{
            CommandFileRoot, CommandResolution, CommandSession, apply_tui_input,
            restore_tui_prompt, save_tui_draft,
        },
        task_volume::{FileKey, Query, TaskFile, TaskVolume},
        tui::{Editor, InputEffect, Screen},
    };
    use acyclic_stream::{MemoryStream, StreamPath};
    struct NoFiles;
    impl acyclic_harness::conversation::ContentResidencyVerifier for NoFiles {}
    let volume = std::sync::Arc::new(
        TaskVolume::open(
            std::sync::Arc::new(MemoryStream::default()),
            StreamPath::new("commands").unwrap(),
        )
        .await
        .unwrap(),
    );
    let identity = OperationId::from_bytes([7; 16]).to_string();
    let mut files = CommandSession::open(volume.clone(), identity.parse().unwrap(), 65_536, 64)
        .await
        .unwrap();
    let before = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
    let submit = |text: &str| InputEffect::Submit {
        text: text.into(),
        queued: false,
        edit: None,
    };
    let error = apply_tui_input(&mut files, &NoFiles, submit("/not-a-command"))
        .await
        .unwrap_err();
    assert!(
        matches!(error, acyclic_harness::Error::NotFound(message) if message == "Unknown command: /not-a-command")
    );
    let error = apply_tui_input(
        &mut files,
        &NoFiles,
        InputEffect::Submit {
            text: "replacement".into(),
            queued: false,
            edit: Some(OperationId::new().to_string()),
        },
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, acyclic_harness::Error::NotFound(message) if message == "editable prompt request")
    );
    assert_eq!(
        volume.snapshot(&Query::Root, 64).await.unwrap().generation,
        before
    );
    apply_tui_input(&mut files, &NoFiles, submit("/diff"))
        .await
        .unwrap();
    assert_eq!(
        files
            .state(CommandFileRoot::Config, "settings.json")
            .unwrap()
            .unwrap()["diff_mode"],
        "unified"
    );
    apply_tui_input(&mut files, &NoFiles, submit("ordinary chat"))
        .await
        .unwrap();
    let request = files.take_request().unwrap();
    assert!(matches!(
        request.key,
        FileKey::Session {
            file: TaskFile::Request(_),
            ..
        }
    ));
    let decoded: CommandResolution = serde_json::from_slice(&request.bytes).unwrap();
    assert!(matches!(decoded, CommandResolution::Prompt { text, .. } if text == "ordinary chat"));
    let input = PromptInput::new(Editor::new("日本語 draft"));
    save_tui_draft(&mut files, &input).await.unwrap();
    let before = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
    save_tui_draft(&mut files, &input).await.unwrap();
    assert_eq!(
        volume.snapshot(&Query::Root, 64).await.unwrap().generation,
        before
    );
    drop(files);
    let mut files = CommandSession::open(volume, identity.parse().unwrap(), 65_536, 64)
        .await
        .unwrap();
    let mut restored = PromptInput::default();
    restore_tui_prompt(&files, &mut restored).unwrap();
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    terminal
        .draw(|frame| {
            Screen::default().render(frame, &restored, &mut Viewport::default(), 0, 0);
        })
        .unwrap();
    assert!(screen_text(&terminal).contains("日本語 draft"));
    apply_tui_input(&mut files, &NoFiles, InputEffect::Quit)
        .await
        .unwrap();
    assert!(files.exit_requested().unwrap());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: malformed history cannot partially replace a valid draft; valid
    // draft publication, replay and repeated restoration preserve normalized text.
    #[test]
    fn prompt_file_restore_is_atomic_and_round_trips(text in ".{0,24}", malformed_history in proptest::bool::ANY) {
        use acyclic_harness::{client::{CommandSession, CommandFileRoot, restore_tui_prompt, save_tui_draft}, tui::Editor, task_volume::TaskVolume};
        use acyclic_stream::{MemoryStream, StreamPath};
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let volume = std::sync::Arc::new(TaskVolume::open(std::sync::Arc::new(MemoryStream::default()), StreamPath::new("prompt").unwrap()).await.unwrap());
            let mut files = CommandSession::open(volume, OperationId::from_bytes([9;16]).to_string().parse().unwrap(), 65_536, 64).await.unwrap();
            files.write_state(CommandFileRoot::Session, "draft.json", &serde_json::json!(text)).await.unwrap();
            files.write_state(CommandFileRoot::Config, "history.json", &if malformed_history { serde_json::json!(false) } else { serde_json::json!(["prior"]) }).await.unwrap();
            let mut input = PromptInput::new(Editor::new("original"));
            let result = restore_tui_prompt(&files, &mut input);
            if malformed_history {
                proptest::prop_assert!(result.is_err());
                proptest::prop_assert_eq!(input.editor.text(), "original");
                proptest::prop_assert!(input.history().entries().is_empty());
            } else {
                proptest::prop_assert!(result.is_ok());
                save_tui_draft(&mut files, &input).await.unwrap();
                let mut replayed = PromptInput::default();
                restore_tui_prompt(&files, &mut replayed).unwrap();
                proptest::prop_assert_eq!(replayed.editor.text(), input.editor.text());
                proptest::prop_assert_eq!(replayed.history().entries(), input.history().entries());
            }
            Ok(())
        })?;
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: every accepted goal/plan layout publication selects only that
    // document panel, without mutating or discarding the other query projection.
    #[test]
    fn followed_command_layout_selects_document_panels(sequence in proptest::collection::vec(proptest::bool::ANY, 1..8)) {
        use acyclic_harness::{client::{CommandResolution, CommandQuery, CommandSession}, task_volume::{FileKey, PlanName, TaskVolume}};
        use acyclic_stream::{MemoryStream, StreamPath};
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let owner = wire::Authority { kind: wire::AggregateKind::Task as i32, id: "task".into() };
            let mut views = file_views::empty();
            file_views::document(&mut views,FileKey::Goal,"Goal contents");
            file_views::document(&mut views,FileKey::Plan(PlanName::new("layout").unwrap()),"Plan contents");
            let mut renderer = ClientRenderer::default();
            renderer.update(&views, &owner);
            let volume = std::sync::Arc::new(TaskVolume::open(std::sync::Arc::new(MemoryStream::default()), StreamPath::new("panels").unwrap()).await.unwrap());
            let mut files = CommandSession::open(volume, OperationId::from_bytes([8;16]).to_string().parse().unwrap(), 65_536, 64).await.unwrap();
            for goal in sequence {
                files.publish_resolution(CommandResolution::Select { query: if goal { CommandQuery::Goal } else { CommandQuery::Plan } }).await.unwrap();
                let screen = renderer.screen_for(&files).unwrap();
                proptest::prop_assert_eq!(!screen.goal.is_empty(), goal);
                proptest::prop_assert_eq!(!screen.plans.is_empty(), !goal);
                let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
                terminal.draw(|frame| { screen.render(frame, &PromptInput::default(), &mut Viewport::default(), 0, 0); }).unwrap();
                let text = screen_text(&terminal);
                proptest::prop_assert_eq!(text.contains("Goal contents"), goal);
                proptest::prop_assert_eq!(text.contains("Plan contents"), !goal);
            }
            proptest::prop_assert!(!renderer.screen().goal.is_empty());
            proptest::prop_assert!(!renderer.screen().plans.is_empty());
            Ok(())
        })?;
    }
}

#[path = "tests/prompt_files.rs"]
mod prompt_files;

#[path = "tests/feedback.rs"]
mod feedback;

#[path = "tests/help.rs"]
mod help;

#[path = "tests/scroll.rs"]
mod scroll;

#[path = "tests/parity.rs"]
mod parity;

#[path = "tests/presentation.rs"]
mod presentation;

#[path = "tests/layout.rs"]
mod layout;

#[path = "tests/completion_files.rs"]
mod completion_files;

#[path = "tests/hostile_text.rs"]
mod hostile_text;

#[path = "tests/terminal_input.rs"]
mod terminal_input;

#[path = "tests/task_files.rs"]
mod task_files;

#[path = "tests/history_bounds.rs"]
mod history_bounds;

#[path = "tests/execution_feedback.rs"]
mod execution_feedback;

#[path = "tests/feedback_overlay.rs"]
mod feedback_overlay;

#[path = "tests/perf.rs"]
mod perf;
