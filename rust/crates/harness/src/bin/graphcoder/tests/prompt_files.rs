use acyclic_harness::{
    OperationId,
    client::{CommandFileRoot, CommandSession, PromptFiles},
    task_volume::TaskVolume,
    tui::{
        PromptInput,
        key_event::{KeyCode, KeyEvent, KeyModifiers},
    },
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

async fn session() -> CommandSession<MemoryStream> {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("followed-prompt").unwrap(),
        )
        .await
        .unwrap(),
    );
    CommandSession::open(
        volume,
        OperationId::from_bytes([6; 16])
            .to_string()
            .parse()
            .unwrap(),
        65_536,
        64,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn prompt_save_follows_owner_progress_but_preserves_a_peer_draft() {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("prompt-contention").unwrap(),
        )
        .await
        .unwrap(),
    );
    let id = OperationId::from_bytes([9; 16])
        .to_string()
        .parse()
        .unwrap();
    let mut files = CommandSession::open(volume.clone(), id, 65_536, 64)
        .await
        .unwrap();
    let mut peer = CommandSession::open(volume.clone(), id, 65_536, 64)
        .await
        .unwrap();
    let mut projection = PromptFiles::default();
    let mut input = PromptInput::default();
    projection.refresh(&files, &mut input).unwrap();
    input.editor.set_text("local draft", 0);
    peer.write_state(
        CommandFileRoot::Session,
        "notifications.json",
        &serde_json::json!(["owner progress"]),
    )
    .await
    .unwrap();
    projection.save(&mut files, &mut input).await.unwrap();
    assert_eq!(
        files.state(CommandFileRoot::Session, "draft.json").unwrap(),
        Some(serde_json::json!("local draft"))
    );
    while peer
        .state(CommandFileRoot::Session, "draft.json")
        .unwrap()
        .is_none()
    {
        peer.observe_change().await.unwrap();
    }
    peer.write_state(
        CommandFileRoot::Session,
        "draft.json",
        &serde_json::json!("peer draft"),
    )
    .await
    .unwrap();
    input.editor.set_text("unsaved local edit", 0);
    assert!(matches!(
        projection.save(&mut files, &mut input).await,
        Err(acyclic_harness::Error::Conflict(_))
    ));
    let mut restored = PromptInput::default();
    let reopened = CommandSession::open(volume, id, 65_536, 64).await.unwrap();
    PromptFiles::default()
        .refresh(&reopened, &mut restored)
        .unwrap();
    assert_eq!(restored.editor.text(), "peer draft");
}

#[tokio::test]
async fn unchanged_files_preserve_navigation_and_invalid_updates_are_atomic() {
    let mut files = session().await;
    files
        .write_state(
            CommandFileRoot::Config,
            "history.json",
            &serde_json::json!(["first", "second"]),
        )
        .await
        .unwrap();
    files
        .write_state(
            CommandFileRoot::Session,
            "draft.json",
            &serde_json::json!("draft"),
        )
        .await
        .unwrap();
    let mut input = PromptInput::default();
    let mut projection = PromptFiles::default();
    projection.refresh(&files, &mut input).unwrap();
    let generation = input.history().generation();
    let up = KeyEvent::new(KeyCode::Up, KeyModifiers::NONE);
    input.editor.set_text("", 0);
    input.key(up, 1, 78);
    assert_eq!(input.editor.text(), "second");
    for _ in 0..3 {
        projection.refresh(&files, &mut input).unwrap();
        assert_eq!(input.editor.text(), "second");
        assert_eq!(input.history().generation(), generation);
    }
    input.key(up, 2, 78);
    assert_eq!(input.editor.text(), "first");
    files
        .write_state(
            CommandFileRoot::Session,
            "draft.json",
            &serde_json::json!("external draft"),
        )
        .await
        .unwrap();
    files
        .write_state(
            CommandFileRoot::Config,
            "history.json",
            &serde_json::json!(false),
        )
        .await
        .unwrap();
    assert!(projection.refresh(&files, &mut input).is_err());
    assert_eq!(input.editor.text(), "first");
    assert_eq!(input.history().generation(), generation);
    files
        .write_state(
            CommandFileRoot::Config,
            "history.json",
            &serde_json::json!(["external history"]),
        )
        .await
        .unwrap();
    projection.refresh(&files, &mut input).unwrap();
    assert_eq!(input.editor.text(), "external draft");
    assert_eq!(input.history().entries(), &["external history"]);
    let mut terminal = super::Terminal::new(super::TestBackend::new(80, 20)).unwrap();
    terminal
        .draw(|frame| {
            acyclic_harness::tui::Screen::default().render(
                frame,
                &input,
                &mut super::Viewport::default(),
                0,
                0,
            );
        })
        .unwrap();
    assert!(super::screen_text(&terminal).contains("external draft"));
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: saves append only local submissions, retain intervening external
    // history, normalize text, suppress adjacent duplicates and survive reopen.
    #[test]
    fn history_save_preserves_followed_entries(sequence in proptest::collection::vec((".{0,12}", ".{0,12}"), 1..6)) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let mut files = session().await;
            let mut projection = PromptFiles::default();
            let mut input = PromptInput::default();
            projection.refresh(&files, &mut input).unwrap();
            let mut expected = acyclic_harness::tui::PromptHistory::default();
            for (local, remote) in sequence {
                let previous = expected.entries().last().cloned();
                input.editor.set_text(&local, 0);
                let submitted = input.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), 1, 78)
                    .into_iter().find_map(|effect| match effect {
                        acyclic_harness::tui::InputEffect::Submit { text, .. } => Some(text),
                        _ => None,
                    });
                expected.push(remote);
                files.write_state(CommandFileRoot::Config, "history.json", &serde_json::json!(expected.entries())).await.unwrap();
                if let Some(submitted) = submitted.filter(|text| previous.as_ref() != Some(text)) {
                    expected.push(submitted);
                }
                projection.save(&mut files, &mut input).await.unwrap();
                projection.refresh(&files, &mut input).unwrap();
                proptest::prop_assert_eq!(input.history().entries(), expected.entries());
                let before = input.history().generation();
                projection.save(&mut files, &mut input).await.unwrap();
                projection.refresh(&files, &mut input).unwrap();
                proptest::prop_assert_eq!(input.history().generation(), before);
                let mut reopened = PromptInput::default();
                PromptFiles::default().refresh(&files, &mut reopened).unwrap();
                proptest::prop_assert_eq!(reopened.history().entries(), expected.entries());
            }
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn continuation_enter_saves_draft_without_recording_a_submission() {
    let mut files = session().await;
    let mut projection = PromptFiles::default();
    let mut input = PromptInput::default();
    projection.refresh(&files, &mut input).unwrap();
    input.editor.set_text("\\", 0);
    let effects = input.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), 1, 78);
    assert!(effects.is_empty());
    projection.save(&mut files, &mut input).await.unwrap();
    assert!(input.history().entries().is_empty());
    assert!(
        files
            .state(CommandFileRoot::Config, "history.json")
            .unwrap()
            .is_none()
    );
    let mut replayed = PromptInput::default();
    PromptFiles::default()
        .refresh(&files, &mut replayed)
        .unwrap();
    assert_eq!(replayed.editor.text(), input.editor.text());
    assert!(replayed.history().entries().is_empty());
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: every followed override drives dispatch and generated help from
    // the same map; invalid replacement files preserve that map and the prompt.
    #[test]
    fn followed_keymaps_drive_unicode_editing_and_reject_partial_updates(sequence in proptest::collection::vec(0u8..4, 1..6)) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let mut files = session().await;
            let mut projection = PromptFiles::default();
            let mut input = PromptInput::default();
            for (index, action) in sequence.into_iter().enumerate() {
                let target = ["cursor.line-start", "cursor.line-end", "select-all"].get(usize::from(action)).copied();
                let value = serde_json::json!({"bindings":{"Ctrl+l":target}});
                files.write_state(CommandFileRoot::Config, "keymap.json", &value).await.unwrap();
                projection.refresh(&files, &mut input).unwrap();
                let loaded = input.keymap.clone();
                let round_trip = acyclic_harness::tui::InputKeymap::parse(&loaded.encode()).unwrap();
                proptest::prop_assert_eq!(&loaded, &round_trip);
                proptest::prop_assert_eq!(input.keymap.help().contains("Ctrl+l\t"), action != 3);
                input.editor = acyclic_harness::tui::Editor::new("日本語 é 👩‍💻");
                input.key(KeyEvent::new(KeyCode::Char('l'), KeyModifiers::CONTROL), 1, 78);
                input.committed_text("!", 2);
                let expected = match action {
                    0 => "!日本語 é 👩‍💻",
                    1 | 3 => "日本語 é 👩‍💻!",
                    _ => "!",
                };
                proptest::prop_assert_eq!(input.editor.text(), expected);
                let mut terminal = super::Terminal::new(super::TestBackend::new(80, 20)).unwrap();
                terminal.draw(|frame| { acyclic_harness::tui::Screen::default().render(frame, &input, &mut super::Viewport::default(), 0, 0); }).unwrap();
                proptest::prop_assert!(super::screen_text(&terminal).contains(expected));
                let pending = format!("pending draft {index}");
                files.write_state(CommandFileRoot::Session, "draft.json", &serde_json::json!(pending)).await.unwrap();
                files.write_state(CommandFileRoot::Config, "keymap.json", &serde_json::json!({"bindings":{"Ctrl+l":"not-an-action"}})).await.unwrap();
                proptest::prop_assert!(projection.refresh(&files, &mut input).is_err());
                proptest::prop_assert_eq!(input.editor.text(), expected);
                proptest::prop_assert_eq!(&input.keymap, &loaded);
                files.write_state(CommandFileRoot::Config, "keymap.json", &value).await.unwrap();
                projection.refresh(&files, &mut input).unwrap();
                proptest::prop_assert_eq!(input.editor.text(), pending.as_str());
            }
            Ok(())
        })?;
    }
}
