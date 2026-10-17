use super::feedback::{NoFiles, painted, session, submit, volume};
use acyclic_harness::{
    client::{CommandFileRoot, CommandResolution, handle_tui_input},
    tui::{
        Action, ClientRenderer, InputEffect, PromptInput, Viewport,
        key_event::{KeyCode, KeyEvent, KeyModifiers},
        ratatui::{Terminal, backend::TestBackend},
    },
};

async fn scenario<const BOUND: bool>(
    row: &serde_json::Value,
    report: &str,
    width: u16,
    height: u16,
) {
    let volume = volume().await;
    let mut files = session(volume.clone(), 37).await;
    files
        .write_state(
            CommandFileRoot::Config,
            "settings.json",
            &serde_json::json!({"renderer":"ratatui"}),
        )
        .await
        .unwrap();
    if row["cancel"] == true {
        // Recreate the exact filtered default that #626 persisted, then upgrade.
        handle_tui_input(&mut files, &NoFiles, submit("/shortcuts"))
            .await
            .unwrap();
        let legacy: Vec<_> = files
            .catalog()
            .commands()
            .filter(|spec| spec.name != "feedback")
            .cloned()
            .collect();
        files
            .install_catalog(serde_json::to_vec(&legacy).unwrap())
            .await
            .unwrap();
    }
    let mut renderer = ClientRenderer::default();
    handle_tui_input(
        &mut files,
        &NoFiles,
        submit(row["command"].as_str().unwrap()),
    )
    .await
    .unwrap();
    assert!(
        files.take_request().is_none(),
        "opening feedback must not dispatch chat"
    );
    let mut screen = renderer.screen_for(&files).unwrap();
    if !BOUND {
        screen.feedback = false;
    }
    let mut terminal = Terminal::new(TestBackend::new(80, 16)).unwrap();
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
    assert!(
        super::screen_text(&terminal).contains("Enter saves locally"),
        "feedback must render before intercepting input: {row}"
    );
    let mut reopened = session(volume.clone(), 37).await;
    let mut replay = ClientRenderer::default();
    assert_eq!(
        painted(&mut renderer, &files),
        painted(&mut replay, &reopened)
    );
    let mut small = Terminal::new(TestBackend::new(width, height)).unwrap();
    small
        .draw(|frame| {
            replay.screen_for(&reopened).unwrap().render(
                frame,
                &PromptInput::default(),
                &mut Viewport::default(),
                0,
                0,
            );
        })
        .unwrap();
    if row["cancel"] == true {
        handle_tui_input(&mut reopened, &NoFiles, InputEffect::Host(Action::Escape))
            .await
            .unwrap();
        assert!(
            reopened
                .state(CommandFileRoot::Session, "feedback.json")
                .unwrap()
                .is_none()
        );
    } else {
        let mut input = PromptInput::default();
        input.committed_text(report, 0);
        let effects = input.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), 1, 80);
        assert_eq!(effects.len(), 1);
        for effect in effects {
            handle_tui_input(&mut reopened, &NoFiles, effect)
                .await
                .unwrap();
        }
        assert_eq!(
            reopened
                .state(CommandFileRoot::Session, "feedback.json")
                .unwrap()
                .unwrap()["text"],
            report
        );
    }
    assert!(reopened.take_request().is_none(), "report must stay local");
    assert!(!replay.screen_for(&reopened).unwrap().feedback);
    handle_tui_input(&mut reopened, &NoFiles, submit("next ordinary prompt"))
        .await
        .unwrap();
    let request = reopened
        .take_request()
        .expect("closed feedback must not intercept chat");
    assert!(
        matches!(serde_json::from_slice::<CommandResolution>(&request.bytes).unwrap(), CommandResolution::Prompt {text,..} if text=="next ordinary prompt")
    );
    let restored = session(volume, 37).await;
    assert_eq!(
        restored
            .state(CommandFileRoot::Session, "feedback.json")
            .unwrap(),
        reopened
            .state(CommandFileRoot::Session, "feedback.json")
            .unwrap()
    );
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(12))]
    // Alias routing, save/cancel and replay preserve the report exactly; closing
    // the form always restores ordinary chat. Resize must not affect admission.
    #[test]
    fn feedback_aliases_save_cancel_replay_and_resize(text in "[a-zA-Z0-9 €日本\n]{0,64}", width in 1u16..100, height in 1u16..28) {
        let rows: Vec<serde_json::Value> = serde_json::from_str(include_str!("feedback_overlay.json")).unwrap();
        futures::executor::block_on(async {
            for row in rows { scenario::<true>(&row,&format!("Report: {text}"),width,height).await; }
        });
    }
}

#[test]
#[should_panic(expected = "feedback must render before intercepting input")]
fn missing_feedback_binding_is_detected_negative_control() {
    futures::executor::block_on(scenario::<false>(
        &serde_json::json!({"command":"/feedback","cancel":false}),
        "report",
        80,
        16,
    ));
}

#[test]
fn shipped_catalog_upgrade_shrunk_seed() {
    futures::executor::block_on(scenario::<true>(
        &serde_json::json!({"command":"/feedback","cancel":true}),
        "Report: ",
        7,
        12,
    ));
}

#[test]
fn catalog_upgrade_preserves_user_edits() {
    futures::executor::block_on(async {
        let mut files = session(volume().await, 39).await;
        files
            .write_state(
                CommandFileRoot::Config,
                "settings.json",
                &serde_json::json!({"renderer":"ratatui"}),
            )
            .await
            .unwrap();
        handle_tui_input(&mut files, &NoFiles, submit("/shortcuts"))
            .await
            .unwrap();
        let mut custom: Vec<_> = files
            .catalog()
            .commands()
            .filter(|spec| spec.name != "feedback")
            .cloned()
            .collect();
        custom[0].description = "User preference".into();
        files
            .install_catalog(serde_json::to_vec(&custom).unwrap())
            .await
            .unwrap();
        handle_tui_input(&mut files, &NoFiles, submit("/feedback"))
            .await
            .unwrap();
        assert_eq!(
            files.catalog().commands().cloned().collect::<Vec<_>>(),
            custom
        );
        assert!(files.take_request().is_none());
    });
}

#[test]
fn stale_feedback_forms_cannot_hide_or_intercept_chat() {
    futures::executor::block_on(async {
        for layout in [
            serde_json::json!({"view_input":"feedback"}),
            serde_json::json!({"open_view":"feedback","view_input":null}),
        ] {
            let mut files = session(volume().await, 41).await;
            files
                .write_state(
                    CommandFileRoot::Config,
                    "settings.json",
                    &serde_json::json!({"renderer":"ratatui"}),
                )
                .await
                .unwrap();
            files
                .write_state(CommandFileRoot::Session, "layout.json", &layout)
                .await
                .unwrap();
            assert!(
                !ClientRenderer::default()
                    .screen_for(&files)
                    .unwrap()
                    .feedback
            );
            handle_tui_input(&mut files, &NoFiles, submit("ordinary chat"))
                .await
                .unwrap();
            let request = files.take_request().expect("stale form swallowed chat");
            assert!(
                matches!(serde_json::from_slice::<CommandResolution>(&request.bytes).unwrap(), CommandResolution::Prompt {text,..} if text=="ordinary chat")
            );
            assert!(
                files
                    .state(CommandFileRoot::Session, "feedback.json")
                    .unwrap()
                    .is_none()
            );
        }
    });
}
