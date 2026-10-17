use acyclic_harness::{
    OperationId,
    client::{CommandFileRoot, CommandSession, DEFAULT_COMMANDS_JSON, handle_tui_input},
    task_volume::TaskVolume,
    tui::{
        Action, ClientRenderer, InputEffect, PromptInput, Viewport,
        ratatui::{Terminal, backend::TestBackend},
    },
    wire,
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;
struct NoFiles;
impl acyclic_harness::conversation::ContentResidencyVerifier for NoFiles {}

fn paint(
    renderer: &mut ClientRenderer,
    files: &CommandSession<MemoryStream>,
    width: u16,
    height: u16,
) -> String {
    let screen = renderer.screen_for(files).unwrap();
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
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

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    // Property: each catalog/keymap publication updates modal help identically
    // on a live renderer and replay; Escape preserves the original transcript.
    #[test]
    fn help_follows_catalog_and_effective_keymap(sequence in proptest::collection::vec(proptest::bool::ANY, 1..5)) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("help").unwrap()).await.unwrap());
            let mut files = CommandSession::open(volume, OperationId::from_bytes([3;16]).to_string().parse().unwrap(), 65_536, 64).await.unwrap();
            let owner = wire::Authority { kind: wire::AggregateKind::Task as i32, id: "task".into() };
            let mut views = super::file_views::empty();
            super::file_views::append_text(&mut views,&owner.id,false,"Original transcript");
            let mut live = ClientRenderer::default();
            live.update(&views, &owner);
            for (index, enabled) in sequence.into_iter().enumerate() {
                let mut catalog: serde_json::Value = serde_json::from_slice(DEFAULT_COMMANDS_JSON).unwrap();
                let description = format!("Followed command description {index}");
                catalog[0]["description"] = serde_json::json!(description);
                files.install_catalog(serde_json::to_vec(&catalog).unwrap()).await.unwrap();
                files.write_state(CommandFileRoot::Config, "keymap.json", &serde_json::json!({"bindings":{"Ctrl+l":enabled.then_some("cursor.line-start")}})).await.unwrap();
                handle_tui_input(&mut files, &NoFiles, InputEffect::Submit { text: "/help".into(), queued: false, edit: None }).await.unwrap();
                let visible = paint(&mut live, &files, 140, 180);
                proptest::prop_assert!(visible.contains("Shortcuts"));
                proptest::prop_assert!(visible.contains(&description));
                proptest::prop_assert_eq!(visible.contains("Ctrl+l · Line start"), enabled);
                proptest::prop_assert!(!visible.contains("Original transcript"));
                let mut replayed = ClientRenderer::default();
                replayed.update(&views, &owner);
                proptest::prop_assert_eq!(paint(&mut replayed, &files, 140, 180), visible);
                // Tiny resizes keep the prompt/layout valid while the modal is open.
                paint(&mut live, &files, 12, 4);
                handle_tui_input(&mut files, &NoFiles, InputEffect::Host(Action::Escape)).await.unwrap();
                proptest::prop_assert!(paint(&mut live, &files, 80, 20).contains("Original transcript"));
                proptest::prop_assert!(live.screen_for(&files).unwrap().overlay.is_empty());
            }
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn help_pages_have_a_separate_followed_offset_from_the_transcript() {
    use acyclic_harness::client::{scroll_tui, tui_scroll};
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("help-pages").unwrap(),
        )
        .await
        .unwrap(),
    );
    let mut files = CommandSession::open(
        volume,
        OperationId::from_bytes([5; 16])
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
            CommandFileRoot::Session,
            "layout.json",
            &serde_json::json!({"transcript_back":12}),
        )
        .await
        .unwrap();
    handle_tui_input(
        &mut files,
        &NoFiles,
        InputEffect::Submit {
            text: "/help".into(),
            queued: false,
            edit: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(tui_scroll(&files).unwrap(), usize::MAX);
    let mut renderer = ClientRenderer::default();
    let screen = renderer.screen_for(&files).unwrap();
    let input = PromptInput::default();
    let area = acyclic_harness::tui::ratatui::layout::Rect::new(0, 0, 80, 20);
    let maximum = screen.tail_scroll(area, &input);
    let expected: String = screen.overlay[5]
        .spans
        .iter()
        .map(|span| span.content.as_ref())
        .collect();
    scroll_tui(&mut files, false, 5, maximum).await.unwrap();
    assert_eq!(maximum - tui_scroll(&files).unwrap(), 5);
    let screen = renderer.screen_for(&files).unwrap();
    let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
    terminal
        .draw(|frame| {
            screen.render(frame, &input, &mut Viewport::default(), 5, 0);
        })
        .unwrap();
    assert!(
        super::screen_text(&terminal)
            .contains(expected.chars().take(40).collect::<String>().as_str())
    );
    handle_tui_input(&mut files, &NoFiles, InputEffect::Host(Action::Escape))
        .await
        .unwrap();
    assert_eq!(tui_scroll(&files).unwrap(), 12);
    assert!(
        files
            .state(CommandFileRoot::Session, "layout.json")
            .unwrap()
            .unwrap()
            .get("view_back")
            .is_none()
    );
}
