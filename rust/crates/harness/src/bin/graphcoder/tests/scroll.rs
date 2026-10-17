use acyclic_harness::{
    OperationId,
    client::{CommandFileRoot, CommandSession, scroll_tui, tui_scroll},
    task_volume::{Query, TaskVolume},
    tui::{
        Action, Editor, InputEffect, PromptInput, Screen, Viewport,
        key_event::{KeyCode, KeyEvent, KeyModifiers},
        ratatui::{Terminal, backend::TestBackend, layout::Rect, text::Line},
    },
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(24))]
    // Property: page intents preserve the prompt, remain within rendered bounds,
    // paint the selected rows, survive reopen, and repeated zero moves append nothing.
    #[test]
    fn transcript_pages_are_bounded_followed_files(sequence in proptest::collection::vec(proptest::bool::ANY, 1..12), height in 6u16..32) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let volume = Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("scroll").unwrap()).await.unwrap());
            let identity = OperationId::from_bytes([11;16]).to_string();
            let mut files = CommandSession::open(volume.clone(), identity.parse().unwrap(), 65_536, 64).await.unwrap();
            let lines: Vec<_> = (0..60).map(|row| Line::raw(format!("Transcript row {row:03}"))).collect();
            let screen = Screen { transcript: &lines, ..Screen::default() };
            let mut input = PromptInput::new(Editor::new("draft"));
            let area = Rect::new(0, 0, 80, height);
            let page = usize::from(screen.areas(area, &input).transcript.height);
            let maximum = screen.tail_scroll(area, &input);
            let mut expected = 0usize;
            for up in sequence {
                let key = KeyEvent::new(if up { KeyCode::PageUp } else { KeyCode::PageDown }, KeyModifiers::NONE);
                let effects = input.key(key, 1, 78);
                proptest::prop_assert_eq!(effects, vec![InputEffect::Host(if up { Action::TranscriptUp } else { Action::TranscriptDown })]);
                scroll_tui(&mut files, up, page, maximum).await.unwrap();
                expected = if up { expected.saturating_add(page).min(maximum) } else { expected.saturating_sub(page) };
                proptest::prop_assert_eq!(tui_scroll(&files).unwrap(), expected);
                proptest::prop_assert_eq!(input.editor.text(), "draft");
                let mut terminal = Terminal::new(TestBackend::new(80, height)).unwrap();
                terminal.draw(|frame| { screen.render(frame, &input, &mut Viewport::default(), maximum - expected, 0); }).unwrap();
                proptest::prop_assert!(super::screen_text(&terminal).contains(&format!("Transcript row {:03}", maximum - expected)), "selected transcript row is visible");
                let before = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
                scroll_tui(&mut files, up, 0, maximum).await.unwrap();
                proptest::prop_assert_eq!(volume.snapshot(&Query::Root, 64).await.unwrap().generation, before);
                let reopened = CommandSession::open(volume.clone(), identity.parse().unwrap(), 65_536, 64).await.unwrap();
                proptest::prop_assert_eq!(tui_scroll(&reopened).unwrap(), expected);
            }
            // Resize clamps stale offsets before the next movement.
            scroll_tui(&mut files, true, usize::MAX, 2).await.unwrap();
            proptest::prop_assert_eq!(tui_scroll(&files).unwrap(), 2);
            files.write_state(CommandFileRoot::Session, "layout.json", &serde_json::json!({"transcript_back":false})).await.unwrap();
            let before = volume.snapshot(&Query::Root, 64).await.unwrap().generation;
            proptest::prop_assert!(scroll_tui(&mut files, true, page, maximum).await.is_err());
            proptest::prop_assert_eq!(volume.snapshot(&Query::Root, 64).await.unwrap().generation, before);
            Ok(())
        })?;
    }
}
