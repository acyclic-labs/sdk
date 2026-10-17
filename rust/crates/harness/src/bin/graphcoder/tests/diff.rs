use acyclic_harness::{
    client::FileDiff,
    tui::{
        PromptInput, Screen, Viewport, diff_lines,
        ratatui::{Terminal, backend::TestBackend, layout::Rect},
    },
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Scenario {
    row: String,
    path: String,
    base: Option<String>,
    current: Option<String>,
    expected: Vec<String>,
    absent: Vec<String>,
}

#[test]
fn diff_parity_rows_render_the_bounded_public_model() {
    let scenarios: Vec<Scenario> = serde_json::from_str(include_str!("diff.json")).unwrap();
    for scenario in scenarios {
        let file = FileDiff::between(
            scenario.base.as_deref().map(str::as_bytes),
            scenario.current.as_deref().map(str::as_bytes),
            4096,
            4096,
        )
        .unwrap()
        .unwrap();
        let files = [(scenario.path, file)].into_iter().collect();
        let rows = diff_lines(&files);
        let screen = Screen {
            diff: &rows,
            ..Screen::default()
        };
        let input = PromptInput::default();
        let mut viewport = Viewport::default();
        let mut terminal = Terminal::new(TestBackend::new(120, 24)).unwrap();
        for (width, visible) in [(120, true), (40, false), (120, true)] {
            terminal.backend_mut().resize(width, 24);
            terminal.resize(Rect::new(0, 0, width, 24)).unwrap();
            terminal
                .draw(|frame| {
                    screen.render(frame, &input, &mut viewport, 0, 0);
                })
                .unwrap();
            let picture = super::screen_text(&terminal);
            for text in &scenario.expected {
                assert_eq!(
                    picture.contains(text),
                    visible,
                    "{}: {text}: {picture}",
                    scenario.row
                );
            }
            for text in &scenario.absent {
                assert!(
                    !picture.contains(text),
                    "{}: {text}: {picture}",
                    scenario.row
                );
            }
        }
    }
}
