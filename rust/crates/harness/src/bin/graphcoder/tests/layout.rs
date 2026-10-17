use acyclic_harness::tui::{
    Editor, PromptInput, Screen, Viewport,
    ratatui::{Terminal, backend::TestBackend, layout::Rect, text::Line},
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Scenario {
    row: String,
    width: u16,
    height: u16,
    #[serde(default)]
    terminal: bool,
    #[serde(default)]
    documents: bool,
    #[serde(default)]
    notices: Vec<String>,
    #[serde(default)]
    tail: bool,
    #[serde(default)]
    overlay: Vec<String>,
    expected: Vec<String>,
    absent: Vec<String>,
}

#[test]
fn layout_rows_compose_the_public_widgets_at_each_size() {
    let scenarios: Vec<Scenario> = serde_json::from_str(include_str!("layout.json")).unwrap();
    let transcript: Vec<_> = (0..40)
        .map(|i| Line::from(format!("message {i:02}")))
        .collect();
    let terminal_rows = [Line::from("shell output")];
    let goal = [Line::from("Goal contents")];
    let plans = [Line::from("Plan contents")];
    let diff = [Line::from("+added code")];
    for scenario in scenarios {
        let notices: Vec<_> = scenario
            .notices
            .iter()
            .map(|s| Line::from(s.as_str()))
            .collect();
        let overlay: Vec<_> = scenario
            .overlay
            .iter()
            .map(|text| Line::from(text.as_str()))
            .collect();
        let screen = Screen {
            header: Line::from("● GraphCoder"),
            transcript: &transcript,
            overlay: &overlay,
            overlay_title: "Shortcuts",
            terminal: if scenario.terminal {
                &terminal_rows
            } else {
                &[]
            },
            goal: if scenario.documents { &goal } else { &[] },
            plans: if scenario.documents { &plans } else { &[] },
            diff: if scenario.documents { &diff } else { &[] },
            notifications: &notices,
            status: Line::from("model · tokens"),
            ..Screen::default()
        };
        let input = PromptInput::new(Editor::new("draft"));
        let mut viewport = Viewport::default();
        // Reuse the same prompt and viewport across a resize, as the executable does.
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        for (width, height) in [(100, 30), (scenario.width, scenario.height)] {
            terminal.backend_mut().resize(width, height);
            terminal.resize(Rect::new(0, 0, width, height)).unwrap();
            terminal
                .draw(|frame| {
                    let scroll = if scenario.tail {
                        screen.tail_scroll(frame.area(), &input)
                    } else {
                        0
                    };
                    screen.render(frame, &input, &mut viewport, scroll, 1000);
                })
                .unwrap();
        }
        let picture = super::screen_text(&terminal);
        for text in scenario.expected {
            assert!(
                picture.contains(&text),
                "{} missing {text}: {picture}",
                scenario.row
            );
        }
        for text in scenario.absent {
            assert!(
                !picture.contains(&text),
                "{} unexpectedly shows {text}: {picture}",
                scenario.row
            );
        }
        assert_eq!(input.editor.text(), "draft", "{}", scenario.row);
    }
}
