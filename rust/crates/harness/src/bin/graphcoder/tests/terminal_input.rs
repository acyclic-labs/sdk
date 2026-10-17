use acyclic_harness::tui::{
    Editor, InputEffect, PromptInput, Screen, Viewport,
    ratatui::{Terminal, backend::TestBackend, layout::Rect},
    term::Decoder,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Scenario {
    row: String,
    text: String,
    bytes: String,
    expected: String,
    visible: Vec<String>,
    copied: Option<String>,
    submitted: Option<String>,
    #[serde(default)]
    folded: bool,
}

#[test]
fn public_terminal_decoder_composes_with_prompt_and_screen_at_transport_boundaries() {
    let scenarios: Vec<Scenario> =
        serde_json::from_str(include_str!("terminal_input.json")).unwrap();
    for scenario in scenarios {
        for size in [1, 2, 64] {
            let mut decoder = Decoder::default();
            let mut input = PromptInput::new(Editor::new(&scenario.text));
            let mut viewport = Viewport::default();
            let mut effects = Vec::new();
            let area = Rect::new(1, 1, 78, 8);
            for chunk in scenario.bytes.as_bytes().chunks(size) {
                decoder.feed(chunk).unwrap();
                for event in decoder.take_events() {
                    effects.extend(input.event(event, area, &mut viewport, 0));
                }
            }
            decoder.finish_input().unwrap();
            for event in decoder.take_events() {
                effects.extend(input.event(event, area, &mut viewport, 0));
            }
            assert_eq!(
                input.editor.text(),
                scenario.expected,
                "{} split={size}",
                scenario.row
            );
            let mut expected_effects: Vec<_> = scenario
                .copied
                .iter()
                .cloned()
                .map(InputEffect::Copy)
                .collect();
            expected_effects.extend(scenario.submitted.iter().cloned().map(|text| {
                InputEffect::Submit {
                    text,
                    queued: false,
                    edit: None,
                }
            }));
            assert_eq!(effects, expected_effects, "{} split={size}", scenario.row);
            assert_eq!(
                !input.editor.collapsed_pastes().is_empty(),
                scenario.folded,
                "{}",
                scenario.row
            );
            let mut terminal = Terminal::new(TestBackend::new(80, 20)).unwrap();
            terminal
                .draw(|frame| {
                    Screen::default().render(frame, &input, &mut viewport, 0, 10_000);
                })
                .unwrap();
            let picture = super::screen_text(&terminal);
            for text in &scenario.visible {
                assert!(
                    picture.contains(text),
                    "{} split={size}: {picture}",
                    scenario.row
                );
            }
        }
    }
}
