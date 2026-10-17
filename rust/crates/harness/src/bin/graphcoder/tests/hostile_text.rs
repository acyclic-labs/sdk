use acyclic_harness::{
    OperationId,
    client::TaskFileViews,
    model::ModelEvent,
    tui::{ClientRenderer, ratatui::style::Modifier},
    wire,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Scenario {
    row: String,
    text: String,
    expected: String,
    usage: Option<serde_json::Value>,
    status_expected: Option<String>,
}

#[test]
fn hostile_text_rows_preserve_markdown_style_without_hidden_controls() {
    let scenarios: Vec<Scenario> = serde_json::from_str(include_str!("hostile_text.json")).unwrap();
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: "text".into(),
    };
    for scenario in scenarios {
        let mut views = TaskFileViews::default();
        views
            .model_event(
                &owner,
                OperationId::new(),
                ModelEvent::Content {
                    delta: scenario.text,
                },
            )
            .unwrap();
        if let Some(usage) = scenario.usage {
            views.usage.insert(
                format!("{}:{}", wire::AggregateKind::Task.as_str_name(), owner.id),
                usage,
            );
        }
        let mut renderer = ClientRenderer::default();
        renderer.update(&views, &owner);
        let terminal = super::painted(&renderer);
        let picture = super::screen_text(&terminal);
        if let Some(expected) = scenario.status_expected {
            assert!(picture.contains(&expected), "{}: {picture}", scenario.row);
        }
        assert!(
            picture.contains(&scenario.expected),
            "{}: {picture}",
            scenario.row
        );
        assert!(
            renderer
                .screen()
                .transcript
                .iter()
                .flat_map(|line| &line.spans)
                .any(|span| {
                    span.content.contains(&scenario.expected)
                        && span.style.add_modifier.contains(Modifier::BOLD)
                }),
            "{} lost emphasis",
            scenario.row
        );
        assert!(
            !picture.chars().any(|character| matches!(
                character,
                '\u{1b}' | '\u{202e}' | '\u{2066}' | '\u{2069}'
            ))
        );
    }
}
