use super::*;
use crate::{Event, Span, Tag, TagEnd};
use proptest::prelude::*;

fn text(value: &str) -> Spans {
    let mut spans = Spans::default();
    if !value.is_empty() {
        spans.push_content(
            Span {
                text: value.into(),
                ..Span::default()
            },
            false,
        );
        spans.freeze();
    }
    spans
}

fn render(events: &[Event<'_>]) -> Vec<Line> {
    let mut renderer = crate::render::Renderer::inline(Line::default());
    for event in events {
        renderer.event(event);
    }
    let mut lines = std::mem::take(&mut renderer.lines);
    lines.push(renderer.into_inline());
    lines
}

fn sequence(actions: &[u8]) -> (Output, Vec<Event<'static>>) {
    let mut output = Output::default();
    let mut events = Vec::new();
    for action in actions {
        match action {
            0..=3 => {
                let value = *["a", "界", "❤️", " "]
                    .get(usize::from(*action))
                    .expect("text action");
                output.append(Output::text(text(value)));
                events.push(Event::Text(value.into()));
            }
            4 | 5 => {
                output.append(Output::newline(*action == 4));
                events.push(if *action == 4 {
                    Event::Text("\n".into())
                } else {
                    Event::HardBreak
                });
            }
            _ => {
                output = output.masked(
                    Style {
                        italic: true,
                        ..Style::default()
                    },
                    None,
                );
                events.insert(0, Event::Start(Tag::Emphasis));
                events.push(Event::End(TagEnd::Emphasis));
            }
        }
    }
    (output, events)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn row_suffix_preserves_scope_body(
        prefix in prop::collection::vec(0u8..7, 0..32),
        body in prop::collection::vec(0u8..7, 0..32),
        context in prop::collection::vec(0u8..7, 0..32),
    ) {
        let (prefix, _) = sequence(&prefix);
        let (body, events) = sequence(&body);
        let mut joined = prefix.clone();
        joined.append(body);
        let suffix = joined.suffix_after(&prefix).expect("sibling prefix");
        prop_assert_eq!(suffix.lines(), render(&events));
        let (mut context, mut model) = sequence(&context);
        context.append(suffix);
        model.extend(events);
        prop_assert_eq!(context.lines(), render(&model));
    }
}

#[test]
fn suffix_property_detects_lost_conditional_breaks() {
    let (prefix, _) = sequence(&[0, 4, 0]);
    let (body, events) = sequence(&[5, 0]);
    let mut joined = prefix.clone();
    joined.append(body);
    FORGET_BREAK_KIND.set(true);
    let broken = joined.suffix_after(&prefix).expect("sibling prefix");
    FORGET_BREAK_KIND.set(false);
    assert_ne!(broken.lines(), render(&events));
}

fn scope_work(count: usize, scan: bool) -> (usize, usize) {
    let (prefix, _) = sequence(&[0, 4, 0, 4, 0]);
    let mut body = Output::default();
    let before = crate::work::current();
    for _ in 0..count {
        body.append(Output::text(text("x")));
        body.append(Output::newline(true));
    }
    SCAN_SUFFIX.set(scan);
    for _ in 0..count {
        let mut joined = prefix.clone();
        joined.append(body);
        body = joined
            .suffix_after(&prefix)
            .expect("sibling prefix")
            .masked(
                Style {
                    italic: true,
                    ..Style::default()
                },
                None,
            );
    }
    SCAN_SUFFIX.set(false);
    let used = crate::work::current() - before;
    let mut nodes = Default::default();
    let mut texts = Default::default();
    let mut links = Default::default();
    let (payload, metadata) = body.middle.retained(&mut nodes, &mut texts, &mut links);
    assert_eq!(body.lines().len(), count + 1);
    (used, payload + metadata)
}

#[test]
fn nested_row_scopes_share_their_bodies() {
    for count in [64, 128, 256, 1024] {
        let small = scope_work(count, false);
        let large = scope_work(count * 2, false);
        assert!(large.0 <= 2 * small.0 + 4096, "work {small:?} -> {large:?}");
        assert!(
            large.1 <= 2 * small.1 + 4096,
            "storage {small:?} -> {large:?}"
        );
    }
}

#[test]
fn scope_work_detects_scanning_the_body() {
    let small = scope_work(128, true);
    let large = scope_work(256, true);
    assert!(large.0 > 2 * small.0 + 4096);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn shared_row_algebra_matches_event_rendering(actions in prop::collection::vec(0u8..12, 0..48)) {
        let mut output = Output::default();
        let mut events = Vec::new();
        let mut saved = None;
        let mut previous = Output::default();
        let mut lines: crate::line_buffer::Buffer = previous.lines().into_iter().collect();
        for action in actions {
            match action {
                0..=3 => {
                    let value = *["a", "界", "❤️", " "].get(usize::from(action)).expect("text action");
                    output.append(Output::text(text(value)));
                    events.push(Event::Text(value.into()));
                }
                4 | 5 => {
                    output.append(Output::newline(action == 4));
                    events.push(if action == 4 { Event::Text("\n".into()) } else { Event::HardBreak });
                }
                6..=9 => {
                    let (tag, end, style, link) = match action {
                        6 => (Tag::Emphasis, TagEnd::Emphasis, Style { italic: true, ..Style::default() }, None),
                        7 => (Tag::Strong, TagEnd::Strong, Style { bold: true, ..Style::default() }, None),
                        8 => (Tag::Strikethrough, TagEnd::Strikethrough, Style { dim: true, strikethrough: true, ..Style::default() }, None),
                        _ => (Tag::Image { link_type: pulldown_cmark::LinkType::Inline, dest_url: "u".into(), title: "".into(), id: "".into() }, TagEnd::Image,
                            Style { foreground: Some(crate::style::BLUE), underline: true, ..Style::default() }, Some(Arc::from("u"))),
                    };
                    output = output.masked(style, link);
                    events.insert(0, Event::Start(tag));
                    events.push(Event::End(end));
                }
                10 => saved = Some((output.clone(), events.clone())),
                _ => if let Some((snapshot, model)) = &saved { output = snapshot.clone(); events = model.clone(); },
            }
            output.update(&previous, &mut lines, 0);
            prop_assert_eq!(&lines, &render(&events));
            previous = output.clone();
            prop_assert_eq!(output.lines(), render(&events));
            prop_assert_eq!(output.visible(), render(&events).iter().rposition(|line| !line.spans.is_empty()).map_or(0, |index| index + 1));
            if let Some((snapshot, model)) = &saved { prop_assert_eq!(snapshot.lines(), render(model)); }
        }
    }
}

fn measure(count: usize, scan: bool) -> (usize, usize) {
    SCAN_SHARED.set(scan);
    let mut prefix = Rows::default();
    let before = crate::work::current();
    for _ in 0..count {
        prefix = prefix.concat(Rows::line(text("x"), true));
        let old = prefix.clone().concat(Rows::line(text("a"), true));
        let next = prefix.clone().concat(Rows::line(text("b"), true));
        let changes = next.changed(&old);
        if !scan {
            assert_eq!(changes.len(), 1);
        }
    }
    let used = crate::work::current() - before;
    SCAN_SHARED.set(false);
    let (payload, metadata) = prefix.retained(
        &mut Default::default(),
        &mut Default::default(),
        &mut Default::default(),
    );
    (used, payload + metadata)
}

#[test]
fn shared_row_updates_have_linear_work_and_storage() {
    for count in [64, 128, 256, 1024] {
        let small = measure(count, false);
        let large = measure(count * 2, false);
        assert!(large.0 <= 2 * small.0 + 4096, "work {small:?} -> {large:?}");
        assert!(
            large.1 <= 2 * small.1 + 4096,
            "storage {small:?} -> {large:?}"
        );
    }
}

#[test]
fn work_property_detects_scanning_shared_row_prefixes() {
    let small = measure(128, true);
    let large = measure(256, true);
    assert!(large.0 > 2 * small.0 + 4096);
}
