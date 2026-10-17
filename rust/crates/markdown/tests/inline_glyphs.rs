use super::*;
use crate::inline_scan::Scanner;
use crate::{References, Style};
use proptest::prelude::*;

fn view(scanner: &mut Scanner, source: &str) -> Vec<crate::Line> {
    let events: Vec<_> = pulldown_cmark::Parser::new_ext(source, crate::options())
        .into_offset_iter()
        .collect();
    scanner.frontier(
        crate::inline_scan::Slice {
            original: source,
            parsed: source,
            start: 0,
            base: 0,
            sentinel: 0,
            boundary: 0,
            classified: true,
        },
        &events,
        &References::default(),
    );
    let parsed_nodes = scanner.state.forest.length;
    let mut lines = crate::line_buffer::Buffer::default();
    scanner
        .render_tree(source, 0, &mut lines, 0, None)
        .expect("retained inline events");
    assert_eq!(
        scanner.state.forest.length, parsed_nodes,
        "rendering changed the parse tree"
    );
    while lines.last().is_some_and(|line| line.spans.is_empty()) {
        lines.pop();
    }
    lines.into_iter().collect()
}

fn full(source: &str) -> Vec<crate::Line> {
    let events: Vec<_> = pulldown_cmark::Parser::new_ext(source, crate::options()).collect();
    let mut lines = crate::render::render_styled(&events, Style::default());
    while lines.last().is_some_and(|line| line.spans.is_empty()) {
        lines.pop();
    }
    lines
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn retained_glyph_views_match_event_rendering(
        parts in prop::collection::vec(prop::sample::select(vec![
            "*", "**", "***", "_", "__", "~", "~~", "`", "``", "```",
            "\\*", "\\`", "\\\\", "\\", "!", "x", " ", "¡", "‐", "🦀",
            "[x](u)", "![x](u)", "[a *b*](<u> \"t\")", "![*x* ![y](v)](u)",
            "x![](u)y", "![a ![](u)b](u)",
            "<https://x>", "<a@b.example>", "&amp;", "&#10;", "&#13;", "&#0;",
            "<b>", "</b>", "<!--\n x\n-->", "\0",
        ]), 0..48),
    ) {
        let mut source = String::from("x ");
        let mut scanner = Scanner::default();
        for part in parts {
            source.push_str(part);
            let before = crate::work::rendered_bytes();
            let actual = view(&mut scanner, &source);
            prop_assert!(crate::work::rendered_bytes() - before <= source.len());
            prop_assert_eq!(actual, full(&source), "source {:?}", source);
        }
    }
}

fn measure(count: usize, replay: bool) -> (usize, usize) {
    let mut source = String::from("x ");
    let mut markdown = crate::Markdown::default();
    markdown.push(&source);
    REPLAY.set(replay);
    let before = crate::work::rendered_bytes();
    for part in ["\\", "*", "!", "*", "x", "*", "`", "x", "`"]
        .into_iter()
        .cycle()
        .take(count)
    {
        source.push_str(part);
        markdown.push(part);
    }
    let used = crate::work::rendered_bytes() - before;
    REPLAY.set(false);
    (used, source.len() - 2)
}

#[test]
fn retained_rendering_feeds_each_source_byte_once() {
    for count in [64, 128, 256, 1024] {
        let (used, bytes) = measure(count, false);
        assert_eq!(used, bytes, "{count} chunks");
    }
}

#[test]
fn rendering_counter_detects_replaying_open_blocks() {
    let small = measure(128, true);
    let large = measure(256, true);
    assert!(small.0 > small.1);
    assert!(large.0 > 2 * small.0 + 4096);
}

#[test]
fn rendering_feed_covers_links_entities_html_and_soft_breaks() {
    for count in [64, 128, 256, 1024] {
        let mut markdown = crate::Markdown::default();
        markdown.push("x ");
        let before = crate::work::rendered_bytes();
        let mut bytes = 0;
        for part in [
            "`code`x",
            "**bold**x",
            "~~gone~~x",
            "[x](u)x",
            "![x](u)x",
            "&amp;x",
            "&#10;x",
            "<b>x",
            "</b>x",
            "<https://x>x",
            "\\*x",
            "!x",
            "word\n",
        ]
        .into_iter()
        .cycle()
        .take(count)
        {
            bytes += part.len();
            markdown.push(part);
        }
        assert_eq!(
            crate::work::rendered_bytes() - before,
            bytes,
            "{count} chunks"
        );
    }
}

fn invisible_tail(count: usize, scan: bool) -> usize {
    super::events::RESCAN_VISIBLE.set(scan);
    let mut markdown = crate::Markdown::default();
    markdown.push("x ");
    let before = crate::work::visibility_current() + markdown.visible_work.get();
    for _ in 0..count {
        markdown.push("&#10;");
    }
    let used = crate::work::visibility_current() + markdown.visible_work.get() - before;
    super::events::RESCAN_VISIBLE.set(false);
    used
}

#[test]
fn row_visibility_does_not_rescan_invisible_tail() {
    for count in [64, 128, 256, 1024] {
        assert_eq!(invisible_tail(count, false), 0);
    }
}

#[test]
fn visibility_property_detects_scanning_invisible_rows() {
    let small = invisible_tail(128, true);
    let large = invisible_tail(256, true);
    assert!(large > 2 * small + 4096);
}

#[test]
fn code_and_paragraph_boundaries_match_event_rendering() {
    for body in [
        " a ",
        "   ",
        "\r\na\n",
        "a\r\nb",
        "\n \r\n",
        "\0x",
        "界\n🦀",
    ] {
        let source = format!("x `{body}` z");
        let mut one_shot = crate::Markdown::default();
        one_shot.push(&source);
        assert_eq!(
            one_shot.lines(),
            crate::tests::full_parse(&source),
            "one-shot body {body:?}"
        );
        let mut markdown = crate::Markdown::default();
        let mut consumer = Vec::new();
        for (at, character) in source.char_indices() {
            let changed = markdown.push(
                source
                    .get(at..at + character.len_utf8())
                    .expect("character boundaries"),
            );
            consumer.truncate(changed);
            consumer.extend(
                (markdown.lines().slice(changed..).unwrap_or_default())
                    .iter()
                    .cloned(),
            );
            assert_eq!(
                consumer,
                crate::tests::full_parse(
                    source
                        .get(..at + character.len_utf8())
                        .expect("prefix boundary")
                ),
                "body {body:?}, prefix {at}"
            );
        }
    }
}

#[test]
fn retained_prefix_preserves_deferred_row_indentation() {
    for source in ["¡\n\t&", "x\n\t&", "¡\n\t!", "hello world\tfoo*x*"] {
        let mut markdown = crate::Markdown::default();
        for (at, character) in source.char_indices() {
            markdown.push(
                source
                    .get(at..at + character.len_utf8())
                    .expect("character boundaries"),
            );
            assert_eq!(
                markdown.lines(),
                crate::tests::full_parse(
                    source
                        .get(..at + character.len_utf8())
                        .expect("prefix boundary")
                ),
                "source {source:?}, prefix {at}"
            );
        }
    }
}
