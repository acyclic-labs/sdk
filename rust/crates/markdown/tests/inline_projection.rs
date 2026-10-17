use super::*;
use crate::inline_scan::{state::State, tokens::Tokens};
use proptest::prelude::*;

fn advance(state: &mut State, tokens: &mut Tokens, source: &str) -> usize {
    tokens.push(source);
    while let Some(token) = tokens.ready.pop_front() {
        state.feed(token.range, token.kind, false);
    }
    state.projection(tokens.preview().as_ref())
}

fn project(
    cache: &mut Cache,
    state: &mut State,
    root: usize,
    source: &str,
    lines: &mut crate::line_buffer::Buffer,
) -> usize {
    let end = source.trim_end_matches([' ', '\t', '\x0b', '\x0c']).len();
    let rendering = crate::work::rendering(true);
    let result = cache.forest(&mut state.forest, root, source, 0..end, lines, 0);
    crate::work::rendering(rendering);
    result.expect("retained parse tree").1
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn tree_projection_matches_full_inline_render(
        parts in prop::collection::vec(prop::sample::select(vec![
            "*", "**", "***", "_", "__", "~", "~~", "`", "``", "```",
            "\\*", "\\`", "\\\\", "\\", "!", "x", " ", "\t", "\x0b", "\x0c", "¡", "‐", "🦀",
            "`code`", "**bold**", "***both***", "~~strike~~",
            "[", "]", "&amp;", "&lt;", "&#10;", "&#13;", "&#0;", "\0",
            "\nword", "\n\x0b", "\n\x0c", "\n    word", "\n\\\nword", "hard  \nword",
        ]), 0..64),
    ) {
        let mut state = State::default();
        let mut tokens = Tokens::default();
        let mut cache = Cache::default();
        let mut source = String::from("x ");
        let mut lines = crate::line_buffer::Buffer::default();
        for part in parts {
            for character in part.chars() {
                source.push(character);
                let root = advance(&mut state, &mut tokens, &source);
                let visible = project(&mut cache, &mut state, root, &source, &mut lines);
                prop_assert_eq!(lines.view(..visible).unwrap(), &crate::tests::full_parse(&source), "source {:?}", source);
            }
        }
    }
}

fn measure(count: usize, replay: bool) -> (usize, usize) {
    let mut state = State::default();
    let mut tokens = Tokens::default();
    let mut cache = Cache::default();
    let mut source = String::from("x ");
    let mut lines = crate::line_buffer::Buffer::default();
    let root = advance(&mut state, &mut tokens, &source);
    project(&mut cache, &mut state, root, &source, &mut lines);
    super::super::REPLAY.set(replay);
    let before = crate::work::rendering_work();
    for part in [
        "`code`", "**bold**", "\\", "*", "!", "*", "x", "*", "`", "x", "`",
    ]
    .into_iter()
    .cycle()
    .take(count)
    {
        source.push_str(part);
        let root = advance(&mut state, &mut tokens, &source);
        project(&mut cache, &mut state, root, &source, &mut lines);
    }
    super::super::REPLAY.set(false);
    (crate::work::rendering_work() - before, source.len() - 2)
}

#[test]
fn tree_projection_processes_only_new_nodes() {
    for count in [64, 128, 256, 1024] {
        let (used, bytes) = measure(count, false);
        assert!(
            used <= 128 * bytes + 4096,
            "{count} chunks: {used} work for {bytes} bytes"
        );
    }
}

#[test]
fn tree_projection_counter_detects_replaying_old_nodes() {
    let small = measure(128, true);
    let large = measure(256, true);
    assert!(large.0 > 2 * small.0 + 4096);
}

fn atom(
    cache: &mut Cache,
    forest: &mut Forest,
    source: &mut String,
    fragment: &str,
) -> Vec<crate::Event<'static>> {
    let start = source.len();
    source.push_str(fragment);
    let parsed = format!("x {fragment}");
    let events: Vec<_> = pulldown_cmark::Parser::new_ext(&parsed, crate::options())
        .into_offset_iter()
        .collect();
    let previous = cache.last;
    cache
        .render(
            crate::inline_scan::Slice {
                original: source,
                parsed: &parsed,
                start: 0,
                base: start,
                sentinel: 2,
                boundary: 2,
                classified: true,
            },
            &events,
            &crate::References::default(),
            &mut crate::line_buffer::Buffer::default(),
            0,
        )
        .expect("compiled inline atom");
    let output = cache.last;
    cache.last = previous;
    forest.atom(start..source.len(), output);
    events
        .into_iter()
        .filter_map(|(event, range)| {
            if matches!(
                event,
                crate::Event::Start(crate::Tag::Paragraph)
                    | crate::Event::End(crate::TagEnd::Paragraph)
            ) || range.end <= 2
            {
                None
            } else if let crate::Event::Text(text) = event {
                Some(crate::Event::Text(
                    text.get(2usize.saturating_sub(range.start)..)
                        .expect("synthetic prefix")
                        .to_owned()
                        .into(),
                ))
            } else {
                Some(event.into_static())
            }
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn tree_scopes_reuse_arbitrary_compiled_atoms(
        fragments in prop::collection::vec(prop::sample::select(vec![
            "[x](u)", "![x](v)", "x![](u)y", "![a ![](u)b](u)",
            "<https://x>", "<a@b.example>", "&amp;", "&#10;", "&#13;", "&#0;",
            "<i>", "</i>", "<!--\n x\n-->", "`code`", "**bold**",
        ]), 0..16),
    ) {
        let mut source = String::from("x ");
        let mut forest = Forest::default();
        let mut cache = Cache::default();
        let mut model = vec![crate::Event::Text("x ".into())];
        forest.text(0..2);
        model.extend(atom(&mut cache, &mut forest, &mut source, "p&#10;q"));
        model.push(crate::Event::Text(" ".into()));
        model.push(crate::Event::Start(crate::Tag::Strong));
        let start = source.len();
        source.push_str(" **");
        forest.text(start..start + 1);
        let opening = start + 1..start + 3;
        let opener_cursor = forest.text(opening.clone());
        for fragment in fragments {
            model.extend(atom(&mut cache, &mut forest, &mut source, fragment));
        }
        let end = source.len();
        source.push_str("z** x");
        forest.text(end..end + 1);
        let closing = end + 1..end + 3;
        let closer_cursor = forest.text(closing.clone());
        forest.wrap(&crate::inline_scan::delimiters::Matched {
            opener: opening, closer: closing, byte: b'*', opener_cursor, closer_cursor,
        }).expect("scope cursors");
        forest.text(end + 3..source.len());
        model.push(crate::Event::Text("z".into()));
        model.push(crate::Event::End(crate::TagEnd::Strong));
        model.push(crate::Event::Text(" x".into()));
        let root = forest.snapshot();
        let mut lines = crate::line_buffer::Buffer::default();
        cache.forest(&mut forest, root, &source, 0..source.len(), &mut lines, 0).expect("scope projection");
        prop_assert_eq!(lines, crate::render::render_styled(&model, Style::default()), "source {:?}", source);
    }
}
