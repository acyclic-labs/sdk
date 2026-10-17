use super::*;
use proptest::prelude::*;

#[test]
fn syntax_and_exact_serialization() {
    for (raw, kind, valid) in [
        (":file[src/lib.rs#L10-L20]", Kind::Inline, true),
        (
            "::file[src/lib.rs#L10-20]{lines=2-4 chars=0:8 symbol=main}",
            Kind::Block,
            true,
        ),
        ("::file[path]{future=value}", Kind::Block, true),
        (":file[]", Kind::Inline, false),
        (":file[path#L0]", Kind::Inline, false),
        (":file[path#L4-2]", Kind::Inline, false),
        (
            ":file[path#L999999999999999999999999999]",
            Kind::Inline,
            false,
        ),
        (":file[path]{lines=1}", Kind::Inline, false),
        ("::file[path]{chars=4:2}", Kind::Block, false),
        ("::file[path]{lines=1-x}", Kind::Block, false),
    ] {
        let node = Directive::parse(raw, kind);
        assert_eq!(node.raw(), raw);
        assert_eq!(node.target().is_some(), valid, "{raw}");
        assert_eq!(Directive::parse(node.raw(), kind), node);
    }
    let node = Directive::parse(
        "::file[ p#L10-20]{lines=2-4 chars=0:8 symbol=main}",
        Kind::Block,
    );
    assert_eq!(
        node.target(),
        Some(&Target {
            path: "p".into(),
            lines: Some(Lines { start: 2, end: 4 }),
            chars: Some(0..8),
            symbol: Some("main".into()),
        })
    );
}

#[test]
fn partial_directives_stay_provisional() {
    let mut stream = Stream::default();
    for chunk in ["prefix :", ":fi", "le[path#L", "2-4", "]"] {
        stream.push(chunk);
        assert!(stream.nodes().is_empty());
    }
    stream.push("{lines=1");
    assert!(stream.nodes().is_empty());
    stream.push("-2} suffix :file[x]");
    assert_eq!(stream.nodes().len(), 2);
    assert_eq!(
        stream.nodes()[0].directive.target().unwrap().lines,
        Some(Lines { start: 1, end: 2 })
    );
    let mut pending = Stream::default();
    pending.push("::file[path]");
    pending.finish();
    assert_eq!(pending.nodes().len(), 1);
}

fn parsed(source: &str, cuts: &[usize]) -> Stream {
    let mut stream = Stream::default();
    let mut previous = 0;
    for end in cuts.iter().copied().chain(std::iter::once(source.len())) {
        if end >= previous && source.is_char_boundary(end) {
            stream.push(&source[previous..end]);
            previous = end;
        }
    }
    stream.finish();
    stream
}

fn source() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            ".{0,20}",
            Just(":file[p#L2-L4]".to_owned()),
            Just("::file[p]{lines=1-2 chars=0:8}".to_owned()),
            Just("\\:file[escaped]".to_owned()),
            Just("::file[bad#L0]".to_owned()),
            Just("::file[p]{symbol=main}".to_owned()),
        ],
        0..20,
    )
    .prop_map(|parts| parts.concat())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn arbitrary_chunks_preserve_ast_and_raw_text(source in source(), cuts in prop::collection::vec(0usize..512, 0..40)) {
        let mut cuts = cuts;
        cuts.sort_unstable();
        let whole = parsed(&source, &[]);
        let streamed = parsed(&source, &cuts);
        prop_assert_eq!(streamed.nodes(), whole.nodes());
        prop_assert_eq!(streamed.source(), source.as_str());
        for node in streamed.nodes() {
            prop_assert_eq!(&source[node.range.clone()], node.directive.raw());
            prop_assert_eq!(Directive::parse(node.directive.raw(), node.directive.kind()), node.directive.clone());
        }
        prop_assert!(streamed.work <= 2 * source.len());
    }
}

#[test]
fn unfinished_candidates_and_replay_negative_control() {
    for count in [64, 128, 256, 1024] {
        for unit in ["::file[", "::file[p]{lines=", ":file[p] ", "!", "\\"] {
            let source = unit.repeat(count);
            let mut retained = Stream::default();
            let mut replay_work = 0;
            for end in 1..=source.len() {
                retained.push(&source[end - 1..end]);
                if count == 64 {
                    let mut replay = Stream::default();
                    replay.push(&source[..end]);
                    replay.finish();
                    replay_work += replay.work;
                }
            }
            retained.finish();
            assert!(retained.work <= 2 * source.len());
            if count == 64 {
                assert!(replay_work > 2 * source.len() + 1024);
            }
        }
    }
}

#[test]
fn losing_pending_escape_is_detected() {
    let mut stream = Stream::default();
    stream.push("\\");
    let mut broken = stream.clone();
    broken.escaped = false;
    stream.push(":file[x]");
    broken.push(":file[x]");
    stream.finish();
    broken.finish();
    assert!(stream.nodes().is_empty());
    assert_ne!(stream.nodes(), broken.nodes());
}

#[test]
fn line_and_character_selection_and_render_snapshot() {
    let directive = Directive::parse("::file[src/example.rs#L2-3]{chars=0:13}", Kind::Block);
    let excerpt = directive
        .target()
        .unwrap()
        .select("ignored\nlet x = 42;\nnext\n")
        .unwrap();
    assert_eq!(excerpt.content, "let x = 42;\nn");
    let rendered = directive.render(Some(&Resolution::Content(excerpt)));
    let text: Vec<String> = rendered
        .iter()
        .map(|line| line.spans.iter().map(|span| span.text.as_str()).collect())
        .collect();
    assert_eq!(
        text,
        ["┌─ src/example.rs:2-3", "│ let x = 42;", "│ n", "└─"]
    );
    assert!(
        rendered[1]
            .spans
            .iter()
            .any(|span| span.style.foreground.is_some())
    );
    let chip = Directive::parse(":file[src/example.rs#L2]", Kind::Inline).render(None);
    assert_eq!(chip[0].spans[0].text, "src/example.rs:2-2");
    assert!(chip[0].spans[0].style.underline);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]
    #[test]
    fn selected_unicode_ranges_match_independent_model(
        source in prop::collection::vec(".{0,12}", 0..8).prop_map(|lines| lines.join("\n")), a in 0usize..12, b in 0usize..12,
        first in 1usize..8, last in 1usize..8,
    ) {
        let (start, end) = (a.min(b), a.max(b));
        let (first, last) = (first.min(last), first.max(last));
        let target = Target { path: "x".into(), lines: Some(Lines { start: first, end: last }), chars: Some(start..end), symbol: None };
        let lines: Vec<_> = source.split('\n').collect();
        let expected = if last > lines.len() { Err(ResolutionError::LineRange) } else {
            let selected = lines[first-1..last].join("\n");
            let chars: Vec<_> = selected.chars().collect();
            if end > chars.len() { Err(ResolutionError::CharRange) }
            else { Ok(chars[start..end].iter().collect::<String>()) }
        };
        prop_assert_eq!(target.select(&source).map(|excerpt| excerpt.content), expected);
    }
}

#[test]
fn unsupported_symbols_and_resolution_failures_are_typed() {
    let directive = Directive::parse("::file[p]{symbol=main}", Kind::Block);
    assert_eq!(
        directive.target().unwrap().select("fn main() {}"),
        Err(ResolutionError::UnsupportedSymbol)
    );
    for error in [
        ResolutionError::Denied,
        ResolutionError::OutsideBoundary,
        ResolutionError::TooLarge,
        ResolutionError::TooMany,
        ResolutionError::Read,
    ] {
        let lines = directive.render(Some(&Resolution::Error(error)));
        assert_eq!(lines.len(), 3);
        assert!(
            lines[1]
                .spans
                .iter()
                .any(|span| span.text.contains(&error.to_string()))
        );
    }
    let invalid = Directive::parse("::file[]", Kind::Block);
    assert_eq!(invalid.render(None)[0].spans[0].text, "::file[]");
}

#[test]
fn colon_runs_keep_the_block_prefix() {
    let stream = parsed("::::file[path]", &[1, 2, 3, 4, 7]);
    assert_eq!(stream.nodes().len(), 1);
    assert_eq!(stream.nodes()[0].range, 2..14);
    assert_eq!(stream.nodes()[0].directive.kind(), Kind::Block);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn invalid_bodies_recover_at_every_chunk_boundary(
        invalid in prop::sample::select(vec![
            ":file[unfinished\n", "::file[unfinished\r", ":file[unfinished:file[",
        ]),
        path in "[a-z]{1,20}",
        cuts in prop::collection::vec(0usize..128, 0..20),
    ) {
        let valid = format!(":file[{path}]");
        let source = format!("{invalid}{valid}");
        let mut cuts = cuts;
        cuts.sort_unstable();
        let streamed = parsed(&source, &cuts);
        prop_assert_eq!(streamed.source(), source.as_str());
        prop_assert!(streamed.nodes().iter().any(|node|
            node.directive.target().is_some_and(|target| target.path == path)
                && node.directive.raw() == valid));
        let whole = parsed(&source, &[]);
        prop_assert_eq!(streamed.nodes(), whole.nodes());
        prop_assert!(streamed.work <= 2 * source.len());
    }
}

#[test]
fn recovery_property_detects_swallowing_an_invalid_body() {
    let source = ":file[unfinished\n:file[ok]";
    let retained = parsed(source, &(1..source.len()).collect::<Vec<_>>());
    assert_eq!(retained.nodes().len(), 1);
    assert_eq!(retained.nodes()[0].directive.target().unwrap().path, "ok");
    let mut broken = Stream {
        swallow_invalid: true,
        ..Stream::default()
    };
    broken.push(source);
    broken.finish();
    assert!(
        !broken
            .nodes()
            .iter()
            .any(|node| node.directive.target().is_some())
    );
}
