use super::*;
use proptest::prelude::*;
use pulldown_cmark::{Event, LinkType, Parser, Tag};

// Whether `construct` is exactly one raw HTML span, autolink or pointy link
// destination when it follows paragraph text.
fn complete(construct: &str) -> bool {
    let text = format!("x {construct}");
    let whole = 2..text.len();
    let parsed = Parser::new(&text).into_offset_iter().any(|(event, range)| {
        range == whole
            && matches!(
                event,
                Event::InlineHtml(_)
                    | Event::Start(Tag::Link {
                        link_type: LinkType::Autolink | LinkType::Email,
                        ..
                    })
            )
    });
    let pointy = construct.len() >= 2
        && construct.ends_with('>')
        && construct.get(1..construct.len() - 1).is_some_and(|inner| {
            let mut escaped = false;
            inner.bytes().all(|byte| {
                // Line endings end a destination even when escaped.
                let ok =
                    !matches!(byte, b'\n' | b'\r') && (escaped || !matches!(byte, b'<' | b'>'));
                escaped = !escaped && byte == b'\\';
                ok
            }) && !escaped
        });
    parsed || pointy
}

// Every construct that the parser completes at byte `end` was reported by
// the byte that completed it, with an opener no later than its own.
fn missed(source: &str) -> Option<(usize, usize)> {
    let mut angles = Angles::at(0);
    for end in 1..=source.len() {
        if !source.is_char_boundary(end) {
            continue;
        }
        let reported = angles.feed(source.get(..end)?);
        let missed = source
            .get(..end)?
            .match_indices('<')
            .map(|(start, _)| start)
            // An escaped `<` opens nothing.
            .filter(|&start| {
                source
                    .as_bytes()
                    .get(..start)
                    .expect("before opener")
                    .iter()
                    .rev()
                    .take_while(|&&byte| byte == b'\\')
                    .count()
                    % 2
                    == 0
            })
            .find(|&start| {
                complete(source.get(start..end).expect("construct boundaries"))
                    && reported.is_none_or(|opener| opener > start)
            });
        if let Some(start) = missed {
            return Some((start, end));
        }
    }
    None
}

const PIECES: &[&str] = &[
    "<", ">", "a", "b1", " ", "\n", "\"", "'", "=", "-", "!", "?", "[", "]", "CDATA", ":", "@",
    "/", "\\", "`", "(", ")", ".", "_", "é", "x:y", "--", "]]",
];

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 512,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]
    #[test]
    fn every_completed_angle_construct_is_reported(
        pieces in prop::collection::vec(prop::sample::select(PIECES), 0..24),
    ) {
        let source = pieces.concat();
        prop_assert_eq!(missed(&source), None, "{:?}", source);
    }
}

#[test]
fn known_constructs_are_reported() {
    for source in [
        "<a>",
        "<a b=\">\" c='>'>",
        "<a\nb\n=\nc/>",
        "</a >",
        "<!---->",
        "<!-->",
        "<!--->",
        "<!-- a -- b -->",
        "<?x ? > ?>",
        "<!X y>",
        "<![CDATA[ ] ]] ]]>",
        "<https://x.y/?a=<>",
        "<a@b.c>",
        "<a\\>b>",
    ] {
        assert_eq!(missed(source), None, "{source:?}");
    }
}

#[test]
fn soundness_property_detects_closing_a_quoted_value_early() {
    QUOTES_CLOSE.set(true);
    let missed = missed("x <a b=\">\">");
    QUOTES_CLOSE.set(false);
    assert_eq!(missed, Some((2, 11)));
}

#[test]
fn openers_close_once_and_merge_by_state() {
    let mut source = String::new();
    let mut angles = Angles::at(0);
    // Unclosed comments share one state; nothing they contain closes them.
    for _ in 0..64 {
        source.push_str("<!-- x - ");
        assert_eq!(angles.feed(&source), None);
    }
    assert!(angles.live.count_ones() <= 4, "{:b}", angles.live);
    source.push_str("-->");
    assert_eq!(angles.feed(&source), Some(0));
    // The comment closed; the same bytes cannot report it again.
    source.push_str("-->");
    assert_eq!(angles.feed(&source), None);
}

#[test]
fn escaped_openers_hold_nothing() {
    let mut angles = Angles::at(0);
    assert_eq!(angles.feed(r"x \<!-- a"), None);
    assert_eq!(angles.floor(), None);
    assert_eq!(angles.feed(r"x \<!-- a -->"), None);
    let mut angles = Angles::at(0);
    angles.feed(r"x \\<!-- a");
    assert_eq!(angles.floor(), Some(4));
}
