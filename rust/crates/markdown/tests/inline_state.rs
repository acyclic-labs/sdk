use super::*;
use crate::inline_scan::tokens::Tokens;
use proptest::prelude::*;

fn feed(state: &mut State, tokens: &mut Tokens, source: &str) {
    tokens.push(source);
    while let Some(token) = tokens.ready.pop_front() {
        state.feed(token.range, token.kind, false);
    }
}

fn full(source: &str) -> crate::Line {
    let events: Vec<_> = pulldown_cmark::Parser::new_ext(source, crate::options()).collect();
    let mut lines = crate::render::render_styled(&events, crate::Style::default());
    assert_eq!(lines.len(), 1);
    lines.pop().expect("one inline paragraph")
}

fn hyphen_is_italic(line: &crate::Line) -> bool {
    line.spans
        .iter()
        .any(|span| span.style.italic && span.text.contains(['‐']))
}

#[test]
fn retained_flanking_preserves_every_ascii_predecessor() {
    for before in 0u8..=127 {
        let source = format!("x{}*‐* x", char::from(before));
        let mut tokens = Tokens::default();
        let mut state = State::default();
        for (offset, character) in source.char_indices() {
            feed(
                &mut state,
                &mut tokens,
                source
                    .get(..offset + character.len_utf8())
                    .expect("UTF-8 prefix"),
            );
        }
        let actual = state
            .forest
            .rendered(&source)
            .expect("retained inline forest");
        assert_eq!(
            hyphen_is_italic(&actual),
            hyphen_is_italic(&full(&source)),
            "ASCII {before}"
        );
    }
}

#[test]
fn flanking_property_detects_substituting_control_with_punctuation() {
    let source = "x\u{e}*‐* x";
    // This neutral boundary is exactly where the old synthetic parser context
    // substituted punctuation for the real non-whitespace control character.
    let mut tokens = Tokens::at(2, Some('.'));
    let mut state = State::default();
    feed(&mut state, &mut tokens, source);
    let actual = state
        .forest
        .rendered(source)
        .expect("retained inline forest");
    assert_ne!(hyphen_is_italic(&actual), hyphen_is_italic(&full(source)));
}

fn parts() -> impl Strategy<Value = Vec<&'static str>> {
    prop::collection::vec(
        prop::sample::select(vec![
            "`", "``", "```", "\\`", "\\``", "\\```", "\\*", "\\_", "\\\\", "*", "**", "***", "_",
            "__", "~", "~~", "x", " ", ".", "!", "[", "]", "¡", "界", "🦀", "…", "\u{e}", "\u{7f}",
            "‐",
        ]),
        0..64,
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/inline_state.txt")
        ))),
        ..ProptestConfig::with_cases(64)
    })]
    #[test]
    fn retained_code_and_delimiter_state_matches_full_parse(
        parts in parts(), cuts in prop::collection::vec(any::<u8>(), 0..32),
    ) {
        let source = format!("x {} x", parts.concat());
        let mut boundaries: Vec<_> = cuts.into_iter().map(|cut| usize::from(cut) % (source.len() + 1))
            .filter(|cut| source.is_char_boundary(*cut)).collect();
        boundaries.push(source.len());
        boundaries.sort_unstable();
        boundaries.dedup();
        let mut tokens = Tokens::default();
        let mut state = State::default();
        let mut replayed = State::default();
        for boundary in boundaries {
            let prefix = source.get(..boundary).expect("UTF-8 prefix");
            feed(&mut state, &mut tokens, prefix);
            prop_assert!(!state.invalid);
            let before = state.snapshot();
            let root = state.projection(tokens.preview().as_ref());
            prop_assert_eq!(state.snapshot(), before, "EOF changed settled scanner state");
            if prefix.is_empty() {
                prop_assert_eq!(root, 0);
            } else {
                prop_assert_eq!(state.forest.rendered_at(root, prefix), Some(full(prefix)), "prefix {:?}", prefix);
            }
            replayed.reset();
            let mut fresh = Tokens::default();
            feed(&mut replayed, &mut fresh, prefix);
            let replayed_root = replayed.projection(fresh.preview().as_ref());
            prop_assert_eq!(replayed.forest.rendered_at(replayed_root, prefix), state.forest.rendered_at(root, prefix));
        }
        prop_assert!(tokens.neutral());
        prop_assert_eq!(state.forest.rendered(&source), Some(full(&source)));
    }

    #[test]
    fn growing_eof_runs_and_their_real_flanks_match_full_parse(
        parts in parts(),
        marker in prop::sample::select(vec!['*', '_', '~', '`']),
        escaped in any::<bool>(),
        count in 1usize..64,
        flank in prop::sample::select(vec!["x", " ", ".", "‐", "¡"]),
    ) {
        let mut source = format!("x {}z", parts.concat());
        let mut tokens = Tokens::default();
        let mut state = State::default();
        feed(&mut state, &mut tokens, &source);
        if escaped { source.push('\\'); }
        for _ in 0..count {
            source.push(marker);
            feed(&mut state, &mut tokens, &source);
            let before = state.snapshot();
            let root = state.projection(tokens.preview().as_ref());
            prop_assert_eq!(state.snapshot(), before);
            prop_assert!(!tokens.neutral());
            prop_assert_eq!(state.forest.rendered_at(root, &source), Some(full(&source)),
                "EOF source {:?}", source);
        }
        source.push_str(flank);
        feed(&mut state, &mut tokens, &source);
        let root = state.projection(tokens.preview().as_ref());
        prop_assert_eq!(state.forest.rendered_at(root, &source), Some(full(&source)),
            "real flank source {:?}", source);
    }
}

fn eof_work(count: usize, replay: bool, partial: bool) -> (usize, usize, usize) {
    let mut source = format!(
        "x {}z",
        if partial {
            "*".repeat(3 * count)
        } else {
            "*x ".repeat(count)
        }
    );
    let mut state = State {
        replay_eof: replay,
        ..State::default()
    };
    let mut tokens = Tokens::default();
    let before = crate::work::current();
    feed(&mut state, &mut tokens, &source);
    for _ in 0..count {
        source.push('*');
        feed(&mut state, &mut tokens, &source);
        state.projection(tokens.preview().as_ref());
    }
    source.push_str(" x");
    feed(&mut state, &mut tokens, &source);
    let work = crate::work::current() - before;
    assert_eq!(state.forest.rendered(&source), Some(full(&source)));
    (work, state.retained() + tokens.retained(), source.len())
}

#[test]
fn eof_projection_has_linear_work_and_storage() {
    for partial in [false, true] {
        for count in [64, 128, 256, 1024] {
            let small = eof_work(count, false, partial);
            let large = eof_work(count * 2, false, partial);
            assert!(
                large.0 <= 128 * large.2 + 4096,
                "partial {partial}: {} work for {} bytes",
                large.0,
                large.2
            );
            assert!(
                large.0 <= 2 * small.0 + 4096,
                "partial {partial}: work {} -> {}",
                small.0,
                large.0
            );
            assert!(
                large.1 <= 2 * small.1 + 4096,
                "partial {partial}: storage {} -> {}",
                small.1,
                large.1
            );
        }
    }
}

#[test]
fn work_property_detects_replaying_eof_matches() {
    let small = eof_work(64, true, false);
    let large = eof_work(128, true, false);
    assert!(large.0 > 2 * small.0 + 4096);
    assert!(large.1 > 2 * small.1 + 4096);
}

fn measure(count: usize, rescan: bool) -> (usize, usize, usize) {
    let mut source = String::from("x `a ");
    let mut state = State {
        rescan,
        ..State::default()
    };
    let mut tokens = Tokens::default();
    let before = crate::work::current();
    feed(&mut state, &mut tokens, &source);
    for _ in 0..count {
        source.push_str("**x**``a`` ");
        feed(&mut state, &mut tokens, &source);
    }
    source.push_str("` x");
    feed(&mut state, &mut tokens, &source);
    (
        crate::work::current() - before,
        state.retained() + tokens.retained(),
        source.len(),
    )
}

#[test]
fn retained_code_state_has_linear_work_and_storage() {
    for count in [64, 128, 256, 1024] {
        let small = measure(count, false);
        let large = measure(count * 2, false);
        assert!(
            large.0 <= 128 * large.2 + 4096,
            "{} work for {} bytes",
            large.0,
            large.2
        );
        assert!(
            large.0 <= 2 * small.0 + 4096,
            "work {} -> {}",
            small.0,
            large.0
        );
        assert!(
            large.1 <= 2 * small.1 + 4096,
            "storage {} -> {}",
            small.1,
            large.1
        );
    }
}

#[test]
fn work_property_detects_scanning_retained_scopes() {
    let small = measure(128, true);
    let large = measure(256, true);
    assert!(large.0 > 2 * small.0 + 4096);
}

#[test]
fn render_property_detects_not_restoring_the_opener() {
    let source = "x `a **b** ` z";
    let mut state = State {
        stale: true,
        ..State::default()
    };
    feed(&mut state, &mut Tokens::default(), source);
    assert_ne!(state.forest.rendered(source), Some(full(source)));
}
