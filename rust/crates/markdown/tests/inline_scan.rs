use super::*;
use proptest::prelude::*;

fn advance(scanner: &mut Scanner, original: &str, base: usize) -> usize {
    scanner.prepare_classification(original, 0);
    let end = scanner.classification_end(original.len()).max(base);
    let context = if base == 0 {
        String::new()
    } else {
        crate::inline::left_context(original.get(..base).unwrap())
    };
    let source = format!("{context}{}", original.get(base..end).unwrap());
    let events: Vec<_> = pulldown_cmark::Parser::new_ext(&source, options())
        .into_offset_iter()
        .collect();
    scanner.frontier(
        Slice {
            original,
            parsed: &source,
            start: 0,
            base,
            sentinel: context.len(),
            boundary: context.len(),
            classified: true,
        },
        &events,
        &References::default(),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn retained_scanner_matches_replay(
        checkpoints in any::<bool>(),
        character_chunks in any::<bool>(),
        parts in prop::collection::vec(prop_oneof![
            any::<char>().prop_map(|c| c.to_string()),
            prop::sample::select(vec![
                "\\", "*", "**", "`", "!", "[", "]", "(", ")", "&", ";", "\n",
                "[x](u)", "![a](u)", "<i>", "</i>", "<https://x>", "<!--x-->",
            ]).prop_map(str::to_owned),
        ], 0..48),
    ) {
        check_sequence(checkpoints, character_chunks, parts)?;
    }
}

fn check_sequence(
    checkpoints: bool,
    character_chunks: bool,
    parts: Vec<String>,
) -> Result<(), proptest::test_runner::TestCaseError> {
    let mut retained = Scanner::default();
    let mut replay = Scanner {
        replay: true,
        ..Scanner::default()
    };
    let mut source = String::new();
    let mut origin = None;
    let mut normalized = crate::Markdown::default();
    let mut base = 0;
    let parts: Vec<String> = if character_chunks {
        parts
            .into_iter()
            .flat_map(|part| part.chars().map(String::from).collect::<Vec<_>>())
            .collect()
    } else {
        parts
    };
    for part in parts {
        normalized.push(&part);
        prop_assert_eq!(
            normalized.lines(),
            crate::tests::full_parse(&normalized.source)
        );
        // The production dispatcher invokes the scanner only for its open
        // paragraph. Code/HTML/list block transitions recreate that cache;
        // do not interpret their literal source as an inline paragraph.
        let leaf = pulldown_cmark::Parser::new_ext(&normalized.source, options())
            .into_offset_iter()
            .filter_map(|(event, range)| match event {
                Event::Start(tag) if crate::is_leaf(&tag) => Some((tag, range)),
                _ => None,
            })
            .last();
        let current = leaf.and_then(|(tag, range)| (tag == Tag::Paragraph).then_some(range));
        if origin != current.as_ref().map(|range| range.start)
            || current.as_ref().is_some_and(|range| {
                !normalized
                    .source
                    .get(range.clone())
                    .expect("paragraph range")
                    .starts_with(&source)
            })
        {
            retained = Scanner::default();
            replay = Scanner {
                replay: true,
                ..Scanner::default()
            };
            base = 0;
            origin = current.as_ref().map(|range| range.start);
        }
        let Some(range) = current else { continue };
        source.clear();
        source.push_str(normalized.source.get(range).expect("normalized range"));
        let frontier = advance(&mut retained, &source, base);
        prop_assert_eq!(
            frontier,
            advance(&mut replay, &source, base),
            "source {:?}",
            source
        );
        for position in source
            .char_indices()
            .map(|(at, _)| at)
            .chain([source.len()])
            .filter(|at| *at >= base)
        {
            prop_assert_eq!(
                retained.checkpoint(position),
                replay.checkpoint(position),
                "source {:?}",
                source
            );
        }
        let left = retained
            .state
            .projection(retained.tokens.preview().as_ref());
        let right = replay.state.projection(replay.tokens.preview().as_ref());
        if checkpoints {
            prop_assert_eq!(
                retained.state.forest.scopes_since(left, base),
                replay.state.forest.scopes_since(right, base),
                "source {:?}",
                source
            );
            base += frontier.saturating_sub(if base == 0 {
                0
            } else {
                crate::inline::left_context(source.get(..base).unwrap()).len()
            });
            retained.discard_before(base);
            replay.discard_before(base);
        } else {
            prop_assert_eq!(
                retained.state.forest.rendered_at(left, &source),
                replay.state.forest.rendered_at(right, &source),
                "source {:?}",
                source
            );
        }
    }
    Ok(())
}

#[test]
fn shrinking_paragraph_recreates_scanner() {
    let parts = ["\u{e}", "\n", "**", "*"].map(str::to_owned).to_vec();
    check_sequence(false, false, parts).unwrap();
}

#[test]
fn growing_tick_previews_keep_foreign_scope_classification() {
    let parts = ["¡", "`", "<i>", "`", "`"].map(str::to_owned).to_vec();
    check_sequence(true, false, parts).unwrap();
}

fn scope_feed_is_bounded(parts: &[&str], replay: bool) -> bool {
    let mut scanner = Scanner {
        replay,
        ..Scanner::default()
    };
    let mut source = String::from("x ");
    for part in parts {
        let start = source.len();
        for character in part.chars() {
            source.push(character);
            let before = scanner.fed_bytes;
            advance(&mut scanner, &source, 0);
            if scanner.fed_bytes - before > 2 * (source.len() - start + 2) {
                return false;
            }
        }
    }
    true
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn resolving_scopes_does_not_replay_the_old_prefix(
        parts in prop::collection::vec(prop::sample::select(vec![
            "[x](u)", "![x](u)", "[a *b*](u)", "![a ![b](v)](u)",
            "<https://x>", "<a@b.example>", "<i>", "</i>", "<!--\n x\n-->", "[x][r]",
        ]), 1..24),
    ) {
        prop_assert!(scope_feed_is_bounded(&parts, false), "parts {:?}", parts);
    }
}

#[test]
fn scope_counter_detects_restoring_the_entire_paragraph() {
    // Independent compiled labels bypass raw feeding even during replay.
    assert!(scope_feed_is_bounded(&["[x](u)"; 16], true));
    // An unmatched code opener keeps raw label state relevant to future input.
    let parts: Vec<_> = std::iter::once("`").chain(["[x](u)"; 16]).collect();
    assert!(scope_feed_is_bounded(&parts, false));
    assert!(!scope_feed_is_bounded(&parts, true));
}

fn fed_bytes(count: usize, replay: bool) -> (usize, usize) {
    let mut scanner = Scanner {
        replay,
        ..Scanner::default()
    };
    let mut source = String::new();
    for part in ["*", "x", "\\", "*", "`", "x", "`", "!", "&", "amp;", "\n"]
        .into_iter()
        .cycle()
        .take(count)
    {
        source.push_str(part);
        // No link/HTML syntax can override the lexical classifications here.
        scanner.frontier(
            Slice {
                original: &source,
                parsed: &source,
                start: 0,
                base: 0,
                sentinel: 0,
                boundary: 0,
                classified: true,
            },
            &[],
            &References::default(),
        );
    }
    (scanner.fed_bytes, source.len())
}

#[test]
fn unchanged_syntax_feeds_each_byte_once() {
    for count in [64, 128, 256, 1024] {
        let (fed, bytes) = fed_bytes(count, false);
        assert!(
            fed <= bytes,
            "{count} pushes: {fed} fed bytes for {bytes} input bytes"
        );
    }
}

#[test]
fn work_property_detects_replaying_settled_tokens() {
    let small = fed_bytes(64, true);
    let large = fed_bytes(128, true);
    assert!(small.0 > small.1);
    assert!(large.0 > 2 * small.0 + 4096);
}

#[test]
fn nested_destination_replay() {
    check_sequence(
        false,
        false,
        ["[", "]", "(", "[x](u)", ")"].map(str::to_owned).to_vec(),
    )
    .unwrap();
}

#[test]
fn nested_destination_model_detects_current_link_floor() {
    state::CURRENT_LINK_FLOOR.set(true);
    let result = check_sequence(
        false,
        false,
        ["[", "]", "(", "[x](u)", ")"].map(str::to_owned).to_vec(),
    );
    state::CURRENT_LINK_FLOOR.set(false);
    assert!(result.is_err());
}
