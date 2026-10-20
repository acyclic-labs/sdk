use super::*;

pub(super) fn replay(source: &str, cuts: &[usize]) {
    let mut markdown = Markdown::default();
    let mut consumer = Vec::new();
    let mut start = 0;
    for end in cuts.iter().copied().chain([source.len()]) {
        if end <= start || end > source.len() || !source.is_char_boundary(end) {
            continue;
        }
        let delta = source.get(start..end).expect("UTF-8 chunk");
        let changed = markdown.push(delta);
        assert!(changed <= markdown.lines().len());
        consumer.truncate(changed);
        consumer.extend(
            (markdown.lines().slice(changed..).expect("change boundary"))
                .iter()
                .cloned(),
        );
        assert_eq!(
            consumer,
            full_parse(source.get(..end).expect("UTF-8 prefix")),
            "source {source:?}, cuts {cuts:?}, prefix {end}"
        );
        start = end;
    }
}

fn units(chunks: &[String], count: usize) -> (usize, usize, [usize; 5]) {
    let before = work::current();
    let parts_before = work::parts();
    let mut markdown = Markdown::default();
    for chunk in chunks.iter().cycle().take(count) {
        markdown.push(chunk);
    }
    let used = work::current() - before
        + markdown.visible_work.get()
        + markdown.reference_cache_visits
        + markdown.reference_moved_lines
        + markdown.boundary_work;
    let parts_after = work::parts();
    (
        used,
        markdown.source.len(),
        parts_after
            .into_iter()
            .zip(parts_before)
            .map(|(after, before)| after - before)
            .collect::<Vec<_>>()
            .try_into()
            .expect("counter categories"),
    )
}

#[test]
fn mixed_links_keep_inline_work_linear() {
    let chunks = ["[x](u)", "[x][r]", "[x](u)"].map(str::to_owned);
    for count in [64, 128, 256, 512, 1024] {
        let (used, bytes, parts) = units(&chunks, count);
        if std::env::var_os("GC_WORK_REPORT").is_some() {
            eprintln!("{count} chunks {used}/{bytes} {parts:?}");
        }
        assert!(
            used <= 128 * bytes + 4096,
            "{count}: {used} units for {bytes} bytes"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/inline_chunks.txt")
        ))),
        ..ProptestConfig::with_cases(24)
    })]
    #[test]
    fn arbitrary_chunk_splits_equal_full_render(
        input in prop::collection::vec(prop_oneof![
            any::<char>().prop_map(|character| character.to_string()),
            prop::sample::select(vec!["\\", "*", "!", "[", "]", "&", ";", "\n", "`", "_", "~",
                "[r]: /r\n[s]: /s\n\nprefix ", "[r]", "[s]", "(u)", "[]",
                "**a**", "`a`", "~~a~~", "[a](u)", "[`x](u)", "![`x](u)", "<https://x>", "\\\n", "\x0b\n",
                "| `a` | b |\n| - | - |\n| x | y |\n",
            ])
                .prop_map(str::to_owned),
        ], 0..64),
        splits in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let source = input.concat();
        let mut cuts: Vec<_> = splits.iter().map(|cut| usize::from(*cut) % (source.len() + 1)).collect();
        cuts.sort_unstable();
        replay(&source, &cuts);
        // Every byte boundary is also a possible split, including escape pairs.
        replay(&source, &(1..source.len()).collect::<Vec<_>>());
    }

    #[test]
    fn adversarial_inline_work_is_linear(
        chunks in prop::collection::vec(prop::sample::select(vec![
            "\\*", "!", "**bold**", "*em*", "_em_", "~~gone~~", "`code`",
            "[x](u)", "[x][r]", "[]", "&amp;", "&#42;", "word", "\n",
            "\\", "*", "[", "]", "&", ";", "x",
        ]).prop_map(str::to_owned), 1..5),
    ) {
        let mut previous = None;
        for count in [64, 128, 256, 512, 1024] {
            let (work, bytes, parts) = units(&chunks, count);
            prop_assert!(work <= 128 * bytes + 4096,
                "{chunks:?}: {count} chunks, {work} work for {bytes} bytes; scan/copy/visibility/feed/parser {parts:?}");
            if let Some(smaller) = previous {
                prop_assert!(10 * work <= 22 * smaller + 40960,
                    "{chunks:?}: doubling to {count} chunks: {smaller} -> {work}");
            }
            if std::env::var_os("GC_WORK_REPORT").is_some() {
                eprintln!("{chunks:?}: {count} chunks, {bytes} bytes, {work} units, {parts:?}");
            }
            previous = Some(work);
        }
    }
}

#[test]
fn checked_in_chunk_corpus_matches_full_render() {
    for data in [
        include_bytes!("../fuzz/corpus/chunk_splits/control-flanking").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/code-literal-nul").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/visible-source-end").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/restored-scope-frontier").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escape-pair").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escaped-image").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/code-escaped-run").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escape-newline").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/opaque-precedence").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/partial-opener").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/reference-label-dependency").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/fence-row-context").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escaped-stars").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/bangs").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/mixed").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/list-indent").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/deferred-row-indent").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/shortcut-inline-continuation").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/reference-inline-suffix").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/collapsed-inline-suffix").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escaped-break-whitespace").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/inline-pass-checkpoint").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/inline-pass-table-shape").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/paragraph-source-start").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escaped-reference-suffix").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/code-steals-link").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/definition-paragraph-start").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/scope-ancestor-frontier").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/table-checkpoint-context").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/native-label-compilation").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/unpublished-prefix-restore").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/frozen-row-scope").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/definition-provisional-tick").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/nested-reference-hint").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/reference-hint-removal").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/escaped-reference-hint").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/inactive-bracket-hint").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/expanded-break-lookahead").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/provisional-code-labels").as_slice(),
        include_bytes!("../fuzz/corpus/chunk_splits/nested-image-destination").as_slice(),
    ] {
        let encoded = data
            .get(..8)
            .expect("corpus header")
            .try_into()
            .expect("eight bytes");
        let flags = usize::try_from(u64::from_le_bytes(encoded)).expect("corpus length");
        let source =
            std::str::from_utf8(data.get(8 + flags..).expect("corpus body")).expect("UTF-8 corpus");
        let cuts: Vec<_> = source
            .char_indices()
            .skip(1)
            .enumerate()
            .filter_map(|(index, (offset, _))| {
                (index < flags && data.get(8 + index).expect("corpus cut") & 1 != 0)
                    .then_some(offset)
            })
            .collect();
        replay(source, &cuts);
        replay(source, &(1..source.len()).collect::<Vec<_>>());
    }
}

#[test]
fn every_punctuation_escape_preserves_terminal_source_coverage() {
    for byte in 0u8..=127 {
        if !byte.is_ascii_punctuation() {
            continue;
        }
        for ending in ["", "\n", "\r\n", "\nnext"] {
            let source = format!("x \\{}{ending}", char::from(byte));
            replay(&source, &(1..source.len()).collect::<Vec<_>>());
        }
    }
}

#[test]
fn shortcut_scope_extension_replaces_the_entire_previous_scope() {
    let source = "[r]: /r\n[s]: /s\n\nprefix [r][s]";
    let start = source.find("prefix").expect("paragraph");
    for first in start..=source.len() {
        for second in first..=source.len() {
            replay(source, &[first, second]);
        }
    }
}

#[test]
fn work_property_detects_classifying_the_entire_unfinished_suffix() {
    let chunks = vec!["**bold**".to_owned()];
    crate::inline::REPLAY_CLASSIFIER.set(true);
    let (small, _, _) = units(&chunks, 128);
    let (large, _, _) = units(&chunks, 256);
    crate::inline::REPLAY_CLASSIFIER.set(false);
    eprintln!("whole-suffix classifier negative control: {small} -> {large}");
    assert!(10 * large > 22 * small + 40960, "{small} -> {large}");
}

#[test]
fn provisional_list_siblings_preserve_lazy_continuations() {
    for marker in ["*", "-", "+", "1.", "1)"] {
        let source = format!("{marker} item\n{marker}word");
        replay(&source, &[source.len() - 4]);
        replay(&source, &(1..source.len()).collect::<Vec<_>>());
    }
}

#[test]
fn reference_initialization_reuses_events_and_expansion_accounting() {
    let mut markdown = Markdown::default();
    let source = "[x][y][x][y]\n\n";
    markdown.push(source);
    let mut complete = source.to_owned();
    for definition in ["[y]: /target\n\n", "[outer [x][y]]: /target\n\n"] {
        let before = references::PARSED_BYTES.get();
        markdown.push(definition);
        let used = references::PARSED_BYTES.get() - before;
        assert!(
            used <= source.len() + definition.len() * 3,
            "{used} parser bytes"
        );
        complete.push_str(definition);
        assert_eq!(markdown.lines(), full_parse(&complete));
    }
}

#[test]
fn link_and_delimiter_chunks_stay_within_the_absolute_work_bound() {
    let chunks = ["[x](u)".to_owned(), "*".to_owned()];
    let retained = units(&chunks, 1024);
    assert!(retained.0 <= 128 * retained.1 + 4096);
}

#[test]
fn short_paragraphs_stay_within_the_total_work_bound() {
    let chunks = ["\n".to_owned(), "[]".to_owned(), "\n".to_owned()];
    let measured = units(&chunks, 128);
    assert!(measured.0 <= 128 * measured.1 + 4096, "{measured:?}");
}

#[test]
fn short_delimiter_paragraphs_keep_compact_opener_snapshots() {
    let chunks = ["*".to_owned(), "!".to_owned(), "\n".to_owned()];
    let mut retained = (0, 0, [0; 5]);
    for count in [128, 256, 512, 1024] {
        let measured = units(&chunks, count);
        retained = measured;
        if std::env::var_os("GC_WORK_REPORT").is_some() {
            println!("delimiter paragraphs {count}: {measured:?}");
        }
        assert!(measured.0 <= 128 * measured.1 + 4096, "{measured:?}");
    }
    crate::spans::COPY_SHARED_TAIL.set(true);
    let copied = units(&chunks, 1024);
    crate::spans::COPY_SHARED_TAIL.set(false);
    // The classifier now leaves enough headroom that redundant snapshot copies
    // fit the total bound. Keep the bound above and pin copy work independently.
    let exact_copy = |parts: [usize; 5]| assert_eq!(parts[1], 93_139);
    exact_copy(retained.2);
    assert_eq!(copied.2[1] - retained.2[1], 8 * (1024 - 1));
    assert!(std::panic::catch_unwind(|| exact_copy(copied.2)).is_err());
}

#[test]
fn closed_empty_brackets_keep_the_existing_work_bound() {
    for count in [64, 128, 256, 512, 1024] {
        let (used, bytes, parts) = units(&["[]".into()], count);
        assert!(
            used <= 128 * bytes + 4096,
            "n={count}: {used} for {bytes}, {parts:?}"
        );
    }
}

#[test]
fn multiline_code_then_pending_ticks_in_link_labels_match_replay() {
    let source = include_str!("../fuzz/corpus/streaming/multiline-code-pending-link-tick");
    let cuts: Vec<_> = (1..source.len()).collect();
    replay(source, &cuts);
    // Negative control: classifying from the row loses the span's opener.
    crate::inline::SKIP_CODE.set(true);
    let broken = std::panic::catch_unwind(|| replay(source, &cuts));
    crate::inline::SKIP_CODE.set(false);
    assert!(broken.is_err());
}

#[test]
fn eof_ticks_decide_a_list_interrupt() {
    let sources = [
        "a  \n1. `c`",
        "a\n1. `c`",
        "a\n- `c`",
        "a\n> `c`",
        "a\n# `c`",
    ];
    for source in sources {
        let cuts: Vec<_> = (1..source.len()).collect();
        replay(source, &cuts);
    }
    // Negative control: classifying the row without its EOF ticks keeps
    // `1. ` as paragraph text.
    crate::inline::SKIP_UNDECIDED.set(true);
    let broken = std::panic::catch_unwind(|| {
        replay(
            sources.first().expect("regression source"),
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
        );
    });
    crate::inline::SKIP_UNDECIDED.set(false);
    assert!(broken.is_err());
}

#[test]
fn blank_start_items_reopen_as_items() {
    let sources = [
        "*\n  e\n\n`",
        "1.\n   e\n\n# h",
        "-\n  e\n\n```\n",
        "> -\n>   e\n>\n> x",
    ];
    for source in sources {
        let cuts: Vec<_> = (1..source.len()).collect();
        replay(source, &cuts);
    }
    // Negative control: a marker glued to its placeholder text is no item.
    crate::context::GLUED_MARKER.set(true);
    let broken =
        std::panic::catch_unwind(|| replay(sources.first().expect("regression source"), &[7]));
    crate::context::GLUED_MARKER.set(false);
    assert!(broken.is_err());
}

#[test]
fn markers_after_escaped_breaks_have_linear_work() {
    let chunks = ["\n", "*", "\\"].map(str::to_owned);
    let bound = |(used, bytes, parts): (usize, usize, [usize; 5])| {
        assert!(used <= 128 * bytes + 4096, "{used} / {bytes}: {parts:?}");
    };
    for count in [64, 128, 256, 512, 1024] {
        let measured = units(&chunks, count);
        eprintln!("escaped-break marker {count}: {measured:?}");
        bound(measured);
    }
    crate::inline::REPLAY_ROWS.set(true);
    let redundant = units(&chunks, 256);
    crate::inline::REPLAY_ROWS.set(false);
    assert!(std::panic::catch_unwind(|| bound(redundant)).is_err());
    let source = "\n*\\".repeat(86);
    replay(&source, &(1..source.len()).collect::<Vec<_>>());
}

#[test]
fn control_whitespace_breaks_are_classified_before_the_next_row() {
    for left in [' ', '\t', '\u{b}', '\u{c}'] {
        for right in [' ', '\t', '\u{b}', '\u{c}'] {
            let source = format!("[r]: /r\n[s]: /s\n\nprefix{left}{right}\n_");
            replay(&source, &(1..source.len()).collect::<Vec<_>>());
        }
    }
    let source = "prefix \u{b}";
    let exact = |result| assert_eq!(result, Some(true));
    exact(crate::inline::hard_break(source, source.len()));
    // Restore the former preview's ASCII-space-only rule.
    let old = Some(source.len() - source.trim_end_matches(' ').len() >= 2);
    assert!(std::panic::catch_unwind(|| exact(old)).is_err());
}
