use super::*;
use std::{collections::HashSet, sync::Arc};

fn retained(markdown: &Markdown) -> (usize, usize) {
    let mut targets = HashSet::new();
    let mut bytes = markdown.source.capacity() + markdown.visibility.retained();
    bytes += markdown
        .context
        .iter()
        .map(|container| container.opening.capacity() + container.continuation.capacity())
        .sum::<usize>();
    for line in &markdown.lines {
        for span in &line.spans {
            bytes += span.text.capacity();
            if let Some(target) = &span.link
                && targets.insert(Arc::as_ptr(target).cast::<u8>())
            {
                bytes += target.len();
            }
        }
    }
    for cache in &markdown.cached {
        bytes += cache.template.source.capacity();
        bytes += cache
            .template
            .keys
            .iter()
            .map(|key| key.as_ref().len())
            .sum::<usize>();
    }
    for (key, (destination, title, _)) in markdown
        .references
        .committed
        .iter()
        .chain(&markdown.references.pending)
    {
        bytes += key.as_ref().len();
        for target in [destination, title] {
            if targets.insert(Arc::as_ptr(target).cast::<u8>()) {
                bytes += target.len();
            }
        }
    }
    let (flanking_payload, flanking_metadata) = markdown
        .flanking
        .as_ref()
        .map_or((0, 0), flanking::Flanking::retained);
    bytes += flanking_payload;
    let (table_payload, table_metadata) = markdown
        .table
        .as_ref()
        .map_or((0, 0), |table| table.retained(&mut targets));
    bytes += table_payload;
    let metadata = table_metadata
        + flanking_metadata
        + markdown.inline.as_ref().map_or(0, inline::Inline::retained)
        + markdown.context.capacity() * std::mem::size_of::<Container>()
        + markdown.lines.capacity() * std::mem::size_of::<Line>()
        + markdown
            .lines
            .iter()
            .map(|line| line.spans.capacity() * std::mem::size_of::<Span>())
            .sum::<usize>()
        + markdown.cached.capacity() * std::mem::size_of::<Cached>()
        + markdown.dependencies.capacity() * std::mem::size_of::<(Key, Vec<usize>)>()
        + markdown
            .dependencies
            .values()
            .map(|indices| indices.capacity() * std::mem::size_of::<usize>())
            .sum::<usize>()
        + (markdown.references.committed.capacity() + markdown.references.pending.capacity())
            * std::mem::size_of::<(Key, (Arc<str>, Arc<str>, usize))>();
    (bytes, bytes + metadata)
}

#[test]
fn backslash_checkpoint_context_has_linear_work() {
    let measure = |count| {
        let before = work::current();
        let mut markdown = Markdown::default();
        for _ in 0..count {
            markdown.push("\\\\");
        }
        markdown.push("*‐*");
        let used = work::current() - before;
        assert_eq!(
            markdown.lines(),
            full_parse(&format!("{}*‐*", "\\\\".repeat(count)))
        );
        used
    };
    let small = measure(512);
    let large = measure(1024);
    assert!(large <= 2 * small + 1024, "work {small} -> {large}");
}

fn measure(
    actions: &[u8],
    count: usize,
    destination: &str,
    padding: usize,
    separators: &[u8],
    copies: usize,
) -> (usize, usize) {
    let mut markdown = Markdown::default();
    let mut total = 0;
    let mut source = String::new();
    let mut consumer = Vec::new();
    let mut peak = 0;
    let mut growth_total = 0;
    let mut push = |delta: &str| {
        let growth_before = work::source_growth();
        let before = work::current()
            + markdown.visible_work.get()
            + markdown.reference_cache_visits
            + markdown.reference_moved_lines
            + markdown.boundary_work;
        let open = markdown.source.len() - markdown.offset;
        let visits_before = markdown.reference_cache_visits;
        // Definition updates reopen dependency blocks; ordinary pushes do not.
        let dependencies: usize = markdown
            .cached
            .iter()
            .map(|cache| cache.template.source.len())
            .sum();
        let changed = markdown.push(delta);
        assert!(changed <= markdown.lines().len());
        consumer.truncate(changed);
        consumer.extend_from_slice(markdown.lines().get(changed..).unwrap_or_default());
        let used = work::current()
            + markdown.visible_work.get()
            + markdown.reference_cache_visits
            + markdown.reference_moved_lines
            + markdown.boundary_work
            - before;
        let dependent = if markdown.reference_cache_visits > visits_before {
            dependencies
        } else {
            0
        };
        let growth = work::source_growth() - growth_before;
        growth_total += growth;
        // Geometric source-buffer growth is prepaid by two copies per input byte.
        assert!(growth_total <= 2 * markdown.source.len() + 64);
        let amortized = used - growth + 2 * delta.len();
        assert!(
            amortized <= 128 * (delta.len() + open + dependent + 1) + 256,
            "push work {used} for {} chunk bytes and {open} open bytes",
            delta.len()
        );
        total += used;
        let (payload, bytes) = retained(&markdown);
        assert!(payload <= markdown.source.len() * 8 + 1024);
        peak = peak.max(bytes);
        source.push_str(delta);
        assert!(
            consumer == full_parse(&source),
            "consumer differs after {delta:?}, source {:?}",
            source.chars().take(256).collect::<String>()
        );
    };
    for index in 0..count * copies {
        for (position, action) in actions.iter().enumerate() {
            production(*action, index, padding, &mut push);
            let separator = separators
                .get(position % separators.len())
                .copied()
                .unwrap();
            push(
                ["", " ", "\n", "!", "x"]
                    .get(usize::from(separator))
                    .copied()
                    .unwrap(),
            );
        }
    }
    if actions.iter().any(|action| matches!(action, 3 | 15)) {
        push(&format!("\n\n[r]: /{destination}\n\n"));
    } else {
        push("\n\n");
    }
    let (payload, bytes) = retained(&markdown);
    assert!(
        payload <= source.len() * 8 + 1024,
        "{bytes} retained for {} source bytes",
        source.len()
    );
    (total, peak.max(bytes))
}

fn production(action: u8, index: usize, padding: usize, push: &mut impl FnMut(&str)) {
    match action {
        0 => push("word"),
        1 => push("*em* **strong** ~~gone~~"),
        2 | 12 => push("[label](u)"),
        3 => push("[x][r]"),
        4 => push("![](u)\n\n"),
        5 => {
            push(&format!("![][i{index:08x}]\n\n"));
            push(&format!("[i{index:08x}]: /u\n\n"));
        }
        6 => {
            push(&format!("-{}parent\n\n", " ".repeat(padding)));
            push(&format!("{}child\n", " ".repeat(padding + 3)));
        }
        7 => {
            push("```rust\n");
            push("let x = 1;\n");
            push("```\n");
        }
        8 => push("\n\n"),
        9 => push("![alt](u)"),
        10 => push("[outer [inner](u)](v)"),
        11 => push("a &amp; b"),
        13 => push("word\n"),
        14 => {
            if index == 0 {
                push("| a | b |\n|---|---|\n");
            }
            push("| x | y |\n");
        }
        15 => {
            push("word ![](u)\n\n");
            push("![][r]\n\n");
        }
        16 => push("`code`"),
        17 => push("*em*"),
        18 => push("**bold**"),
        19 => push("~~strike~~"),
        20 => push("<https://example.org>"),
        21 => push("<i>"),
        22 => push("\\*"),
        23 => push("&amp;"),
        24 => push("![](u)"),
        25 => push("[a][]"),
        26 => push("_em_"),
        27 => push("__bold__"),
        28 => push("</i>"),
        29 => push("<a@example.org>"),
        30 => push("&#42;"),
        31 => push("\\\\"),
        32 => push("&#10;"),
        _ => push("  \n"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/sequence_work.txt")
        ))),
        rng_seed: proptest::test_runner::RngSeed::Fixed(0x6d61_726b_0000_0004),
        ..ProptestConfig::with_cases(24)
    })]
    #[test]
    fn push_grammar_has_linear_work_and_storage(
        actions in prop_oneof![
            prop::collection::vec(0u8..4, 1..5),
            prop::collection::vec(prop::sample::select(vec![4u8, 5, 8]), 1..5),
            prop::collection::vec(0u8..16, 1..5),
            prop::collection::vec(12u8..16, 1..5),
            prop::sample::select(vec![vec![12u8], vec![13], vec![14]]),
            prop::collection::vec(0u8..34, 1..5),
            prop::collection::vec(16u8..34, 1..5),
        ],
        count in 1usize..65,
        destination in "[a-z]{64,1024}",
        padding in 1usize..5,
        separators in prop::collection::vec(0u8..5, 1..5),
    ) {
        let small = measure(&actions, count, &destination, padding, &separators, 1);
        let large = measure(&actions, count, &destination, padding, &separators, 2);
        prop_assert!(large.0 <= 2 * small.0 + 1024, "work {} -> {}", small.0, large.0);
        prop_assert!(large.1 <= 2 * small.1 + 1024, "storage {} -> {}", small.1, large.1);
    }
}

#[test]
fn hard_break_checkpoint_preserves_following_indentation() {
    measure(&[16, 16, 33], 1, "unused", 4, &[1, 3], 2);
}

#[test]
fn repeated_inline_spans_have_linear_work() {
    for (actions, count, separators) in [
        (&[16][..], 6, &[0, 1, 0, 1][..]),
        (&[26][..], 7, &[0, 3][..]),
        (&[26, 16][..], 16, &[4, 3, 4, 1][..]),
        (&[26, 26][..], 2, &[4, 3, 4, 1][..]),
        (&[26, 26][..], 16, &[4, 3, 4, 1][..]),
        (&[17][..], 6, &[0][..]),
        (&[19][..], 5, &[0][..]),
        (&[13, 14][..], 1, &[1, 0, 0][..]),
    ] {
        let small = measure(actions, count, "unused", 3, separators, 1);
        let large = measure(actions, count, "unused", 3, separators, 2);
        assert!(
            large.0 <= 2 * small.0 + 1024,
            "actions {actions:?}: work {} -> {}",
            small.0,
            large.0
        );
    }
}

#[test]
fn inline_spans_preserve_every_cut() {
    for span in [
        "`a`",
        "``a`b``",
        "*a*",
        "_a_",
        "**a**",
        "__a__",
        "~~a~~",
        "~a~",
        "[a](u)",
        "![a](u)",
        "&amp;",
        "\\*",
        "***a***",
        "` a `",
        "*a b*",
        "`a\nb`",
        "*!`¡À*",
        "[*x][y][z*][ref]",
    ] {
        for middle in ["", " ", "x", "!", "  \n ", "\\\n "] {
            super::block_checkpoints::check_inline_cuts(&[&format!(
                "{span}{middle}{span}{middle}{span}\n\na"
            )]);
        }
    }
}

#[test]
fn retained_span_content_is_processed_once() {
    for span in [
        "`code`",
        "_em_",
        "*em*",
        "**bold**",
        "__bold__",
        "~~strike~~",
    ] {
        let measure = |count| {
            let mut markdown = Markdown::default();
            let mut used = 0;
            for index in 0..count {
                let before = work::current();
                markdown.push(span);
                used += work::current() - before;
                assert_eq!(
                    markdown.lines(),
                    full_parse(&span.repeat(index + 1)),
                    "span {span:?}, count {}",
                    index + 1
                );
            }
            assert_eq!(markdown.lines(), full_parse(&span.repeat(count)));
            used
        };
        let small = measure(128);
        let large = measure(256);
        assert!(large <= 2 * small + 1024, "{span}: work {small} -> {large}");
    }
}

#[test]
fn partial_html_block_names_remain_provisional() {
    super::block_checkpoints::check_inline_cuts(&[
        "[x](url)```rust\n<div>\na",
        "word\n</div>\na",
        "word\n <table>\na",
    ]);
}

#[test]
fn short_inline_spans_preserve_all_partitions() {
    for source in [
        "*a*b", "_a_b", "`a`b", "**a**b", "~~a~~b", "`a``a`", "_a__a_", "*a**a*",
    ] {
        super::block_checkpoints::check_all_partitions(source);
    }
}

#[test]
fn intraword_underscore_sequences_have_linear_work() {
    for count in [4, 16, 64, 128] {
        let small = measure(&[26], count, &"u".repeat(1024), 2, &[4, 3, 4, 1], 1);
        let large = measure(&[26], count, &"u".repeat(1024), 2, &[4, 3, 4, 1], 2);
        assert!(
            large.0 <= 2 * small.0 + 1024,
            "count {count}: work {} -> {}",
            small.0,
            large.0
        );
    }
}

#[test]
fn flanking_rewrites_preserve_every_cut() {
    for span in ["_a_", "__a__", "*a*", "**a**", "~a~", "~~a~~"] {
        for flank in ["x", "x ", "!", "é", " x", "_", "*"] {
            let source = format!("{span}{flank}{span}{flank}{span}\n\na");
            block_checkpoints::check_inline_cuts(&[&source]);
        }
    }
}

#[test]
fn fence_reference_visibility_has_linear_storage() {
    let destination = "a".repeat(547);
    for count in [20, 21, 22, 42] {
        let small = measure(&[7, 15, 9], count, &destination, 2, &[1, 0, 0], 1);
        let large = measure(&[7, 15, 9], count, &destination, 2, &[1, 0, 0], 2);
        assert!(
            large.0 <= 2 * small.0 + 1024,
            "work {} -> {}",
            small.0,
            large.0
        );
        assert!(
            large.1 <= 2 * small.1 + 1024,
            "storage {} -> {}",
            small.1,
            large.1
        );
    }
}

#[test]
fn intraword_openers_preserve_left_flanking() {
    let mut markdown = Markdown::default();
    markdown.push("a*b*");
    markdown.push("*");
    assert_eq!(markdown.lines(), full_parse("a*b**"));
    for source in ["a*b**", ".*+b**", "a***b** c**", ".**+b***"] {
        block_checkpoints::check_all_partitions(source);
    }
    for prefix in ["a", ".", "é", " "] {
        for marker in ["*", "**", "_", "__", "~", "~~"] {
            for body in ["b", "+b", "b+", "éb"] {
                for suffix in ["*", "**", "_"] {
                    block_checkpoints::check_inline_cuts(&[&format!(
                        "{prefix}{marker}{body}{marker}{suffix}"
                    )]);
                }
            }
        }
    }
}
