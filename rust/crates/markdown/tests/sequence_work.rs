use super::*;
use std::{cell::Cell, collections::HashSet, sync::Arc};

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
    let (table_payload, table_metadata) = markdown
        .table
        .as_ref()
        .map_or((0, 0), |table| table.retained(&mut targets));
    bytes += table_payload;
    let metadata = table_metadata
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
        let before = acyclic_markdown_work::work::current();
        let mut markdown = Markdown::default();
        for _ in 0..count {
            markdown.push("\\\\");
        }
        markdown.push("*‐*");
        let used = acyclic_markdown_work::work::current() - before;
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

// Cumulative totals after a block of production indices.
#[derive(Clone, Copy)]
struct Block {
    work: usize,
    churn: usize,
    peak: usize,
}

// Visible bytes a push restyles, rewrites or removes, beyond the unchanged
// span prefix of each line from `changed`. A consumer must receive at least
// these, so they bound the work any incremental parser can avoid.
fn churn(old: &[Line], new: &[Line]) -> usize {
    let bytes = |line: &Line| line.spans.iter().map(|span| span.text.len()).sum::<usize>();
    let mut total = old.iter().skip(new.len()).map(bytes).sum();
    for (index, line) in new.iter().enumerate() {
        let mut kept = 0;
        if let Some(previous) = old.get(index) {
            for (before, after) in previous.spans.iter().zip(&line.spans) {
                if before == after {
                    kept += after.text.len();
                    continue;
                }
                if before.style == after.style && before.link == after.link {
                    kept += before
                        .text
                        .bytes()
                        .zip(after.text.bytes())
                        .take_while(|(left, right)| left == right)
                        .count();
                }
                break;
            }
        }
        total += bytes(line) - kept;
    }
    total
}

// Push runs of the given numbers of production indices, then close the
// document.
fn measure(
    actions: &[u8],
    runs: &[usize],
    destination: &str,
    padding: usize,
    separators: &[u8],
) -> Vec<Block> {
    let mut markdown = Markdown::default();
    let total = Cell::new(0);
    let churned = Cell::new(0);
    let mut source = String::new();
    let mut consumer = Vec::new();
    let peak = Cell::new(0);
    let mut growth_total = 0;
    let mut push = |delta: &str| {
        let growth_before = acyclic_markdown_work::work::source_growth();
        let before = acyclic_markdown_work::work::current()
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
        churned.set(
            churned.get()
                + churn(
                    consumer.get(changed..).unwrap_or_default(),
                    markdown.lines().get(changed..).unwrap_or_default(),
                ),
        );
        consumer.truncate(changed);
        consumer.extend_from_slice(markdown.lines().get(changed..).unwrap_or_default());
        let used = acyclic_markdown_work::work::current()
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
        let growth = acyclic_markdown_work::work::source_growth() - growth_before;
        growth_total += growth;
        // Geometric source-buffer growth is prepaid by two copies per input byte.
        assert!(growth_total <= 2 * markdown.source.len() + 64);
        let amortized = used - growth + 2 * delta.len();
        assert!(
            amortized <= 128 * (delta.len() + open + dependent + 1) + 256,
            "push work {used} for {} chunk bytes and {open} open bytes",
            delta.len()
        );
        total.set(total.get() + amortized);
        let (payload, bytes) = retained(&markdown);
        assert!(payload <= markdown.source.len() * 8 + 1024);
        peak.set(peak.get().max(bytes));
        source.push_str(delta);
        assert!(
            consumer == full_parse(&source),
            "consumer differs after {delta:?}, source {:?}",
            source.chars().take(256).collect::<String>()
        );
    };
    let mut totals = Vec::with_capacity(runs.len());
    let mut start = 0;
    for run in runs {
        for index in start..start + run {
            for (position, action) in actions.iter().enumerate() {
                production(*action, index, padding, &mut push);
                let separator = separators
                    .get(position % separators.len())
                    .and_then(|index| ["", " ", "\n", "!", "x"].get(usize::from(*index)))
                    .copied()
                    .unwrap_or_default();
                push(separator);
            }
        }
        totals.push(Block {
            work: total.get(),
            churn: churned.get(),
            peak: peak.get(),
        });
        start += run;
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
    totals
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

// Each push is checked against a full parse, so a case takes time quadratic
// in its length. The production families run as separate tests in parallel.
fn grammar(
    actions: impl Strategy<Value = Vec<u8>>,
) -> impl Strategy<Value = (Vec<u8>, usize, String, usize, Vec<u8>)> {
    (
        actions,
        1usize..65,
        "[a-z]{64,1024}",
        1usize..5,
        prop::collection::vec(0u8..5, 1..5),
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            acyclic_test_support::crate_path_str!("tests/sequence_work.txt")
        ))),
        rng_seed: proptest::test_runner::RngSeed::Fixed(0x6d61_726b_0000_0004),
        ..ProptestConfig::with_cases(8)
    })]
    #[test]
    fn push_grammar_spans_have_linear_work_and_storage(
        (actions, count, destination, padding, separators) in grammar(prop_oneof![
            prop::collection::vec(0u8..4, 1..5),
            prop::collection::vec(prop::sample::select(vec![4u8, 5, 8]), 1..5),
        ]),
    ) {
        work_and_storage_scale_linearly(&actions, count, &destination, padding, &separators)?;
    }

    #[test]
    fn push_grammar_links_have_linear_work_and_storage(
        (actions, count, destination, padding, separators) in grammar(prop_oneof![
            prop::collection::vec(0u8..16, 1..5),
            prop::collection::vec(12u8..16, 1..5),
            prop::sample::select(vec![vec![12u8], vec![13], vec![14]]),
        ]),
    ) {
        work_and_storage_scale_linearly(&actions, count, &destination, padding, &separators)?;
    }

    #[test]
    fn push_grammar_has_linear_work_and_storage(
        (actions, count, destination, padding, separators) in grammar(prop_oneof![
            prop::collection::vec(0u8..34, 1..5),
            prop::collection::vec(16u8..34, 1..5),
        ]),
    ) {
        work_and_storage_scale_linearly(&actions, count, &destination, padding, &separators)?;
    }
}

fn work_and_storage_scale_linearly(
    actions: &[u8],
    count: usize,
    destination: &str,
    padding: usize,
    separators: &[u8],
) -> Result<(), TestCaseError> {
    // Productions can flip binary block state each index: `![alt](u)```rust`
    // is paragraph text but the later "```" line opens a fence. The index
    // sequence then has period two, and an odd count would compare a document
    // ending inside a fence (its final definition is code) with a balanced one.
    let count = count.next_multiple_of(2);
    // Each push is checked against a full parse, so the measurement costs
    // time quadratic in its length. A two-index warm-up (one period) pays the
    // start-up difference instead of a third full block.
    let blocks = measure(
        actions,
        &[2, count, count],
        destination,
        padding,
        separators,
    );
    let [first, second, third] = blocks[..] else {
        unreachable!("three blocks");
    };
    let (work, churn) = (
        [second.work - first.work, third.work - second.work],
        [second.churn - first.churn, third.churn - second.churn],
    );
    // Linear work is a·n + b. The warm-up also pays the start-up
    // difference b (no left context, label history or openers yet), which
    // can exceed any fixed slack; equal later blocks must cost the same.
    // Output churn is excepted: `_em_x_em_` restyles the whole paragraph on
    // every push, so any parser does work quadratic in such input.
    prop_assert!(
        work[1] + 128 * churn[0] <= work[0] + 128 * churn[1] + 1024,
        "work per block {} -> {} with churn {} -> {}",
        work[0],
        work[1],
        churn[0],
        churn[1]
    );
    prop_assert!(
        third.peak <= 2 * second.peak + 1024,
        "storage {} -> {}",
        second.peak,
        third.peak
    );
    Ok(())
}

// Shrunk failures from the merge gate and a 1000-case run. The first charged
// buffer growth as a step at each reallocation; the second reparsed a whole
// paragraph-long code span (`code``code`...) on every push; the third had an
// odd index count; the fourth is exactly linear (3624n - 1056) but its first
// index costs less, which a ratio against the whole first run rejected; the
// fifth indents its table header, which never started a streamed table; the
// sixth restyles its whole paragraph per push (churn, not parser work); the
// seventh reparsed a paragraph-long emphasis (`*em**em*...`) on every push
// once a lengthened closing run matched an inner opener; the eighth did once
// `_em_x` left a reopened span unclosed. Both now reparse from that opener.
// The ninth failed the merge gate under the old ratio check.
#[test]
fn push_grammar_regressions_scale_linearly() {
    let check = |actions: &[u8], count, destination: &str, padding, separators: &[u8]| {
        work_and_storage_scale_linearly(actions, count, destination, padding, separators)
            .unwrap_or_else(|error| panic!("{actions:?} x {count}: {error}"));
    };
    let destination = "aaaaaaaaaaaaaamaznnkhpmrbztxxgmjlnxsqgysqooulfxomualkduubszvducl";
    check(&[2, 2], 11, destination, 4, &[3, 2, 4, 2]);
    check(&[16], 6, &"a".repeat(64), 3, &[0, 2, 1]);
    check(&[15, 9, 7], 31, &"a".repeat(64), 1, &[0]);
    check(&[25, 16, 16, 17], 1, &"a".repeat(64), 3, &[1, 0]);
    check(&[13, 14], 1, &"a".repeat(64), 1, &[1]);
    check(&[26], 3, &"a".repeat(64), 1, &[4]);
    check(&[17], 3, &"a".repeat(64), 2, &[0, 1, 3, 1]);
    check(&[16, 26], 8, &"a".repeat(64), 1, &[0, 4]);
    check(&[26, 17], 2, &"a".repeat(64), 1, &[4, 4]);
}

// A span closed at the input edge resumes inside its content. Each push must
// still match the full parse when a later run lengthens the closing run,
// matches an inner opener, reopens the span, strips edge spaces or crosses a
// line.

// A checkpoint after a hard break would resume at the next line's start,
// keeping indentation the paragraph strips from a continuation line.
#[test]
fn hard_break_checkpoint_preserves_following_indentation() {
    measure(&[16, 16, 33], &[1, 1], "unused", 4, &[1, 3]);
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
        work_and_storage_scale_linearly(actions, count, "unused", 3, separators)
            .unwrap_or_else(|error| panic!("actions {actions:?}: {error}"));
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
                let before = acyclic_markdown_work::work::current();
                markdown.push(span);
                used += acyclic_markdown_work::work::current() - before;
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
fn intraword_underscore_sequences_have_linear_work() {
    for count in [4, 16, 64, 128] {
        work_and_storage_scale_linearly(&[26], count, &"u".repeat(1024), 2, &[4, 3, 4, 1])
            .unwrap_or_else(|error| panic!("count {count}: {error}"));
    }
}

#[test]
fn fence_reference_visibility_has_linear_storage() {
    let destination = "a".repeat(547);
    for count in [20, 21, 22, 42] {
        work_and_storage_scale_linearly(&[7, 15, 9], count, &destination, 2, &[1, 0, 0])
            .unwrap_or_else(|error| panic!("count {count}: {error}"));
    }
}
