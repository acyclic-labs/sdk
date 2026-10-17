use super::*;
use std::{collections::HashSet, sync::Arc};

fn retained(
    markdown: &Markdown,
    nodes: &mut HashSet<usize>,
    texts: &mut HashSet<usize>,
    targets: &mut HashSet<*const u8>,
) -> (usize, usize) {
    nodes.clear();
    texts.clear();
    targets.clear();
    let mut text_metadata = 0;
    let mut bytes = markdown.source.capacity() + markdown.visibility.retained();
    let (state_bytes, state_metadata) = markdown.state.text_retained(nodes, texts, targets);
    bytes += state_bytes;
    text_metadata += state_metadata;
    bytes += markdown
        .context
        .iter()
        .map(|container| container.opening.capacity() + container.continuation.capacity())
        .sum::<usize>();
    for line in markdown.lines.iter().chain(&markdown.spare.lines) {
        let (payload, metadata) = line.spans.retained(nodes, texts, targets);
        bytes += payload;
        text_metadata += metadata;
    }
    for cache in &markdown.cached {
        let (payload, metadata) = cache.state.text_retained(nodes, texts, targets);
        bytes += payload;
        text_metadata += metadata;
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
        .map_or((0, 0), |table| table.retained(targets, texts, nodes));
    bytes += table_payload;
    let (inline_payload, inline_metadata) = markdown
        .inline
        .as_ref()
        .map_or((0, 0), |inline| inline.retained(nodes, texts, targets));
    bytes += inline_payload;
    let metadata = text_metadata
        + table_metadata
        + inline_metadata
        + markdown.context.capacity() * std::mem::size_of::<Container>()
        + markdown.lines.retained()
        + markdown.spare.lines.retained()
        + markdown.cached.capacity() * std::mem::size_of::<Cached>()
        + markdown.cached.len() * std::mem::size_of::<render::Renderer>()
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

fn oracle(source: &str, rendered: &mut Vec<(Vec<Event<'static>>, Vec<Line>)>) -> Arc<Vec<Line>> {
    let source = source.replace("\r\n", "\n").replace('\r', "\n");
    let mut lines = Vec::new();
    let mut count = 0;
    for (_, events) in blocks(Parser::new_ext(&source, options()).into_offset_iter()) {
        if rendered
            .get(count)
            .is_none_or(|(previous, _)| *previous != events)
        {
            let lines = render::render_styled(&events, Style::default());
            let fixture = (events.into_iter().map(Event::into_static).collect(), lines);
            if let Some(previous) = rendered.get_mut(count) {
                *previous = fixture;
            } else {
                rendered.push(fixture);
            }
        }
        lines.extend_from_slice(&rendered.get(count).expect("oracle block").1);
        lines.push(Line::default());
        count += 1;
    }
    rendered.truncate(count);
    while lines.last().is_some_and(|line| line.spans.is_empty()) {
        lines.pop();
    }
    Arc::new(lines)
}

fn measure(
    actions: &[u8],
    count: usize,
    destination: &str,
    padding: usize,
    separators: &[u8],
    copies: usize,
) -> (usize, usize) {
    let mut rendered = Vec::new();
    measure_with(
        actions,
        count,
        destination,
        padding,
        separators,
        copies,
        &mut |_, source| oracle(source, &mut rendered),
    )
}

#[test]
fn fence_followed_by_paragraph_has_linear_storage() {
    let (small, large) = measure_pair(&[7], 1, "unused", 1, &[3]);
    assert!(
        large.1 <= 2 * small.1 + 1024,
        "storage {small:?} -> {large:?}"
    );
    work::QUADRUPLE_TOKEN_PAGE.set(true);
    let (small, large) = measure_pair(&[7], 1, "unused", 1, &[3]);
    work::QUADRUPLE_TOKEN_PAGE.set(false);
    assert!(
        large.1 > 2 * small.1 + 1024,
        "control {small:?} -> {large:?}"
    );
}

fn maintenance(markdown: &Markdown) -> [usize; 4] {
    [
        markdown.visible_work.get(),
        markdown.reference_cache_visits,
        markdown.reference_moved_lines,
        markdown.boundary_work,
    ]
}

fn dependent_bytes(markdown: &Markdown, cached: usize, visits_before: usize) -> usize {
    if markdown.reference_cache_visits > visits_before {
        markdown
            .cached
            .get(..cached)
            .expect("retained dependency blocks")
            .iter()
            .map(|cache| cache.template.source.len())
            .sum::<usize>()
    } else {
        0
    }
}

fn accumulate(
    breakdown: &mut [usize; 11],
    parts: ([usize; 5], [usize; 5]),
    maintenance: ([usize; 4], [usize; 4]),
    growth: usize,
    consumer: usize,
) {
    for (total, (after, before)) in breakdown.iter_mut().zip(parts.0.into_iter().zip(parts.1)) {
        *total += after - before;
    }
    breakdown[1] -= growth;
    breakdown[5] += growth;
    for (total, (after, before)) in breakdown
        .iter_mut()
        .skip(6)
        .zip(maintenance.0.into_iter().zip(maintenance.1))
    {
        *total += after - before;
    }
    breakdown[10] += consumer;
}

fn measure_with(
    actions: &[u8],
    count: usize,
    destination: &str,
    padding: usize,
    separators: &[u8],
    copies: usize,
    oracle: &mut impl FnMut(&str, &str) -> Arc<Vec<Line>>,
) -> (usize, usize) {
    let mut nodes = HashSet::new();
    let mut texts = HashSet::new();
    let mut targets = HashSet::new();
    let mut markdown = Markdown::default();
    let mut total = 0;
    let mut source = String::new();
    let mut consumer = Vec::new();
    let mut expected = Arc::new(Vec::new());
    let mut peak = 0;
    let mut growth_total = 0;
    let mut breakdown = [0usize; 11];
    let mut push = |delta: &str, finish: bool| {
        let parts_before = work::parts();
        let maintenance_before = maintenance(&markdown);
        let growth_before = work::source_growth();
        let before = work::current() + maintenance_before.iter().sum::<usize>();
        let open = markdown.source.len() - markdown.offset;
        let visits_before = markdown.reference_cache_visits;
        // Definition updates reopen dependency blocks; ordinary pushes do not.
        let cached = markdown.cached.len();
        let changed = markdown.push(delta);
        let parts_after = work::parts();
        let maintenance_after = maintenance(&markdown);
        let production = work::current() + maintenance_after.iter().sum::<usize>() - before;
        assert!(changed <= markdown.lines().len());
        consumer.truncate(changed);
        consumer.extend(
            (markdown.lines().slice(changed..).unwrap_or_default())
                .iter()
                .cloned(),
        );
        let used = work::current() + maintenance_after.iter().sum::<usize>() - before;
        let dependent = dependent_bytes(&markdown, cached, visits_before);
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
        accumulate(
            &mut breakdown,
            (parts_after, parts_before),
            (maintenance_after, maintenance_before),
            growth,
            used - production,
        );
        let (payload, bytes) = retained(&markdown, &mut nodes, &mut texts, &mut targets);
        assert!(payload <= markdown.source.len() * 8 + 1024);
        peak = peak.max(bytes);
        source.push_str(delta);
        // Empty chunks have the same one-shot oracle as the preceding push.
        if !delta.is_empty() {
            expected = oracle(delta, &source);
        }
        assert!(
            consumer.as_slice() == expected.as_slice(),
            "consumer differs after {delta:?}, source {:?}",
            source.chars().take(256).collect::<String>()
        );
        finish
            .then(|| {
                markdown.fence.as_ref().map(|fence| {
                    if fence.indented {
                        "\nend\n\n".to_owned()
                    } else {
                        format!(
                            "\n{}{}\n\n",
                            fence.opening,
                            char::from(fence.marker).to_string().repeat(fence.count)
                        )
                    }
                })
            })
            .flatten()
    };
    production_stream(
        actions,
        count,
        destination,
        padding,
        separators,
        copies,
        &mut push,
    );
    let (payload, bytes) = retained(&markdown, &mut nodes, &mut texts, &mut targets);
    assert!(
        payload <= source.len() * 8 + 1024,
        "{bytes} retained for {} source bytes",
        source.len()
    );
    assert_eq!(breakdown.iter().sum::<usize>(), total);
    if std::env::var_os("GC_WORK_REPORT").is_some() {
        eprintln!(
            "copies={copies}: scan/copy/visibility/feed/parser/source-growth/visible/cache/moved/boundary/consumer {breakdown:?}; total={total}, retained={}",
            peak.max(bytes)
        );
    }
    (total, peak.max(bytes))
}

// Both repetitions share the same original prefix. Parse its oracle fixtures
// once; work/storage measurement still uses independent production renderers.
fn measure_pair(
    actions: &[u8],
    count: usize,
    destination: &str,
    padding: usize,
    separators: &[u8],
) -> ((usize, usize), (usize, usize)) {
    let mut fixtures = Vec::new();
    let mut rendered = Vec::new();
    let small = measure_with(
        actions,
        count,
        destination,
        padding,
        separators,
        1,
        &mut |delta, source| {
            let expected = oracle(source, &mut rendered);
            fixtures.push((delta.to_owned(), expected.clone()));
            expected
        },
    );
    let mut index = 0;
    let mut same = true;
    let large = measure_with(
        actions,
        count,
        destination,
        padding,
        separators,
        2,
        &mut |delta, source| {
            let fixture = fixtures.get(index);
            index += 1;
            same &= fixture.is_some_and(|(raw, _)| raw == delta);
            if same {
                fixture.expect("shared prefix fixture").1.clone()
            } else {
                oracle(source, &mut rendered)
            }
        },
    );
    (small, large)
}

fn production_stream(
    actions: &[u8],
    count: usize,
    destination: &str,
    padding: usize,
    separators: &[u8],
    copies: usize,
    push: &mut impl FnMut(&str, bool) -> Option<String>,
) {
    for index in 0..count * copies {
        for (position, action) in actions.iter().enumerate() {
            production(*action, index, padding, &mut |delta| {
                push(delta, false);
            });
            push(
                ["", " ", "\n", "!", "x"]
                    .get(usize::from(
                        *separators
                            .get(position % separators.len())
                            .expect("separator action"),
                    ))
                    .expect("separator"),
                false,
            );
        }
    }
    let definition = actions
        .iter()
        .any(|action| matches!(action, 3 | 15))
        .then(|| format!("\n\n[r]: /{destination}\n\n"));
    let closing = push(definition.as_deref().unwrap_or("\n\n"), true);
    // Preserve the original prefix checks, then actually exercise late
    // resolution even when repetition left the first definition inside code.
    if let Some(closing) = closing {
        push(&closing, false);
        if let Some(definition) = definition {
            push(&definition, false);
        }
    }
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
        34 => {
            push("***");
            push("\n");
        }
        _ => push("  \n"),
    }
}

type Sequence = (Vec<u8>, usize, String, usize, Vec<u8>);

fn sequence() -> impl Strategy<Value = Sequence> {
    (
        prop_oneof![
            prop::collection::vec(0u8..4, 1..5),
            prop::collection::vec(prop::sample::select(vec![4u8, 5, 8]), 1..5),
            prop::collection::vec(0u8..16, 1..5),
            prop::collection::vec(12u8..16, 1..5),
            prop::sample::select(vec![vec![12u8], vec![13], vec![14]]),
            prop::collection::vec(0u8..35, 1..5),
            prop::collection::vec(16u8..35, 1..5),
        ],
        1usize..65,
        "[a-z]{64,1024}",
        1usize..5,
        prop::collection::vec(0u8..5, 1..5),
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/sequence_work.txt")
        ))),
        ..ProptestConfig::with_cases(24)
    })]
    #[test]
    fn rendered_oracle_fixtures_equal_fresh_full_renders(
        parts in prop::collection::vec(prop::sample::select(vec![
            "text", "\r\n", "\n\n", "*em*", "**strong**", "[r]", "![a](u)",
            "[r]: /url\n\n", "- item\n", "> quote\n", "&amp;", "\0",
            "```rust\nlet x = 1;\n```\n", "| a | b |\n| - | - |\n",
        ]), 0..12),
    ) {
        let mut source = String::new();
        let mut rendered = Vec::new();
        for part in parts {
            source.push_str(part);
            let cached = oracle(&source, &mut rendered);
            prop_assert_eq!(cached.as_ref(), &full_parse(&source));
        }
    }

}

proptest! {
    #![proptest_config(ProptestConfig {
        // Keep 24 independently generated streams: eight parallel batches of three.
        cases: 8,
        max_shrink_iters: 96,
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/sequence_work.txt")
        ))),
        ..ProptestConfig::default()
    })]
    #[test]
    fn push_grammar_has_linear_work_and_storage(sequences in prop::collection::vec(sequence(), 3)) {
        let results = std::thread::scope(|scope| {
            let workers: Vec<_> = sequences.into_iter().map(|(actions, count, destination, padding, separators)| {
                scope.spawn(move || {
                    let (small, large) = measure_pair(&actions, count, &destination, padding, &separators);
                    prop_assert!(large.0 <= 2 * small.0 + 1024, "work {} -> {}", small.0, large.0);
                    prop_assert!(large.1 <= 2 * small.1 + 1024, "storage {} -> {}", small.1, large.1);
                    Ok(())
                })
            }).collect();
            workers.into_iter().map(|worker| worker.join().expect("grammar worker")).collect::<Vec<_>>()
        });
        for result in results { result?; }
    }
}

#[test]
fn grammar_property_detects_copying_the_reference_suffix() {
    let measure_pair = |copy: bool| {
        crate::reference_render::COPY_SUFFIX.set(copy);
        let small = measure(&[15, 16, 7], 7, &"x".repeat(540), 2, &[0], 1).0;
        let large = measure(&[15, 16, 7], 7, &"x".repeat(540), 2, &[0], 2).0;
        crate::reference_render::COPY_SUFFIX.set(false);
        (small, large)
    };
    let retained = measure_pair(false);
    println!("retained batch: {} -> {}", retained.0, retained.1);
    assert!(retained.1 <= 2 * retained.0 + 1024);
    let copied = measure_pair(true);
    println!(
        "copied suffix negative control: {} -> {}",
        copied.0, copied.1
    );
    assert!(copied.0 > retained.0 && copied.1 > retained.1);
}

#[test]
fn grammar_property_detects_reclassifying_the_entire_suffix() {
    let measure_pair = |replay| {
        crate::inline::REPLAY_CLASSIFIER.set(replay);
        let small = measure(&[18], 64, "unused", 1, &[0], 1).0;
        let large = measure(&[18], 64, "unused", 1, &[0], 2).0;
        crate::inline::REPLAY_CLASSIFIER.set(false);
        (small, large)
    };
    let retained = measure_pair(false);
    assert!(retained.1 <= 2 * retained.0 + 1024);
    let replayed = measure_pair(true);
    println!(
        "classifier grammar negative control: {} -> {}",
        replayed.0, replayed.1
    );
    assert!(replayed.1 > 2 * replayed.0 + 1024);
}

#[test]
fn late_reference_definition_closes_the_open_paragraph_once() {
    let input = include_bytes!("../fuzz/corpus/chunk_splits/late-reference-blank-row");
    let size = usize::try_from(u64::from_le_bytes(
        input.get(..8).unwrap().try_into().unwrap(),
    ))
    .unwrap();
    let source = std::str::from_utf8(input.get(8 + size..).unwrap()).unwrap();
    let destination = source.split_once("[r]: /").unwrap().1.trim_end();
    let small = measure(&[3], 42, destination, 4, &[0], 1);
    let large = measure(&[3], 42, destination, 4, &[0], 2);
    assert!(large.0 <= 2 * small.0 + 1024);
    assert!(large.1 <= 2 * small.1 + 1024);
}

#[test]
fn storage_property_detects_retaining_frozen_operation_keys() {
    let pair = |keep| {
        crate::inline_scan::KEEP_KEYS.set(keep);
        let small = measure(&[17, 26], 50, "unused", 3, &[2, 1], 1).1;
        let large = measure(&[17, 26], 50, "unused", 3, &[2, 1], 2).1;
        crate::inline_scan::KEEP_KEYS.set(false);
        (small, large)
    };
    let retained = pair(false);
    assert!(retained.1 <= 2 * retained.0 + 1024);
    let redundant = pair(true);
    assert!(redundant.1 > 2 * redundant.0 + 1024);
}

#[test]
fn mixed_styles_and_html_have_linear_total_work() {
    let small = measure(&[17, 28], 57, "unused", 3, &[2, 1], 1);
    let large = measure(&[17, 28], 57, "unused", 3, &[2, 1], 2);
    assert!(large.0 <= 2 * small.0 + 1024);
    assert!(large.1 <= 2 * small.1 + 1024);
}

#[test]
fn image_reference_list_work_is_linear() {
    let (small, large) = measure_pair(&[9, 5, 6], 13, "unused", 2, &[1, 1, 1, 4]);
    assert!(large.0 <= 2 * small.0 + 1024, "{} -> {}", small.0, large.0);
    assert!(large.1 <= 2 * small.1 + 1024);
}

#[test]
fn paragraph_and_table_work_is_linear() {
    // Large enough that reparsing each table from its start exceeds the
    // allowance below.
    let (small, large) = measure_pair(&[13, 14], 2, "unused", 2, &[1]);
    assert!(large.0 <= 2 * small.0 + 1024, "{} -> {}", small.0, large.0);
    assert!(large.1 <= 2 * small.1 + 1024);
    crate::stream_table::REJECT_TABLE_PREFIX.set(true);
    let (small, large) = measure_pair(&[13, 14], 2, "unused", 2, &[1]);
    crate::stream_table::REJECT_TABLE_PREFIX.set(false);
    assert!(large.0 > 2 * small.0 + 1024, "{small:?} -> {large:?}");
}

#[test]
fn empty_reference_context_storage_is_linear() {
    let (small, large) = measure_pair(&[4, 5, 5, 5], 11, "unused", 1, &[1, 3, 3, 2]);
    assert!(large.1 <= 2 * small.1 + 1024, "{small:?} -> {large:?}");
    work::EAGER_RENDER_PAGE.set(true);
    let (small, large) = measure_pair(&[4, 5, 5, 5], 11, "unused", 1, &[1, 3, 3, 2]);
    work::EAGER_RENDER_PAGE.set(false);
    assert!(large.1 > 2 * small.1 + 1024);
}

#[test]
fn image_entities_and_list_work_is_linear() {
    for count in [16, 20] {
        let (small, large) = measure_pair(&[9, 11, 11, 6], count, "unused", 4, &[1, 4, 2, 1]);
        assert!(large.0 <= 2 * small.0 + 1024, "{small:?} -> {large:?}");
        assert!(large.1 <= 2 * small.1 + 1024);
    }
}

#[test]
fn image_link_entities_and_list_work_is_linear() {
    let (small, large) = measure_pair(&[9, 12, 11, 6], 5, "unused", 4, &[1, 4, 2, 1]);
    assert!(large.0 <= 2 * small.0 + 1024, "{small:?} -> {large:?}");
    assert!(large.1 <= 2 * small.1 + 1024);
    crate::reference_render::COMPILE_PROVISIONAL.set(true);
    crate::references::REPARSE_DEFINITIONS.set(true);
    crate::render::CLONE_CONTEXT.set(true);
    crate::RESCAN_NEWLINE.set(true);
    crate::line_buffer::COPY_HISTORY.set(true);
    let broken = std::panic::catch_unwind(|| {
        let (small, large) = measure_pair(&[9, 12, 11, 6], 5, "unused", 4, &[1, 4, 2, 1]);
        assert!(large.0 <= 2 * small.0 + 1024, "{small:?} -> {large:?}");
    });
    crate::reference_render::COMPILE_PROVISIONAL.set(false);
    crate::references::REPARSE_DEFINITIONS.set(false);
    crate::render::CLONE_CONTEXT.set(false);
    crate::RESCAN_NEWLINE.set(false);
    crate::line_buffer::COPY_HISTORY.set(false);
    assert!(broken.is_err());
}

#[test]
fn entities_and_list_work_is_linear() {
    for (actions, count) in [
        ([11, 32, 11, 6], 1),
        ([11, 32, 11, 6], 11),
        ([11, 34, 11, 6], 23),
    ] {
        let (small, large) = measure_pair(&actions, count, "unused", 4, &[1, 4, 2, 1]);
        assert!(large.0 <= 2 * small.0 + 1024, "{small:?} -> {large:?}");
        assert!(large.1 <= 2 * small.1 + 1024);
    }
}

#[test]
fn code_rule_and_list_work_is_linear() {
    let (small, large) = measure_pair(&[16, 34, 11, 6], 1, "unused", 4, &[1, 4, 2, 1]);
    assert!(large.0 <= 2 * small.0 + 1024, "{small:?} -> {large:?}");
    assert!(large.1 <= 2 * small.1 + 1024);
    crate::RESCAN_NEWLINE.set(true);
    crate::line_buffer::COPY_HISTORY.set(true);
    let broken = std::panic::catch_unwind(|| {
        let (small, large) = measure_pair(&[16, 34, 11, 6], 1, "unused", 4, &[1, 4, 2, 1]);
        assert!(large.0 <= 2 * small.0 + 1024);
    });
    crate::RESCAN_NEWLINE.set(false);
    crate::line_buffer::COPY_HISTORY.set(false);
    assert!(broken.is_err());
}

#[test]
fn local_definitions_share_the_document_expansion_budget() {
    let (small, large) = measure_pair(&[15, 3], 31, &"x".repeat(880), 1, &[1, 3]);
    assert!(large.0 <= 2 * small.0 + 1024, "{small:?} -> {large:?}");
    assert!(large.1 <= 2 * small.1 + 1024);
    let prefix = "word ![](u)\n\n![][r]\n\n [x][r]!".repeat(62);
    let definition = format!("\n\n[r]: /{}\n\n", "x".repeat(880));
    let expected = full_parse(&format!("{prefix}{definition}"));
    for unchecked in [false, true] {
        crate::events::UNCHECKED_LOCAL_BUDGET.set(unchecked);
        let mut markdown = Markdown::default();
        markdown.push(&prefix);
        markdown.push(&definition);
        crate::events::UNCHECKED_LOCAL_BUDGET.set(false);
        assert_eq!(markdown.lines() == expected, !unchecked);
    }
}

#[test]
fn repeated_references_with_late_definitions_share_snapshot_contents() {
    for destination in [
        "x".repeat(942),
        "x".repeat(1000),
        include_str!("../fuzz/corpus/streaming/late-reference-destination").to_owned(),
    ] {
        let (small, large) = measure_pair(&[3], 28, &destination, 4, &[1, 4, 3, 0]);
        assert!(large.0 <= 2 * small.0 + 1024, "{small:?} -> {large:?}");
    }
}
