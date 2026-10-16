use super::*;
use std::{collections::HashSet, sync::Arc};

fn retained(markdown: &Markdown) -> (usize, usize) {
    let mut targets = HashSet::new();
    let mut bytes = markdown.source.capacity() + markdown.visibility.retained();
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

fn measure(
    actions: &[u8],
    count: usize,
    destination: &str,
    padding: usize,
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
            "consumer differs after {delta:?}"
        );
    };
    for index in 0..count * copies {
        for action in actions {
            production(*action, index, padding, &mut push);
        }
    }
    if actions.iter().any(|action| matches!(action % 16, 3 | 15)) {
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
    match action % 16 {
        0 => push("word "),
        1 => push("*em* **strong** ~~gone~~ "),
        2 => push("[label](u) "),
        3 => push("[x][r] "),
        4 => push("![](u)\n\n"),
        5 => {
            push(&format!("![][i{index:08x}]\n\n"));
            push(&format!("[i{index:08x}]: /u\n\n"));
        }
        6 => {
            push(&format!("\n\n-{}parent\n\n", " ".repeat(padding)));
            push(&format!("{}child\n\n", " ".repeat(padding + 3)));
        }
        7 => {
            push("\n\n```rust\n");
            push("let x = 1;\n");
            push("```\n\n");
        }
        8 => push("\n\n"),
        9 => push("![alt](u) "),
        10 => push("[outer [inner](u)](v) "),
        11 => push("a &amp; b "),
        12 => push("[label](u)"),
        13 => push("word\n"),
        14 => {
            if index == 0 {
                push("| a | b |\n|---|---|\n");
            }
            push("| x | y |\n");
        }
        _ => {
            push("word ![](u)\n\n");
            push("![][r]\n\n");
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/sequence_work.txt")
        ))),
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
        ],
        count in 1usize..65,
        destination in "[a-z]{64,1024}",
        padding in 1usize..5,
    ) {
        let small = measure(&actions, count, &destination, padding, 1);
        let large = measure(&actions, count, &destination, padding, 2);
        prop_assert!(large.0 <= 2 * small.0 + 1024, "work {} -> {}", small.0, large.0);
        prop_assert!(large.1 <= 2 * small.1 + 1024, "storage {} -> {}", small.1, large.1);
    }
}
