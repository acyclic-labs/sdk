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
    let metadata = markdown.lines.capacity() * std::mem::size_of::<Line>()
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
    let mut push = |delta: &str| {
        let before = work::current()
            + markdown.visible_work.get()
            + markdown.reference_cache_visits
            + markdown.reference_moved_lines;
        markdown.push(delta);
        total += work::current()
            + markdown.visible_work.get()
            + markdown.reference_cache_visits
            + markdown.reference_moved_lines
            - before;
        source.push_str(delta);
        assert_eq!(markdown.lines(), full_parse(&source), "after {delta:?}");
    };
    for index in 0..count * copies {
        for action in actions {
            match action % 12 {
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
                _ => push("a &amp; b "),
            }
        }
    }
    push(&format!("\n\n[r]: /{destination}\n\n"));
    let (payload, bytes) = retained(&markdown);
    assert!(
        payload <= source.len() * 8 + 1024,
        "{bytes} retained for {} source bytes",
        source.len()
    );
    (total, bytes)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn push_grammar_has_linear_work_and_storage(
        actions in prop_oneof![
            prop::collection::vec(0u8..4, 1..5),
            prop::collection::vec(prop::sample::select(vec![4u8, 5, 8]), 1..5),
            prop::collection::vec(0u8..12, 1..5),
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
