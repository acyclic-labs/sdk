use super::*;

#[path = "fixtures/mixed.rs"]
mod mixed;

fn work(chunk: usize) -> (usize, usize) {
    // 1024 copies of a 1 KiB record of every construct; doubling exposes any
    // per-push cost that grows with the document.
    let source = mixed::document(2 * 1024 * 1024);
    let (small, remainder) = source.split_at(1024 * 1024);
    let mut totals = Vec::new();
    let before = work::current();
    let mut markdown = Markdown::default();
    let mut first = None;
    // The 1 MiB run is exactly the prefix of the 2 MiB run. Retain its
    // counter checkpoint instead of executing that identical fixture twice.
    for part in [small, remainder] {
        for delta in part.as_bytes().chunks(chunk) {
            let stable = markdown.stable_lines.min(markdown.lines().len());
            let old = work::current()
                + markdown.visible_work.get()
                + markdown.reference_cache_visits
                + markdown.reference_moved_lines
                + markdown.boundary_work;
            let growth = work::source_growth();
            let open = markdown.source.len() - markdown.offset;
            let delta = std::str::from_utf8(delta).expect("ASCII mixed fixture");
            let changed = markdown.push(delta);
            let used = work::current()
                + markdown.visible_work.get()
                + markdown.reference_cache_visits
                + markdown.reference_moved_lines
                + markdown.boundary_work
                - old;
            assert!(changed >= stable, "an append changed finalized output");
            // Source buffer relocation is prepaid amortized work, as in the grammar property.
            let amortized = used - (work::source_growth() - growth) + 2 * delta.len();
            assert!(
                amortized <= 128 * (open + delta.len() + 1) + 256,
                "{amortized} units for {open} open + {} chunk bytes",
                delta.len()
            );
            if markdown.stable_lines > 0 {
                let spans = &markdown.lines().first().expect("stable first row").spans;
                if let Some(first) = &first {
                    assert!(spans.same_snapshot(first), "a finalized line was replaced");
                } else {
                    first = Some(spans.clone());
                }
            }
        }
        totals.push(
            work::current() - before
                + markdown.visible_work.get()
                + markdown.reference_cache_visits
                + markdown.reference_moved_lines
                + markdown.boundary_work,
        );
    }
    let [small, large]: [usize; 2] = totals.try_into().expect("paired totals");
    (small, large)
}

#[test]
fn mixed_document_total_work_is_linear() {
    std::thread::scope(|scope| {
        let workers: Vec<_> = [1, 7, 4096]
            .into_iter()
            .map(|chunk| {
                scope.spawn(move || {
                    let (small, large) = work(chunk);
                    println!("chunk {chunk}: 1 MiB / 2 MiB use {small} / {large} units");
                    assert!(
                        large * 10 <= small * 22,
                        "chunk {chunk}: {small} -> {large} work"
                    );
                })
            })
            .collect();
        for worker in workers {
            worker.join().expect("mixed work counter");
        }
    });
}

#[test]
fn finalized_history_does_not_change_append_parser_work() {
    let mut previous = None;
    for bytes in [1024, 1024 * 1024] {
        let mut markdown = Markdown::default();
        markdown.push(&mixed::document(bytes));
        let mut work = Vec::new();
        for delta in ["next **bold** [ref]\n\n", "[ref]: /url\n\n"] {
            let before = references::PARSED_BYTES.get();
            markdown.push(delta);
            work.push(references::PARSED_BYTES.get() - before);
        }
        if let Some(previous) = previous {
            assert_eq!(work, previous, "finalized history was parsed again");
        }
        previous = Some(work);
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/linear_work.txt")
        ))),
        ..ProptestConfig::with_cases(32)
    })]
    #[test]
    fn mixed_document_matches_every_random_chunk_boundary(
        bytes in 1usize..4096,
        chunks in prop::collection::vec(1usize..128, 1..16),
    ) {
        let source = mixed::document(bytes);
        let mut markdown = Markdown::default();
        let mut consumer = Vec::new();
        let mut offset = 0;
        for size in chunks.iter().cycle() {
            if offset == source.len() { break; }
            let end = (offset + size).min(source.len());
            let changed = markdown.push(source.get(offset..end).expect("ASCII chunk"));
            prop_assert!(changed <= markdown.lines().len());
            consumer.truncate(changed);
            consumer.extend((markdown.lines().slice(changed..).expect("changed boundary")).iter().cloned());
            prop_assert_eq!(&consumer, &full_parse(source.get(..end).expect("ASCII prefix")));
            offset = end;
        }
    }
}
