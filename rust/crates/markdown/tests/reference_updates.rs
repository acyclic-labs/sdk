use super::*;

fn update(source: &str, definition: &str) -> (usize, usize) {
    let mut markdown = Markdown::default();
    markdown.push(source);
    let cached = markdown.cached.len();
    let visits = markdown.reference_cache_visits;
    let moved = markdown.reference_moved_lines;
    markdown.push(definition);
    let lines = markdown.lines.len();
    let visits = markdown.reference_cache_visits - visits;
    let moved = markdown.reference_moved_lines - moved;
    assert!(
        visits <= 2 * cached && moved <= lines,
        "{cached} caches / {lines} lines: {visits} cache visits, {moved} moved lines"
    );
    assert_eq!(
        markdown.lines(),
        full_parse(&format!("{source}{definition}"))
    );
    for pair in markdown.cached.windows(2) {
        if let [before, after] = pair {
            assert!(before.lines.end <= after.lines.start);
        }
    }
    (visits, moved)
}

#[test]
fn batch_maintenance_scales_linearly() {
    let mut previous = None;
    for count in [2048, 4096] {
        let source = "[x][a&#10;b]\n\n".repeat(count);
        let work = update(&source, "[a&#10;b]: /url\n\n");
        println!(
            "{count} blocks: {} cache visits, {} moved lines",
            work.0, work.1
        );
        if let Some((visits, moved)) = previous {
            assert!(work.0 * 10 <= visits * 22);
            assert!(work.1 * 10 <= moved * 22);
        }
        previous = Some(work);
    }
}

#[test]
fn simultaneous_replacements_preserve_unaffected_caches_and_task_context() {
    let source = "[x][a&#10;b]\n\n[untouched]\n\n- [x] [x][c&#10;d]\n\n[x][a&#10;b]\n\n";
    let definitions = "[a&#10;b]: /a\n\n[c&#10;d]: /c\n\n[untouched]: /u\n\n";
    check_chunks(&format!("{source}{definitions}"), &[1, 7, 31]);
    update(source, definitions);
    update(
        "![][a&#10;b]\n\n[unaffected]\n\n![][a&#10;b]\n\n",
        "[a&#10;b]: /empty\n\n",
    );
}

#[test]
fn batch_replacements_preserve_unchanged_line_allocations() {
    let mut markdown = Markdown::default();
    markdown.push("[x][a&#10;b]\n\n**unaffected**\n\n[x][a&#10;b]\n\n");
    let pointer = |markdown: &Markdown| {
        markdown
            .lines()
            .iter()
            .flat_map(|line| &line.spans)
            .find(|span| span.text == "unaffected")
            .map(|span| span.text.as_ptr())
    };
    let before = pointer(&markdown);
    assert!(before.is_some());
    markdown.push("[a&#10;b]: /url\n\n");
    assert_eq!(pointer(&markdown), before);
    let visits = markdown.reference_cache_visits;
    markdown.push("[unused]: /url\n\n");
    assert_eq!(markdown.reference_cache_visits, visits);
    assert_eq!(pointer(&markdown), before);
}

#[test]
fn one_batch_can_expand_and_shrink_different_blocks() {
    let source = "[outer [x][ref]](u&#10;v)\n\n[x][a&#10;b]\n\n[keep]\n\n";
    let definitions = "[ref]: /r\n\n[a&#10;b]: /a\n\n[keep]: /k\n\n";
    let mut markdown = Markdown::default();
    markdown.push(source);
    let old = markdown.cached.first().map(|cache| cache.lines.len());
    let lines = markdown.lines.len();
    markdown.push(definitions);
    assert!(markdown.cached.first().map(|cache| cache.lines.len()) > old);
    assert_eq!(markdown.lines.len(), lines);
    assert_eq!(
        markdown.lines(),
        full_parse(&format!("{source}{definitions}"))
    );
    check_chunks(&format!("{source}{definitions}"), &[1, 3, 7]);
}

#[test]
fn doubling_keeps_the_same_mix_of_affected_blocks() {
    let pattern = "[x][a&#10;b]\n\n[unaffected]\n\n[unaffected]\n\n";
    let small = update(pattern, "[a&#10;b]: /url\n\n");
    let large = update(&pattern.repeat(2), "[a&#10;b]: /url\n\n");
    assert_eq!(large, (2 * small.0, 2 * small.1));
}

#[test]
fn document_budget_growth_keeps_unrelated_open_checkpoints() {
    for (opening, delta) in [("*x* ", "*x* "), ("\n\n| x |\n|---|\n", "| x |\n")] {
        let references = "![][r]\n\n".repeat(60);
        let definition = format!("[r]: /{}\n\n", "a".repeat(2000));
        let padding = "x".repeat(100_001 - references.len() - definition.len());
        let mut markdown = Markdown::default();
        let mut consumer = Vec::new();
        let mut push = |delta: &str, bounded: bool| {
            let before = references::PARSED_BYTES.get();
            let changed = markdown.push(delta);
            let parsed = references::PARSED_BYTES.get() - before;
            if bounded {
                assert!(parsed <= 512, "{parsed} parser bytes for {delta:?}");
            }
            assert!(changed <= markdown.lines().len());
            consumer.truncate(changed);
            consumer.extend_from_slice(markdown.lines().get(changed..).unwrap_or_default());
            assert_eq!(consumer, full_parse(&markdown.source));
        };
        push(&references, false);
        push(&definition, false);
        push(&padding, false);
        push(opening, false);
        for _ in 0..100 {
            push(delta, true);
        }
        push(&format!("\n\n{}\n\n", "x".repeat(24_000)), false);
        for _ in 0..4 {
            push("![][r]\n\n", true);
        }
    }
}

fn expansion_calls(charges: &[usize], mut budget: usize) -> Vec<(usize, usize)> {
    charges
        .iter()
        .enumerate()
        .map(|(index, charge)| {
            let admitted = budget > 0;
            budget = budget.saturating_sub(*charge);
            (index, if admitted { *charge } else { usize::MAX })
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig { rng_seed: proptest::test_runner::RngSeed::Fixed(0x6d61_726b_0000_0003), ..ProptestConfig::with_cases(64) })]
    #[test]
    fn reference_budget_intervals_preserve_every_resolution(
        charges in prop::collection::vec(0usize..1024, 0..16),
        budget in 0usize..16384,
        probe in 0usize..16384,
        split in 0usize..17,
    ) {
        let calls = expansion_calls(&charges, budget);
        let (cost, interval) = references::expansion(calls.iter());
        prop_assert!(interval.contains(&budget));
        let decisions = |limit| expansion_calls(&charges, limit).iter()
            .map(|(_, cost)| *cost != usize::MAX).collect::<Vec<_>>();
        for point in [probe, interval.start, interval.start.saturating_sub(1),
            interval.end.saturating_sub(1), interval.end.min(usize::MAX - 1)] {
            prop_assert_eq!(interval.contains(&point), decisions(point) == decisions(budget));
        }
        let split = split.min(charges.len());
        let (first, rest) = charges.split_at(split);
        let (prefix, valid) = references::expansion(expansion_calls(first, budget).iter());
        let tail = expansion_calls(rest, budget.saturating_sub(prefix));
        prop_assert_eq!(references::extend_expansion(prefix, &valid, tail.iter()), (cost, interval));
    }

    #[test]
    fn reference_resize_work_is_linear(
        count in 1usize..64,
        affected in prop::collection::vec(any::<bool>(), 1..8),
        entity in prop_oneof![Just("&#10;"), Just("&#13;"), Just("&NewLine;")],
        image in any::<bool>(),
        growing in prop::collection::vec(any::<bool>(), 1..8),
    ) {
        let mut pattern = String::new();
        for index in 0..count {
            if index == 0 || affected.get(index % affected.len()).copied().unwrap_or(false) {
                if growing.get(index % growing.len()).copied().unwrap_or(false) {
                    pattern.push_str(&format!("[outer [x][ref]](u{entity}v)\n\n"));
                } else {
                    pattern.push_str(&format!("{}[x][a{entity}b]\n\n", if image { "!" } else { "" }));
                }
            } else { pattern.push_str("[unaffected]\n\n"); }
        }
        let mut previous = None;
        for copies in [1, 2] {
            let source = pattern.repeat(copies);
            let work = update(&source, &format!("[a{entity}b]: /url\n\n[ref]: /ref\n\n"));
            if let Some((visits, moved)) = previous {
                prop_assert!(work.0 * 10 <= visits * 22, "{visits} -> {} cache visits", work.0);
                prop_assert!(work.1 * 10 <= moved * 22, "{moved} -> {} moved lines", work.1);
            }
            previous = Some(work);
        }
    }
}

#[test]
fn malformed_definition_does_not_repeat_reference_parsing() {
    let source = "[x][y][x][y]\n\n";
    let mut markdown = Markdown::default();
    markdown.push(source);
    for definition in ["[y]: /target\n\n", "[outer [x][y]]: /target\n\n"] {
        let before = references::PARSED_BYTES.get();
        markdown.push(definition);
        let used = references::PARSED_BYTES.get() - before;
        assert!(
            used <= source.len() + definition.len() * 3,
            "{used} parser bytes for {definition:?}"
        );
    }
}

#[test]
fn reference_definitions_preserve_every_cut() {
    for body in [
        "[x][r]",
        "[r]",
        "[r][]",
        "![x][r]",
        "[outer [x][r]][s]",
        "[*x*][r]",
        "[x][r][x][r]",
    ] {
        for definitions in [
            "[r]: /one\n\n",
            "[r]: /one \"title\"\n\n",
            "[r]: /one\n  \"title\"\n\n",
            "[r]: /one\n\n[r]: /two\n\n",
            "[r]: /one\n\n[outer [x][r]]: /two\n\n",
            "[s]: /s\n\n[r]: /r\n\n",
        ] {
            super::block_checkpoints::check_inline_cuts(&[
                &format!("{body}\n\n{definitions}tail"),
                &format!("{definitions}{body}\n\ntail"),
            ]);
        }
    }
}

#[test]
fn pending_definition_titles_reopen_under_every_cut() {
    super::block_checkpoints::check_inline_cuts(&[
        "[x][a\\]b]\n\n[a\\]b]: /one\n  \"title\"\n\ntail",
        "[x][a b]\n\n[a b]: /one\n  \"title\"\n\ntail",
        "[r]\n\n[r]: /one\n  'title'\n\ntail",
        "[r]\n\n[r]: /one\n  (title)\n\ntail",
    ]);
}
