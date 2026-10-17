use super::*;
use std::sync::Arc;

#[test]
fn inline_boundary_work_is_linear() {
    let work = |count| {
        let source = format!("{}\n\n", "[x](u)".repeat(count));
        let mut markdown = Markdown::default();
        markdown.push(&source);
        assert_eq!(markdown.lines(), full_parse(&source));
        assert!(markdown.boundary_work <= source.len() * 3);
        println!(
            "{count} inline links: {} boundary units",
            markdown.boundary_work
        );
        markdown.boundary_work
    };
    assert!(work(4096) * 10 <= work(2048) * 22);
}

#[test]
fn link_destination_storage_is_linear() {
    let work = |count| {
        let source = format!(
            "[{}](https://example.com/{})\n\n",
            "a *b* ".repeat(count),
            "u".repeat(count)
        );
        let mut markdown = Markdown::default();
        markdown.push(&source);
        assert_eq!(markdown.lines(), full_parse(&source));
        let mut targets: Vec<Arc<str>> = Vec::new();
        for span in markdown.lines().iter().flat_map(|line| &line.spans) {
            if let Some(target) = &span.link
                && !targets.iter().any(|previous| Arc::ptr_eq(previous, target))
            {
                targets.push(target.clone());
            }
        }
        let retained: usize = targets.iter().map(|target| target.len()).sum();
        assert!(retained <= source.len());
        assert_eq!(targets.len(), 1);
        println!("{count} emphasis pairs: {retained} destination bytes");
        retained
    };
    assert!(work(2048) * 10 <= work(1024) * 22);
}

#[test]
fn invisible_history_work_is_linear() {
    let work = |count| {
        let mut markdown = Markdown::default();
        for _ in 0..count {
            markdown.push("![](u)\n\n");
            assert!(markdown.lines().is_empty());
        }
        let work = markdown.visible_work.get();
        println!("{count} invisible chunks: {work} line visits");
        assert!(work <= count * 4, "{work} visits for {count} chunks");
        markdown.push("visible");
        assert_eq!(
            markdown.lines(),
            full_parse(&format!("{}visible", "![](u)\n\n".repeat(count)))
        );
        work
    };
    assert!(work(2048) * 10 <= work(1024) * 22);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn visible_checkpoint_matches_chunkings(
        count in 1usize..24,
        cuts in prop::collection::vec(1usize..16, 1..6),
    ) {
        let source = format!("{}![][r]\n\n[r]: /url\n\nvisible\n\n{}", "![](u)\n\n".repeat(count), "![](u)\n\n".repeat(count));
        check_chunks(&source, &cuts);
    }
}

#[test]
fn ordinary_budget_growth_keeps_the_visibility_boundary() {
    let visits = |rescan| {
        let mut markdown = Markdown::default();
        markdown.push(&format!("{}word", "![](u)\n\n".repeat(12_501)));
        markdown.push(" warm-up");
        crate::reference_render::RESCAN_BUDGET.set(rescan);
        let before = markdown.visible_work.get();
        for _ in 0..64 {
            markdown.push("x");
        }
        let used = markdown.visible_work.get() - before;
        crate::reference_render::RESCAN_BUDGET.set(false);
        assert_eq!(
            markdown
                .lines()
                .last()
                .expect("visible row")
                .spans
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>(),
            format!("word warm-up{}", "x".repeat(64)),
        );
        used
    };
    assert_eq!(visits(false), 0);
    assert!(visits(true) >= 64);
}
