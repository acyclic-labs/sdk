use super::REPLAY;
use crate::{Markdown, work};
use proptest::prelude::*;

fn samples(
    parts: &[&str],
    separator: &str,
    counts: &[usize],
    replay: bool,
) -> Vec<(usize, usize, [usize; 4])> {
    let mut markdown = Markdown::default();
    markdown.push("x ");
    REPLAY.set(replay);
    let before = work::rendering_work();
    let parts_before = work::rendering_parts();
    let mut bytes = 0;
    let mut snapshots = Vec::new();
    for (index, part) in parts
        .iter()
        .cycle()
        .take(*counts.last().expect("sample count"))
        .enumerate()
    {
        markdown.push(part);
        markdown.push(separator);
        bytes += part.len() + separator.len();
        if counts.contains(&(index + 1)) {
            let parts_after = work::rendering_parts();
            snapshots.push((
                work::rendering_work() - before,
                bytes,
                parts_after
                    .into_iter()
                    .zip(parts_before)
                    .map(|(after, before)| after - before)
                    .collect::<Vec<_>>()
                    .try_into()
                    .expect("counter categories"),
            ));
        }
    }
    REPLAY.set(false);
    snapshots
}

fn measure(
    parts: &[&str],
    separator: &str,
    count: usize,
    replay: bool,
) -> (usize, usize, [usize; 4]) {
    samples(parts, separator, &[count], replay)
        .pop()
        .expect("one sample")
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/inline_render_work.txt")
        ))),
        ..ProptestConfig::with_cases(24)
    })]
    #[test]
    fn inline_render_work_is_linear(
        parts in prop_oneof![prop::collection::vec(prop::sample::select(vec![
            "*", "**", "***", "_", "__", "~", "~~", "`", "``", "```",
            "[", "]", "![", "](u)", "\\*", "\\`", "\\\\", "\\", "!", "x", " ", "¡", "‐", "🦀",
            "[x](u)", "![x](u)", "[a *b*](<u> \"t\")", "![*x* ![y](v)](u)",
            "x![](u)y", "![a ![](u)b](u)", "[x][r]", "[r]: /url\n\n",
            "<https://x>", "<a@b.example>", "&amp;", "&#10;", "&#13;", "&#0;",
            "<b>", "</b>", "<!--\n x\n-->", "\0", "word\n", "`code`", "**bold**",
            "- item\n\n", "-   parent\n\n      child\n", "![][r]\n\n", "\n\n", "```\nx\n```\n",
        ]), 1..5), (
            prop::sample::select(vec!["&#10;", "&#13;", "&amp;", "[x][r]", "![x](u)", "![][r]"]),
            prop::sample::select(vec!["*", "**", "_", "~", "\\", "[", "]"]),
            prop::sample::select(vec!["<!--\n x\n-->", "<div>\n x\n</div>", "- item\n\n", "[r]: /url\n\n", "\n\n", "```text\nx\n```\n"]),
        ).prop_map(|(inline, delimiter, block)| vec![inline, delimiter, block])],
        separator in prop::sample::select(vec!["", " ", "\n", "!", "x"]),
    ) {
        // Each smaller run is an identical prefix of the largest run. Retain
        // the counter checkpoints instead of executing those prefixes again.
        let counts = [64, 128, 256, 512, 1024, 2048];
        let snapshots = samples(&parts, separator, &counts, false);
        for count in [64, 128, 256, 1024] {
            let small = *snapshots.get(counts.iter().position(|n| *n == count).unwrap()).expect("smaller checkpoint");
            let (used, bytes, breakdown) = *snapshots.get(counts.iter().position(|n| *n == 2 * count).unwrap()).expect("larger checkpoint");
            prop_assert!(used <= 128 * bytes + 4096,
                "{parts:?} separated by {separator:?}: {} chunks, {used} render units for {bytes} bytes", count * 2);
            prop_assert!(used <= 2 * small.0 + 4096,
                "{parts:?} separated by {separator:?}: {count} -> {} chunks use {} -> {used} render units; copy/visibility/feed/scan {:?} -> {breakdown:?}", count * 2, small.0, small.2);
            for category in 0..4 {
                prop_assert!(*breakdown.get(category).expect("category") <= 2 * *small.2.get(category).expect("category") + 4096,
                    "{parts:?} separated by {separator:?}: category {category}, {count} -> {} chunks use {:?} -> {breakdown:?}", count * 2, small.2);
            }
        }
    }
}

#[test]
fn rendering_work_counts_replayed_scans_and_copies() {
    let small = measure(&["`code`"], "", 128, true);
    let large = measure(&["`code`"], "", 256, true);
    assert!(small.0 > 128 * small.1 + 4096);
    assert!(large.0 > 2 * small.0 + 4096);
}

#[test]
fn rendering_work_detects_replaying_completed_rules() {
    let retained = measure(&["***"], "\n", 128, false);
    assert!(retained.0 <= 128 * retained.1 + 4096);
    crate::SKIP_RULE_CHECKPOINT.set(true);
    let replayed = measure(&["***"], "\n", 128, false);
    crate::SKIP_RULE_CHECKPOINT.set(false);
    assert!(replayed.0 > 128 * replayed.1 + 4096);
}

#[test]
fn mixed_link_labels_keep_rendering_work_linear() {
    let snapshots = samples(&["_", "[a *b*](<u> \"t\")", "~"], "x", &[1024, 2048], false);
    let (small, _, _) = *snapshots.first().expect("smaller sample");
    let (large, bytes, breakdown) = *snapshots.get(1).expect("larger sample");
    assert!(large <= 128 * bytes + 4096);
    assert!(
        large <= 2 * small + 4096,
        "{small} -> {large}: {breakdown:?}"
    );
}

#[test]
fn provisional_multiline_previews_reuse_rendered_rows() {
    let snapshots = samples(
        &["_", "&#10;"],
        "x",
        &[64, 128, 256, 512, 1024, 2048],
        false,
    );
    for pair in snapshots.windows(2) {
        assert!(
            pair.get(1).expect("larger sample").0
                <= 128 * pair.get(1).expect("larger sample").1 + 4096,
            "{pair:?}"
        );
        assert!(
            pair.get(1).expect("larger sample").0
                <= 2 * pair.first().expect("smaller sample").0 + 4096,
            "{pair:?}"
        );
    }
}

#[test]
fn rendering_work_detects_replaying_provisional_rows() {
    crate::inline_views::REPLAY.set(true);
    let small = measure(&["_", "&#10;"], "x", 64, false);
    let large = measure(&["_", "&#10;"], "x", 128, false);
    crate::inline_views::REPLAY.set(false);
    assert!(large.0 > 128 * large.1 + 4096);
    assert!(large.0 > 2 * small.0 + 4096);
}

#[test]
fn provisional_views_preserve_finalized_prefixes_and_consumer_snapshots() {
    let mut markdown = Markdown::default();
    let mut consumer = Vec::new();
    for _ in 0..8 {
        for part in ["prefix\n\n", "x "]
            .into_iter()
            .chain(["_", "x", "&#10;", "x"].into_iter().cycle().take(24))
            .chain(["\n\n"])
        {
            let previous = markdown.lines().to_vec();
            let mut expected_previous = Markdown::default();
            expected_previous.push(&markdown.source);
            let changed = markdown.push(part);
            assert_eq!(previous, expected_previous.lines());
            consumer.truncate(changed);
            consumer.extend(
                (markdown.lines().slice(changed..).unwrap_or_default())
                    .iter()
                    .cloned(),
            );
            assert_eq!(consumer, crate::tests::full_parse(&markdown.source));
        }
    }
}

#[test]
fn escaped_reference_text_does_not_relocate_backing_chunks() {
    let snapshots = samples(&["\\*", "[x][r]", "](u)"], "", &[1024, 2048], false);
    assert!(
        snapshots.get(1).expect("larger sample").0
            <= 2 * snapshots.first().expect("smaller sample").0 + 4096,
        "{snapshots:?}"
    );
}

#[test]
fn entity_emphasis_comment_previews_share_committed_rows() {
    let snapshots = samples(
        &["&#10;", "*", "<!--\n x\n-->"],
        "",
        &[64, 128, 256, 512, 1024, 2048],
        false,
    );
    if std::env::var_os("GC_WORK_REPORT").is_some() {
        println!("entity/emphasis/comment snapshots: {snapshots:?}");
    }
    for pair in snapshots.windows(2) {
        assert!(
            pair.get(1).expect("larger sample").0
                <= 2 * pair.first().expect("smaller sample").0 + 4096,
            "{pair:?}"
        );
    }
}

#[test]
fn rendering_property_detects_replaying_row_history_on_append() {
    crate::line_buffer::COPY_HISTORY.set(true);
    let snapshots = samples(
        &["&#10;", "*", "<!--\n x\n-->"],
        "",
        &[64, 128, 256, 512, 1024, 2048],
        false,
    );
    crate::line_buffer::COPY_HISTORY.set(false);
    assert!(
        snapshots
            .windows(2)
            .any(|pair| pair.get(1).expect("larger sample").0
                > 2 * pair.first().expect("smaller sample").0 + 4096)
    );
}
