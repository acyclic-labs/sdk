use super::*;
use proptest::prelude::*;

fn partition(text: &str, cuts: &[bool]) {
    let mut stream = StreamText::default();
    let mut rows = Vec::new();
    let mut start = 0;
    for end in 1..=text.len() {
        if end != text.len() && !cuts.get(end - 1).copied().unwrap_or(false) {
            continue;
        }
        let changed = stream.push(&text.as_bytes()[start..end]);
        start = end;
        rows.truncate(changed);
        rows.extend(stream.rows(changed));
    }
    let mut unsplit = StreamText::default();
    let changed = unsplit.push(text.as_bytes());
    assert_eq!(rows, unsplit.rows(changed));
    assert_eq!(stream.work, text.len(), "stable bytes are parsed once");
}

#[test]
fn provisional_archive_row_is_invalidated() {
    let seed = include_str!("../../../../harness-fuzz/corpus/ui_prompt/utf8-archive-row-repair");
    let text = format!("retained archive\n\n{seed}");
    let mut cuts = vec![false; text.len()];
    cuts[text.len() - 3] = true;
    partition(&text, &cuts);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn byte_partitions_preserve_rows_and_linear_work(
        text in "[a-z日é€🙂\\n*# ]{1,40}",
        cuts in prop::collection::vec(any::<bool>(), 0..200)
    ) { partition(&format!("retained archive\n\n{text}"), &cuts); }
}

fn fragmented_text_cost(old: bool) -> (Line<'static>, acyclic_test_support::AllocationInfo) {
    let mut markdown = Markdown::default();
    markdown.push("first");
    markdown.push("second");
    let row = markdown.lines().first().expect("paragraph row");
    let span = row.spans.iter().next().expect("canonical run");
    assert!(matches!(span.text.as_str(), std::borrow::Cow::Owned(_)));
    let mut rendered = Line::default();
    let cost = acyclic_test_support::measure(|| {
        rendered = if old {
            Line::from(
                row.spans
                    .iter()
                    .map(|span| {
                        Span::styled(
                            super::super::display_text(&span.text.as_str()).into_owned(),
                            Style::default(),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        } else {
            line(row)
        };
    });
    (rendered, cost)
}

#[test]
fn fragmented_text_conversion_preserves_the_flattened_allocation() {
    let [(baseline, before), (candidate, after)] =
        acyclic_test_support::differential::pair::<_, _, (&str, usize)>(
            || (),
            |(), old| {
                let (text, cost) = fragmented_text_cost(old);
                acyclic_test_support::differential::record(("display", text.spans.len()));
                (text, cost)
            },
        );
    assert_eq!(before, after);
    assert_eq!(baseline.0, candidate.0);
    assert_eq!(candidate.0, Line::raw("firstsecond"));
    eprintln!(
        "conversion baseline={:?} candidate={:?}",
        baseline.1, candidate.1
    );
    conversion_cost(candidate.1);
    assert_eq!((baseline.1.count_total, baseline.1.bytes_total), (6, 382));
    assert_eq!(candidate.1.count_total + 1, baseline.1.count_total);
    assert_eq!(candidate.1.bytes_total + 11, baseline.1.bytes_total);
}

fn conversion_cost(cost: acyclic_test_support::AllocationInfo) {
    assert_eq!(
        (cost.count_total, cost.bytes_total),
        (5, 371),
        "fragmented row conversion"
    );
}

#[test]
#[should_panic(expected = "fragmented row conversion")]
fn fragmented_text_allocation_control_detects_second_copy() {
    conversion_cost(fragmented_text_cost(true).1);
}

#[test]
fn every_partition_repairs_same_length_utf8() {
    let provisional = String::from_utf8_lossy(&[0xe6]).into_owned();
    let repaired = "日";
    assert_ne!(
        format!("{provisional}{}", &repaired[provisional.len()..]),
        repaired,
        "the former length-only cursor misses this repair"
    );
    let text = "日a€";
    for mask in 0..(1usize << (text.len() - 1)) {
        let cuts: Vec<_> = (0..text.len() - 1)
            .map(|bit| mask & (1 << bit) != 0)
            .collect();
        partition(text, &cuts);
        partition(&format!("retained archive\n\n{text}"), &cuts);
    }
}

#[test]
fn incomplete_utf8_continues_the_current_markdown_row() {
    partition("a日", &[true, true, true]);
}

#[test]
fn archived_query_cannot_steal_the_active_task_selection() {
    use crate::{
        client::ClientViews,
        task_volume::{Component, Query, TaskVolume},
    };
    use acyclic_stream::{MemoryStream, StreamPath};
    use std::sync::Arc;
    futures::executor::block_on(async {
        let volume = TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("selection").unwrap(),
        )
        .await
        .unwrap();
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: "active".into(),
        };
        let mut views = ClientViews::new(
            StreamPath::new("root").unwrap(),
            crate::client::ClientViewLimits {
                agents: 1,
                selected: 1,
                overview_values: 16,
                overview_bytes: 4096,
                query: crate::client::QueryViewLimits {
                    files: 16,
                    segments: 16,
                    bytes: 4096,
                },
            },
        )
        .unwrap();
        // Choose an archived key ordered before the active query in the real
        // selection index, reproducing the previous first-query implementation.
        let query = Query::TaskView {
            task: Component::new("active").unwrap(),
            recent: 2,
            reasoning: true,
            details: true,
            terminal: true,
        };
        let snapshot = volume.snapshot(&query, 16).await.unwrap();
        let active_digest = snapshot.cursor.query;
        views.query_snapshot(query, snapshot, 16, 4096).unwrap();
        let mut inserted = false;
        for number in 0..32 {
            let query = Query::Task(Component::new(format!("archived{number}")).unwrap());
            let snapshot = volume.snapshot(&query, 16).await.unwrap();
            if snapshot.cursor.query < active_digest {
                views.query_snapshot(query, snapshot, 16, 4096).unwrap();
                inserted = true;
                break;
            }
        }
        assert!(inserted);
        let check = |chosen: Option<(&crate::client::FileQueryView, &Component)>| {
            assert_eq!(chosen.unwrap().1.as_str(), "active");
        };
        check(selection::selected::<true>(&views, &owner));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(
                selection::selected::<false>(&views, &owner)
            )))
            .is_err()
        );
    });
}
