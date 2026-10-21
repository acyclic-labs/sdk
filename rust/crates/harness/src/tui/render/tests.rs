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

#[test]
fn provisional_list_continuation_is_invalidated() {
    let text = "retained archive\n\n*\n  €\n\na";
    // The main-gate shrinker's exact cuts (including the retained prefix).
    partition(
        text,
        &[
            false, false, true, true, true, false, false, false, true, false, true, true, true,
            true, false, true, true, false, true, true, false, false, true, true, true, false,
            true,
        ],
    );
    // Include cuts through the marker, indentation and the multibyte scalar.
    for first in 1..text.len() {
        for second in first..text.len() {
            let mut cuts = vec![false; text.len()];
            cuts[first - 1] = true;
            cuts[second - 1] = true;
            partition(text, &cuts);
        }
    }
    partition(text, &vec![true; text.len()]);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn byte_partitions_preserve_rows_and_linear_work(
        text in "[a-z日é€🙂\\n*# ]{1,40}",
        cuts in prop::collection::vec(any::<bool>(), 0..200)
    ) { partition(&format!("retained archive\n\n{text}"), &cuts); }
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
