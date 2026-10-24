use super::*;
use crate::{TranscriptSearch, transcript_matches};

fn closed<const EAGER: bool>() {
    for bytes in [1000, 10000, 100000] {
        let mut source = WrappedLines::default();
        source.update(&[Line::raw("x".repeat(bytes))], 0, 80);
        if EAGER {
            source.search_index();
        }
        source.view(Top::Tail, 10);
        source.reflow(40);
        source.view(Top::Tail, 10);
        source.append_line("tail", Style::default());
        assert!(
            source.index.get().is_none(),
            "closed Find retains no folded source"
        );
    }
}

#[test]
fn ingestion_append_paint_and_resize_do_not_create_search_storage() {
    closed::<false>();
}

#[test]
#[should_panic(expected = "closed Find retains no folded source")]
fn negative_control_eager_index_retains_transcript_sized_storage() {
    closed::<true>();
}

fn reopen<const RETAIN: bool>() {
    let mut source = WrappedLines::default();
    let mut search = TranscriptSearch::default();
    source.update(&[Line::raw("Straße Σ hello")], 0, 80);
    source.wrap_all();
    search.update(&source, "STRASSE", 0, Style::default());
    search.prepare(&source, 0, 10);
    while search.pending() {
        search.advance(&source, 256);
    }
    assert_eq!(search.count(), 1);
    assert!(source.index.get().is_some());
    search.project(&source, false);
    if !RETAIN {
        source.clear_search_index();
    }
    assert!(
        source.index.get().is_none(),
        "closing Find releases the index"
    );
    source.append_line(" STRASSE", Style::default());
    source.view(Top::Tail, 10);
    search.update(&source, "strasse", 0, Style::default());
    search.prepare(&source, 0, 10);
    while search.pending() {
        search.advance(&source, 256);
    }
    assert_eq!(
        search.count(),
        transcript_matches(source.text(), "strasse").len()
    );
    source.append_line(" strasse", Style::default());
    source.view(Top::Tail, 10);
    search.update(&source, "strasse", 0, Style::default());
    while search.pending() {
        search.advance(&source, 256);
    }
    assert_eq!(search.count(), 3, "an open index follows appended source");
}

#[test]
fn closing_and_reopening_find_rebuilds_current_unicode_source() {
    reopen::<false>();
}

#[test]
#[should_panic(expected = "closing Find releases the index")]
fn negative_control_closed_find_retains_its_old_index() {
    reopen::<true>();
}

#[test]
#[ignore = "manual folded-index lifetime measurement"]
fn lazy_search_storage_measurement() {
    for bytes in [1000, 10000, 100000] {
        let mut source = WrappedLines::default();
        source.replace(&[Line::raw("x".repeat(bytes))], 0);
        eprintln!(
            "CLIENT_INDEX_AFTER source_bytes={bytes} closed_indices={}",
            usize::from(source.index.get().is_some())
        );
        let allocation = acyclic_test_support::measure(|| {
            source.search_index();
        });
        source.clear_search_index();
        eprintln!(
            "CLIENT_INDEX_OPEN source_bytes={bytes} allocations={} allocated_bytes={} closed_indices={}",
            allocation.count_total,
            allocation.bytes_total,
            usize::from(source.index.get().is_some())
        );
    }
}
