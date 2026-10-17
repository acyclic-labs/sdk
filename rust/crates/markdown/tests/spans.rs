use super::*;

fn shared_tail_append(
    copy: bool,
    frozen: bool,
) -> (Spans, Spans, usize, acyclic_test_support::AllocationInfo) {
    let mut spans = Spans::default();
    spans.push_content(
        Span {
            text: "retained".into(),
            ..Span::default()
        },
        false,
    );
    if frozen {
        spans.freeze();
    }
    let snapshot = spans.clone();
    let suffix = Span {
        text: " suffix".into(),
        ..Span::default()
    };
    COPY_SHARED_TAIL.set(copy);
    let before = crate::work::parts()[1];
    let allocations = acyclic_test_support::measure(|| spans.push_content(suffix, false));
    let copied = crate::work::parts()[1] - before;
    COPY_SHARED_TAIL.set(false);
    (spans, snapshot, copied, allocations)
}

#[test]
fn appending_after_a_snapshot_never_copies_the_shared_tail() {
    for frozen in [false, true] {
        let previous_work =
            std::cell::Cell::new((0, acyclic_test_support::AllocationInfo::default()));
        let current_work =
            std::cell::Cell::new((0, acyclic_test_support::AllocationInfo::default()));
        let ((actual, snapshot), _) = acyclic_test_support::differential(
            || {
                let (spans, snapshot, copied, allocations) = shared_tail_append(true, frozen);
                previous_work.set((copied, allocations));
                (spans.to_vec(), snapshot.to_vec())
            },
            || {
                let (spans, snapshot, copied, allocations) = shared_tail_append(false, frozen);
                current_work.set((copied, allocations));
                (spans.to_vec(), snapshot.to_vec())
            },
        );
        let (copied, allocations) = current_work.get();
        let (redundant, previous_allocations) = previous_work.get();
        assert_eq!(allocations.count_total, if frozen { 2 } else { 3 });
        assert_eq!(previous_allocations.count_total, if frozen { 2 } else { 4 });
        assert_eq!(
            copied,
            if frozen {
                std::mem::size_of::<Arc<Node>>()
            } else {
                0
            }
        );
        assert_eq!(
            redundant,
            std::mem::size_of::<Contents>()
                + if frozen {
                    0
                } else {
                    std::mem::size_of::<Span>()
                }
        );
        assert_eq!(
            actual
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>(),
            "retained suffix"
        );
        assert_eq!(snapshot.first().unwrap().text.as_str(), "retained");
    }
}

#[test]
fn frozen_suffix_views_preserve_prefixes_and_mutations() {
    let mut all = Spans::default();
    let mut cursors = vec![all.clone()];
    for text in ["a", "界", "❤️", "z"] {
        all.push(crate::Span {
            text: text.into(),
            ..crate::Span::default()
        });
        all.freeze();
        cursors.push(all.clone());
    }
    let owned = all.to_vec();
    for (at, cursor) in cursors.iter().enumerate() {
        let mut suffix = all.suffix_after(cursor);
        assert_eq!(suffix.to_vec(), owned.get(at..).expect("suffix range"));
        if let Some(last) = suffix.last_mut() {
            last.text.push_str("!");
        }
        assert_eq!(all.to_vec(), owned);
        suffix.truncate(1);
        assert!(suffix.len() <= 1);
        assert_eq!(cursor.to_vec(), owned.get(..at).expect("prefix range"));
    }
}
use crate::Line;
use proptest::prelude::*;

#[derive(Clone)]
struct Expected {
    text: String,
    style: Style,
    link: Option<Arc<str>>,
    forced: bool,
}

fn canonical(entries: &[Expected]) -> Vec<Expected> {
    let mut result: Vec<Expected> = Vec::new();
    for entry in entries {
        if !entry.forced
            && let Some(last) = result.last_mut()
            && last.style == entry.style
            && match (&last.link, &entry.link) {
                (None, None) => true,
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                _ => false,
            }
        {
            last.text.push_str(&entry.text);
        } else {
            result.push(entry.clone());
        }
    }
    result
}

fn value(entries: &[Expected]) -> Vec<Span> {
    canonical(entries)
        .into_iter()
        .map(|entry| Span {
            text: entry.text.into(),
            style: entry.style,
            link: entry.link,
        })
        .collect()
}

fn attributes(bits: u8) -> Style {
    Style {
        foreground: (bits & 32 != 0).then_some(crate::Color(1, 2, 3)),
        bold: bits & 1 != 0,
        italic: bits & 2 != 0,
        dim: bits & 4 != 0,
        underline: bits & 8 != 0,
        strikethrough: bits & 16 != 0,
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/spans.txt")
        ))),
        ..ProptestConfig::with_cases(64)
    })]
    #[test]
    fn shared_styled_snapshots_match_owned_runs(
        actions in prop::collection::vec((0u8..9, prop::collection::vec(any::<char>(), 0..8), 0u8..64, any::<u8>()), 0..64),
    ) {
        let targets: [Arc<str>; 3] = [Arc::from("a"), Arc::from("b"), Arc::from("a")];
        let mut actual = Spans::default();
        let mut expected: Vec<Expected> = Vec::new();
        let mut saved = vec![(actual.clone(), expected.clone())];
        for (action, chars, flags, index) in actions {
            let text: String = chars.into_iter().collect();
            let style = attributes(flags);
            let link = targets.get(usize::from(index) % 4).cloned();
            match action {
                0 => {
                    actual.push(Span { text: text.as_str().into(), style, link: link.clone() });
                    expected.push(Expected { text, style, link, forced: true });
                }
                1 => {
                    let mut line = Line { spans: actual };
                    line.push(&text, style, link.as_ref());
                    actual = line.spans;
                    if !text.is_empty() {
                        expected = canonical(&expected);
                        let same = expected.last().is_some_and(|last| last.style == style
                            && match (&last.link, &link) {
                                (None, None) => true,
                                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                                _ => false,
                            });
                        if same { expected.last_mut().expect("last run").text.push_str(&text); }
                        else { expected.push(Expected { text, style, link, forced: false }); }
                    }
                }
                2 => saved.push((actual.clone(), expected.clone())),
                3 => {
                    let (snapshot, model) = saved.get(usize::from(index) % saved.len()).expect("saved snapshot");
                    actual = snapshot.clone(); expected = model.clone();
                }
                4 => {
                    expected = canonical(&expected);
                    let length = usize::from(index) % (expected.len() + 2);
                    actual.truncate(length); expected.truncate(length);
                }
                5 => {
                    actual = actual.styled(style);
                    for entry in &mut expected {
                        if entry.style.foreground.is_none() { entry.style.foreground = style.foreground; }
                        entry.style.bold |= style.bold; entry.style.italic |= style.italic;
                        entry.style.dim |= style.dim; entry.style.underline |= style.underline;
                        entry.style.strikethrough |= style.strikethrough;
                    }
                }
                6 => { actual.clear(); expected.clear(); }
                8 => {
                    expected = canonical(&expected);
                    if let Some(last) = actual.last_mut() { last.style = style; last.link = link.clone(); }
                    if let Some(last) = expected.last_mut() { last.style = style; last.link = link; }
                }
                _ => {
                    let (snapshot, model) = saved.get(usize::from(index) % saved.len()).expect("saved snapshot");
                    actual.concat(snapshot.clone()); expected.extend(model.iter().cloned());
                }
            }
            prop_assert_eq!(actual.len(), value(&expected).len());
            prop_assert_eq!(actual.to_vec(), value(&expected));
            let joined: String = actual.iter().map(|span| span.text.as_str()).collect();
            prop_assert_eq!(joined, expected.iter().map(|entry| entry.text.as_str()).collect::<String>());
            for (snapshot, model) in &saved { prop_assert_eq!(snapshot.to_vec(), value(model)); }
        }
    }
}

fn measure(count: usize, copy: bool) -> (usize, usize, usize) {
    COPY_PREFIX.set(copy);
    let mut line = Line::default();
    let mut snapshots = Vec::new();
    let before = crate::work::current();
    for index in 0..count {
        line.push("x", attributes(u8::from(!index.is_multiple_of(2))), None);
        if snapshots.len() == snapshots.capacity() {
            crate::work::copy(snapshots.len() * std::mem::size_of::<Spans>());
        }
        snapshots.push(line.spans.clone());
    }
    let work = crate::work::current() - before;
    COPY_PREFIX.set(false);
    let (mut nodes, mut texts, mut links) = (HashSet::new(), HashSet::new(), HashSet::new());
    let mut payload = 0;
    let mut retained = snapshots.capacity() * std::mem::size_of::<Spans>();
    for snapshot in snapshots {
        let (bytes, size) = snapshot.retained(&mut nodes, &mut texts, &mut links);
        payload += bytes;
        retained += bytes + size;
    }
    (work, retained, payload)
}

#[test]
fn span_snapshot_work_and_retained_bytes_are_linear() {
    for count in [64, 128, 256, 1024] {
        let small = measure(count, false);
        let large = measure(2 * count, false);
        assert!(
            large.0 <= 128 * (2 * count) + 4096,
            "{} work for {} bytes",
            large.0,
            2 * count
        );
        assert!(
            large.0 <= 2 * small.0 + 4096,
            "work {} -> {}",
            small.0,
            large.0
        );
        assert!(
            large.1 <= 2 * small.1 + 4096,
            "retained {} -> {}",
            small.1,
            large.1
        );
        assert_eq!(large.2, 2 * count);
    }
}

#[test]
fn work_property_detects_copying_prefix_runs() {
    let small = measure(64, true);
    let large = measure(128, true);
    assert!(large.0 > 2 * small.0 + 4096);
    assert!(large.1 > 2 * small.1 + 4096);
}

#[test]
fn dropping_deep_span_snapshots_does_not_recurse() {
    let mut spans = Spans::default();
    for _ in 0..20_000 {
        spans.push(Span {
            text: "x".into(),
            ..Span::default()
        });
    }
    drop(spans);
}

#[test]
fn every_line_snapshot_shares_its_complete_contents() {
    let mut line = crate::Line::default();
    line.push("unfrozen tail", Style::default(), None);
    let before = crate::work::parts()[1];
    let snapshots: Vec<_> = (0..128).map(|_| line.clone()).collect();
    assert_eq!(
        crate::work::parts()[1] - before,
        128 * std::mem::size_of::<usize>()
    );
    for snapshot in &snapshots {
        assert!(Arc::ptr_eq(
            line.spans.value.as_ref().unwrap(),
            snapshot.spans.value.as_ref().unwrap()
        ));
    }
    line.spans.last_mut().unwrap().text.push_str(" changed");
    assert_eq!(
        snapshots
            .first()
            .expect("first snapshot")
            .spans
            .first()
            .unwrap()
            .text
            .as_str(),
        "unfrozen tail"
    );
    assert_eq!(
        line.spans.first().unwrap().text.as_str(),
        "unfrozen tail changed"
    );
}

#[test]
fn snapshot_copy_property_detects_materializing_contents() {
    let mut line = crate::Line::default();
    line.push("a", Style::default(), None);
    COPY_PREFIX.set(true);
    let before = crate::work::parts()[1];
    let snapshots: Vec<_> = (0..128).map(|_| line.clone()).collect();
    let copies = crate::work::parts()[1] - before;
    COPY_PREFIX.set(false);
    assert_eq!(snapshots.len(), 128);
    assert!(copies > 128 * std::mem::size_of::<usize>());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn mutable_runs_use_the_same_canonical_view(
        flags in any::<u8>(),
        text in prop::collection::vec("[a-z]{1,8}", 3),
        linked in any::<bool>(),
    ) {
        let style = attributes(flags);
        let link = linked.then(|| Arc::<str>::from("target"));
        let mut expected: Vec<_> = text.into_iter().enumerate().map(|(i, text)| Expected {
            text, style: if i == 1 { style } else { Style::default() },
            link: if i == 1 { link.clone() } else { None }, forced: false,
        }).collect();
        let mut actual = Spans::default();
        for entry in &expected { actual.push_content(Span { text: entry.text.as_str().into(), style: entry.style, link: entry.link.clone() }, false); }
        let before = actual.clone();
        let last = actual.last_mut().unwrap();
        last.style = style;
        last.link = link.clone();
        prop_assert_eq!(before.to_vec(), value(&expected));
        expected.get_mut(2).expect("last model run").style = style;
        expected.get_mut(2).expect("last model run").link = link;
        for frozen in [false, true] {
            if frozen { actual.freeze(); }
            let wanted = value(&expected);
            prop_assert_eq!(actual.len(), wanted.len());
            prop_assert_eq!(actual.last().map(SpanRef::owned), wanted.last().cloned());
            prop_assert_eq!(actual.to_vec(), wanted.clone());
            for keep in 0..=wanted.len()+1 {
                let mut shortened = actual.clone();
                shortened.truncate(keep);
                prop_assert_eq!(shortened.to_vec(), wanted.get(..keep.min(wanted.len())).expect("truncated model").to_vec());
            }
        }
    }
}

#[test]
fn mutable_run_property_detects_stale_counts() {
    let mut spans = Spans::default();
    for (text, italic) in [("a", false), ("b", true), ("c", false)] {
        spans.push_content(
            Span {
                text: text.into(),
                style: Style {
                    italic,
                    ..Style::default()
                },
                link: None,
            },
            false,
        );
    }
    spans.last_mut().unwrap().style.italic = true;
    STALE_COUNTS.set(true);
    let broken = std::panic::catch_unwind(|| assert_eq!(spans.len(), spans.iter().count()));
    STALE_COUNTS.set(false);
    assert!(broken.is_err());
    assert_eq!(spans.last().unwrap().text.as_str(), "bc");
    spans.truncate(2);
    assert_eq!(
        spans
            .iter()
            .map(|span| span.text.as_str())
            .collect::<String>(),
        "abc"
    );
}
