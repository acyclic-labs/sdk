use super::*;
use crate::{Editor, Layout};
use proptest::prelude::*;
use ratatui::text::Line;

// Old eager wrapping, initialized from source strings independently of the
// candidate's decoder, source table, layout cache, or selected window.
fn eager(source: &[Vec<Line<'_>>; 3], width: u16) -> (Vec<String>, Vec<TranscriptAnchor>) {
    let mut rows = Vec::new();
    let mut anchors = Vec::new();
    for (part, lines) in source.iter().enumerate() {
        for (line, text) in lines.iter().enumerate() {
            let text: String = text
                .spans
                .iter()
                .map(|span| span.content.as_ref())
                .collect();
            let layout = Layout::new(&Editor::new(&text), usize::from(width));
            let mut row = 0;
            for (at, wrapped) in layout.rows.iter().enumerate() {
                if wrapped.cells.is_empty() && at > 0 {
                    let previous = layout.rows.get(at - 1).unwrap();
                    if previous.end == wrapped.start
                        && previous
                            .cells
                            .last()
                            .is_some_and(|cell| cell.column + cell.width == layout.width)
                    {
                        continue;
                    }
                }
                rows.push(
                    wrapped
                        .cells
                        .iter()
                        .map(|cell| cell.symbol.as_str())
                        .collect(),
                );
                anchors.push(TranscriptAnchor {
                    origin: None,
                    part: TranscriptPart::at(part),
                    position: RowPosition { line, row },
                });
                row += 1;
            }
        }
    }
    (rows, anchors)
}

fn rank(anchor: Option<TranscriptAnchor>, positions: &[TranscriptAnchor], maximum: usize) -> usize {
    let Some(anchor) = anchor else {
        return maximum;
    };
    let last = positions
        .iter()
        .rposition(|position| {
            position.part == anchor.part && position.position.line == anchor.position.line
        })
        .unwrap();
    let first = positions
        .iter()
        .position(|position| {
            position.part == anchor.part && position.position.line == anchor.position.line
        })
        .unwrap();
    (first + anchor.position.row.min(last - first)).min(maximum)
}

fn shown(parts: &[WrappedLines; 3]) -> Vec<String> {
    parts
        .iter()
        .flat_map(|part| part.shown())
        .map(|row| row.spans.iter().map(|span| span.content.as_ref()).collect())
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // Ordered moves, part crossings, zero-sized views, width changes, and tail
    // appends have exactly the same visible text and source identity as eager
    // wrapping with global row clamping. Production never computes that rank.
    #[test]
    fn composed_windows_equal_independent_eager_navigation(
        reasoning in prop::collection::vec("[a-z日🙂 ]{0,35}",0..10),
        answer in prop::collection::vec("[a-z日🙂 ]{0,35}",0..10),
        tools in prop::collection::vec("[a-z日🙂 ]{0,35}",0..10),
        width in 1u16..20, height in 0usize..10, busy in any::<bool>(), reasoning_visible in any::<bool>(),
        motions in prop::collection::vec(-30isize..30,1..16),
    ) {
        let mut source = [reasoning,answer,tools].map(|lines|lines.into_iter().map(Line::raw).collect::<Vec<_>>());
        let mut parts: [WrappedLines;3] = std::array::from_fn(|_|WrappedLines::default());
        for (part,lines) in parts.iter_mut().zip(&source) { part.update(lines,0,width); }
        let mut anchor = None;
        let mut width = width;
        for (frame,motion) in motions.into_iter().enumerate() {
            if frame % 3 == 1 {
                width = width % 19 + 1;
                for (part,lines) in parts.iter_mut().zip(&source) { part.update(lines,lines.len(),width); }
            }
            let mut visible = source.clone();
            if !reasoning_visible { visible[0].clear(); }
            let (old, positions) = eager(&visible,width);
            let maximum = (old.len()+usize::from(busy)).saturating_sub(height);
            let expected = if height == 0 { anchor } else {
                let first = rank(anchor,&positions,maximum).saturating_add_signed(motion).min(maximum);
                (first < maximum).then(||*positions.get(first).unwrap())
            };
            let before = anchor;
            anchor = move_transcript({ let [a,b,c] = parts.each_mut(); [reasoning_visible.then_some(a),Some(b),Some(c)] },anchor,motion,height,usize::from(busy));
            prop_assert_eq!(anchor,expected);
            view_transcript({ let [a,b,c] = parts.each_mut(); [reasoning_visible.then_some(a),Some(b),Some(c)] },anchor,height,usize::from(busy));
            if height == 0 {
                prop_assert_eq!(anchor,before);
                prop_assert!(shown(&parts).is_empty());
            } else {
                let first = rank(anchor,&positions,maximum).min(old.len());
                let actual = shown(&parts);
                let expected = old.get(first..(first+height).min(old.len())).unwrap();
                prop_assert_eq!(actual.as_slice(), expected);
            }
            let changed = source[2].len();
            source[2].push(Line::raw("appended tool output"));
            parts[2].update(&source[2],changed,width);
        }
    }

    #[test]
    fn source_anchors_round_trip_without_losing_large_offsets(line in any::<usize>(), row in any::<usize>(), part in 0usize..3) {
        let anchor = TranscriptAnchor { origin: Some(super::TranscriptOrigin {file: [41;32], line}),part:TranscriptPart::at(part), position:RowPosition {line,row}};
        let bytes = serde_json::to_vec(&anchor).unwrap();
        prop_assert_eq!(serde_json::from_slice::<TranscriptAnchor>(&bytes).unwrap(),anchor);
        let mut value = serde_json::to_value(anchor).unwrap();
        value.get_mut("position").unwrap().as_object_mut().unwrap().insert("unknown".into(), serde_json::json!(true));
        prop_assert!(serde_json::from_value::<TranscriptAnchor>(value).is_err());
    }
}

#[test]
fn zero_height_does_not_drop_a_saved_source_position() {
    let source = [Line::raw("one"), Line::raw("two")];
    let mut parts: [WrappedLines; 3] = std::array::from_fn(|_| WrappedLines::default());
    parts[1].update(&source, 0, 3);
    let anchor = Some(TranscriptAnchor {
        origin: None,
        part: TranscriptPart::Answer,
        position: RowPosition { line: 1, row: 0 },
    });
    assert_eq!(
        view_transcript(parts.each_mut().map(Some), anchor, 0, 0),
        anchor
    );
    assert_eq!(
        move_transcript(parts.each_mut().map(Some), anchor, -2, 0, 0),
        anchor
    );
    // The old 'first shown row' representation erased anchors in hidden views.
    assert_ne!(
        parts
            .iter()
            .find_map(|part| (!part.shown().is_empty()).then_some(part.window().top)),
        anchor.map(|anchor| anchor.position)
    );
}

#[test]
#[should_panic(expected = "every source section participates")]
fn omitted_tool_section_fails_the_same_visible_text_assertion() {
    let mut parts: [WrappedLines; 3] = std::array::from_fn(|_| WrappedLines::default());
    parts[1].update(&[Line::raw("answer")], 0, 80);
    // Deliberately omit production's tool source; the independent oracle has it.
    view_transcript(parts.each_mut().map(Some), None, 4, 0);
    assert_eq!(
        shown(&parts),
        vec!["answer", "tool output"],
        "every source section participates"
    );
}

#[test]
fn hiding_a_section_preserves_its_source_and_index() {
    let mut parts: [WrappedLines; 3] = std::array::from_fn(|_| WrappedLines::default());
    parts[0].update(&[Line::raw("reasoning")], 0, 80);
    parts[1].update(&[Line::raw("answer")], 0, 80);
    let revision = parts[0].revision();
    view_transcript(parts.each_mut().map(Some), None, 4, 0);
    assert_eq!(shown(&parts), ["reasoning", "answer"]);
    parts[0].hide();
    let [_, answer, tools] = parts.each_mut();
    view_transcript([None, Some(answer), Some(tools)], None, 4, 0);
    assert_eq!(shown(&parts), ["answer"]);
    assert_eq!(parts[0].text(), "reasoning");
    assert_eq!(parts[0].revision(), revision);
    let measured = acyclic_test_support::measure(|| {
        view_transcript(parts.each_mut().map(Some), None, 4, 0);
    });
    assert_eq!(shown(&parts), ["reasoning", "answer"]);
    assert_eq!((measured.count_total, measured.bytes_total), (0, 0));
}

#[test]
fn seeking_into_the_last_page_follows_future_appends() {
    let mut parts: [WrappedLines; 3] = std::array::from_fn(|_| WrappedLines::default());
    let mut source = (0..20)
        .map(|line| Line::raw(format!("row {line}")))
        .collect::<Vec<_>>();
    parts[1].update(&source, 0, 80);
    let anchor = view_transcript(
        parts.each_mut().map(Some),
        Some(TranscriptAnchor {
            origin: None,
            part: TranscriptPart::Answer,
            position: RowPosition { line: 19, row: 0 },
        }),
        4,
        0,
    );
    assert_eq!(anchor, None);
    source.push(Line::raw("new tail"));
    parts[1].replace(&source, 20);
    view_transcript(parts.each_mut().map(Some), anchor, 4, 0);
    assert_eq!(shown(&parts), ["row 17", "row 18", "row 19", "new tail"]);
}

fn endpoint_jumps(broken: bool) {
    for length in [1000, 2000] {
        let mut parts: [WrappedLines; 3] = std::array::from_fn(|_| WrappedLines::default());
        parts[1].update(&vec![Line::raw("plain transcript row"); length], 0, 80);
        WrappedLines::take_work();
        let jumped = if broken {
            // Restore the former row walk instead of the endpoint shortcut.
            let last = parts[1].last_position().unwrap();
            let (first, _) = parts[1].step(last, -isize::MAX).unwrap();
            view_transcript(
                parts.each_mut().map(Some),
                Some(TranscriptAnchor {
                    origin: None,
                    part: TranscriptPart::Answer,
                    position: first,
                }),
                10,
                0,
            )
        } else {
            move_transcript(parts.each_mut().map(Some), None, -isize::MAX, 10, 0)
        };
        assert_eq!(
            jumped,
            Some(TranscriptAnchor {
                origin: None,
                part: TranscriptPart::Answer,
                position: RowPosition::default()
            })
        );
        assert_eq!(
            WrappedLines::take_work(),
            (10, 200, 10),
            "endpoint jumps wrap one page"
        );
        assert_eq!(
            move_transcript(parts.each_mut().map(Some), None, isize::MAX, 10, 0),
            None
        );
        assert_eq!(
            WrappedLines::take_work(),
            (10, 200, 10),
            "endpoint jumps wrap one page"
        );
    }
}

#[test]
fn endpoint_jumps_wrap_exactly_one_page_at_n_and_2n() {
    endpoint_jumps(false);
}
#[test]
#[should_panic(expected = "endpoint jumps wrap one page")]
fn negative_control_endpoint_row_walk_fails_the_same_work_assertion() {
    endpoint_jumps(true);
}

#[test]
fn advancing_past_the_last_source_row_reaches_an_activity_only_frame() {
    let mut parts: [WrappedLines; 3] = std::array::from_fn(|_| WrappedLines::default());
    parts[2].update(&[Line::raw("output")], 0, 1);
    let previous = move_transcript(parts.each_mut().map(Some), None, -1, 1, 1);
    assert_eq!(
        previous,
        Some(TranscriptAnchor {
            origin: None,
            part: TranscriptPart::Tools,
            position: RowPosition { line: 0, row: 5 }
        })
    );
    assert_eq!(
        move_transcript(parts.each_mut().map(Some), previous, 1, 1, 1),
        None
    );
    // The old exhausted-part branch returned the same last source row.
    assert_ne!(previous, None);
}

#[test]
fn persisted_anchor_requires_source_provenance() {
    let anchor = TranscriptAnchor {
        part: TranscriptPart::Answer,
        position: RowPosition { line: 50, row: 2 },
        origin: Some(super::TranscriptOrigin {
            file: [19; 32],
            line: 7,
        }),
    };
    let mut encoded = serde_json::to_value(anchor).unwrap();
    assert_eq!(
        serde_json::from_value::<TranscriptAnchor>(encoded.clone()).unwrap(),
        anchor
    );
    encoded.as_object_mut().unwrap().remove("origin");
    assert!(serde_json::from_value::<TranscriptAnchor>(encoded.clone()).is_err());
    encoded
        .as_object_mut()
        .unwrap()
        .insert("origin".into(), serde_json::Value::Null);
    assert!(serde_json::from_value::<TranscriptAnchor>(encoded).is_err());
}
