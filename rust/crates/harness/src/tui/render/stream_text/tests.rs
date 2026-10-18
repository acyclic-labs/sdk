use super::*;
use crate::task_volume::{Change, Component, FileKey, FileValue, TaskFile};
use proptest::prelude::*;

fn replaced_head(bodies: &[String], forget_prefix: bool) {
    let mut views = crate::test_support::empty_client_views();
    let task = Component::new("task").unwrap();
    let key = FileKey::Task {
        task: task.clone(),
        file: TaskFile::TextHead {
            turn: 0,
            channel: Channel::Text,
        },
    };
    let mut decoder = StreamText::default();
    let mut rows = Vec::new();
    for body in bodies {
        let mut cursor = views.selections().next().unwrap().1.cursor().clone();
        cursor.position += 1;
        cursor.accepted = cursor.position;
        cursor.durable = cursor.position;
        views
            .query_change(Change {
                cursor,
                file: Some(FileValue {
                    requester: None,
                    key: key.clone(),
                    bytes: body.as_bytes().to_vec().into(),
                    channel: Some(Channel::Text),
                    publication: None,
                }),
                files: Vec::new(),
                removed: Vec::new(),
            })
            .unwrap();
        if forget_prefix {
            decoder.prefix = None;
        }
        let task = views.selections().next().unwrap().1.task(&task);
        let changed = decoder.update(&task, Channel::Text);
        rows.truncate(changed);
        rows.extend(decoder.rows(changed));
        let actual: String = rows
            .iter()
            .flat_map(|line| line.spans.iter())
            .map(|span| span.content.as_ref())
            .collect();
        assert_eq!(
            &actual, body,
            "a replaced logical head cannot retain old text"
        );
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn logical_head_replacement_matches_current_file(bodies in prop::collection::vec("[a-z日€🙂]{1,24}",1..12)) {
        replaced_head(&bodies,false);
    }
}
#[test]
fn lost_prefix_identity_negative_control() {
    let bodies = vec!["old".into(), "new".into()];
    replaced_head(&bodies, false);
    assert!(std::panic::catch_unwind(|| replaced_head(&bodies, true)).is_err());
}

fn bounded_tool(lines: usize, chunk: usize, broken: bool) {
    let text = (0..lines)
        .map(|index| format!("line{index}\n"))
        .collect::<String>();
    let mut actual = StreamText {
        tool_limit: 20,
        ..StreamText::default()
    };
    for bytes in text.as_bytes().chunks(chunk) {
        if broken {
            actual.push(bytes);
        } else {
            actual.push_tool(bytes);
        }
    }
    let mut baseline = Markdown::default();
    baseline.push(&text.lines().take(20).collect::<Vec<_>>().join("\n"));
    assert_eq!(actual.markdown.lines(), baseline.lines());
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn tool_output_bounds_source_lines_before_markdown(lines in 0usize..40, chunk in 1usize..32) {
        bounded_tool(lines,chunk,false);
    }
}
#[test]
fn markdown_row_cap_negative_control() {
    bounded_tool(21, 3, false);
    assert!(std::panic::catch_unwind(|| bounded_tool(21, 3, true)).is_err());
}

fn separate_entries(entries: &[String], broken: bool) {
    let mut actual = StreamText::default();
    let mut rows = Vec::new();
    let mut expected = Vec::new();
    let mut bytes = 0;
    for text in entries {
        bytes += text.len();
        let changed = if broken {
            if bytes == text.len() {
                actual.len()
            } else {
                actual.push(b"\n\n")
            }
        } else {
            actual.boundary()
        };
        let changed = changed.min(actual.push(text.as_bytes()));
        rows.truncate(changed);
        rows.extend(actual.rows(changed));
        // The former per-message renderer, initialized independently. A code
        // fence, emphasis or reference definition cannot capture another turn.
        let mut old = Markdown::default();
        old.push(text);
        let old: Vec<_> = old.lines().iter().map(line).collect();
        if !old.is_empty() {
            if !expected.is_empty() {
                expected.push(Line::default());
            }
            expected.extend(old);
        }
        assert_eq!(rows, expected);
        assert_eq!(actual.work, bytes);
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn markdown_turns_have_independent_block_state(entries in prop::collection::vec(
        prop_oneof![Just("```rust\nlet a = 1;".to_owned()),Just("next answer".to_owned()),
            Just("[ref]: /a\n\n[ref]".to_owned()),"[a-z日*# ]{1,16}"],1..8)) {
        separate_entries(&entries,false);
    }
}
#[test]
fn open_fence_cannot_capture_the_next_turn_negative_control() {
    let entries = vec!["```rust\nlet a = 1;".into(), "next answer".into()];
    separate_entries(&entries, false);
    assert!(std::panic::catch_unwind(|| separate_entries(&entries, true)).is_err());
}
