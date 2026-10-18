//! Markdown task-list parsing and canonical round trips.

use acyclic_harness::client::{TaskList, TaskState, parse_task_list};
use proptest::prelude::*;

#[test]
fn checked_in_fuzz_corpus() {
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../harness-fuzz/corpus/plan_markdown");
    let expected = [
        ("escaped", None),
        ("fenced", Some(1)),
        ("indented", None),
        ("lifecycle", Some(3)),
        ("malformed", None),
        ("mixed", Some(5)),
        ("unicode", Some(3)),
    ];
    // Every checked-in seed has an expectation, and nothing expected is missing.
    let mut seeds = std::fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    seeds.sort();
    assert_eq!(seeds, expected.map(|(name, _)| name));
    for (name, count) in expected {
        let text = std::fs::read_to_string(directory.join(name)).unwrap();
        assert_eq!(
            parse_task_list(&text).map(|list| list.items().count()),
            count,
            "{name}"
        );
    }
}

#[test]
fn task_lists_borrow_descriptions_and_ignore_fenced_examples() {
    let markdown = "# Release\r\n\r\n- [ ] Design 🦀\r\n  - [~] Build\r\n- [X] Ship\r\n```md\r\n- [?] example\r\n```\r\n~~~\r\n- [ ] another example\r\n~~~\r\n";
    let items: Vec<_> = parse_task_list(markdown).unwrap().items().collect();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].state, TaskState::Pending);
    assert_eq!(items[0].text, "Design 🦀");
    assert_eq!(items[1].state, TaskState::InProgress);
    assert_eq!(items[2].state, TaskState::Completed);
    let offset = markdown.find("Design").unwrap();
    assert_eq!(items[0].text.as_ptr(), markdown[offset..].as_ptr());
}

#[test]
fn malformed_or_non_task_files_use_raw_text() {
    for markdown in [
        "",
        "# Plan\nNo checkboxes yet\n",
        "- [?] Unknown state\n",
        "- [ ]\n",
        "- [x]    \n",
        "- [~]No separator\n",
        "- [🦀] Unicode marker\n",
        "- [ ] Valid\n- [oops] Malformed\n",
        "```\n- [ ] Example only\n```\n",
    ] {
        assert!(parse_task_list(markdown).is_none(), "{markdown:?}");
    }
}

#[test]
fn fenced_code_closes_only_with_a_matching_delimiter_and_length() {
    for markdown in [
        "````md\n```\n- [?] still code\n````\n- [x] Real\n",
        "~~~\n```\n- [?] still code\n~~~\n- [x] Real\n",
        "```\n```not a closing fence\n- [?] still code\n```\n- [x] Real\n",
    ] {
        let items: Vec<_> = parse_task_list(markdown).unwrap().items().collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].text, "Real");
    }
}

#[test]
fn task_paragraphs_keep_their_separators() {
    for (markdown, expected) in [
        (
            "- [ ] First paragraph\n\n  Second paragraph",
            "First paragraph\n\nSecond paragraph",
        ),
        (
            "1. [~] First **paragraph**\n\n   Second `paragraph`",
            "First paragraph\n\nSecond paragraph",
        ),
        (
            "- [x] Parent\n  - [ ] Child\n\n    Second paragraph",
            "Child\n\nSecond paragraph",
        ),
    ] {
        let items: Vec<_> = parse_task_list(markdown).unwrap().items().collect();
        assert_eq!(items.last().unwrap().text, expected, "{markdown}");
    }
}

#[test]
fn escaped_and_entity_markers_remain_literal() {
    for marker in [
        r"\[~]",
        r"[\~]",
        r"[~\]",
        "&#91;~]",
        "[&#126;]",
        "[~&#93;",
        "&lbrack;~&rbrack;",
    ] {
        for prefix in ["- ", "* ", "+ ", "1. "] {
            let markdown = format!("{prefix}{marker} literal\n");
            assert!(parse_task_list(&markdown).is_none(), "{markdown}");
        }
    }
}

proptest! {
    #[test]
    fn canonical_task_lists_round_trip(
        tasks in prop::collection::vec((0u8..3, "[^\\p{C}\\r\\n]{1,80}"), 1..40)
    ) {
        let mut markdown = String::from("# Plan\n\n");
        for (state, text) in &tasks {
            let state = match state {
                0 => TaskState::Pending,
                1 => TaskState::InProgress,
                _ => TaskState::Completed,
            };
            // Descriptions must contain some non-whitespace text.
            markdown.push_str(&format!("- {} {}\n", state.marker(), escaped(&format!("Task {text}"))));
        }
        let parsed = parse_task_list(&markdown).unwrap();
        let mut rendered = String::from("# Plan\n\n");
        for item in parsed.items() {
            rendered.push_str(&format!("- {} {}\n", item.state.marker(), escaped(&item.text)));
        }
        prop_assert_eq!(rendered, markdown);
    }

    #[test]
    fn arbitrary_utf8_is_total(markdown in any::<String>()) {
        if let Some(parsed) = parse_task_list(&markdown) {
            for item in parsed.items() {
                prop_assert!(!item.text.trim().is_empty());
            }
        }
    }
}

fn escaped(text: &str) -> String {
    let mut result = String::new();
    for character in text.chars() {
        // Entities preserve descriptions' whitespace across CommonMark's
        // indentation, line-ending and hard-break normalization.
        if character.is_whitespace() {
            result.push_str(&format!("&#{};", u32::from(character)));
            continue;
        } else if character.is_ascii_punctuation() {
            result.push('\\');
        }
        result.push(character);
    }
    result
}

fn code_block_oracle(parse: for<'a> fn(&'a str) -> Option<TaskList<'a>>) -> bool {
    [
        "    - [ ] Example",
        "\t- [ ] Example",
        "```\n- [ ] Example\n```",
        "~~~\n- [~] Example\n~~~",
    ]
    .iter()
    .all(|input| parse(input).is_none())
}

#[test]
fn code_block_negative_control_rejects_indentation_blind_parsing() {
    fn indentation_blind(input: &str) -> Option<TaskList<'_>> {
        parse_task_list(input.trim_start())
    }
    assert!(code_block_oracle(parse_task_list));
    assert!(!code_block_oracle(indentation_blind));
}

#[test]
fn mixed_bullets_ordered_lists_and_inline_markdown_keep_every_task() {
    let list = parse_task_list(
        "- [ ] First\n* [x] **Second**\n+ [~] Third\n1. [ ] Fourth\n2. [x] `Fifth`\n",
    )
    .unwrap();
    assert_eq!(
        list.items()
            .map(|item| (item.state, item.text.into_owned()))
            .collect::<Vec<_>>(),
        vec![
            (TaskState::Pending, "First".into()),
            (TaskState::Completed, "Second".into()),
            (TaskState::InProgress, "Third".into()),
            (TaskState::Pending, "Fourth".into()),
            (TaskState::Completed, "Fifth".into()),
        ]
    );
}

proptest! {
    #[test]
    fn indented_and_fenced_code_never_becomes_work(text in "[a-zA-Z0-9 ]{1,80}") {
        for markdown in [format!("    - [ ] {text}\n"), format!("\t- [~] {text}\n"), format!("```md\n* [x] {text}\n```\n")] {
            prop_assert!(parse_task_list(&markdown).is_none());
        }
    }
}

#[tokio::test]
async fn plan_rows_reuse_unchanged_file_revisions_and_clip_without_collection() {
    use acyclic_harness::{
        client::FileQueryView,
        task_volume::{FileKey, Query, TaskVolume, Write},
    };
    use acyclic_stream::{MemoryStream, StreamPath};
    use futures::StreamExt as _;
    use std::sync::Arc;
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("plan-cache").unwrap(),
    )
    .await
    .unwrap();
    let mut generation = volume.snapshot(&Query::Plans, 2).await.unwrap().generation;
    for (path, markdown) in [
        ("plans/a.md", "- [ ] Same\n".repeat(1000)),
        ("plans/b.md", "- [~] Before".into()),
    ] {
        generation = volume
            .append(
                generation,
                &Write::new(FileKey::parse(path).unwrap(), markdown.into_bytes()),
            )
            .await
            .unwrap();
    }
    let snapshot = volume.snapshot(&Query::Plans, 2).await.unwrap();
    let mut changes = volume
        .follow(Query::Plans, snapshot.cursor.clone())
        .await
        .unwrap();
    let mut view = FileQueryView::from_snapshot(Query::Plans, snapshot, 2, 65_536).unwrap();
    let original = view.clone();
    for _ in 0..10 {
        assert_eq!(
            view.plan_rows().nth(1).unwrap().as_ptr(),
            original.plan_rows().nth(1).unwrap().as_ptr()
        );
        assert_eq!(
            view.plan_rows().take(2).collect::<Vec<_>>(),
            ["plans/a.md", "[ ] Same"]
        );
    }
    generation = volume
        .append(
            generation,
            &Write::new(FileKey::parse("plans/b.md").unwrap(), "- [x] After"),
        )
        .await
        .unwrap();
    view.apply(changes.next().await.unwrap().unwrap()).unwrap();
    assert_eq!(
        view.plan_rows().nth(1).unwrap().as_ptr(),
        original.plan_rows().nth(1).unwrap().as_ptr()
    );
    assert_eq!(view.plan_rows().last(), Some("[x] After"));
    assert_ne!(
        view.plan_rows().last().unwrap().as_ptr(),
        original.plan_rows().last().unwrap().as_ptr()
    );
    let before_checkpoint = view.clone();
    volume
        .append(generation, &Write::new(FileKey::Goal, "unrelated"))
        .await
        .unwrap();
    view.apply(changes.next().await.unwrap().unwrap()).unwrap();
    assert_eq!(
        view.plan_rows().last().unwrap().as_ptr(),
        before_checkpoint.plan_rows().last().unwrap().as_ptr()
    );
}
