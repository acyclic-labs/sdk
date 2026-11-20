//! Markdown task-list parsing and canonical round trips.

use acyclic_harness_client::{TaskList, TaskState, parse_task_list};
use proptest::prelude::*;

#[test]
fn checked_in_fuzz_corpus() {
    let directory =
        acyclic_test_support::source_path("rust/crates/harness-fuzz/corpus/plan_markdown");
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
    assert_eq!(
        items[0].text.as_ptr(),
        markdown
            .get(offset..)
            .expect("offset is a char boundary")
            .as_ptr()
    );
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
    fn indented_and_fenced_code_never_becomes_work(
        text in "[a-zA-Z0-9 ]{1,80}",
        fence in prop_oneof![Just('`'), Just('~')],
        length in 4usize..8,
    ) {
        for markdown in [format!("    - [ ] {text}\n"), format!("\t- [~] {text}\n"), format!("```md\n* [x] {text}\n```\n")] {
            prop_assert!(parse_task_list(&markdown).is_none());
        }
        // A fence closes only with the same delimiter, at least as long, and
        // with no info string; anything else stays code.
        let outer = fence.to_string().repeat(length);
        let other = if fence == '`' { '~' } else { '`' };
        for inner in [
            other.to_string().repeat(length),
            fence.to_string().repeat(length - 1),
            format!("{outer}not a closing fence"),
        ] {
            let markdown = format!("{outer}md\n{inner}\n- [?] {text}\n{outer}\n- [x] Real\n");
            let items: Vec<_> = parse_task_list(&markdown)
                .expect("a real task follows the fence")
                .items()
                .collect();
            prop_assert_eq!(items.len(), 1);
            prop_assert_eq!(&*items[0].text, "Real");
        }
    }

    // A file is a task list only when every checkbox line is a valid item:
    // one malformed marker rejects the whole file, and text without a marker
    // is never a list.
    #[test]
    fn malformed_markers_reject_the_whole_file(
        text in "[^\\[\\]\\n\\r]{0,80}",
        tail in "[^\\s\\[\\]\\n\\r]{0,80}",
        state in "[^ xX~\\[\\]\\n\\r]{1,6}",
        prefix in prop::sample::select(vec!["- ", "* ", "+ ", "1. "]),
    ) {
        prop_assert!(parse_task_list(&text).is_none());
        for malformed in [
            format!("{prefix}[{state}] {text}"),
            format!("{prefix}[ ]"),
            format!("{prefix}[x]   "),
            format!("{prefix}[~]{tail}"),
        ] {
            let markdown = format!("- [ ] Valid\n{malformed}\n");
            prop_assert!(parse_task_list(&markdown).is_none(), "{markdown:?}");
        }
    }

    // An escaped or entity-encoded marker is literal text, never a task.
    #[test]
    fn escaped_and_entity_markers_are_literal(text in "[^\\n]{0,40}") {
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
                let markdown = format!("{prefix}{marker} {text}\n");
                prop_assert!(parse_task_list(&markdown).is_none(), "{markdown:?}");
            }
        }
    }
}
