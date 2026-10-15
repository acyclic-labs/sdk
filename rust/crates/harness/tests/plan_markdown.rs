//! Markdown task-list parsing and canonical round trips.

use acyclic_harness::client::{TaskState, parse_task_list};
use proptest::prelude::*;

#[test]
fn checked_in_fuzz_corpus() {
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fuzz/corpus/plan_markdown");
    for (name, count) in [
        ("lifecycle", Some(3)),
        ("malformed", None),
        ("fenced", Some(1)),
        ("unicode", Some(3)),
    ] {
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
            markdown.push_str(&format!("- {} Task {text}\n", state.marker()));
        }
        let parsed = parse_task_list(&markdown).unwrap();
        let mut rendered = String::from("# Plan\n\n");
        for item in parsed.items() {
            rendered.push_str(&format!("- {} {}\n", item.state.marker(), item.text));
        }
        prop_assert_eq!(rendered, markdown);
    }

    #[test]
    fn arbitrary_utf8_is_total_and_borrows_only_original_text(markdown in any::<String>()) {
        if let Some(parsed) = parse_task_list(&markdown) {
            for item in parsed.items() {
                prop_assert!(!item.text.trim().is_empty());
                prop_assert!(markdown.contains(item.text));
            }
        }
    }
}
