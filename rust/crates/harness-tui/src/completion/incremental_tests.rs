use super::*;
use proptest::prelude::*;

fn unchanged_work(count: usize, repeats: usize, broken: bool) {
    let editor = Editor::new("@file");
    let mut menu = CompletionMenu::default();
    for index in 0..count {
        menu.set_file(&format!("workspace/file{index}.rs"), Some(b"content"))
            .unwrap();
    }
    menu.refresh(&editor);
    let before = menu.work;
    let allocations = acyclic_test_support::measure(|| {
        for _ in 0..repeats {
            if broken {
                menu.version = None;
            }
            menu.refresh(&editor);
            assert!(menu.active(&editor));
        }
    });
    assert_eq!(menu.work, before, "unchanged refresh must do zero work");
    assert_eq!(
        allocations.count_total, 0,
        "unchanged refresh must allocate nothing"
    );
    menu.set_file("workspace/file-new.rs", Some(b"new"))
        .unwrap();
    assert_eq!(menu.work.candidates - before.candidates, 1);
    assert_eq!(menu.work.prompt_bytes, before.prompt_bytes);
    let before = menu.work;
    menu.set_file("workspace/file-new.rs", Some(b"replacement"))
        .unwrap();
    assert_eq!(menu.work.candidates - before.candidates, 2);
    assert_eq!(menu.work.prompt_bytes, before.prompt_bytes);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]
    #[test]
    fn work_depends_on_changes_not_refreshes(count in 1usize..1001, repeats in 1usize..40) {
        unchanged_work(count, repeats, false);
    }
    #[test]
    fn edits_only_scan_the_active_token(prefix in 0usize..4096, edits in 1usize..24) {
        let mut editor = Editor::new(&format!("{} @a", "z".repeat(prefix)));
        let mut menu = CompletionMenu::default();
        menu.set_file("workspace/aaaaaaaaaaaaaaaaaaaaaaaa.rs", Some(b"")).unwrap();
        menu.refresh(&editor);
        for i in 0..edits {
            let before = menu.work;
            editor.insert("a", i as u64);
            menu.refresh(&editor);
            prop_assert!(menu.work.prompt_bytes - before.prompt_bytes <= 2 * (i + 4));
            prop_assert!(menu.work.candidates - before.candidates <= 1);
            prop_assert_eq!(menu.request.clone(), completion_request(&editor));
        }
    }
    #[test]
    fn plain_typing_only_examines_inserted_bytes(prefix in 0usize..4096, edits in 1usize..32) {
        let mut editor = Editor::new(&"z".repeat(prefix));
        let mut menu = CompletionMenu::default();
        menu.refresh(&editor);
        let before = menu.work;
        for i in 0..edits {
            editor.insert("z", i as u64);
            menu.refresh(&editor);
        }
        prop_assert_eq!(menu.work.prompt_bytes - before.prompt_bytes, edits);
        prop_assert_eq!(menu.request.clone(), completion_request(&editor));
    }

    #[test]
    fn delta_requests_agree_with_full_parsing(prefix in "[a-z @/\\\"]{0,48}", edits in proptest::collection::vec("[a-z @/\\\"\n]{0,6}", 1..24)) {
        let mut editor = Editor::new(&prefix);
        let mut menu = CompletionMenu::default();
        menu.set_file("workspace/a.rs", Some(b"")).unwrap();
        menu.set_file("commands/a.md", Some(b"a")).unwrap();
        menu.refresh(&editor);
        for (i, text) in edits.iter().enumerate() {
            editor.insert(text, i as u64);
            menu.refresh(&editor);
            prop_assert_eq!(menu.request.clone(), completion_request(&editor));
            if editor.undo() {
                menu.refresh(&editor);
                prop_assert_eq!(menu.request.clone(), completion_request(&editor));
                editor.redo(); menu.refresh(&editor);
                prop_assert_eq!(menu.request.clone(), completion_request(&editor));
            }
        }
    }

    #[test]
    fn batched_edits_and_cursor_moves_agree_with_full_scan(
        prefix in "[a-z @/\\\"\n]{0,48}",
        batches in prop::collection::vec(prop::collection::vec(
            (0u8..8, "[a-z @/\\\"\n界]{0,6}", any::<u16>()), 1..8), 1..24)
    ) {
        let mut editor = Editor::new(&prefix);
        let mut menu = CompletionMenu::default();
        menu.set_file("workspace/src/main.rs", Some(b"")).unwrap();
        menu.set_file("commands/theme.md", Some(b"theme")).unwrap();
        menu.refresh(&editor);
        for (step, batch) in batches.iter().enumerate() {
            for (op, text, offset) in batch {
                match op {
                    0 => editor.insert(text, step as u64),
                    1 => editor.delete(true, step as u64),
                    2 => editor.delete(false, step as u64),
                    3 | 7 => editor.set_cursor(usize::from(*offset) % (editor.text().len()+1), *op == 7),
                    4 => { editor.undo(); }
                    5 => { editor.redo(); }
                    _ => editor.set_text(text, step as u64),
                }
            }
            menu.refresh(&editor);
            prop_assert_eq!(menu.request.clone(), completion_request(&editor));
        }
    }

}

#[test]
fn short_quoted_prefixes_agree_after_every_append() {
    let mut prefixes = vec![String::new()];
    for _ in 0..7 {
        let mut next = Vec::new();
        for prefix in &prefixes {
            for suffix in ["a", " ", "@", "\""] {
                let mut editor = Editor::new(prefix);
                let mut menu = CompletionMenu::default();
                menu.refresh(&editor);
                editor.insert(suffix, 0);
                menu.refresh(&editor);
                assert_eq!(
                    menu.request,
                    completion_request(&editor),
                    "{prefix:?} + {suffix:?}"
                );
                next.push(format!("{prefix}{suffix}"));
            }
        }
        prefixes = next;
    }
}

#[test]
fn dismissal_without_refresh_invalidates_the_prefix_checkpoint() {
    let mut editor = Editor::new("long plain prefix");
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor);
    editor.set_text("@a", 0);
    menu.dismiss(&editor);
    editor.set_text("@b", 1);
    menu.refresh(&editor);
    assert_eq!(menu.request, completion_request(&editor));
}

#[test]
#[should_panic(expected = "unchanged refresh must do zero work")]
fn negative_control_detects_repeated_scanning() {
    unchanged_work(1000, 2, true);
}

#[test]
#[should_panic(expected = "incremental requests must match the full scan")]
fn full_scan_oracle_detects_a_cursor_before_the_cached_token() {
    let mut editor = Editor::new("@src @src");
    let start = completion_request(&editor).unwrap().range.start;
    editor.insert("/", 0);
    editor.set_cursor(4, false);
    let broken = request_from(&editor, start, &mut 0, None);
    assert_eq!(
        broken,
        completion_request(&editor),
        "incremental requests must match the full scan"
    );
}
