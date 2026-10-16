use acyclic_harness_tui::{Editor, Motion, PromptHistory};

#[test]
fn continuation_removes_only_the_backslash_of_a_collapsed_paste() {
    let paste = format!("{}\\", "x".repeat(1100));
    let mut editor = Editor::default();
    editor.paste(&paste, 0);
    assert_eq!(editor.collapsed_pastes().len(), 1);
    assert_eq!(editor.enter(false, 1), None);
    assert_eq!(editor.text(), format!("{}\n", "x".repeat(1100)));
    editor.undo();
    assert_eq!(editor.text(), paste);
    assert_eq!(editor.collapsed_pastes().len(), 1);
    editor.redo();
    assert_eq!(editor.text(), format!("{}\n", "x".repeat(1100)));
}

#[test]
fn selecting_a_separator_or_word_never_crosses_the_other_class() {
    for (offset, selected, replacement) in [
        (0, "one", "! two"),
        (3, " ", "one!two"),
        (4, "two", "one !"),
        (7, "two", "one !"),
    ] {
        let mut editor = Editor::new("one two");
        editor.select_word(offset);
        assert_eq!(editor.selected_text(), selected);
        editor.insert("!", 0);
        assert_eq!(editor.text(), replacement);
    }
}

#[test]
fn a_megabyte_paste_is_not_retained_again_for_each_following_edit() {
    let mut editor = Editor::default();
    let paste = "x".repeat(1024 * 1024);
    editor.paste(&paste, 0);
    for i in 0..8 {
        editor.insert(" ", i + 1);
    }
    assert!(editor.retained_undo_bytes() <= paste.len() + 9 * 512);
    for _ in 0..8 {
        assert!(editor.undo());
    }
    assert_eq!(editor.text(), paste);
    assert_eq!(editor.collapsed_pastes().len(), 1);
}

#[test]
fn keymap_is_unambiguous_and_help_comes_from_the_bindings() {
    use acyclic_harness_tui::{Action, KEYMAP, keymap_help};
    let help = keymap_help(KEYMAP);
    let mut seen = std::collections::BTreeSet::new();
    for binding in KEYMAP {
        assert!(seen.insert(binding.chord), "duplicate {}", binding.chord);
        assert!(help.contains(&format!("{}\t{}\n", binding.chord, binding.help)));
    }
    assert_eq!(
        KEYMAP
            .iter()
            .find(|b| b.chord == "Ctrl+y")
            .map(|b| b.action),
        Some(Action::Yank)
    );
}

#[test]
fn wrapping_cache_survives_cursor_changes_and_invalidates_on_text_changes() {
    let mut editor = Editor::new("界abc");
    let before = editor.layout(4);
    editor.set_cursor(0, false);
    assert!(std::rc::Rc::ptr_eq(&before, &editor.layout(4)));
    let narrow = editor.layout(1);
    assert_eq!(before.width, 4);
    assert_eq!(narrow.width, 1);
    editor.insert("z", 0);
    assert_ne!(before.as_ref(), editor.layout(4).as_ref());
    editor.undo();
    assert_eq!(before.as_ref(), editor.layout(4).as_ref());
}

#[test]
fn grouping_breaks_on_time_size_whitespace_newline_and_paste() {
    for separator in [" ", "\n"] {
        let mut editor = Editor::default();
        editor.insert("a", 0);
        editor.insert("b", 100);
        editor.insert(separator, 101);
        editor.insert("c", 102);
        assert!(editor.undo());
        assert_eq!(editor.text(), format!("ab{separator}"));
        assert!(editor.undo());
        assert_eq!(editor.text(), "ab");
        assert!(editor.undo());
        assert_eq!(editor.text(), "");
    }
    let mut editor = Editor::default();
    for i in 0..21 {
        editor.insert("x", i);
    }
    editor.undo();
    assert_eq!(editor.text().len(), 20);
    editor.insert("a", 1000);
    editor.insert("b", 1301);
    editor.undo();
    assert!(editor.text().ends_with('a'));
    editor.paste("lots of\ntext", 1400);
    editor.undo();
    assert!(editor.text().ends_with('a'));
}

#[test]
fn selection_graphemes_word_delete_and_sticky_display_column() {
    let mut editor = Editor::new("a👩‍💻e\u{301}");
    editor.move_cursor(Motion::Left, true, 80);
    assert_eq!(editor.selected_text(), "e\u{301}");
    editor.insert("界", 0);
    assert_eq!(editor.text(), "a👩‍💻界");
    editor.undo();
    assert_eq!(editor.selected_text(), "e\u{301}");
    editor.move_cursor(Motion::Left, false, 80);
    assert!(editor.selection().is_empty());
    editor.delete_word(true, 1);
    assert_eq!(editor.text(), "e\u{301}");
    let mut editor = Editor::new("abcd\nx\n界界z");
    editor.set_cursor(3, false);
    editor.move_cursor(Motion::LogicalDown, false, 80);
    assert_eq!(editor.cursor(), 6);
    editor.move_cursor(Motion::LogicalDown, false, 80);
    assert_eq!(editor.cursor(), 10);
    editor.move_cursor(Motion::LogicalUp, false, 80);
    assert_eq!(editor.cursor(), 6);
    editor.move_cursor(Motion::LogicalUp, false, 80);
    assert_eq!(editor.cursor(), 3);
}

#[test]
fn kills_accumulate_and_yank_rotation_is_cancelled_by_other_actions() {
    let mut editor = Editor::new("one two three");
    editor.kill(Motion::WordLeft, 0);
    editor.kill(Motion::WordLeft, 1);
    assert_eq!(editor.text(), "one ");
    editor.yank(2);
    assert_eq!(editor.text(), "one two three");
    editor.set_cursor(0, false);
    editor.kill(Motion::LineEnd, 3);
    editor.yank(4);
    assert_eq!(editor.text(), "one two three");
    editor.rotate_yank(5);
    assert_eq!(editor.text(), "two three");
    editor.insert("!", 6);
    editor.rotate_yank(7);
    assert_eq!(editor.text(), "two three!");
}

#[test]
fn history_preserves_draft_and_reverse_search_order() {
    let mut history = PromptHistory::default();
    for text in ["first", "second", "first match"] {
        history.push(text.to_owned());
    }
    assert_eq!(history.previous("draft"), Some("first match".into()));
    assert_eq!(history.previous("ignored"), Some("second".into()));
    assert_eq!(history.next_entry(), Some("first match".into()));
    assert_eq!(history.next_entry(), Some("draft".into()));
    assert_eq!(history.reverse_search("first"), Some("first match".into()));
    assert_eq!(history.reverse_search("first"), Some("first".into()));
    assert_eq!(history.reverse_search("first"), None);
    let mut editor = Editor::new("draft");
    let mut history = acyclic_harness_tui::PromptHistory::default();
    history.push("older".into());
    assert!(!editor.recall(&mut history, true, 0));
    editor.set_cursor(0, false);
    assert!(editor.recall(&mut history, true, 0));
    editor.move_cursor(Motion::End, false, 10);
    assert!(editor.recall(&mut history, false, 0));
    assert_eq!(editor.text(), "draft");
}

#[test]
fn continuation_and_submission_expand_atomic_pastes() {
    let mut editor = Editor::new("hello\\");
    assert_eq!(editor.enter(false, 0), None);
    assert_eq!(editor.text(), "hello\n");
    editor.undo();
    assert_eq!(editor.text(), "hello\\");
    editor.enter(true, 1);
    assert_eq!(editor.text(), "hello\\\n");
    let paste = "界\r\n".repeat(600);
    editor.paste(&paste, 2);
    assert_eq!(editor.collapsed_pastes().len(), 1);
    let full = editor.text().to_owned();
    editor.delete(true, 3);
    assert_eq!(editor.text(), "hello\\\n");
    editor.undo();
    assert_eq!(editor.text(), full);
    assert_eq!(editor.enter(false, 4), Some(full));
    assert_eq!(editor.text(), "");
    editor.undo();
    assert_eq!(editor.collapsed_pastes().len(), 1);
}

#[test]
fn adjacent_unicode_edits_never_leave_invalid_fold_boundaries() {
    let mut editor = Editor::new("a");
    editor.paste(&"b".repeat(1100), 0);
    editor.insert("\u{301}", 1);
    assert!(editor.collapsed_pastes().is_empty());
    editor.undo();
    assert_eq!(editor.collapsed_pastes().len(), 1);
    editor.set_cursor(1, false);
    editor.insert("x", 2);
    assert_eq!(editor.collapsed_pastes()[0].start, 2);
}

#[test]
fn yank_rotation_preserves_neighbours_when_graphemes_join() {
    let mut editor = Editor::new("x\n\u{301}");
    editor.kill(Motion::LineStart, 0);
    editor.set_cursor(0, false);
    editor.kill(Motion::LineEnd, 1);
    editor.insert("a", 2);
    editor.yank(3);
    assert_eq!(editor.text(), "ax\n");
    editor.rotate_yank(4);
    assert_eq!(editor.text(), "a\u{301}\n");
    editor.rotate_yank(5);
    assert_eq!(editor.text(), "ax\n");
    editor.undo();
    assert_eq!(editor.text(), "a\u{301}\n");
}

#[test]
fn collapsed_paste_newlines_do_not_split_the_visible_logical_line() {
    let mut editor = Editor::new("prefix");
    editor.paste(&"a\nb\n".repeat(10), 0);
    editor.kill(Motion::LineStart, 1);
    assert_eq!(editor.text(), "");
    editor.undo();
    editor.set_cursor(0, false);
    editor.kill(Motion::LineEnd, 2);
    assert_eq!(editor.text(), "");
}
