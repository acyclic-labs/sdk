use super::*;
use proptest::prelude::*;

fn streamed_keystroke_work(broken: bool) {
    let mut input = Editor::new("@rs");
    let mut menu = CompletionMenu::default();
    let mut best = PathMatches::new("rs");
    for index in 0..1_000_000 {
        best.push(format!("generated/{index:06}.rs"));
    }
    let paths: Vec<_> = best.paths().collect();
    menu.set_file(
        "search/results.json",
        Some(&serde_json::to_vec(&serde_json::json!({"query":"rs", "paths":paths})).unwrap()),
    )
    .unwrap();
    if broken {
        for index in 0..128 {
            // Inject the removed unbounded model directly. Production has no
            // parser accepting an individual namespace file as a candidate.
            menu.files
                .get_mut("search/results.json")
                .unwrap()
                .1
                .push(Entry {
                    kind: CompletionKind::Mention,
                    theme: false,
                    query: None,
                    item: Rc::new(Completion {
                        label: String::new(),
                        value: format!("old-{index}.rs"),
                        description: String::new(),
                        tool: false,
                        append_space: true,
                    }),
                });
        }
    }
    menu.refresh(&input);
    let started = std::time::Instant::now();
    let before_keystrokes = menu.work.candidates;
    for text in ["x", "y", "z"] {
        let before = menu.work.candidates;
        input.insert(text, 1);
        menu.refresh(&input);
        assert!(
            menu.work.candidates - before <= PATH_RESULT_COUNT,
            "keystroke scored the namespace"
        );
    }
    eprintln!(
        "volume-search benchmark component=editor-keystrokes namespace_entries=1000000 keystrokes=3 elapsed_us={} scored_candidates={}",
        started.elapsed().as_micros(),
        menu.work.candidates - before_keystrokes,
    );
}

#[test]
fn streamed_million_entry_results_bound_per_keystroke_scoring() {
    streamed_keystroke_work(false);
}

#[test]
#[should_panic(expected = "keystroke scored the namespace")]
fn negative_control_detects_namespace_candidates_in_the_editor() {
    streamed_keystroke_work(true);
}

fn mention_catalog_work(broken: bool) {
    let mut editor = Editor::new("@file");
    let mut menu = CompletionMenu::default();
    let commands: Vec<_> = (0..10_000)
        .map(|i| serde_json::json!({"name":format!("command{i}")}))
        .collect();
    menu.set_file(
        "commands/catalog.json",
        Some(&serde_json::to_vec(&commands).unwrap()),
    )
    .unwrap();
    menu.set_file(
        "search/results.json",
        Some(br#"{"query":"file","paths":["file.rs"]}"#),
    )
    .unwrap();
    let memory = acyclic_test_support::measure(|| {
        // Reproduce the removed full catalog collection independently of the
        // production mention branch, so the allocation guard has a control.
        let retained = broken.then(|| {
            menu.files
                .values()
                .flat_map(|(_, entries)| entries.iter().cloned())
                .collect::<Vec<_>>()
        });
        for text in ["x", "y", "z"] {
            editor.insert(text, 0);
            menu.refresh(&editor);
        }
        std::hint::black_box(retained);
    });
    assert!(
        memory.bytes_max < 64 * 1024,
        "mention refresh collected the command catalog"
    );
}

#[test]
fn mention_keystrokes_do_not_collect_unrelated_catalog_entries() {
    mention_catalog_work(false);
}

#[test]
#[should_panic(expected = "mention refresh collected the command catalog")]
fn negative_control_detects_collecting_the_whole_catalog() {
    mention_catalog_work(true);
}

#[test]
fn server_results_are_bound_to_the_query_that_was_scored() {
    let mut editor = Editor::new("@a");
    let mut menu = CompletionMenu::default();
    menu.set_file(
        "search/results.json",
        Some(br#"{"query":"a","paths":["abc.rs","another.rs"]}"#),
    )
    .unwrap();
    menu.refresh(&editor);
    assert_eq!(menu.items().len(), 2);
    editor.insert("b", 1);
    menu.refresh(&editor);
    assert_eq!(
        menu.items().len(),
        0,
        "an old top-k cannot stand in for a new narrower query"
    );
    menu.set_file(
        "search/results.json",
        Some(br#"{"query":"ab","paths":["abc.rs"]}"#),
    )
    .unwrap();
    assert_eq!(menu.items().len(), 1);
    menu.set_file("workspace/layout.json", Some(b"{}")).unwrap();
    menu.set_file("names/removed-static-source.rs", Some(b"{}"))
        .unwrap();
    assert_eq!(
        menu.items().len(),
        1,
        "UI state cannot enter pathname candidates"
    );
    let oversized: Vec<_> = (0..PATH_RESULT_COUNT + 1)
        .map(|i| format!("a{i}.rs"))
        .collect();
    assert!(
        menu.set_file(
            "search/results.json",
            Some(
                &serde_json::to_vec(&serde_json::json!({"query":"ab", "paths":oversized})).unwrap()
            )
        )
        .is_err()
    );
    assert_eq!(
        menu.items().len(),
        1,
        "an invalid owner publication must preserve the previous fold"
    );
}

#[test]
fn incomplete_empty_query_results_remain_visible_and_stale_progress_is_ignored() {
    let editor = Editor::new("@missing");
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor);
    menu.set_file(
        "search/progress.json",
        Some(br#"{"query":"missing","pending_dirs":7,"complete":false,"unavailable_entries":0}"#),
    )
    .unwrap();
    assert!(menu.active(&editor));
    assert_eq!(menu.indexing(), Some(7));
    menu.set_file(
        "search/progress.json",
        Some(br#"{"query":"old","pending_dirs":9,"complete":false,"unavailable_entries":0}"#),
    )
    .unwrap();
    assert_eq!(menu.indexing(), None);
    assert!(!menu.active(&editor));
}

fn unchanged_work(count: usize, repeats: usize, broken: bool) {
    let editor = Editor::new("@file");
    let mut menu = CompletionMenu::default();
    let mut best = PathMatches::new("file");
    for index in 0..count {
        best.push(format!("file{index}.rs"));
    }
    let size = best.paths().len();
    menu.set_file(
        "search/results.json",
        Some(
            &serde_json::to_vec(
                &serde_json::json!({"query":"file","paths":best.paths().collect::<Vec<_>>()}),
            )
            .unwrap(),
        ),
    )
    .unwrap();
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
    menu.set_file(
        "search/results.json",
        Some(br#"{"query":"file","paths":["file-new.rs"]}"#),
    )
    .unwrap();
    assert_eq!(menu.work.candidates - before.candidates, size + 1);
    assert_eq!(menu.work.prompt_bytes, before.prompt_bytes);
    let before = menu.work;
    menu.set_file(
        "search/results.json",
        Some(br#"{"query":"file","paths":["file-new.rs"]}"#),
    )
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
    let broken = request_from(&editor, start, &mut 0, None, None);
    assert_eq!(
        broken,
        completion_request(&editor),
        "incremental requests must match the full scan"
    );
}

/// One keystroke: type a character, Backspace, Delete, Left or Right.
fn press(editor: &mut Editor, key: usize, typed: &[&str]) {
    let cursor = editor.cursor();
    let text = editor.text();
    let left = text[..cursor]
        .char_indices()
        .next_back()
        .map_or(0, |(at, _)| at);
    let right = text[cursor..]
        .chars()
        .next()
        .map_or(cursor, |c| cursor + c.len_utf8());
    match key.checked_sub(typed.len()) {
        None => editor.insert(typed[key], 0),
        Some(0) => editor.delete(true, 0),
        Some(1) => editor.delete(false, 0),
        Some(2) => editor.set_cursor(left, false),
        _ => editor.set_cursor(right, false),
    }
}

/// Every prompt over a trigger-heavy alphabet up to four characters, every
/// cursor, then every pair of keystrokes. `forget` drops the recorded token
/// extents, which is the old cache's error: resuming inside an open token.
fn short_prompts_agree_after_two_keystrokes(forget: bool) {
    const TYPED: [&str; 5] = ["a", " ", "@", "\"", "/"];
    let mut texts = vec![String::new()];
    let mut level = texts.clone();
    for _ in 0..4 {
        level = level
            .iter()
            .flat_map(|text| TYPED.iter().map(move |typed| format!("{text}{typed}")))
            .collect();
        texts.extend(level.iter().cloned());
    }
    for text in &texts {
        for cursor in 0..=text.len() {
            for keys in 0..(TYPED.len() + 4).pow(2) {
                let mut editor = Editor::new(text);
                editor.set_cursor(cursor, false);
                let mut menu = CompletionMenu::default();
                menu.refresh(&editor);
                for key in [keys % (TYPED.len() + 4), keys / (TYPED.len() + 4)] {
                    if forget {
                        menu.marks.spans.clear();
                    }
                    press(&mut editor, key, &TYPED);
                    menu.refresh(&editor);
                    assert_eq!(
                        menu.request,
                        completion_request(&editor),
                        "incremental requests must match the full scan: {text:?} at {cursor}, keys {keys}"
                    );
                }
            }
        }
    }
}

#[test]
fn every_two_keystrokes_on_short_prompts_agree_with_full_parsing() {
    short_prompts_agree_after_two_keystrokes(false);
}

#[test]
#[should_panic(expected = "incremental requests must match the full scan")]
fn negative_control_resuming_inside_an_open_token_fails_the_property() {
    short_prompts_agree_after_two_keystrokes(true);
}

/// Prompt bytes examined by each keystroke near the end of `lines` closed
/// mention and word lines followed by `local` words. `forget` reparses the
/// whole prompt on every keystroke, as an O(prompt) completion would.
fn keystroke_work(
    lines: usize,
    local: &[usize],
    at: usize,
    keys: &[usize],
    forget: bool,
) -> Vec<usize> {
    const WORDS: [&str; 6] = ["plain", "@a.rs", "@\"a b.rs\"", "界界", "a@b", "@\"x\" "];
    const TYPED: [&str; 4] = ["a", " ", "@", "界"];
    let mut text: Vec<_> = (0..lines)
        .rev()
        .map(|line| WORDS[line % WORDS.len()])
        .collect();
    let offset = text.iter().map(|line| line.len() + 1).sum::<usize>();
    text.extend(local.iter().map(|word| WORDS[*word]));
    let text = text.join("\n");
    let mut editor = Editor::new(&text);
    let mut menu = CompletionMenu::default();
    menu.refresh(&editor);
    let starts: Vec<_> = text
        .char_indices()
        .map(|(at, _)| at)
        .filter(|at| *at >= offset)
        .collect();
    editor.set_cursor(
        starts
            .get(at % (starts.len() + 1))
            .copied()
            .unwrap_or(text.len()),
        false,
    );
    let mut work = vec![];
    for &key in keys {
        if forget {
            menu.version = None;
        }
        let before = menu.work.prompt_bytes;
        menu.refresh(&editor);
        work.push(menu.work.prompt_bytes - before);
        assert_eq!(menu.request, completion_request(&editor));
        press(&mut editor, key % (TYPED.len() + 4), &TYPED);
    }
    work
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 32, ..ProptestConfig::default() })]
    /// Each keystroke moves the cursor at most one character, so 24 keystrokes
    /// never leave the last 24 prefix lines. Identical counted work for any
    /// number of earlier lines means per-keystroke cost is independent of the
    /// prompt and depends only on the edit and the tokens around the cursor.
    #[test]
    fn keystroke_work_is_independent_of_prompt_length(
        local in prop::collection::vec(0usize..6, 0..40),
        extra in 1usize..1000,
        at in any::<usize>(),
        keys in prop::collection::vec(any::<usize>(), 1..24),
    ) {
        prop_assert_eq!(
            keystroke_work(24, &local, at, &keys, false),
            keystroke_work(24 + extra, &local, at, &keys, false)
        );
    }
}

#[test]
#[should_panic(expected = "keystroke work grew with the prompt")]
fn negative_control_full_reparse_grows_with_the_prompt() {
    assert_eq!(
        keystroke_work(24, &[1], 0, &[0, 6], true),
        keystroke_work(48, &[1], 0, &[0, 6], true),
        "keystroke work grew with the prompt"
    );
}
