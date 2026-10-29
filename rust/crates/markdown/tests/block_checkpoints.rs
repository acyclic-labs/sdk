use super::*;

pub(super) fn check_all_partitions(source: &str) {
    let archive = "retained archive\n\n";
    let ends: Vec<_> = source
        .char_indices()
        .map(|(index, c)| index + c.len_utf8())
        .collect();
    let expected: Vec<_> = ends
        .iter()
        .map(|end| full_parse(&format!("{archive}{}", source.get(..*end).unwrap())))
        .collect();
    for mask in 0..1usize << ends.len().saturating_sub(1) {
        let mut markdown = Markdown::default();
        markdown.push(archive);
        let mut rows = markdown.lines().to_vec();
        let mut start = 0;
        for (bit, &end) in ends.iter().enumerate() {
            if bit + 1 != ends.len() && mask & (1 << bit) == 0 {
                continue;
            }
            let changed = markdown.push(source.get(start..end).unwrap());
            start = end;
            rows.truncate(changed);
            rows.extend_from_slice(markdown.lines().get(changed..).unwrap());
            assert_eq!(
                Some(rows.as_slice()),
                expected.get(bit).map(Vec::as_slice),
                "source {source:?}, mask {mask:b}, end {end}"
            );
        }
    }
}

#[test]
fn empty_sibling_marker_preserves_list_indentation() {
    check_all_partitions(" *  a\n*\n€ a*");
    check_all_partitions(">\t* a\n> *\n>\n\na");
    check_all_partitions("- 0.\n     a\n\na");
}

#[test]
fn short_block_grammar_preserves_every_partition() {
    let lines = [
        "a", "*", "-", "+", "1.", "1)", " *", "* ", "*\t", ">", "> a", "> *", "=", "---", "```",
        "~~~",
    ];
    for first in lines {
        for second in lines {
            check_all_partitions(&format!("{first}\n{second}\na"));
        }
    }
}

#[test]
fn partial_fence_interrupts_the_previous_paragraph() {
    check_all_partitions("a\n~~~\na");
    check_all_partitions("a\n```\na");
}

#[test]
fn completed_indented_lists_and_fences_have_linear_work() {
    for block in [
        " *  a\n*\n€ a*\n\n",
        "  * \ta\n  a\n\n",
        "a\n~~~\na\n~~~\n\n",
        "> 1. a\n> 1.\n\na\n\n",
        ">\t* a\n> *\n>\n\na\n\n",
        "- 0.\n     a\n\na\n\n",
    ] {
        let measure = |count| {
            let source = block.repeat(count);
            let mut markdown = Markdown::default();
            let before = work::current();
            for character in source.chars() {
                markdown.push(&character.to_string());
            }
            let used =
                work::current() - before + markdown.boundary_work + markdown.visible_work.get();
            assert_eq!(markdown.lines(), full_parse(&source));
            used
        };
        let small = measure(128);
        let large = measure(256);
        assert!(
            large <= 2 * small + 1024,
            "{block:?}: work {small} -> {large}"
        );
    }
}

#[test]
fn list_marker_variants_preserve_every_partition() {
    let markers = ["*", "-", "+", "0.", "1.", "9.", "0)", "1)", "9)"];
    for first in markers {
        for second in markers {
            check_all_partitions(&format!(" {first} a\n{second}\na"));
        }
    }
}

#[test]
fn list_indentation_and_padding_preserve_every_cut() {
    let mut fixtures = Vec::new();
    for marker in [
        "*",
        "-",
        "+",
        "0.",
        "1.",
        "9.",
        "0)",
        "1)",
        "9)",
        "123456789.",
        "123456789)",
    ] {
        for indent in ["", " ", "  ", "   "] {
            for padding in ["", " ", "  ", "    ", "     ", "\t", " \t", "\t "] {
                for content in ["a", ""] {
                    for next in ["  a", "   a", "    a", "     a", "\ta", "*"] {
                        fixtures.push(format!("{indent}{marker}{padding}{content}\n{next}\n\na"));
                        fixtures.push(format!(
                            "> {indent}{marker}{padding}{content}\n> {next}\n>\n\na"
                        ));
                        fixtures.push(format!(
                            "- {indent}{marker}{padding}{content}\n  {next}\n\na"
                        ));
                    }
                }
            }
        }
    }

    fixtures.sort_unstable();
    fixtures.dedup();
    let workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(16);
    let work = std::thread::scope(|scope| {
        let mut jobs = Vec::new();
        for batch in fixtures.chunks(fixtures.len().div_ceil(workers)) {
            jobs.push(scope.spawn(move || {
                let sources: Vec<_> = batch.iter().map(String::as_str).collect();
                check_rewrites(&sources)
            }));
        }
        jobs.into_iter()
            .fold(RewriteWork::default(), |mut total, job| {
                let work = job.join().unwrap();
                total.initializations += work.initializations;
                total.appends += work.appends;
                total.assertions += work.assertions;
                total.peak_snapshots += work.peak_snapshots;
                total
            })
    });
    eprintln!("MARKDOWN_REWRITE_WORK {work:?}");
}

pub(super) fn check_rewrites(sources: &[&str]) -> RewriteWork {
    check_rewrite_histories(sources, true)
}

#[derive(Debug, Default)]
pub(super) struct RewriteWork {
    initializations: usize,
    appends: usize,
    assertions: usize,
    peak_snapshots: usize,
}

#[derive(Clone)]
struct RewriteCheckpoint {
    end: usize,
    markdown: Markdown,
    rows: Vec<Line>,
}

fn resume_checkpoint(
    history: &mut Vec<RewriteCheckpoint>,
    next_shared: usize,
) -> RewriteCheckpoint {
    if history.last().unwrap().end > next_shared {
        history.pop().unwrap()
    } else {
        history.last().unwrap().clone()
    }
}

#[test]
fn rewrite_checkpoint_moves_storage_when_the_next_fixture_cannot_reuse_it() {
    let mut markdown = Markdown::default();
    markdown.push(&"owned parser checkpoint ".repeat(64));
    let rows = markdown.lines().to_vec();
    let source_storage = markdown.source.as_ptr();
    let row_storage = rows.as_ptr();
    let end = markdown.source.len();
    let mut history = vec![RewriteCheckpoint {
        end,
        markdown,
        rows,
    }];
    let checkpoint = resume_checkpoint(&mut history, 0);
    assert!(
        history.is_empty(),
        "unused checkpoint retained a second owner"
    );
    assert_eq!(checkpoint.markdown.source.as_ptr(), source_storage);
    assert_eq!(checkpoint.rows.as_ptr(), row_storage);

    history.push(checkpoint);
    let mut branch = resume_checkpoint(&mut history, end);
    assert_eq!(history.len(), 1);
    assert_ne!(branch.markdown.source.as_ptr(), source_storage);
    branch.markdown.push("independent branch");
    assert_eq!(history.first().unwrap().markdown.source.len(), end);
}

fn shared_prefix(left: &str, right: &str) -> usize {
    let mut shared = left
        .bytes()
        .zip(right.bytes())
        .take_while(|(a, b)| a == b)
        .count();
    while !right.is_char_boundary(shared) {
        shared -= 1;
    }
    shared
}

fn check_rewrite_histories(sources: &[&str], reuse: bool) -> RewriteWork {
    let mut sources = sources.to_vec();
    sources.sort_unstable();
    let mut previous = String::new();
    let mut expected = Vec::new();
    // Keep only checkpoints belonging to the preceding fixture. Sorting makes
    // equal append histories adjacent; no fixture or cut is removed. Immutable
    // inline checkpoints can be shared, while each branch owns its other state.
    let mut histories: Vec<Vec<RewriteCheckpoint>> = Vec::new();
    let mut work = RewriteWork::default();
    for (index, raw) in sources.iter().enumerate() {
        let source = format!("retained archive\n\n{raw}");
        let shared = shared_prefix(&previous, &source);
        // A checkpoint is retained only when the next fixture can use it.
        // In particular, single-fixture calls need no saved parser copies.
        let next_shared = sources.get(index + 1).map_or(0, |next| {
            shared_prefix(&source, &format!("retained archive\n\n{next}"))
        });
        expected.truncate(shared + 1);
        for end in expected.len()..=source.len() {
            expected.push(source.get(..end).map(full_parse));
        }
        histories.truncate(shared + 1);
        histories.resize_with(source.len() + 1, Vec::new);
        for history in &mut histories {
            history.retain(|checkpoint| reuse && checkpoint.end <= shared);
        }
        for cut in source.char_indices().map(|(index, _)| index) {
            let history = histories.get_mut(cut).unwrap();
            if history.is_empty() {
                let mut markdown = Markdown::default();
                markdown.push(source.get(..cut).unwrap());
                work.initializations += 1;
                let rows = markdown.lines().to_vec();
                history.push(RewriteCheckpoint {
                    end: cut,
                    markdown,
                    rows,
                });
            }
            let check = |rows: &[Line], end: usize| {
                assert_eq!(
                    rows,
                    expected
                        .get(end)
                        .and_then(Option::as_ref)
                        .unwrap()
                        .as_slice(),
                    "cut {cut}, end {end}: {source:?}"
                );
            };
            // Repeat every original oracle assertion, including reused states.
            for checkpoint in history.iter().skip(1) {
                check(&checkpoint.rows, checkpoint.end);
                work.assertions += 1;
            }
            let checkpoint = resume_checkpoint(history, next_shared);
            let mut markdown = checkpoint.markdown;
            let mut rows = checkpoint.rows;
            for character in source.get(checkpoint.end..).unwrap().chars() {
                let changed = markdown.push(&character.to_string());
                work.appends += 1;
                rows.truncate(changed);
                rows.extend_from_slice(markdown.lines().get(changed..).unwrap());
                let end = markdown.source.len();
                check(&rows, end);
                work.assertions += 1;
                if end <= next_shared {
                    history.push(RewriteCheckpoint {
                        end,
                        markdown: markdown.clone(),
                        rows: rows.clone(),
                    });
                }
            }
            history.retain(|checkpoint| checkpoint.end <= next_shared);
        }
        work.peak_snapshots = work
            .peak_snapshots
            .max(histories.iter().map(Vec::len).sum());
        previous = source;
    }
    work
}

fn assert_unique_rewrite_work(sources: &[&str], work: &RewriteWork) {
    let mut initializations = HashSet::new();
    let mut appends = HashSet::new();
    let mut assertions = 0;
    let mut longest = 0;
    for source in sources {
        let source = format!("retained archive\n\n{source}");
        longest = longest.max(source.chars().count());
        for cut in source.char_indices().map(|(index, _)| index) {
            initializations.insert(source.get(..cut).unwrap().to_owned());
            for (offset, character) in source.get(cut..).unwrap().char_indices() {
                let end = cut + offset + character.len_utf8();
                appends.insert((cut, source.get(..end).unwrap().to_owned()));
                assertions += 1;
            }
        }
    }
    assert_eq!(
        work.assertions, assertions,
        "every cut/prefix oracle must run"
    );
    assert_eq!(
        (work.initializations, work.appends),
        (initializations.len(), appends.len()),
        "identical append histories were parsed again"
    );
    assert!(work.peak_snapshots <= longest * (longest + 3) / 2);
}

#[test]
fn rewrite_driver_reuses_histories_and_checks_every_original_case() {
    let sources = [
        "- € one\n  α\n\na",
        "- € two\n  β\n\na",
        "- € one\n  α\n\na",
        "- €",
    ];
    let work = check_rewrites(&sources);
    assert_unique_rewrite_work(&sources, &work);
    assert!(work.appends < work.assertions / 2, "{work:?}");
}

#[test]
#[should_panic(expected = "identical append histories were parsed again")]
fn negative_control_reparsing_shared_histories_fails_the_same_work_bound() {
    let sources = [
        "- € one\n  α\n\na",
        "- € two\n  β\n\na",
        "- € one\n  α\n\na",
        "- €",
    ];
    assert_unique_rewrite_work(&sources, &check_rewrite_histories(&sources, false));
}

pub(super) fn check_inline_cuts(sources: &[&str]) {
    check_rewrites(sources);
    for source in sources {
        let source = format!("retained archive\n\n{source}");
        let expected = full_parse(&source);
        let mut whole = Markdown::default();
        whole.push(&source);
        assert_eq!(whole.lines(), expected);
        for cut in source
            .char_indices()
            .map(|(index, _)| index)
            .chain([source.len()])
        {
            let mut markdown = Markdown::default();
            markdown.push(source.get(..cut).unwrap());
            let mut rows = markdown.lines().to_vec();
            let changed = markdown.push(source.get(cut..).unwrap());
            rows.truncate(changed);
            rows.extend_from_slice(markdown.lines().get(changed..).unwrap());
            assert_eq!(rows, expected, "two chunks at {cut}: {source:?}");
        }
    }
}

#[test]
fn empty_bullet_markers_keep_their_container_context() {
    check_rewrites(&["*\n  €\n\na", "-\n  €\n\na", "+\n  €\n\na"]);
}

#[test]
fn ordered_list_checkpoints_preserve_later_line_rewrites() {
    check_rewrites(&["1.\n   €\n\na", "7)\n   €\n\na", "para\n1. item\n\na"]);
}

#[test]
fn quote_checkpoints_preserve_later_line_rewrites() {
    check_rewrites(&[
        "> *\n>   €\n>\n\na",
        "- > quote\n  >\n\na",
        "para\n> quote\n\na",
        ">\t* a\n> *\n>\n\na",
        ">\t*\ta\n>\t*\n>\n\na",
        "-\t* a\n\t*\n\na",
    ]);
}

#[test]
fn setext_headings_remain_provisional_until_the_underline_is_complete() {
    check_rewrites(&[
        "title\n---\n\na",
        "> title\n> ===\n>\n\na",
        "title\n---x\n\na",
    ]);
}

#[test]
fn fence_delimiters_remain_provisional_until_the_line_is_complete() {
    check_rewrites(&[
        "```text\ncode\n``` trailing\n```\na",
        "- ```text\n  code\n  ```\n\na",
    ]);
}

#[test]
fn table_delimiters_and_widths_remain_provisional() {
    check_rewrites(&[
        "| a | b |\n| - | - |\n| x | y |\n\na",
        "> | a | b |\n> | - | - |\n> | x | y |\n>\n\na",
        "| a | b |\n| - | - x\n\na",
    ]);
}

#[test]
fn completed_empty_marker_items_have_linear_work() {
    let measure = |count| {
        let source = "*\n  €\n\na\n\n".repeat(count);
        let before = work::current();
        let mut markdown = Markdown::default();
        for character in source.chars() {
            markdown.push(&character.to_string());
        }
        let used = work::current() - before;
        assert_eq!(markdown.lines(), full_parse(&source));
        used
    };
    let small = measure(128);
    let large = measure(256);
    assert!(large <= 2 * small + 1024, "work {small} -> {large}");
}
