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
    std::thread::scope(|scope| {
        for batch in fixtures.chunks(fixtures.len().div_ceil(workers)) {
            scope.spawn(move || {
                let sources: Vec<_> = batch.iter().map(String::as_str).collect();
                check_rewrites(&sources);
            });
        }
    });
}

pub(super) fn check_rewrites(sources: &[&str]) {
    let mut previous = String::new();
    let mut expected = Vec::new();
    for source in sources {
        let source = format!("retained archive\n\n{source}");
        let shared = previous
            .bytes()
            .zip(source.bytes())
            .take_while(|(a, b)| a == b)
            .count();
        expected.truncate(shared + 1);
        for end in expected.len()..=source.len() {
            expected.push(source.get(..end).map(full_parse));
        }
        for cut in source.char_indices().map(|(index, _)| index) {
            let mut markdown = Markdown::default();
            markdown.push(source.get(..cut).unwrap());
            let mut rows = markdown.lines().to_vec();
            for character in source.get(cut..).unwrap().chars() {
                let changed = markdown.push(&character.to_string());
                rows.truncate(changed);
                rows.extend_from_slice(markdown.lines().get(changed..).unwrap());
                let end = markdown.source.len();
                assert_eq!(
                    rows,
                    expected
                        .get(end)
                        .and_then(Option::as_ref)
                        .unwrap()
                        .as_slice(),
                    "cut {cut}, end {end}: {source:?}"
                );
            }
        }
        previous = source;
    }
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
