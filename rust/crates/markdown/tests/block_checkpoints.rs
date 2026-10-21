use super::*;

fn check_rewrites(sources: &[&str]) {
    for source in sources {
        let source = format!("retained archive\n\n{source}");
        for cut in source.char_indices().map(|(index, _)| index) {
            let mut markdown = Markdown::default();
            markdown.push(&source[..cut]);
            let mut rows = markdown.lines().to_vec();
            for character in source[cut..].chars() {
                let changed = markdown.push(&character.to_string());
                rows.truncate(changed);
                rows.extend_from_slice(&markdown.lines()[changed..]);
                let end = markdown.source.len();
                assert_eq!(
                    rows,
                    full_parse(&source[..end]),
                    "cut {cut}, end {end}: {source:?}"
                );
            }
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
