use super::*;
use proptest::prelude::*;

#[path = "inline_chunks.rs"]
mod inline_chunks;

#[path = "linear_work.rs"]
mod linear_work;

#[path = "sequence_work.rs"]
mod sequence_work;

#[path = "maintenance_work.rs"]
mod maintenance_work;

#[path = "reference_updates.rs"]
mod reference_updates;

#[path = "whitespace.rs"]
mod whitespace;

#[path = "adversarial_work.rs"]
mod adversarial_work;

#[path = "inline_hold.rs"]
mod inline_hold;

pub(super) fn full_parse(source: &str) -> Vec<Line> {
    // CommonMark normalizes CRLF and CR to LF before block parsing.
    let source = if source.contains('\r') {
        std::borrow::Cow::Owned(source.replace("\r\n", "\n").replace('\r', "\n"))
    } else {
        std::borrow::Cow::Borrowed(source)
    };
    let mut lines = Vec::new();
    for (_, events) in blocks(Parser::new_ext(&source, options()).into_offset_iter()) {
        lines.extend(render::render_styled(&events, Style::default()));
        lines.push(Line::default());
    }
    while lines.last().is_some_and(|line| line.spans.is_empty()) {
        lines.pop();
    }
    lines
}

#[test]
fn all_newline_forms_render_the_same_code_and_following_paragraph() {
    let lf = "```rust\nlet x = 1;\n```\n\nend";
    let expected = full_parse(lf);
    let text = expected
        .iter()
        .map(|line| {
            line.spans
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(text, "┌─ rust\n│ let x = 1;\n└─\n\nend");
    for newline in ["\n", "\r", "\r\n"] {
        let mut markdown = Markdown::default();
        for character in lf.replace('\n', newline).chars() {
            markdown.push(&character.to_string());
        }
        assert_eq!(markdown.lines(), expected);
    }
}

#[test]
fn incremental_frontier_matches_full_commonmark_parse() {
    for source in [
        include_str!("fixtures/grammar.md"),
        include_str!("fixtures/selection.md"),
        "  ~~~~rust\n  let café = 1;\n  ~~~~\n\nend",
        "```rust\r\n/* comment\r\nstill */\r\n```\r\nend",
        "```rust\r/* comment\rstill */\r```\rend",
        "```rust\nlet x = 1;\n````\n\nend",
        "```rust\nlet x = 1;\n``` trailing\n```\nend",
        "```rust\nlet x = 1;\n```\u{a0}\nstill code\n```\nend",
        "```rust\nlet x = 1;\n```\t\nend",
        "[r]\n\n[r]: /url\n\n[r]",
        "[r]: /old\n\n# Heading\n\n[r]: /new\n\n[r]",
        "[SS]: /case\n\n# Heading\n\nparagraph\n\n[ß]",
        "[a b]: /one\n\n[a\u{a0}b]: /two\n\n[a b] [a\u{a0}b]",
        "<div>\nhello\n</div>\n\nend",
        "[a][b]\n\n[a]: /one\n\n[b]: /two\n\nend",
        "- ```rust\n  let x = 1;\n  ```\n- second\n",
        "*a [r] b*\n\n[r]: /url\n\nend",
        "> [r]\n> extra\n>\n\n[r]: /url\n\nend",
        "- [r]\n- other\n\n[r]: /url\n\nend",
        "| [r] | b |\n| - | - |\n| 1 | 2 |\n\n[r]: /url\n\nend",
        "[a][b  \nc]\n\nStable\n\n[b c]: /url\n\nend",
        "- ```text\n  code\n  ```\nend",
        "- - ```text\n    code\n    ```\n    after\n",
        "- > ```text\n  > code\n  > ```\n  > after\n",
        "[r]\n\n- parent\n  - child\n\n    [r]: /nested\n\n    more\n",
        "[r]\n\n> quote\n>\n> [r]: /quoted\n>\n> more\n",
        "```rust\n**bold****bold**    indented\n\n  ",
        "```\n| a | b |\n    ",
        "    indented\n\n    more\n\nend",
        "\tindented\n  \n\tmore\nend",
        "    indented\n      \n    more\nend",
        "- parent\n\n      indented\n\n      more\nend",
        ">     indented\n>\n>     more\nend",
        "paragraph &copy; &copyleft; &amp; word; (word) #hash",
        "paragraph [word](url) (word); [r] (word); [r]: /url",
        "# paragraph word # word #\n\ntext\n1. next\n+ next",
        "[r][x](url)[r]: url\ntext 🦀> quote\n- item\n- item\ntext 🦀\n| a | b |\n| - | - |\n[r]: url\n\n[r]: u",
        "[r][x][y](url) more\n\n[r]: /r\n\n[y]: /y\n\n[x]: /x\n\nend",
        "- item\n<div>\nhello\n</div>\n",
        "  ```text\n\tfoo\n  ```\n\nend",
        "   ```text\n \tfoo\n   ```\n\nend",
        "- [x] task [r]\n\n[r]: /url\n\n",
        "3. [ ] task [r]\n\n   more\n\n[r]: /url\n\n",
    ] {
        let mut markdown = Markdown::default();
        let mut end = 0;
        for character in source.chars() {
            let delta = character.to_string();
            markdown.push(&delta);
            end += delta.len();
            let prefix = source.get(..end).unwrap_or_default();
            assert_eq!(markdown.lines(), full_parse(prefix), "prefix {prefix:?}");
        }
    }
}

#[test]
fn unused_definitions_never_parse_or_highlight_finalized_fences() {
    let mut markdown = Markdown::default();
    markdown.push(&format!("```rust\n{}```\n\n", "let x = 1;\n".repeat(1000)));
    let first = markdown
        .lines()
        .get(1)
        .and_then(|line| line.spans.first())
        .and_then(|span| span.text.allocation());
    let highlighted = code::HIGHLIGHTED.get();
    let before = markdown.parsed_bytes;
    for index in 0..100 {
        markdown.push(&format!("[unused{index}]: /url\n\n"));
    }
    assert_eq!(code::HIGHLIGHTED.get(), highlighted);
    assert!(markdown.parsed_bytes - before < 4096);
    assert_eq!(
        first,
        markdown
            .lines()
            .get(1)
            .and_then(|line| line.spans.first())
            .and_then(|span| span.text.allocation())
    );
}

#[test]
fn completed_nested_fences_have_parser_and_lexer_checkpoints() {
    for lines in [100, 10_000] {
        let source = format!(
            "- parent\n\n  ```rust\n{}  ```\n",
            "  let x = 1;\n".repeat(lines)
        );
        let mut markdown = Markdown::default();
        markdown.push(&source);
        let highlighted = code::HIGHLIGHTED.get();
        let before = markdown.parsed_bytes;
        markdown.push("- next\n");
        assert_eq!(code::HIGHLIGHTED.get(), highlighted);
        assert!(markdown.parsed_bytes - before < 64);
        assert_eq!(markdown.lines(), full_parse(&format!("{source}- next\n")));
    }
}

#[test]
fn long_plain_paragraph_appends_only_visit_the_delta() {
    for length in [1000, 100_000] {
        let mut markdown = Markdown::default();
        markdown.push(&"x".repeat(length));
        let before = markdown.parsed_bytes;
        for delta in ["a", " ", " ", "b", "c"] {
            markdown.push(delta);
        }
        assert_eq!(markdown.parsed_bytes, before);
        assert_eq!(
            markdown.lines(),
            full_parse(&format!("{}a  bc", "x".repeat(length)))
        );
    }
}

#[test]
fn styled_paragraph_text_reuses_the_inline_checkpoint() {
    for prefix in ["**bold** text ", "*unfinished text ", "# heading text "] {
        let mut markdown = Markdown::default();
        markdown.push(&format!("{prefix}{}", "x".repeat(100_000)));
        let before = markdown.parsed_bytes;
        for delta in ["a", " ", " ", "b", "c"] {
            markdown.push(delta);
        }
        assert_eq!(markdown.parsed_bytes, before);
        assert_eq!(
            markdown.lines(),
            full_parse(&format!("{prefix}{}a  bc", "x".repeat(100_000)))
        );
    }
}

#[test]
fn ordinary_append_does_not_visit_committed_reference_definitions() {
    for definitions in [100, 10_000] {
        let mut markdown = Markdown::default();
        for index in 0..definitions {
            markdown.push(&format!("[r{index}]: /url\n\n"));
        }
        markdown.push("unfinished ");
        let before = markdown.parsed_bytes;
        for _ in 0..100 {
            markdown.push("x");
        }
        assert_eq!(markdown.parsed_bytes, before);
    }
    let source: String = (0..10_000)
        .map(|index| format!("[r{index}]: /url\n\n"))
        .collect();
    let mut markdown = Markdown::default();
    markdown.push(&format!("{source}a *unfinished "));
    let before = markdown.parsed_bytes;
    for _ in 0..100 {
        markdown.push("x");
    }
    assert_eq!(markdown.parsed_bytes, before);
    let work = markdown.references.work;
    markdown.push("!");
    assert!(markdown.parsed_bytes - before < 128);
    assert_eq!(markdown.references.work, work);
}

#[test]
fn late_definitions_resolve_only_dependent_blocks() {
    for source in [
        "[r]\n\n",
        "[**bold**][r]\n\n",
        "[a][r]\n\n",
        "![image][r]\n\n",
    ] {
        let mut markdown = Markdown::default();
        markdown.push(source);
        let before = markdown.parsed_bytes;
        let delta = "[r]: /url\n\n";
        markdown.push(delta);
        assert!(markdown.parsed_bytes - before <= delta.len());
        assert_eq!(markdown.lines(), full_parse(&format!("{source}{delta}")));
    }
}

#[test]
fn html_spans_are_physical_display_lines() {
    let mut markdown = Markdown::default();
    markdown.push("<div>\nhello\n</div>\n");
    assert_eq!(markdown.lines().len(), 3);
    assert!(
        markdown
            .lines()
            .iter()
            .flat_map(|line| &line.spans)
            .all(|span| !span.text.contains(['\r', '\n']))
    );
    markdown.clear();
    // Script HTML blocks retain interior blank lines.
    markdown.push("<script>\n\nhello\n</script>\n");
    assert_eq!(markdown.lines().len(), 4);
}

#[test]
fn nested_paragraph_checkpoint_context() {
    let source = "- parent\n  - child\n\n    pa";
    let mut markdown = Markdown::default();
    let mut end = 0;
    for character in source.chars() {
        let delta = character.to_string();
        markdown.push(&delta);
        end += delta.len();
        assert_eq!(
            markdown.lines(),
            full_parse(source.get(..end).unwrap_or_default())
        );
    }
    assert_eq!(markdown.lines(), full_parse(source));
}

const LAZY_LIST: &str =
    "> quote\n- item\n- item\ntext 🦀\n| a | b |\n| - | - |\n[r]: url\n\n[r]: u";

fn check_chunks(source: &str, cuts: &[usize]) {
    let mut streamed = Markdown::default();
    let mut consumer = Vec::new();
    let mut offset = 0;
    for size in cuts.iter().cycle() {
        if offset == source.len() {
            break;
        }
        let mut end = (offset + size).min(source.len());
        while !source.is_char_boundary(end) {
            end += 1;
        }
        let changed = streamed.push(source.get(offset..end).unwrap_or_default());
        assert!(changed <= streamed.lines().len());
        consumer.truncate(changed);
        consumer.extend(
            (streamed.lines().slice(changed..).unwrap_or_default())
                .iter()
                .cloned(),
        );
        assert_eq!(
            consumer,
            full_parse(source.get(..end).unwrap_or_default()),
            "prefix {:?}, checkpoint {:?}",
            source.get(..end),
            source.get(..streamed.offset)
        );
        offset = end;
    }
}

#[test]
fn task_reference_frontier_flushes_before_checkpoint() {
    check_chunks("- [x] task [r]\n\n[r]: /url\n\n", &[8, 2, 12]);
}

#[test]
fn review_regressions_match_every_chunk_boundary() {
    check_chunks(LAZY_LIST, &[2, 2, 1, 1]);
    for source in [
        "    indented\n\n    more",
        "text word, word. yes! x: y - z + 2 = 4? (ok)",
    ] {
        check_chunks(source, &[1]);
    }
}

#[test]
fn punctuation_appends_have_linear_parser_work() {
    for length in [1024, 100 * 1024] {
        let mut markdown = Markdown::default();
        markdown.push(&"x".repeat(length));
        let before = markdown.parsed_bytes;
        for _ in 0..100 {
            markdown.push("word, (word); #yes! ");
        }
        assert_eq!(markdown.parsed_bytes, before);
        assert_eq!(
            markdown.lines(),
            full_parse(&format!(
                "{}{}",
                "x".repeat(length),
                "word, (word); #yes! ".repeat(100)
            ))
        );
    }
}

#[test]
fn lazy_list_reference_updates_only_parse_dependent_paragraphs() {
    let mut previous = None;
    for count in [100, 10_000] {
        let mut source = "- earlier\n".repeat(count);
        source.push_str(
            LAZY_LIST
                .split_once("\n\n")
                .map_or(LAZY_LIST, |(first, _)| first),
        );
        source.push_str("\n\n");
        let mut markdown = Markdown::default();
        markdown.push(&source);
        let before = markdown.parsed_bytes;
        let parsed = references::PARSED_BYTES.get();
        markdown.push("[r]: u\n\n");
        assert!(markdown.parsed_bytes - before <= 16);
        let work = references::PARSED_BYTES.get() - parsed;
        if let Some(previous) = previous {
            assert_eq!(work, previous);
        }
        previous = Some(work);
        source.push_str("[r]: u\n\n");
        assert_eq!(markdown.lines(), full_parse(&source));
    }
}

#[test]
fn indented_code_retains_lines_and_defers_blank_lines() {
    for count in [100, 10_000] {
        let mut source = "    indented\n".repeat(count);
        let mut markdown = Markdown::default();
        markdown.push(&source);
        let pointer = |markdown: &Markdown| {
            markdown
                .lines()
                .get(1)
                .and_then(|line| line.spans.first())
                .and_then(|span| span.text.allocation())
        };
        let first = pointer(&markdown);
        assert!(first.is_some());
        let before = markdown.parsed_bytes;
        for delta in ["\n", " ", " ", " ", " ", "m", "o", "r", "e", "\n"] {
            let highlighted = code::HIGHLIGHTED.get();
            markdown.push(delta);
            assert!(code::HIGHLIGHTED.get() - highlighted <= 2);
            source.push_str(delta);
            assert_eq!(markdown.lines(), full_parse(&source));
        }
        assert!(markdown.parsed_bytes - before < 512);
        assert_eq!(pointer(&markdown), first);
        let highlighted = code::HIGHLIGHTED.get();
        markdown.push("\n    next\n");
        assert_eq!(code::HIGHLIGHTED.get() - highlighted, 2);
    }
}

#[test]
fn completed_fences_in_one_chunk_have_linear_parse_work() {
    for count in [10, 100, 1000] {
        let source = "```text\nx\n```\n\n".repeat(count);
        let mut markdown = Markdown::default();
        let highlighted = code::HIGHLIGHTED.get();
        markdown.push(&source);
        assert_eq!(markdown.parsed_bytes, source.len());
        assert_eq!(code::HIGHLIGHTED.get() - highlighted, count);
        assert_eq!(markdown.lines(), full_parse(&source));
        let before = markdown.parsed_bytes;
        markdown.push("next");
        assert_eq!(markdown.parsed_bytes - before, 4);
    }
}

#[test]
fn reference_updates_preserve_task_list_events() {
    for source in [
        "- [x] task [r]\n\n",
        "- [X] task [r]\n\n",
        "- [ ] task [r]\n\n",
        "> - [x] task [r]\n>\n\n",
        "100. [x] task [r]\n\n",
    ] {
        let mut markdown = Markdown::default();
        markdown.push(source);
        let before = markdown.parsed_bytes;
        assert!(
            markdown.offset > 0,
            "task paragraph was not finalized: {source:?}"
        );
        let budget = "[r]: /url\n\n".len() + context_prefix(&markdown.context).len() + source.len()
            - markdown.offset;
        markdown.push("[r]: /url\n\n");
        assert!(
            markdown.parsed_bytes - before <= budget,
            "source {source:?}"
        );
        assert_eq!(
            markdown.lines(),
            full_parse(&format!("{source}[r]: /url\n\n"))
        );
    }
}

#[test]
fn numbered_tasks_align_continuations_to_checkbox_width() {
    for number in [3u32, 100] {
        let prefix = " ".repeat(number.to_string().len() + 2);
        let source = format!(
            "{number}. [ ] task\n\n{prefix}more\n\n{prefix}```text\n{prefix}code\n{prefix}```\n"
        );
        let mut markdown = Markdown::default();
        markdown.push(&source);
        let text = markdown
            .lines()
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.text.as_str())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(text, "[ ] task\n    more\n    ┌─ text\n    │ code\n    └─");
        check_chunks(&source, &[1, 7]);
    }
}

#[test]
fn quoted_table_cells_inherit_surrounding_style() {
    let source = "> | a | **b** |\n> | - | - |\n> | [r] | d |\n>\n\n";
    let mut markdown = Markdown::default();
    markdown.push(source);
    markdown.push("[r]: /url\n\n");
    for text in ["a", "b", "r", "d"] {
        let span = markdown
            .lines()
            .iter()
            .flat_map(|line| &line.spans)
            .find(|span| span.text == text);
        assert!(
            span.is_some_and(|span| span.style.italic && span.style.dim),
            "cell {text}"
        );
    }
    assert_eq!(
        markdown.lines(),
        full_parse(&format!("{source}[r]: /url\n\n"))
    );
    check_chunks(&format!("{source}[r]: /url\n\n"), &[1, 7]);
}

#[test]
fn decoded_newlines_keep_the_display_line_contract() {
    for (source, count) in [("before&NewLine;after", 2), ("before&#13;&#10;after", 3)] {
        let mut markdown = Markdown::default();
        markdown.push(source);
        assert_eq!(markdown.lines().len(), count);
        assert!(
            markdown
                .lines()
                .iter()
                .flat_map(|line| &line.spans)
                .all(|span| !span.text.contains(['\r', '\n']))
        );
        check_chunks(source, &[1]);
    }
}

#[test]
fn reference_blocks_parse_once_when_definitions_change() {
    for count in [16, 32, 64, 128] {
        let source = format!("{}\n\n", "[r][r]".repeat(count));
        let mut markdown = Markdown::default();
        markdown.push(&source);
        let before = references::PARSED_BYTES.get();
        let definition = "[r]: /url\n\n";
        markdown.push(definition);
        let parsed = references::PARSED_BYTES.get() - before;
        assert!(
            parsed <= source.len() + 2 * definition.len(),
            "{parsed} parser bytes"
        );
        assert_eq!(
            markdown.lines(),
            full_parse(&format!("{source}{definition}"))
        );
    }
}

#[test]
fn collapsed_reference_updates_match_every_chunk_boundary() {
    let source = "[a][][c]\n\n";
    let definition = "[a]: /a\n\n";
    let mut markdown = Markdown::default();
    markdown.push(source);
    markdown.push(definition);
    assert_eq!(
        markdown.lines(),
        full_parse(&format!("{source}{definition}"))
    );
    for cuts in [&[1][..], &[2, 3][..], &[11, 4, 1][..]] {
        check_chunks(&format!("{source}{definition}"), cuts);
    }
}

#[test]
fn adversarial_reference_endings_have_linear_parser_work() {
    for pattern in ["[text][ref]", "[text*][ref][text*][ref]"] {
        let mut previous = 0;
        for count in [64, 128] {
            let source = format!("{}(url)\n\n", pattern.repeat(count));
            let before = references::PARSED_BYTES.get();
            let mut markdown = Markdown::default();
            markdown.push(&source);
            let parsed = references::PARSED_BYTES.get() - before;
            assert!(
                parsed <= source.len() * 16,
                "{} bytes: {parsed} parser bytes",
                source.len()
            );
            if previous > 0 {
                assert!(parsed * 10 <= previous * 22, "{previous} -> {parsed}");
            }
            println!("{pattern:?}, {} bytes: {parsed} parser bytes", source.len());
            previous = parsed;
            assert_eq!(markdown.lines(), full_parse(&source));
        }
    }
}

#[test]
fn adjacent_reference_parser_bytes_scale_linearly() {
    let mut previous = 0;
    for count in [16, 32, 64, 128] {
        let source = format!("{}\n\n", "[text][ref]".repeat(count));
        let before = references::PARSED_BYTES.get();
        let mut markdown = Markdown::default();
        markdown.push(&source);
        let parsed = references::PARSED_BYTES.get() - before;
        assert!(
            parsed <= source.len() * 16,
            "{} bytes parsed {parsed}",
            source.len()
        );
        if previous > 0 {
            assert!(parsed * 10 <= previous * 22, "{previous} -> {parsed}");
        }
        println!("{} input bytes: {parsed} parser bytes", source.len());
        previous = parsed;
        assert_eq!(markdown.lines(), full_parse(&source));
    }
}

#[test]
fn late_definitions_do_not_reinterpret_literal_indented_code() {
    let source = "[x](url)    indented\n\n    more[r]: url\ntext\n\n[r]: /later\n\n";
    check_chunks(source, &[2, 2]);
}

#[test]
fn nested_reference_fuzz_regression() {
    let data = b"[a][b][c\xec\xec\xec\xec\xec[]\xec\xec\xec\xec\xec\xec\xec\xec\xec\xec\xec\xec\xec[a][b]\xec\xec\xec\xec\xec\xec\xec\xec\xec\xec\xec\xec\xec](url)[d*[e]\n\x08[b]: /one\n\n[e]: /two\n\n";
    let source = String::from_utf8_lossy(data);
    let cuts: Vec<_> = data
        .iter()
        .map(|byte| usize::from(*byte) % 31 + 1)
        .collect();
    check_chunks(&source, &cuts);
}

#[test]
fn linear_work_preserves_quoted_tasks_when_references_resolve() {
    let data = b"> 3. [x] t\0\0\0\x80ask [r]\n>\n>    more\n\n[r]: /url\n\n";
    let source = String::from_utf8_lossy(data);
    let cuts: Vec<_> = data
        .iter()
        .map(|byte| usize::from(*byte) % 31 + 1)
        .collect();
    check_chunks(&source, &cuts);
}

proptest! {
    #![proptest_config(ProptestConfig {
        failure_persistence: Some(Box::new(proptest::test_runner::FileFailurePersistence::Direct(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/full_parse.txt")
        ))),
        ..ProptestConfig::with_cases(64)
    })]
    #[test]
    fn adversarial_reference_total_parser_work_is_linear(
        count in 2usize..20,
        fragments in prop::collection::vec(0usize..9, 1..6),
        defined in prop::array::uniform5(any::<bool>()),
        ending in any::<bool>(),
        cuts in prop::collection::vec(1usize..24, 1..8),
    ) {
        let words = ["[x][y]", "[x][]", "[x](u)", "[outer [x][y]][ref]", "[x*][y][z*][ref]", "[*x][y][z*][ref]", "[**x**][ref]", "[`[x]`][ref]", "[a\\]b][ref]"];
        let pattern: String = fragments.iter().filter_map(|index| words.get(*index)).copied().collect();
        let pattern = pattern.repeat(count);
        let mut previous = 0;
        for copies in [1, 2] {
            let source = format!("{}{}\n\n", pattern.repeat(copies), if ending { "(u)" } else { "" });
            let before = references::PARSED_BYTES.get();
            let mut markdown = Markdown::default();
            markdown.push(&source);
            let parsed = references::PARSED_BYTES.get() - before;
            prop_assert!(parsed <= source.len() * 16, "{} input bytes: {parsed} parser bytes", source.len());
            if previous > 0 { prop_assert!(parsed * 10 <= previous * 22, "{previous} -> {parsed}"); }
            previous = parsed;
            prop_assert_eq!(markdown.lines(), full_parse(&source));
            let mut complete = source.clone();
            for (label, present) in ["x", "y", "z", "ref", "outer [x][y]"].into_iter().zip(defined) {
                if present {
                    let definition = format!("[{label}]: /target\n\n");
                    let before = references::PARSED_BYTES.get();
                    markdown.push(&definition);
                    let work = references::PARSED_BYTES.get() - before;
                    prop_assert!(work <= source.len() + definition.len() * 3);
                    complete.push_str(&definition);
                    prop_assert_eq!(markdown.lines(), full_parse(&complete));
                }
            }
        }
        check_chunks(&format!("{pattern}\n\n[x]: /x\n\n[y]: /y\n\n[ref]: /ref\n\n"), &cuts);
    }

    #[test]
    fn decoded_table_newlines_preserve_every_cell_segment(
        parts in prop::collection::vec("[a-zA-Z0-9]{1,12}", 2..6),
        entity in prop_oneof![Just("&#10;"), Just("&#13;"), Just("&NewLine;")],
    ) {
        let source = format!("| title |\n| - |\n| {} |\n", parts.join(entity));
        let mut markdown = Markdown::default();
        markdown.push(&source);
        let rendered: String = markdown.lines().iter().flat_map(|line| &line.spans).map(|span| span.text.as_str()).collect();
        prop_assert!(rendered.contains(&parts.join(" ↵ ")), "{rendered:?}");
        check_chunks(&source, &[1, 7]);
    }

    #[test]
    fn nested_links_match_random_chunkings(
        prefix in "[a-z ]{0,8}",
        suffix in "[a-z ]{0,8}",
        defined in prop::array::uniform3(any::<bool>()),
        cuts in prop::collection::vec(1usize..20, 1..8),
    ) {
        let mut source = format!("[a][b][{prefix}[a][b]{suffix}](url)[d*[e]\n\n");
        for (label, present) in ["a", "b", "e"].into_iter().zip(defined) {
            if present {
                source.push_str(&format!("[{label}]: /{label}\n\n"));
            }
        }
        check_chunks(&source, &cuts);
    }

    #[test]
    fn random_chunks_match_the_full_parser(parts in prop::collection::vec(0usize..20, 1..20), cuts in prop::collection::vec(1usize..20, 1..12)) {
        let vocabulary = ["text 🦀", "**bold**", "\n", "\n\n", "- item\n", "```rust\n", "let x = 1;\n", "```\n", "[x](url)", "| a | b |\n", "| - | - |\n", "[r]: url\n", "[r]", "<div>\nhello\n</div>\n", "> quote\n", "[a\u{a0}b]: /two\n", LAZY_LIST, "    indented\n\n    more", "word, ", "word. yes! x: y - z + 2 = 4? "];
        let source: String = parts.iter().filter_map(|index| vocabulary.get(*index)).copied().collect();
        let mut streamed = Markdown::default();
        let mut offset = 0;
        for size in cuts.iter().cycle() {
            if offset == source.len() { break; }
            let mut end = (offset + size).min(source.len());
            while !source.is_char_boundary(end) { end += 1; }
            streamed.push(source.get(offset..end).unwrap_or_default());
            prop_assert_eq!(streamed.lines(), full_parse(source.get(..end).unwrap_or_default()), "prefix {:?}", source.get(..end));
            offset = end;
        }
    }

    #[test]
    fn review_cases_match_random_chunkings(case in 0usize..9, cuts in prop::collection::vec(1usize..20, 1..12)) {
        let cases = [LAZY_LIST, "    indented\n\n    more", "paragraph word, word. yes! x: y - z + 2 = 4? ", "```text\na\n```\n\n```text\nb\n```\n\n", "- [x] task [r]\n\n[r]: /url\n\n", "> | a | b |\n> | - | - |\n> | [r] | d |\n>\n\n[r]: /url\n\n", "100. [x] task\n\n     more\n", "[r][r][r][r]\n\n[r]: /url\n\n", "> 3. [x] task [r]\n>\n>    more\n\n[r]: /url\n\n"];
        check_chunks(cases.get(case).copied().unwrap_or_default(), &cuts);
    }

    #[test]
    fn punctuation_matches_random_chunkings(parts in prop::collection::vec(0usize..12, 1..30), cuts in prop::collection::vec(1usize..12, 1..8)) {
        let punctuation = ["word, ", "(word); ", "#word! ", "'word' : ", "&copy", "; ", "[word]", "(url)", "*word", "* ", "\\", "[r]: /url\n"];
        let mut source = String::from("paragraph ");
        for index in parts { source.push_str(punctuation.get(index).copied().unwrap_or_default()); }
        check_chunks(&source, &cuts);
    }

    // Source serialization preserves normalized input, and replaying it must
    // preserve every styled span and link regardless of the original chunking.
    #[test]
    fn markdown_source_round_trip(source in any::<String>(), cuts in prop::collection::vec(1usize..32, 1..8)) {
        let mut markdown = Markdown::default();
        let mut offset = 0;
        for size in cuts.iter().cycle() {
            if offset == source.len() { break; }
            let mut end = (offset + size).min(source.len());
            while !source.is_char_boundary(end) { end += 1; }
            markdown.push(source.get(offset..end).unwrap_or_default());
            prop_assert_eq!(markdown.lines(), full_parse(source.get(..end).unwrap_or_default()));
            offset = end;
        }
        let expected = markdown.lines().to_vec();
        let serialized = markdown.into_source();
        prop_assert_eq!(&serialized, &source.replace("\r\n", "\n").replace('\r', "\n"));
        let mut replay = Markdown::default();
        replay.push(&serialized);
        prop_assert_eq!(replay.lines(), expected);
        prop_assert_eq!(replay.into_source(), serialized);
    }
}

#[test]
fn reference_titles_can_complete_after_a_provisional_plain_list_tail() {
    check_chunks("paragraph [r]: /url\n* [r]: /url\n(url)", &[1]);
}
