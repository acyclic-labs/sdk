//! Golden rendering and delta-boundary invariants ported from `GraphCoder`.

use acyclic_markdown::{Line, Markdown};
use proptest::prelude::*;

// Ported from Q:/graphcoder lib/client/src/markdown/parser.test.ts:
// representative streaming grammar and literal/currency fixtures. Selection
// fixture: lib/native/src/cli/messages/selection.rs mixed-content test.
const FIXTURES: &[(&str, &str)] = &[
    (
        include_str!("fixtures/grammar.md"),
        include_str!("fixtures/grammar.txt"),
    ),
    (
        include_str!("fixtures/selection.md"),
        include_str!("fixtures/selection.txt"),
    ),
    (
        include_str!("fixtures/literals.md"),
        include_str!("fixtures/literals.txt"),
    ),
];

fn plain(lines: &[Line]) -> String {
    lines
        .iter()
        .map(|line| {
            line.spans
                .iter()
                .map(|span| span.text.as_str())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn fresh(source: &str) -> Vec<Line> {
    let mut markdown = Markdown::default();
    markdown.push(source);
    markdown.lines().to_vec()
}

#[test]
fn old_graphcoder_golden_fixtures() {
    for (source, expected) in FIXTURES {
        assert_eq!(plain(&fresh(source)), expected.trim_end_matches('\n'));
    }
}

fn check_prefixes(source: &str) {
    let mut markdown = Markdown::default();
    let mut presentation = Vec::new();
    let mut end = 0;
    for character in source.chars() {
        let delta = character.to_string();
        end += delta.len();
        let changed = markdown.push(&delta);
        assert!(changed <= presentation.len());
        presentation.truncate(changed);
        presentation.extend_from_slice(markdown.lines().get(changed..).unwrap_or_default());
        assert_eq!(presentation, markdown.lines());
        assert_eq!(
            markdown.lines(),
            fresh(source.get(..end).unwrap_or_default()),
            "prefix {end}: {source:?}"
        );
    }
}

#[test]
fn every_fixture_prefix_matches_a_fresh_parse() {
    for (source, _) in FIXTURES {
        check_prefixes(source);
    }
}

#[test]
fn delimiters_containers_unicode_and_reference_links() {
    for source in [
        "**bold and *nested* text** then ~~deleted~~ and ![image](a.png)",
        "7. first\n8. second\n   - nested\n\n- [x] done\n- [ ] later\n",
        "> a\n>\n> **b**\n\nend",
        "Title\n-----\n\ntext\n\n---\n",
        "```rust\n/* open\nclose */ let café = \"🦀\";\n````\nafter",
        "  ~~~~rust\n  let x = 1;\n  ~~~\n  ~~~~\n\nend",
        "```rust\nlet x = 1;\n```oops\n\n```\n",
        "[earlier][ref]\n\n# Stable\n\n[ref]: https://example.com \"title\"\n\n[ref]",
        "[ref]: https://example.com\n\n# Stable\n\n[ref] and [a][ref]",
        "| 界 | **b** |\n| :- | -: |\n| 🦀 | [x](url) |\n",
        "line  \nnext\n\n<https://example.com> &amp; \\*literal*",
    ] {
        check_prefixes(source);
    }
}

#[test]
fn styles_links_and_highlighting_survive_the_port() {
    let lines = fresh(FIXTURES.first().map_or("", |fixture| fixture.0));
    let spans: Vec<_> = lines.iter().flat_map(|line| &line.spans).collect();
    assert!(
        spans
            .iter()
            .any(|span| span.text == "strong" && span.style.bold)
    );
    assert!(
        spans
            .iter()
            .any(|span| span.text == "emphasis" && span.style.italic)
    );
    assert!(
        spans
            .iter()
            .any(|span| span.text == "link" && span.link.as_deref() == Some("https://example.com"))
    );
    assert!(
        spans
            .iter()
            .any(|span| span.text == "code" && span.style.foreground.is_some())
    );
    let code = lines
        .iter()
        .find(|line| plain(std::slice::from_ref(line)).contains("const value"));
    assert!(code.is_some_and(|line| {
        let colors: std::collections::HashSet<_> = line
            .spans
            .iter()
            .filter_map(|span| {
                span.style
                    .foreground
                    .map(|color| (color.0, color.1, color.2))
            })
            .collect();
        colors.len() >= 2
    }));
}

#[test]
fn nested_lists_quotes_code_and_tables_keep_their_prefixes() {
    let source = "- parent\n  - child\n\n    paragraph\n\n    ```text\n    code\n    ```\n\n> quote\n>\n> ```text\n> nested\n> ```\n>\n> | a | b |\n> | - | - |\n> | 1 | 2 |\n";
    assert_eq!(
        plain(&fresh(source)),
        "• parent\n  • child\n    paragraph\n    ┌─ text\n    │ code\n    └─\n\n│ quote\n│ ┌─ text\n│ │ nested\n│ └─\n│ ┌─────┬─────┐\n│ │ a   │ b   │\n│ ├─────┼─────┤\n│ │ 1   │ 2   │\n│ └─────┴─────┘"
    );
    check_prefixes(source);
    let source = "3. parent\n\n   > quote\n   >\n   > ```text\n   > inner\n   > ```\n";
    assert_eq!(
        plain(&fresh(source)),
        "3. parent\n   │ quote\n   │ ┌─ text\n   │ │ inner\n   │ └─"
    );
    check_prefixes(source);
}

#[test]
fn stable_blocks_and_fence_lines_are_not_replaced() {
    let mut markdown = Markdown::default();
    markdown.push("# Stable\n\n```rust\n");
    let stable = markdown.lines().first().cloned();
    for _ in 0..1000 {
        let before = markdown.lines().len();
        assert!(markdown.push("let value = 1;\n") >= before.saturating_sub(1));
        assert_eq!(markdown.lines().first(), stable.as_ref());
    }
    markdown.push("```\n\nDone");
    assert!(plain(markdown.lines()).ends_with("└─\n\nDone"));
    assert_eq!(markdown.push(""), markdown.lines().len());
    markdown.clear();
    assert!(markdown.lines().is_empty());
    markdown.push("fresh");
    assert_eq!(plain(markdown.lines()), "fresh");
}

proptest! {
    #![proptest_config(ProptestConfig { rng_seed: proptest::test_runner::RngSeed::Fixed(0x6d61_726b_0000_0005), ..ProptestConfig::with_cases(64) })]
    #[test]
    fn arbitrary_delta_partitions(parts in prop::collection::vec(0usize..12, 1..25), cuts in prop::collection::vec(1usize..20, 1..12)) {
        let vocabulary = ["text 🦀", "**bold**", "\n", "\n\n", "- item\n", "```rust\n", "let x = 1;\n", "```\n", "[x](url)", "| a | b |\n", "| - | - |\n", "[r]: url\n"];
        let source: String = parts.iter().filter_map(|index| vocabulary.get(*index)).copied().collect();
        let mut streamed = Markdown::default();
        let mut offset = 0;
        for size in cuts.iter().cycle() {
            if offset == source.len() { break; }
            let mut end = (offset + size).min(source.len());
            while !source.is_char_boundary(end) { end += 1; }
            streamed.push(source.get(offset..end).unwrap_or_default());
            prop_assert_eq!(streamed.lines(), fresh(source.get(..end).unwrap_or_default()));
            offset = end;
        }
    }
}
