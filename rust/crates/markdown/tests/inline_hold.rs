use super::adversarial_work::{Cycle, superlinear};
use super::inline_chunks::replay;
use super::*;

const PIECES: &[&str] = &[
    "a", "b c", "*em*", "**", "*", "_", "__", "-", "--", "=", "==", " ", "   ", "  \n", "\n",
    "\\\n", "\\", "[x](u)", "`c`", "`", "<b>", "&amp;", "# ", "> ", "- x", "1. ",
];

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 96,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]
    // Rows of rule and setext markers render as the full parse at every
    // prefix, whatever bytes later return them to paragraph text.
    #[test]
    fn held_rows_match_full_render_at_every_prefix(
        pieces in prop::collection::vec(prop::sample::select(PIECES), 0..32),
        splits in prop::collection::vec(any::<u8>(), 0..24),
    ) {
        let source = pieces.concat();
        let mut cuts: Vec<_> = splits
            .iter()
            .map(|cut| usize::from(*cut) % (source.len() + 1))
            .collect();
        cuts.sort_unstable();
        replay(&source, &cuts);
        replay(&source, &(1..source.len()).collect::<Vec<_>>());
    }
}

#[test]
fn known_held_rows_match_full_render() {
    for source in [
        "a *b\nc* d\n___- item\n",
        "a\n_ _ _x",
        "a\n* *",
        "a\n- -",
        "a\n-- -",
        "a\n= =",
        "a  \n_x",
        "a  \nb\n===",
        "*a* [b](u)\n---x",
        "a\\\n__",
        "x\n\n[r]: /u\n=",
        "   ***",
        "a\n   __ _ ",
        "a  \n1. `c`",
    ] {
        replay(source, &(1..source.len()).collect::<Vec<_>>());
    }
}

fn cycle(prefix: &'static str, parts: &[&'static str]) -> Cycle {
    Cycle {
        prefix,
        parts: parts.to_vec(),
        separator: "",
    }
}

// Each excursion into a rule or heading row is O(row), so cycles that keep
// returning to paragraph text stay linear in total.
#[test]
fn rule_and_heading_excursions_are_linear() {
    let cases = [
        cycle("", &["_", "__", "- item\n"]),
        cycle("", &["*", "**", "x\n"]),
        cycle("", &["=", "[s]: /s\n"]),
        cycle("", &["-", "--", "- item\n"]),
        cycle("[r]\n\n", &["=", "```\n"]),
        cycle("x ", &["=", "</div>\n"]),
    ];
    for case in &cases {
        assert_eq!(superlinear(case, 24), None);
    }
    // Negative control: feeding candidate rows to the engine rebuilds the
    // paragraph on every excursion.
    crate::inline::SKIP_HOLD.set(true);
    let found = superlinear(&cases[0], 24);
    crate::inline::SKIP_HOLD.set(false);
    assert!(found.is_some());
}
