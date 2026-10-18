//! Line selection and `cat -n` rendering for model-facing text reads.
use super::super::edit::{charge, reserve_string};
use crate::{NativeCustody, Result};

/// Width of the right-aligned line-number column, as in `cat -n`.
const NUMBER_WIDTH: usize = 6;

/// Rendering of a selection that contains no lines (only an empty file).
pub(super) const EMPTY_FILE: &str = "(empty file)";

/// Renders selected lines as `cat -n` text: a right-aligned one-based number,
/// a tab and the line without its `\n` terminator, joined by `\n`.
pub(super) fn numbered(text: &str, offset: u64, work: &mut NativeCustody) -> Result<String> {
    if text.is_empty() {
        return Ok(EMPTY_FILE.to_owned());
    }
    let capacity = rendered_len(text, offset);
    let mut output = String::new();
    reserve_string(work, &mut output, capacity)?;
    charge(work, capacity as u64, 0)?;
    for (index, line) in (offset..).zip(lines(text)) {
        if !output.is_empty() {
            output.push('\n');
        }
        use std::fmt::Write as _;
        // Writing to a reserved String cannot fail.
        let _ = write!(output, "{index:>NUMBER_WIDTH$}\t{line}");
    }
    Ok(output)
}

/// Exact byte length of `numbered(text, offset)`, so callers can bound the
/// rendered projection before producing it.
pub(super) fn rendered_len(text: &str, offset: u64) -> usize {
    if text.is_empty() {
        return EMPTY_FILE.len();
    }
    (offset..)
        .zip(lines(text))
        .map(|(index, line)| digits(index).max(NUMBER_WIDTH) + 1 + line.len() + 1)
        .sum::<usize>()
        - 1
}

/// The lines a `cat -n` rendering numbered from line 1 shows, without their
/// terminators, or `None` when `rendered` is not exactly such a rendering.
pub(crate) fn unnumbered(rendered: &str) -> Option<impl Iterator<Item = &str> + Clone> {
    let lines = rendered.split('\n').map(|line| line.split_once('\t'));
    let exact = (1_u64..).zip(lines.clone()).all(|(expected, line)| {
        line.is_some_and(|(number, _)| {
            let digits = number.trim_start_matches(' ');
            number.len() == self::digits(expected).max(NUMBER_WIDTH)
                && digits.bytes().all(|byte| byte.is_ascii_digit())
                && digits.parse() == Ok(expected)
        })
    });
    exact.then(|| lines.map(|line| line.map_or("", |(_, text)| text)))
}

fn lines(text: &str) -> std::str::Split<'_, char> {
    text.strip_suffix('\n').unwrap_or(text).split('\n')
}

fn digits(mut value: u64) -> usize {
    let mut count = 1;
    while value >= 10 {
        value /= 10;
        count += 1;
    }
    count
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn model_lines(source: &str) -> Vec<&str> {
        source.split_inclusive('\n').collect()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]
        // Property: removing the number column restores the exact selected
        // text (up to its final terminator), and numbers count up from offset.
        #[test]
        fn numbering_round_trips(source in "[ab\\n\\ré🦀]{1,24}", offset in 1_u64..2_000_000) {
            let rendered = numbered(&source, offset, &mut None).unwrap();
            prop_assert_eq!(rendered.len(), rendered_len(&source, offset));
            let shown = |rendered: &str| unnumbered(rendered).map(|lines| lines.collect::<Vec<_>>().join("\n"));
            let whole = numbered(&source, 1, &mut None).unwrap();
            let restored_whole = shown(&whole);
            prop_assert_eq!(restored_whole.as_deref(), Some(source.strip_suffix('\n').unwrap_or(&source)));
            // Negative control: a rendering from any other line is not a whole file.
            prop_assert_eq!(shown(&rendered).is_some(), offset == 1);
            let mut restored = Vec::new();
            for (expected, line) in (offset..).zip(rendered.split('\n')) {
                let (number, text) = line.split_once('\t').unwrap();
                prop_assert!(number.len() >= NUMBER_WIDTH);
                prop_assert_eq!(number.trim_start().parse::<u64>().unwrap(), expected);
                restored.push(text);
            }
            let mut joined = restored.join("\n");
            if source.ends_with('\n') {
                joined.push('\n');
            }
            prop_assert_eq!(joined, source);
        }
    }

    #[test]
    fn only_exact_renderings_from_line_one_are_unnumbered() {
        let shown = |rendered: &str| {
            unnumbered(rendered).map(|lines| lines.map(str::to_owned).collect::<Vec<_>>())
        };
        assert_eq!(
            shown("     1\ta\t b\n     2\t"),
            Some(vec!["a\t b".to_owned(), String::new()])
        );
        for rendered in [
            "     2\ta",
            "1\ta",
            "      1\ta",
            "    +1\ta",
            "     1\ta\n     3\tb",
            "     1 a",
            EMPTY_FILE,
        ] {
            assert_eq!(shown(rendered), None, "{rendered:?}");
        }
    }

    #[test]
    fn empty_files_render_a_marker_that_no_numbered_line_can_equal() -> Result<()> {
        assert_eq!(numbered("", 1, &mut None)?, EMPTY_FILE);
        assert_eq!(rendered_len("", 1), EMPTY_FILE.len());
        assert_eq!(
            numbered("(empty file)", 1, &mut None)?,
            "     1\t(empty file)"
        );
        assert_eq!(
            numbered("a\r\nb\n", 9, &mut None)?,
            "     9\ta\r\n    10\tb"
        );
        Ok(())
    }
}
