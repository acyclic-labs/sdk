//! Presentation of one admitted diff page. Source paging belongs to its caller.
use crate::{
    QueryPage,
    config::{ThemeStyles, Token},
    display_text,
};
use ratatui::{
    style::Modifier,
    text::{Line, Span},
};
use std::{collections::BTreeSet, ops::Range};
use unicode_segmentation::UnicodeSegmentation as _;

enum Row {
    File(String),
    Hunk(String),
    Patch(String),
    Hint(String),
}

/// One source page, reflowed without reading or retaining any other page.
#[derive(Default)]
pub struct DiffPage {
    rows: Vec<Row>,
    selected: usize,
    folded: BTreeSet<usize>,
    presentation: Option<(u16, bool, ThemeStyles)>,
}

impl DiffPage {
    /// Add a file header and its admitted patch fragment. Header text is never
    /// parsed as patch syntax, including filenames beginning with `+` or `@@`.
    pub fn file(&mut self, header: Option<String>, patch: &str) {
        if let Some(header) = header {
            self.rows
                .push(Row::File(display_text(&header).into_owned()));
        }
        self.rows.extend(patch.lines().map(|line| {
            let text = display_text(line).into_owned();
            if line.starts_with("@@ ") {
                Row::Hunk(text)
            } else {
                Row::Patch(text)
            }
        }));
        self.presentation = None;
    }

    /// Add source-owned pagination or explicit omission information.
    pub fn hint(&mut self, hint: impl Into<String>) {
        self.rows.push(Row::Hint(hint.into()));
        self.presentation = None;
    }

    /// Move among the hunks in this admitted page.
    pub fn select_hunk(&mut self, backwards: bool) -> bool {
        let count = self
            .rows
            .iter()
            .filter(|row| matches!(row, Row::Hunk(_)))
            .count();
        if count == 0 {
            return false;
        }
        self.selected = if backwards {
            self.selected.checked_sub(1).unwrap_or(count - 1)
        } else {
            (self.selected + 1) % count
        };
        self.presentation = None;
        true
    }

    /// Fold or reveal the selected admitted hunk, preserving its source text.
    pub fn toggle_hunk(&mut self) -> bool {
        if !self.rows.iter().any(|row| matches!(row, Row::Hunk(_))) {
            return false;
        }
        if !self.folded.remove(&self.selected) {
            self.folded.insert(self.selected);
        }
        self.presentation = None;
        true
    }

    /// Rebuild only on source, geometry, theme or selection changes. The
    /// returned rows replace the caller's single existing query presentation.
    pub fn render(
        &mut self,
        columns: u16,
        wide: bool,
        theme: ThemeStyles,
    ) -> Option<Vec<Line<'static>>> {
        // Unified rows use QueryPage's one grapheme wrapper. Only the two-pane
        // layout depends on columns here.
        let key = (if wide { columns } else { 0 }, wide, theme);
        if self.presentation == Some(key) {
            return None;
        }
        self.presentation = Some(key);
        let mut result = Vec::new();
        let mut index = 0;
        let mut hunk = 0;
        let mut hidden = false;
        while let Some(row) = self.rows.get(index) {
            match row {
                Row::File(text) => {
                    hidden = false;
                    result.push(Line::styled(
                        text.clone(),
                        theme.token(Token::Accent).add_modifier(Modifier::BOLD),
                    ));
                }
                Row::Hint(text) => {
                    result.push(Line::styled(text.clone(), theme.token(Token::Muted)))
                }
                Row::Hunk(text) => {
                    hidden = self.folded.contains(&hunk);
                    let mut style = theme.token(Token::Accent);
                    if hunk == self.selected {
                        style = style.add_modifier(Modifier::UNDERLINED);
                    }
                    result.push(Line::styled(
                        format!("{} {text}", if hidden { "▶" } else { "▼" }),
                        style,
                    ));
                    hunk += 1;
                }
                Row::Patch(_) if hidden => {}
                Row::Patch(text) if text.starts_with('-') => {
                    let removed = index;
                    while matches!(self.rows.get(index), Some(Row::Patch(text)) if text.starts_with('-'))
                    {
                        index += 1;
                    }
                    let added = index;
                    while matches!(self.rows.get(index), Some(Row::Patch(text)) if text.starts_with('+'))
                    {
                        index += 1;
                    }
                    let old = &self.rows[removed..added];
                    let new = &self.rows[added..index];
                    let pairs = old.len().max(new.len());
                    let mut before = Vec::new();
                    let mut after = Vec::new();
                    for position in 0..pairs {
                        let a = patch_text(old.get(position));
                        let b = patch_text(new.get(position));
                        let (a_range, b_range) = match (a, b) {
                            (Some(a), Some(b)) => changed_words(&a[1..], &b[1..]),
                            _ => (0..0, 0..0),
                        };
                        if wide {
                            side_by_side(
                                &mut result,
                                a.map(|a| changed(a, a_range, false, theme)),
                                b.map(|b| changed(b, b_range, true, theme)),
                                columns,
                                theme,
                            );
                        } else {
                            if let Some(a) = a {
                                before.push(changed(a, a_range, false, theme));
                            }
                            if let Some(b) = b {
                                after.push(changed(b, b_range, true, theme));
                            }
                        }
                    }
                    result.extend(before);
                    result.extend(after);
                    continue;
                }
                Row::Patch(text) => {
                    if text.starts_with('+') {
                        let line = changed(text, 0..0, true, theme);
                        if wide {
                            side_by_side(&mut result, None, Some(line), columns, theme);
                        } else {
                            result.push(line);
                        }
                    } else {
                        result.push(Line::styled(text.clone(), theme.token(Token::Fg)));
                    }
                }
            }
            index += 1;
        }
        Some(result)
    }
}

fn patch_text(row: Option<&Row>) -> Option<&str> {
    match row {
        Some(Row::Patch(text)) => Some(text),
        _ => None,
    }
}

// A linear replacement envelope at Unicode word boundaries: retain the common
// word prefix and suffix and emphasize the replacement between them. This
// needs neither a second edit algorithm nor a quadratic token comparison.
fn changed_words(old: &str, new: &str) -> (Range<usize>, Range<usize>) {
    let prefix: usize = old
        .split_word_bounds()
        .zip(new.split_word_bounds())
        .take_while(|(a, b)| a == b)
        .map(|(a, _)| a.len())
        .sum();
    let suffix: usize = old[prefix..]
        .split_word_bounds()
        .rev()
        .zip(new[prefix..].split_word_bounds().rev())
        .take_while(|(a, b)| a == b)
        .map(|(a, _)| a.len())
        .sum();
    (prefix..old.len() - suffix, prefix..new.len() - suffix)
}

fn changed(text: &str, range: Range<usize>, added: bool, theme: ThemeStyles) -> Line<'static> {
    let style = theme.token(if added {
        Token::DiffAdd
    } else {
        Token::DiffDel
    });
    let start = range.start + 1;
    let end = range.end + 1;
    Line::from(vec![
        Span::styled(text[..start].to_owned(), style),
        Span::styled(
            text[start..end].to_owned(),
            style.add_modifier(Modifier::BOLD),
        ),
        Span::styled(text[end..].to_owned(), style),
    ])
}

fn side_by_side(
    result: &mut Vec<Line<'static>>,
    old: Option<Line<'static>>,
    new: Option<Line<'static>>,
    columns: u16,
    theme: ThemeStyles,
) {
    let left = columns.saturating_sub(1) / 2;
    let right = columns.saturating_sub(left + 1);
    let wrap = |line: Option<Line<'static>>, width| {
        let mut page = QueryPage::<()>::new(line.into_iter().collect(), None, false);
        page.wrap(width);
        page.lines
    };
    let before = wrap(old, left);
    let after = wrap(new, right);
    for row in 0..before.len().max(after.len()) {
        let mut line = before.get(row).cloned().unwrap_or_default();
        line.spans.push(Span::raw(
            " ".repeat(usize::from(left).saturating_sub(line.width())),
        ));
        line.spans
            .push(Span::styled("│", theme.token(Token::Muted)));
        if let Some(right) = after.get(row) {
            line.spans.extend(right.spans.iter().cloned());
        }
        result.push(line);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        PromptInput,
        config::{ConfigFile, ConfigView, Scope},
        term::{Capabilities, ColourDepth},
    };

    fn theme(light: bool, depth: ColourDepth) -> ThemeStyles {
        let mut config = ConfigView::default();
        config.set_file(
            Scope::User,
            ConfigFile::Theme,
            Some(if light {
                r#"{"mode":"light"}"#
            } else {
                r#"{"mode":"dark"}"#
            }),
        );
        let mut input = PromptInput::default();
        config
            .apply(
                &mut input,
                &Capabilities {
                    colour: depth,
                    ..Capabilities::default()
                },
            )
            .expect("valid theme");
        input.theme
    }

    #[test]
    fn diff_word_emphasis_preserves_unicode_context_across_themes() {
        for light in [false, true] {
            for depth in [ColourDepth::TrueColour, ColourDepth::Monochrome] {
                let theme = theme(light, depth);
                let mut page = DiffPage::default();
                page.file(Some("+untrusted (modified) +1 -1".into()),
                    "@@ -1 +1 @@\n-let answer = old_value; // 界 👩‍💻 e\u{301}\n+let answer = new_value; // 界 👩‍💻 e\u{301}\n");
                let lines = page.render(78, false, theme).expect("first render");
                assert_eq!(lines[0].to_string(), "+untrusted (modified) +1 -1");
                for (line, marker, word, token) in [
                    (&lines[2], '-', "old_value", Token::DiffDel),
                    (&lines[3], '+', "new_value", Token::DiffAdd),
                ] {
                    assert_eq!(
                        line.to_string(),
                        format!("{marker}let answer = {word}; // 界 👩‍💻 e\u{301}")
                    );
                    let emphasized = line
                        .spans
                        .iter()
                        .filter(|span| span.style.add_modifier.contains(Modifier::BOLD))
                        .map(|span| span.content.as_ref())
                        .collect::<String>();
                    assert_eq!(emphasized, word);
                    // A whole-line highlighter is the negative control: the
                    // unchanged context must have the base diff token only.
                    assert!(
                        line.spans
                            .iter()
                            .any(|span| span.content.contains("let answer = ")
                                && !span.style.add_modifier.contains(Modifier::BOLD))
                    );
                    assert!(
                        line.spans
                            .iter()
                            .all(|span| span.style.fg == theme.token(token).fg)
                    );
                }
            }
        }
    }

    #[test]
    fn diff_wide_reflow_and_hunk_folding_retain_every_source_grapheme() {
        let theme = theme(false, ColourDepth::TrueColour);
        let tail = "界 e\u{301} 👩‍💻 ".repeat(36);
        let old = format!("-first old_value {tail}");
        let new = format!("+first new_value {tail}");
        let mut diff = DiffPage::default();
        diff.file(
            Some("evidence.rs (modified) +2 -2".into()),
            &format!("@@ -1 +1 @@\n{old}\n{new}\n@@ -8 +8 @@\n-second before\n+second after\n"),
        );
        for (columns, wide) in [(78, false), (158, true), (198, true), (52, false)] {
            let lines = diff.render(columns, wide, theme).expect("changed geometry");
            let collect = |token| {
                lines
                    .iter()
                    .flat_map(|line| &line.spans)
                    .filter(|span| span.style.fg == theme.token(token).fg)
                    .map(|span| span.content.as_ref())
                    .collect::<String>()
            };
            assert_eq!(collect(Token::DiffDel), format!("{old}-second before"));
            assert_eq!(collect(Token::DiffAdd), format!("{new}+second after"));
            if wide {
                assert!(
                    lines
                        .iter()
                        .all(|line| line.width() <= usize::from(columns))
                );
            }
        }
        assert!(diff.toggle_hunk());
        let folded = diff.render(78, false, theme).expect("folded");
        assert!(
            !folded
                .iter()
                .any(|line| line.to_string().contains("first old_value"))
        );
        assert!(
            folded
                .iter()
                .any(|line| line.to_string().contains("second before"))
        );
        assert!(diff.select_hunk(false));
        assert!(diff.toggle_hunk());
        let both = diff.render(78, false, theme).expect("both folded");
        assert!(
            !both
                .iter()
                .any(|line| line.to_string().starts_with(['-', '+']))
        );
        assert!(diff.select_hunk(true));
        assert!(diff.toggle_hunk());
        let restored = diff.render(78, false, theme).expect("restored");
        assert!(restored.iter().any(|line| line.to_string() == old));
        assert!(restored.iter().any(|line| line.to_string() == new));
    }
}
