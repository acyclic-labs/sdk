use super::{Directive, Kind, Lines, Target};
use crate::{Line, Style, code, style::MUTED};
use std::{fmt, sync::Arc};

/// Stable classification of an unavailable excerpt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResolutionError {
    /// No symbol index is bound to the task.
    UnsupportedSymbol,
    /// Inclusive source lines exceed the file.
    LineRange,
    /// Character offsets exceed the selected lines.
    CharRange,
    /// The task's filesystem policy denied reading.
    Denied,
    /// The logical path escapes the task namespace.
    OutsideBoundary,
    /// File exceeds the resolver's read bound.
    TooLarge,
    /// A message exceeds 256 block citations.
    TooMany,
    /// The provider could not return UTF-8 source text.
    Read,
}

impl fmt::Display for ResolutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnsupportedSymbol => "symbol selection is unsupported",
            Self::LineRange => "line range exceeds the file",
            Self::CharRange => "character range exceeds the excerpt",
            Self::Denied => "read denied",
            Self::OutsideBoundary => "path is outside the task view",
            Self::TooLarge => "file exceeds the read bound",
            Self::TooMany => "too many file citations",
            Self::Read => "file is unavailable",
        })
    }
}

/// Selected content stored beside the message, independent of later file edits.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Excerpt {
    /// Selected UTF-8 text, never re-read by the renderer.
    pub content: String,
    /// Original one-based lines, before the character selection.
    pub lines: Option<Lines>,
}

/// Resolution folded from a task-volume snapshot file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Resolution {
    /// The authorized file's selected bytes.
    Content(Excerpt),
    /// A typed, displayable failure.
    Error(ResolutionError),
}

impl Target {
    /// Select lines, then zero-based Unicode scalar offsets. Filesystem reads
    /// and access decisions belong to the harness, before calling this function.
    pub fn select(&self, source: &str) -> Result<Excerpt, ResolutionError> {
        if self.symbol.is_some() {
            return Err(ResolutionError::UnsupportedSymbol);
        }
        let selected = if let Some(lines) = &self.lines {
            let mut start = None;
            let mut end = None;
            let mut offset = 0;
            for (index, line) in source.split('\n').enumerate() {
                if index + 1 == lines.start {
                    start = Some(offset);
                }
                if index + 1 == lines.end {
                    end = Some(offset + line.len());
                    break;
                }
                offset += line.len() + 1;
            }
            source
                .get(
                    start.ok_or(ResolutionError::LineRange)?
                        ..end.ok_or(ResolutionError::LineRange)?,
                )
                .ok_or(ResolutionError::LineRange)?
        } else {
            source
        };
        let selected = if let Some(chars) = &self.chars {
            let mut start = None;
            let mut end = None;
            for (index, byte) in selected
                .char_indices()
                .map(|(byte, _)| byte)
                .chain(std::iter::once(selected.len()))
                .enumerate()
            {
                if index == chars.start {
                    start = Some(byte);
                }
                if index == chars.end {
                    end = Some(byte);
                    break;
                }
            }
            selected
                .get(
                    start.ok_or(ResolutionError::CharRange)?
                        ..end.ok_or(ResolutionError::CharRange)?,
                )
                .ok_or(ResolutionError::CharRange)?
        } else {
            selected
        };
        Ok(Excerpt {
            content: selected.to_owned(),
            lines: self.lines.clone(),
        })
    }
}

impl Directive {
    /// Render a chip or an immutable resolved excerpt using the ordinary syntect
    /// code renderer. Unresolved and invalid block citations retain raw text.
    pub fn render(&self, resolution: Option<&Resolution>) -> Vec<Line> {
        let Some(target) = self.target() else {
            return vec![Line::plain(self.raw(), Style::default())];
        };
        let label = |lines: &Option<Lines>| {
            let mut label = target.path.clone();
            if let Some(lines) = lines {
                label.push_str(&format!(":{}-{}", lines.start, lines.end));
            }
            label
        };
        if self.kind() == Kind::Inline {
            let mut line = Line::default();
            line.push(
                &label(&target.lines),
                Style {
                    underline: true,
                    ..Style::default()
                },
                Some(&Arc::from(target.path.as_str())),
            );
            return vec![line];
        }
        let Some(resolution) = resolution else {
            return vec![Line::plain(self.raw(), Style::default())];
        };
        match resolution {
            Resolution::Content(excerpt) => {
                let mut lines = vec![code::header(&label(&excerpt.lines))];
                let extension = target.path.rsplit('.').next().unwrap_or_default();
                let mut highlighter = code::highlighter(extension);
                lines.extend(
                    excerpt
                        .content
                        .split_inclusive('\n')
                        .map(|line| code::highlight(&mut highlighter, line)),
                );
                lines.push(code::footer());
                lines
            }
            Resolution::Error(error) => vec![
                code::header(&label(&target.lines)),
                Line::plain(&format!("│ {error}"), MUTED),
                code::footer(),
            ],
        }
    }
}
