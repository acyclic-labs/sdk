//! Renderer-independent presentation values.

use std::sync::Arc;

/// A semantic foreground. The renderer's theme gives each its colour, so
/// presentation never fixes a palette.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Tone {
    /// Headings, links, and code keywords.
    Accent,
    /// Inline code, and code numbers and constants.
    Attention,
    /// Code strings.
    Success,
    /// Invalid code.
    Error,
}

/// Additive text attributes, independent of a terminal or browser.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Style {
    /// Foreground; absent means the renderer's default.
    pub foreground: Option<Tone>,
    /// Strong emphasis.
    pub bold: bool,
    /// Emphasis.
    pub italic: bool,
    /// Muted text, unless a foreground is set.
    pub dim: bool,
    /// Link decoration.
    pub underline: bool,
    /// Deleted text.
    pub strikethrough: bool,
}

/// Text with a style and optional link destination.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Span {
    /// Visible text, with Markdown delimiters removed.
    pub text: String,
    /// Presentation attributes.
    pub style: Style,
    /// Link target, retained independently of the visible label.
    pub link: Option<Arc<str>>,
}

impl Clone for Span {
    fn clone(&self) -> Self {
        acyclic_markdown_work::record!(span_copy(self.text.len()));
        Self {
            text: self.text.clone(),
            style: self.style,
            link: self.link.clone(),
        }
    }
}

/// One unwrapped display line. The renderer owns viewport layout.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Line {
    /// A fenced or indented code row, including its language label. Renderers
    /// paint a continuous background band without inventing a syntax palette.
    pub code: bool,
    /// Deferred syntax for the raw code suffix. A viewport requests its styles
    /// only when painting this line; the printable spans remain usable alone.
    pub syntax: Option<crate::CodeLine>,
    /// Ordered styled runs.
    pub spans: Vec<Span>,
}

impl Line {
    pub(crate) fn push(&mut self, text: &str, style: Style, link: Option<&Arc<str>>) {
        if text.is_empty() {
            return;
        }
        #[cfg(test)]
        crate::work::copy(text.len());
        if let Some(last) = self.spans.last_mut()
            && last.style == style
            && match (&last.link, link) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            }
        {
            acyclic_markdown_work::record!(span_copy(text.len()));
            #[cfg(test)]
            crate::work::grow(text.len());
            last.text.push_str(text);
        } else {
            self.push_new(text, style, link);
        }
    }

    pub(crate) fn push_new(&mut self, text: &str, style: Style, link: Option<&Arc<str>>) {
        if !text.is_empty() {
            acyclic_markdown_work::record!(span_copy(text.len()));
            #[cfg(test)]
            crate::work::grow(std::mem::size_of::<Span>());
            if self.spans.len() == self.spans.capacity() {
                // Most display rows contain one or two spans. Keep geometric
                // growth without Vec's four-element minimum allocation.
                self.spans.reserve_exact(self.spans.capacity().max(1));
            }
            self.spans.push(Span {
                text: text.into(),
                style,
                link: link.cloned(),
            });
        }
    }

    pub(crate) fn plain(text: &str, style: Style) -> Self {
        let mut line = Self::default();
        line.push(text, style, None);
        line
    }
}

pub(crate) const MUTED: Style = Style {
    dim: true,
    ..Style::DEFAULT
};

/// Text in `tone`, otherwise unstyled.
pub(crate) const fn toned(tone: Tone) -> Style {
    Style {
        foreground: Some(tone),
        ..Style::DEFAULT
    }
}

impl Style {
    pub(crate) const DEFAULT: Self = Self {
        foreground: None,
        bold: false,
        italic: false,
        dim: false,
        underline: false,
        strikethrough: false,
    };
}
