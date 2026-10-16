//! Renderer-independent presentation values.

use std::sync::Arc;

/// An RGB foreground color.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Color(pub u8, pub u8, pub u8);

/// Additive text attributes, independent of a terminal or browser.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Style {
    /// Foreground; absent means the renderer's default.
    pub foreground: Option<Color>,
    /// Strong emphasis.
    pub bold: bool,
    /// Emphasis.
    pub italic: bool,
    /// Muted text.
    pub dim: bool,
    /// Link decoration.
    pub underline: bool,
    /// Deleted text.
    pub strikethrough: bool,
}

/// Text with a style and optional link destination.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Span {
    /// Visible text, with Markdown delimiters removed.
    pub text: String,
    /// Presentation attributes.
    pub style: Style,
    /// Link target, retained independently of the visible label.
    pub link: Option<Arc<str>>,
}

/// One unwrapped display line. The renderer owns viewport layout.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Line {
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
            #[cfg(test)]
            if last.text.capacity() - last.text.len() < text.len() {
                crate::work::copy(last.text.len());
            }
            last.text.push_str(text);
        } else {
            self.push_new(text, style, link);
        }
    }

    pub(crate) fn push_new(&mut self, text: &str, style: Style, link: Option<&Arc<str>>) {
        if !text.is_empty() {
            #[cfg(test)]
            if self.spans.len() == self.spans.capacity() {
                crate::work::copy(self.spans.len() * std::mem::size_of::<Span>());
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
pub(crate) const BLUE: Color = Color(97, 175, 239);
pub(crate) const CYAN: Color = Color(86, 182, 194);
pub(crate) const YELLOW: Color = Color(229, 192, 123);
pub(crate) const MAGENTA: Color = Color(198, 120, 221);

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
