use core::num::NonZeroU16;

use compact_str::CompactString;

use crate::buffer::cell_width::CellWidth;
use crate::style::{Color, Modifier, Style};
use crate::symbols::merge::MergeStrategy;

/// Cell diffing options
#[derive(Debug, Default, Clone, Eq, PartialEq, Hash, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CellDiffOption {
    #[default]
    /// No special option.
    None,
    /// Skip this cell when diffing.
    ///
    /// This is helpful when it is necessary to prevent the buffer from overwriting a cell that is
    /// covered by something from an escape sequence, such as graphics or links.
    Skip,
    /// Always update this cell when diffing.
    ///
    /// This bypasses the equality check against the previous buffer. Use it when another
    /// renderer may draw over the same area, such as an external image pipeline, so Ratatui can
    /// redraw text there on the next render.
    AlwaysUpdate,
    /// Force a width regardless of the symbol text width.
    ///
    /// Escape sequences will have some computed width that does match what is written to the
    /// screen.
    ForcedWidth(NonZeroU16),
}

/// A buffer cell
#[derive(Debug, Default, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Cell {
    /// The string to be drawn in the cell.
    ///
    /// This accepts unicode grapheme clusters which might take up more than one cell.
    ///
    /// This is a [`CompactString`] which is a wrapper around [`String`] that uses a small inline
    /// buffer for short strings.
    ///
    /// See <https://github.com/ratatui/ratatui/pull/601> for more information.
    symbol: Option<CompactString>,
    // Reset cells keep heap storage, but still behave exactly like a missing symbol.
    symbol_reset: bool,

    /// The foreground color of the cell.
    pub fg: Color,

    /// The background color of the cell.
    pub bg: Color,

    /// The underline color of the cell.
    #[cfg(feature = "underline-color")]
    pub underline_color: Color,

    /// The modifier of the cell.
    pub modifier: Modifier,

    /// Special option applied when copying (diffing) the buffer to the screen (or another buffer).
    pub diff_option: CellDiffOption,

    /// Whether the cell should be skipped when copying (diffing) the buffer to the screen.
    ///
    /// Use [`CellDiffOption::Skip`] via [`set_diff_option`](Self::set_diff_option) instead.
    #[deprecated(
        since = "0.30.1",
        note = "use `set_diff_option(CellDiffOption::Skip)` instead"
    )]
    pub skip: bool,
}

impl Cell {
    /// An empty `Cell`
    #[allow(deprecated)]
    pub const EMPTY: Self = Self {
        symbol: None,
        symbol_reset: false,
        fg: Color::Reset,
        bg: Color::Reset,
        #[cfg(feature = "underline-color")]
        underline_color: Color::Reset,
        modifier: Modifier::empty(),
        diff_option: CellDiffOption::None,
        skip: false,
    };

    /// Creates a new `Cell` with the given symbol.
    ///
    /// This works at compile time and puts the symbol onto the stack. Fails to build when the
    /// symbol doesn't fit onto the stack and requires to be placed on the heap. Use
    /// `Self::default().set_symbol()` in that case. See [`CompactString::const_new`] for more
    /// details on this.
    pub const fn new(symbol: &'static str) -> Self {
        Self {
            symbol: Some(CompactString::const_new(symbol)),
            ..Self::EMPTY
        }
    }

    /// Gets the symbol of the cell.
    ///
    /// If the cell has no symbol, returns a single space character.
    #[must_use]
    pub fn symbol(&self) -> &str {
        if self.symbol_reset {
            " "
        } else {
            self.symbol.as_ref().map_or(" ", |s| s.as_str())
        }
    }

    /// Merges the symbol of the cell with the one already on the cell, using the provided
    /// [`MergeStrategy`].
    ///
    /// Merges [Box Drawing Unicode block] characters to create a single character representing
    /// their combination, useful for [border collapsing]. Currently limited to box drawing
    /// characters, with potential future support for others.
    ///
    /// Merging may not be perfect due to Unicode limitations; some symbol combinations might not
    /// produce a valid character. [`MergeStrategy`] defines how to handle such cases, e.g.,
    /// `Exact` for valid merges only, or `Fuzzy` for close matches.
    ///
    /// If the cell has no symbol set, it will set the symbol to the provided one rather than
    /// merging.
    ///
    /// # Example
    ///
    /// ```
    /// # use ratatui_core::buffer::Cell;
    /// use ratatui_core::symbols::merge::MergeStrategy;
    ///
    /// assert_eq!(
    ///     Cell::new("┘")
    ///         .merge_symbol("┏", MergeStrategy::Exact)
    ///         .symbol(),
    ///     "╆",
    /// );
    ///
    /// assert_eq!(
    ///     Cell::new("╭")
    ///         .merge_symbol("┘", MergeStrategy::Fuzzy)
    ///         .symbol(),
    ///     "┼",
    /// );
    /// ```
    ///
    /// [border collapsing]: https://ratatui.rs/recipes/layout/collapse-borders/
    /// [Box Drawing Unicode block]: https://en.wikipedia.org/wiki/Box_Drawing
    pub fn merge_symbol(&mut self, symbol: &str, strategy: MergeStrategy) -> &mut Self {
        let merged_symbol = self
            .symbol
            .as_ref()
            .filter(|_| !self.symbol_reset)
            .map_or(symbol, |s| strategy.merge(s, symbol));
        self.symbol = Some(CompactString::new(merged_symbol));
        self.symbol_reset = false;
        self
    }

    /// Sets the symbol of the cell.
    pub fn set_symbol(&mut self, symbol: &str) -> &mut Self {
        self.symbol_reset = false;
        if let Some(storage) = &mut self.symbol {
            storage.clear();
            storage.push_str(symbol);
        } else {
            self.symbol = Some(CompactString::new(symbol));
        }
        self
    }

    /// Appends a symbol to the cell.
    ///
    /// This is particularly useful for adding zero-width characters to the cell.
    pub(crate) fn append_symbol(&mut self, symbol: &str) -> &mut Self {
        self.symbol_reset = false;
        self.symbol.get_or_insert_default().push_str(symbol);
        self
    }

    /// Sets the symbol of the cell to a single character.
    pub fn set_char(&mut self, ch: char) -> &mut Self {
        let mut buf = [0; 4];
        self.set_symbol(ch.encode_utf8(&mut buf))
    }

    /// Sets the foreground color of the cell.
    pub const fn set_fg(&mut self, color: Color) -> &mut Self {
        self.fg = color;
        self
    }

    /// Sets the background color of the cell.
    pub const fn set_bg(&mut self, color: Color) -> &mut Self {
        self.bg = color;
        self
    }

    /// Sets the style of the cell.
    ///
    ///  `style` accepts any type that is convertible to [`Style`] (e.g. [`Style`], [`Color`], or
    /// your own type that implements [`Into<Style>`]).
    pub fn set_style<S: Into<Style>>(&mut self, style: S) -> &mut Self {
        let style = style.into();
        if let Some(c) = style.fg {
            self.fg = c;
        }
        if let Some(c) = style.bg {
            self.bg = c;
        }
        #[cfg(feature = "underline-color")]
        if let Some(c) = style.underline_color {
            self.underline_color = c;
        }
        self.modifier.insert(style.add_modifier);
        self.modifier.remove(style.sub_modifier);
        self
    }

    /// Returns the style of the cell.
    #[must_use]
    pub const fn style(&self) -> Style {
        Style {
            fg: Some(self.fg),
            bg: Some(self.bg),
            #[cfg(feature = "underline-color")]
            underline_color: Some(self.underline_color),
            add_modifier: self.modifier,
            sub_modifier: Modifier::empty(),
        }
    }

    /// Sets the cell to be skipped when copying (diffing) the buffer to the screen.
    ///
    /// This is helpful when it is necessary to prevent the buffer from overwriting a cell that is
    /// covered by an image from some terminal graphics protocol (Sixel / iTerm / Kitty ...).
    #[deprecated(
        since = "0.30.1",
        note = "use `set_diff_option(CellDiffOption::Skip)` instead"
    )]
    #[allow(deprecated)]
    pub const fn set_skip(&mut self, skip: bool) -> &mut Self {
        self.skip = skip;
        self
    }

    /// Sets cell [`CellDiffOption`].
    ///
    /// The diff options are for dealing with cells that are wider than a unit, that should always
    /// be updated, or that should not be updated at all (skip output due to preceding wider
    /// cells).
    pub const fn set_diff_option(&mut self, diff_option: CellDiffOption) -> &mut Self {
        self.diff_option = diff_option;
        self
    }

    /// Resets the cell to the empty state, retaining symbol capacity for the next frame.
    #[allow(deprecated)]
    pub fn reset(&mut self) {
        let symbol = self
            .symbol
            .take()
            .filter(CompactString::is_heap_allocated)
            .map(|mut storage| {
                storage.clear();
                storage
            });
        *self = Self {
            symbol_reset: symbol.is_some(),
            symbol,
            ..Self::EMPTY
        };
    }
}

impl PartialEq for Cell {
    /// Compares two `Cell`s for equality.
    ///
    /// Note that cells with no symbol (i.e., `Cell::EMPTY`) are considered equal to cells with a
    /// single space symbol. This is to ensure that empty cells are treated uniformly,
    /// regardless of how they were created
    fn eq(&self, other: &Self) -> bool {
        // Treat None and Some(" ") as equal
        let symbols_eq = self.symbol() == other.symbol();

        #[cfg(feature = "underline-color")]
        let underline_color_eq = self.underline_color == other.underline_color;
        #[cfg(not(feature = "underline-color"))]
        let underline_color_eq = true;

        #[allow(deprecated)]
        let skip_eq = self.skip == other.skip;

        symbols_eq
            && underline_color_eq
            && skip_eq
            && self.fg == other.fg
            && self.bg == other.bg
            && self.modifier == other.modifier
            && self.diff_option == other.diff_option
    }
}

impl Eq for Cell {}

impl core::hash::Hash for Cell {
    /// Hashes the cell.
    ///
    /// This treats symbols with Some(" ") as equal to None, so that empty cells are
    /// treated uniformly, regardless of how they were created.
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.symbol().hash(state);
        self.fg.hash(state);
        self.bg.hash(state);
        #[cfg(feature = "underline-color")]
        self.underline_color.hash(state);
        self.modifier.hash(state);
        self.diff_option.hash(state);
        #[allow(deprecated)]
        self.skip.hash(state);
    }
}

impl From<char> for Cell {
    fn from(ch: char) -> Self {
        let mut cell = Self::EMPTY;
        cell.set_char(ch);
        cell
    }
}

impl CellWidth for Cell {
    /// Returns [`CellDiffOption::ForcedWidth`] when set, otherwise computes the width from the
    /// cell's symbol.
    fn cell_width(&self) -> u16 {
        match self.diff_option {
            CellDiffOption::ForcedWidth(w) => w.get(),
            _ => self.symbol().cell_width(),
        }
    }
}

#[cfg(test)]
#[path = "cell_tests.rs"]
mod tests;
