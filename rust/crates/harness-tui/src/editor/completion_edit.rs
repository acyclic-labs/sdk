use super::{Editor, normalize};
use std::ops::Range;

impl Editor {
    /// Atomically replace a grapheme/placeholder-aligned range, preserving the
    /// original cursor and selection for undo. Invalid ranges leave state intact.
    pub fn replace_range(&mut self, range: Range<usize>, text: &str, now_ms: u64) -> bool {
        if range.start > range.end
            || range.end > self.text().len()
            || self.buffer.floor(range.start) != range.start
            || self.buffer.floor(range.end) != range.end
        {
            return false;
        }
        self.replace(range, &normalize(text), now_ms, 1, true, false);
        true
    }
}

#[cfg(feature = "tui")]
impl Editor {
    pub(crate) fn completion_version(&self) -> (std::rc::Rc<()>, usize, usize) {
        (self.buffer.revision.clone(), self.cursor(), self.anchor())
    }
    pub(crate) fn completion_change(&self) -> Option<&(std::rc::Rc<()>, Range<usize>, usize)> {
        self.buffer.change.as_ref()
    }
    pub(crate) fn is_boundary(&self, offset: usize) -> bool {
        self.buffer.floor(offset) == offset
    }
}
