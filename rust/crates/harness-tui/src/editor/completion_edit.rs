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
