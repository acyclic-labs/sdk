//! Link metadata stays outside printable text and participates in frame diffs.
use ratatui::{
    buffer::{Buffer, CellDiffOption},
    layout::Rect,
};
use std::{collections::BTreeMap, ops::Range, sync::Arc};

/// One hyperlink's display columns in an already wrapped row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowLink {
    /// Half-open display-column range.
    pub columns: Range<usize>,
    /// The original destination, never interpreted as terminal text.
    pub target: Arc<str>,
}

/// Links on the visible screen, supplied with a frame. Empty for plain widgets.
/// Cells, including wide-glyph trails, have independent link identity, so a
/// destination-only edit, removal, scroll or recovery repaints the real label.
#[derive(Default)]
pub struct Hyperlinks(BTreeMap<(u16, u16), Arc<str>>);

impl Hyperlinks {
    /// Place a wrapped row's links in its clipped viewport rectangle.
    /// Control characters in a destination refuse that link, preventing OSC
    /// injection while preserving its ordinary printable label.
    pub fn row(&mut self, area: Rect, links: &[RowLink]) {
        if area.height == 0 {
            return;
        }
        for link in links {
            if link.target.chars().any(char::is_control) {
                continue;
            }
            for column in link
                .columns
                .clone()
                .take(usize::from(area.width))
                .filter(|&x| x < usize::from(area.width))
            {
                if let Ok(column) = u16::try_from(column) {
                    self.0
                        .insert((area.x + column, area.y), Arc::clone(&link.target));
                }
            }
        }
    }

    pub(crate) fn at(&self, x: u16, y: u16) -> Option<&str> {
        self.0.get(&(x, y)).map(AsRef::as_ref)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn repair(&self, old: &Self, next: &mut Buffer, repaired: &mut Vec<usize>) {
        for (x, y) in old.0.keys().chain(self.0.keys()) {
            if self.at(*x, *y) == old.at(*x, *y) || !next.area.contains((*x, *y).into()) {
                continue;
            }
            let index = usize::from(*y - next.area.y) * usize::from(next.area.width)
                + usize::from(*x - next.area.x);
            let cell = &mut next.content[index];
            if cell.diff_option == CellDiffOption::None {
                cell.diff_option = CellDiffOption::AlwaysUpdate;
                repaired.push(index);
            }
        }
    }

    pub(crate) fn scroll(&mut self, scroll: crate::term::RegionScroll) {
        self.0 = std::mem::take(&mut self.0)
            .into_iter()
            .filter_map(|((x, y), target)| {
                if !scroll.region.contains((x, y).into()) {
                    return Some(((x, y), target));
                }
                let shifted = isize::try_from(y).ok()?.checked_sub(scroll.rows)?;
                let y = u16::try_from(shifted).ok()?;
                scroll
                    .region
                    .contains((x, y).into())
                    .then_some(((x, y), target))
            })
            .collect();
    }
}
