//! Scrolling a band of whole rows in place on the device, so a frame that
//! moves content repaints only the rows it exposes.
use std::io::{self, Write};
use std::ops::Range;

use ratatui::{buffer::Buffer, layout::Rect};

/// Rows of `region` whose content moved up by `rows` lines since the previous
/// frame (down when negative), as when a transcript scrolls.
///
/// A hint only: the device scrolls the band and the previous frame is
/// shifted to match, so the diff still repaints every cell that differs.
/// A wrong hint costs bytes, never correctness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegionScroll {
    /// The band, in screen cells; only full-width bands scroll on the device.
    pub region: Rect,
    /// Lines the content moved up (negative: down).
    pub rows: isize,
}

/// Every coordinate used for slicing and device output comes from this check.
/// Rect's saturated endpoints cannot establish containment.
struct Band {
    cells: Range<usize>,
    moved: usize,
    distance: usize,
    top: u32,
    bottom: u32,
    up: bool,
}

impl Band {
    fn checked(scroll: RegionScroll, area: Rect, cells: usize) -> Option<Self> {
        let region = scroll.region;
        let bottom = u32::from(region.y) + u32::from(region.height);
        let area_bottom = u32::from(area.y) + u32::from(area.height);
        let distance = scroll.rows.unsigned_abs();
        let width = usize::from(area.width);
        if region.x != area.x
            || region.width != area.width
            || width == 0
            || region.y < area.y
            || bottom > area_bottom
            || distance == 0
            || distance >= usize::from(region.height)
        {
            return None;
        }
        let start = usize::from(region.y - area.y).checked_mul(width)?;
        let end = start.checked_add(usize::from(region.height).checked_mul(width)?)?;
        (end <= cells).then_some(Self {
            cells: start..end,
            moved: distance.checked_mul(width)?,
            distance,
            top: u32::from(region.y) + 1,
            bottom,
            up: scroll.rows > 0,
        })
    }
}

impl RegionScroll {
    /// The scroll of `region` from showing line `from` at its top to `to`, if
    /// the region is unchanged.
    pub fn between(previous: Option<(Rect, usize)>, region: Rect, to: usize) -> Option<Self> {
        let (shown, from) = previous?;
        let rows = isize::try_from(to).ok()? - isize::try_from(from).ok()?;
        (shown == region && rows != 0).then_some(Self { region, rows })
    }

    /// Shift the retained baseline in place and encode the device scroll.
    /// Exposed rows
    /// are blank, as the device erases them. Returns whether it applied.
    pub(crate) fn apply(self, shown: &mut Buffer, out: &mut impl Write) -> io::Result<bool> {
        let Some(checked) = Band::checked(self, shown.area, shown.content.len()) else {
            return Ok(false);
        };
        let band = &mut shown.content[checked.cells];
        let exposed = if checked.up {
            band.rotate_left(checked.moved);
            band.len() - checked.moved..band.len()
        } else {
            band.rotate_right(checked.moved);
            0..checked.moved
        };
        for cell in &mut band[exposed] {
            cell.reset();
        }
        // DECSTBM bounds the scroll to the band, SU/SD scroll it, and the
        // margins are restored at once. Both leave the cursor at the origin.
        let direction = if checked.up { 'S' } else { 'T' };
        write!(
            out,
            "\x1b[{};{}r\x1b[{}{direction}\x1b[r",
            checked.top, checked.bottom, checked.distance
        )?;
        Ok(true)
    }
}

#[cfg(test)]
#[path = "region_tests.rs"]
mod tests;
