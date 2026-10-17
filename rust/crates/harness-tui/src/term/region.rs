//! Scrolling a band of whole rows in place on the device, so a frame that
//! moves content repaints only the rows it exposes.
use std::io::{self, Write};

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

impl RegionScroll {
    /// The scroll of `region` from showing line `from` at its top to `to`, if
    /// the region is unchanged.
    pub fn between(previous: Option<(Rect, usize)>, region: Rect, to: usize) -> Option<Self> {
        let (shown, from) = previous?;
        let rows = isize::try_from(to).ok()? - isize::try_from(from).ok()?;
        (shown == region && rows != 0).then_some(Self { region, rows })
    }

    /// The band and distance, when scrolling it in place leaves rows to keep:
    /// full-width rows of `area`, moved by less than the band's height.
    fn band(self, area: Rect) -> Option<(usize, usize, usize)> {
        let region = self.region;
        let full_width = region.x == area.x && region.width == area.width;
        let inside = region.y >= area.y && region.bottom() <= area.bottom();
        let distance = usize::try_from(self.rows.unsigned_abs()).ok()?;
        let height = usize::from(region.height);
        (full_width && inside && distance > 0 && distance < height).then(|| {
            let top = usize::from(region.y - area.y);
            (top, top + height, distance)
        })
    }

    /// Shift `shown` (the frame on the device) as the device will shift it,
    /// into `previous`, and encode the device scroll into `out`. Exposed rows
    /// are blank, as the device erases them. Returns whether it applied.
    pub(crate) fn apply(
        self,
        shown: &mut Buffer,
        previous: &mut Buffer,
        out: &mut impl Write,
    ) -> io::Result<bool> {
        let area = shown.area;
        let Some((top, bottom, distance)) = self.band(area) else {
            return Ok(false);
        };
        if previous.area != area {
            return Ok(false);
        }
        let width = usize::from(area.width);
        let band = &mut shown.content[top * width..bottom * width];
        let moved = distance * width;
        let exposed = if self.rows > 0 {
            band.rotate_left(moved);
            band.len() - moved..band.len()
        } else {
            band.rotate_right(moved);
            0..moved
        };
        for cell in &mut band[exposed] {
            cell.reset();
        }
        previous.content.clone_from(&shown.content);
        // DECSTBM bounds the scroll to the band, SU/SD scroll it, and the
        // margins are restored at once. Both leave the cursor at the origin.
        let direction = if self.rows > 0 { 'S' } else { 'T' };
        write!(
            out,
            "\x1b[{};{}r\x1b[{distance}{direction}\x1b[r",
            self.region.y + 1,
            self.region.bottom()
        )?;
        Ok(true)
    }
}
