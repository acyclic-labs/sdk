use crate::config::{ThemeRole, ThemeStyles};
use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
use std::ops::Range;

/// Source progress of the borrowed window. Geometry never asks for a global
/// wrapped-row count: ranges and total use normalized source bytes.
pub struct TranscriptScrollbar<'a> {
    /// Source extent painted in the current window.
    pub visible: Range<usize>,
    /// Complete source length.
    pub total: usize,
    /// Followed theme styles.
    pub theme: &'a ThemeStyles,
}

impl TranscriptScrollbar<'_> {
    /// Map the track to source extent, with exact top/tail endpoints. Wide
    /// multiplication also covers a source length near the address-space limit.
    pub fn source_at(area: Rect, row: u16, total: usize) -> usize {
        let travel = area.height.saturating_sub(1);
        if travel == 0 {
            return 0;
        }
        let at = row.saturating_sub(area.y).min(travel);
        usize::try_from(u128::from(at) * total as u128 / u128::from(travel)).unwrap_or(total)
    }
}

impl Widget for TranscriptScrollbar<'_> {
    fn render(self, area: Rect, buffer: &mut Buffer) {
        let area = area.intersection(buffer.area);
        if self.total == 0 || area.width == 0 || area.height == 0 {
            return;
        }
        let visible = self
            .visible
            .end
            .min(self.total)
            .saturating_sub(self.visible.start.min(self.total));
        if visible == self.total {
            return;
        }
        let height = u128::from(area.height);
        let thumb = u16::try_from(
            (height * visible as u128 / self.total as u128)
                .max(1)
                .min(height),
        )
        .unwrap_or(area.height);
        let travel = area.height - thumb;
        let maximum = self.total - visible;
        let start = u16::try_from(
            self.visible.start.min(maximum) as u128 * u128::from(travel) / maximum as u128,
        )
        .unwrap_or(travel);
        for row in 0..area.height {
            let active = start <= row && row < start + thumb;
            buffer[(area.x, area.y + row)]
                .set_symbol(if active { "█" } else { "░" })
                .set_style(self.theme.style(if active {
                    ThemeRole::Text
                } else {
                    ThemeRole::Placeholder
                }));
        }
    }
}

#[cfg(test)]
mod tests;
