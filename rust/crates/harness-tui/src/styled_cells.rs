use ratatui::{style::Style, text::Span};
use std::ops::Range;

/// Resolve ordered cell offsets against disjoint source styles in one pass.
/// Gaps and offsets after the final range use the default style.
pub(crate) fn ordered_styles(
    ranges: impl IntoIterator<Item = (Range<usize>, Style)>,
) -> impl FnMut(usize) -> Style {
    let mut ranges = ranges.into_iter().peekable();
    move |offset| {
        while ranges.next_if(|(range, _)| range.end <= offset).is_some() {}
        ranges
            .peek()
            .filter(|(range, _)| range.start <= offset)
            .map_or_else(Style::default, |(_, style)| *style)
    }
}

/// Append wrapped cells, preserving the prefix and joining adjacent cell styles.
pub(crate) fn append(
    spans: &mut Vec<Span<'static>>,
    cells: impl IntoIterator<Item = (usize, String)>,
    mut style_at: impl FnMut(usize) -> Style,
) {
    let first = spans.len();
    for (offset, symbol) in cells {
        let style = style_at(offset);
        let joinable = spans.len() > first;
        match spans.last_mut() {
            Some(span) if joinable && span.style == style => {
                span.content.to_mut().push_str(&symbol);
            }
            _ => spans.push(Span::styled(symbol, style)),
        }
    }
}
