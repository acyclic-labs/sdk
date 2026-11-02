use ratatui::{style::Style, text::Span};

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
