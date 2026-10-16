use super::*;

#[derive(Default, Clone, Copy)]
struct Mark {
    spans: usize,
    bytes: usize,
}

#[derive(Default)]
pub(super) struct Inline {
    offset: usize,
    mark: Mark,
    start: usize,
    pub(super) cost: usize,
    pub(super) valid: Range<usize>,
}

impl Markdown {
    pub(super) fn advance_inline(&mut self, previous: usize) -> bool {
        let Some(inline) = self.inline.as_ref() else {
            return false;
        };
        let cost = inline.cost;
        let base = inline.offset;
        let mark = inline.mark;
        let tail = self.source.get(base..).unwrap_or_default();
        let prefix = if base == inline.start {
            String::new()
        } else if self
            .source
            .get(previous..)
            .unwrap_or_default()
            .contains('\n')
            || tail.contains('\n')
        {
            row_prefix(self.source.get(..base).unwrap_or_default())
        } else {
            String::from("x ")
        };
        let sentinel = prefix.len();
        let source = format!("{prefix}{tail}");
        #[cfg(test)]
        work::copy(source.len() + prefix.len());
        let (events, calls) = events::scoped(
            &source,
            &self.references,
            options(),
            self.source
                .len()
                .max(100_000)
                .saturating_sub(self.reference_cost)
                .saturating_sub(cost),
        );
        if !paragraph(&source, &events)
            || events.iter().any(|(event, _)| {
                matches!(event,
            Event::Text(text) | Event::Code(text) if text.contains(['\r','\n']))
            })
        {
            return false;
        }
        let boundary = boundary(&source, &events, sentinel);
        let (cost, valid) = references::extend_expansion(
            cost,
            &inline.valid,
            calls.iter().filter(|(end, _)| *end <= boundary),
        );
        let Some(line) = self.lines.get_mut(self.stable_lines) else {
            return false;
        };
        line.spans.truncate(mark.spans);
        if let Some(span) = line.spans.last_mut() {
            span.text.truncate(mark.bytes);
        }
        let mut renderer = render::Renderer::inline(std::mem::take(line));
        let mut committed = mark;
        for (event, range) in &events {
            match event {
                Event::Start(Tag::Paragraph) | Event::End(TagEnd::Paragraph) => continue,
                Event::Text(text) if range.start < sentinel => {
                    let text = text.get(sentinel - range.start..).unwrap_or_default();
                    renderer.event_in(&Event::Text(text.into()), Some(&self.references));
                }
                _ => renderer.event_in(event, Some(&self.references)),
            }
            if range.end == boundary {
                committed = renderer.inline_mark();
            }
        }
        *line = renderer.into_inline();
        self.inline = Some(Inline {
            offset: base + boundary.saturating_sub(sentinel),
            mark: committed,
            start: inline.start,
            cost,
            valid,
        });
        self.visible_lines = if line.spans.is_empty() {
            self.visibility.end
        } else {
            self.stable_lines + 1
        };
        let tail = source.get(sentinel..).unwrap_or_default();
        self.plain_links = tail.contains('[')
            && tail
                .rsplit_once(']')
                .is_none_or(|(_, rest)| rest.starts_with('('));
        self.plain_entity = tail
            .rsplit_once('&')
            .is_some_and(|(_, rest)| rest.chars().all(|c| c.is_ascii_alphanumeric() || c == '#'));
        self.plain_spaces = tail.bytes().rev().take_while(|byte| *byte == b' ').count();
        self.plain = tail.len() >= 8 && !table_candidate(&source) && !block_pending(tail.rsplit('\n').next().unwrap_or_default()) && events.iter().rev().find(|(event, _)| !matches!(event, Event::End(_)))
            .is_some_and(|(event, range)| matches!(event, Event::Text(text) if text.chars().next_back().is_some_and(plain_character))
                && range.end > sentinel && range.end == source.trim_end_matches(' ').len());
        true
    }

    pub(super) fn begin_inline(
        &mut self,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        base: usize,
    ) {
        let content = source.trim_start_matches([' ', '\t']);
        if !content.chars().any(char::is_alphabetic)
            || content.starts_with('<')
            || possible_definition(content)
            || !self.context.is_empty()
            || self.offset != base
            || !paragraph(source, events)
        {
            return;
        }
        self.inline = Some(Inline {
            offset: base + source.len() - source.trim_start_matches([' ', '\t']).len(),
            mark: Mark::default(),
            start: base + source.len() - source.trim_start_matches([' ', '\t']).len(),
            cost: 0,
            valid: 0..usize::MAX,
        });
        // Replay only this initial open tail; subsequent pushes retain its prefix.
        let plain = self.plain;
        self.advance_inline(self.source.len());
        self.plain = plain;
    }
}

fn table_candidate(source: &str) -> bool {
    source.rsplit_once('\n').is_some_and(|(_, next)| {
        next.bytes()
            .all(|byte| matches!(byte, b'|' | b'-' | b':' | b' ' | b'\t'))
    })
}

// Only column structure is needed to recognize a table interrupt after a soft
// break. Keep its source text in the rendered checkpoint, not in parser input.
fn row_prefix(source: &str) -> String {
    let row = source.rsplit('\n').next().unwrap_or_default();
    #[cfg(test)]
    work::scan(row.len() * 2);
    if !row.contains('|') {
        return String::from("x ");
    }
    #[cfg(test)]
    work::scan(row.len());
    let (mut escaped, mut content) = (false, false);
    let mut prefix = String::new();
    for byte in row.bytes() {
        if byte == b'|' && !escaped {
            prefix.push('|');
            content = false;
        } else if !byte.is_ascii_whitespace() && !content {
            prefix.push('x');
            content = true;
        }
        escaped = byte == b'\\' && !escaped;
    }
    prefix.push(' ');
    prefix
}

// Until content follows an empty marker it can still interrupt this paragraph.
fn block_pending(row: &str) -> bool {
    let row = row.trim_start_matches([' ', '\t']);
    if row
        .bytes()
        .all(|byte| matches!(byte, b' ' | b'\t' | b'-' | b'_' | b'*'))
    {
        return true;
    }
    let marker = row.trim_start_matches(|c: char| c.is_ascii_digit());
    let digits = row.len() - marker.len();
    (1..=9).contains(&digits)
        && marker
            .strip_prefix(['.', ')'])
            .is_some_and(|rest| rest.bytes().all(|byte| matches!(byte, b' ' | b'\t')))
        || row
            .strip_prefix(['-', '+', '*'])
            .is_some_and(|rest| rest.bytes().all(|byte| matches!(byte, b' ' | b'\t')))
}

fn paragraph(source: &str, events: &[(Event<'_>, Range<usize>)]) -> bool {
    matches!(events.first(), Some((Event::Start(Tag::Paragraph), _)))
        && events.iter().filter(|(event, _)| matches!(event, Event::Start(Tag::Paragraph))).count() == 1
        && !events.iter().any(|(event, _)| matches!(event, Event::HardBreak)
            || matches!(event, Event::Start(tag) if super::is_leaf(tag) && !matches!(tag, Tag::Paragraph)))
        && events.last().is_some_and(|(event, range)| matches!(event, Event::End(TagEnd::Paragraph))
            && source.get(range.end..).unwrap_or_default().is_empty())
}

fn possible_definition(source: &str) -> bool {
    source.starts_with('[')
        && source
            .split_once(']')
            .is_none_or(|(_, rest)| rest.starts_with(':') || rest.chars().all(char::is_whitespace))
}

fn boundary(source: &str, events: &[(Event<'_>, Range<usize>)], sentinel: usize) -> usize {
    let mut limit = source
        .rsplit_once('\n')
        .filter(|(_, row)| block_pending(row))
        .map_or(source.len(), |(prefix, _)| prefix.len());
    if table_candidate(source) {
        limit = limit.min(source.rfind('\n').unwrap_or(source.len()));
    }
    let mut brackets = Vec::new();
    let mut labels = Vec::new();
    let mut opaque_until = 0;
    for (event, range) in events {
        match event {
            Event::Text(text) => {
                if range.end <= opaque_until {
                    continue;
                }
                if source.get(range.clone()).unwrap_or_default() != text.as_ref() {
                    continue;
                }
                for (index, byte) in source
                    .get(range.clone())
                    .unwrap_or_default()
                    .bytes()
                    .enumerate()
                {
                    let offset = range.start + index;
                    match byte {
                        b'[' => brackets.push(offset),
                        b']' => {
                            if let Some(start) = brackets.pop()
                                && brackets.is_empty()
                            {
                                labels.push(start);
                            }
                        }
                        b'*' | b'_' | b'~' | b'`' | b'&' | b'<' | b'\\' => {
                            limit = limit.min(offset);
                        }
                        _ => {}
                    }
                }
            }
            Event::Start(Tag::Link { link_type, .. } | Tag::Image { link_type, .. }) => {
                if matches!(event, Event::Start(Tag::Link { .. })) {
                    // A successful inner link deactivates enclosing link brackets.
                    // Image brackets remain active: image labels can contain links.
                    brackets.retain(|start: &usize| {
                        source.as_bytes().get(start.saturating_sub(1)) == Some(&b'!')
                    });
                }
                if matches!(
                    link_type,
                    pulldown_cmark::LinkType::Inline
                        | pulldown_cmark::LinkType::Autolink
                        | pulldown_cmark::LinkType::Email
                ) {
                    opaque_until = range.end;
                } else {
                    labels.push(range.start);
                }
            }
            _ => {}
        }
    }
    if let Some(start) = brackets.first() {
        limit = limit.min(*start);
    }
    if let Some(start) = labels.get(labels.len().saturating_sub(2)) {
        limit = limit.min(*start);
    }
    let mut depth = 0usize;
    let mut boundary = sentinel;
    for (event, range) in events {
        match event {
            Event::Start(Tag::Paragraph) | Event::End(TagEnd::Paragraph) | Event::SoftBreak => {
                continue;
            }
            Event::Start(_) => depth += 1,
            Event::End(_) => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0
            && range.end > sentinel
            && range.end <= limit
            && (matches!(event, Event::End(TagEnd::Link | TagEnd::Image))
                || source
                    .as_bytes()
                    .get(range.end)
                    .is_some_and(u8::is_ascii_whitespace)
                || source
                    .as_bytes()
                    .get(range.end - 1)
                    .is_some_and(u8::is_ascii_whitespace))
        {
            boundary = range.end;
        }
    }
    boundary
}

impl render::Renderer {
    fn inline_mark(&self) -> Mark {
        let line = self.inline_line();
        Mark {
            spans: line.spans.len(),
            bytes: line.spans.last().map_or(0, |span| span.text.len()),
        }
    }
}
