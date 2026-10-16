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
}

impl Markdown {
    pub(super) fn advance_inline(&mut self, previous: usize) -> bool {
        let Some(inline) = self.inline.as_ref() else {
            return false;
        };
        if self
            .source
            .get(previous..)
            .unwrap_or_default()
            .contains('\n')
        {
            return false;
        }
        let base = inline.offset;
        let mark = inline.mark;
        let source = format!("x {}", self.source.get(base..).unwrap_or_default());
        #[cfg(test)]
        work::copy(source.len());
        let events = events::parse(&source, &self.references);
        if events.iter().any(|(event, _)| {
            matches!(event,
            Event::Text(text) | Event::Code(text) if text.contains(['\r','\n']))
        }) {
            return false;
        }
        let boundary = boundary(&source, &events);
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
                Event::Text(text) if range.start < 2 => {
                    let text = text.get(2 - range.start..).unwrap_or_default();
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
            offset: base + boundary.saturating_sub(2),
            mark: committed,
        });
        self.visible_lines = if line.spans.is_empty() {
            self.visibility.end
        } else {
            self.stable_lines + 1
        };
        let tail = source.get(2..).unwrap_or_default();
        self.plain_links = tail.contains('[')
            && tail
                .rsplit_once(']')
                .is_none_or(|(_, rest)| rest.starts_with('('));
        self.plain_entity = tail
            .rsplit_once('&')
            .is_some_and(|(_, rest)| rest.chars().all(|c| c.is_ascii_alphanumeric() || c == '#'));
        self.plain_spaces = tail.bytes().rev().take_while(|byte| *byte == b' ').count();
        self.plain = tail.len() >= 8 && events.iter().rev().find(|(event, _)| !matches!(event, Event::End(_)))
            .is_some_and(|(event, range)| matches!(event, Event::Text(text) if text.chars().next_back().is_some_and(plain_character))
                && range.end == source.trim_end_matches(' ').len());
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
            || source.contains('\n')
            || !matches!(events.first(), Some((Event::Start(Tag::Paragraph), _)))
        {
            return;
        }
        self.inline = Some(Inline {
            offset: base + source.len() - source.trim_start_matches([' ', '\t']).len(),
            mark: Mark::default(),
        });
        // Replay only this initial open tail; subsequent pushes retain its prefix.
        let plain = self.plain;
        self.advance_inline(self.source.len());
        self.plain = plain;
    }
}

fn possible_definition(source: &str) -> bool {
    source.starts_with('[')
        && source
            .split_once(']')
            .is_none_or(|(_, rest)| rest.starts_with(':') || rest.chars().all(char::is_whitespace))
}

fn boundary(source: &str, events: &[(Event<'_>, Range<usize>)]) -> usize {
    let mut limit = source.len();
    let mut brackets = Vec::new();
    let mut labels = Vec::new();
    for (event, range) in events {
        match event {
            Event::Text(text) => {
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
                            limit = limit.min(offset)
                        }
                        _ => {}
                    }
                }
            }
            Event::Start(Tag::Link { link_type, .. } | Tag::Image { link_type, .. })
                if !matches!(
                    link_type,
                    pulldown_cmark::LinkType::Inline
                        | pulldown_cmark::LinkType::Autolink
                        | pulldown_cmark::LinkType::Email
                ) =>
            {
                labels.push(range.start)
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
    let mut boundary = 2;
    for (event, range) in events {
        match event {
            Event::Start(Tag::Paragraph) | Event::End(TagEnd::Paragraph) => continue,
            Event::Start(_) => depth += 1,
            Event::End(_) => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0
            && range.end > 2
            && range.end <= limit
            && (source
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
