//! Retain both renderings of a span whose closing flanking is provisional.
use super::*;

pub(super) struct Flanking {
    opening: String,
    kind: TagEnd,
    style: Style,
    other: Line,
    closed: bool,
    raw_end: usize,
    body_end: usize,
}

impl Flanking {
    pub(super) fn new(
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        base: usize,
    ) -> Option<Self> {
        let (Event::Start(tag), range) = events.get(1)? else {
            return None;
        };
        let style = match tag {
            Tag::Emphasis => Style {
                italic: true,
                ..Style::default()
            },
            Tag::Strong => Style {
                bold: true,
                ..Style::default()
            },
            Tag::Strikethrough => Style {
                strikethrough: true,
                dim: true,
                ..Style::default()
            },
            _ => return None,
        };
        if range.start != 0 || range.end != source.len() {
            return None;
        }
        let (Event::Text(_), body) = events.get(2)? else {
            return None;
        };
        let opening = source.get(..body.start)?.to_owned();
        if opening.is_empty()
            || !opening
                .bytes()
                .all(|byte| Some(&byte) == opening.as_bytes().first())
        {
            return None;
        }
        let body_end = Self::shape(source, events, tag.to_end(), opening.len())??;
        Self::validate(source, events, opening.len()..body_end)?;
        let mut other = Line::default();
        other.push(&opening, Style::default(), None);
        Self::append(
            &mut other,
            source,
            events,
            opening.len()..body_end,
            Style::default(),
        )?;
        other.push(source.get(body_end..)?, Style::default(), None);
        Some(Self {
            opening,
            kind: tag.to_end(),
            style,
            other,
            closed: true,
            raw_end: base + source.len(),
            body_end: base + body_end,
        })
    }

    // Some(None) is the literal interpretation; Some(Some(end)) closes the
    // original outer span. Nested/opaque syntax requires the ordinary parser.
    fn shape(
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        kind: TagEnd,
        opening: usize,
    ) -> Option<Option<usize>> {
        let mut styled = false;
        let mut body_end = None;
        for (event, range) in events {
            match event {
                Event::Start(Tag::Paragraph) | Event::End(TagEnd::Paragraph) => {}
                Event::Start(tag)
                    if tag.to_end() == kind
                        && range.start == 0
                        && range.end == source.len()
                        && !styled =>
                {
                    styled = true;
                }
                Event::End(end) if *end == kind && styled => {}
                Event::Text(_) | Event::Code(_) => {
                    if styled {
                        body_end = Some(range.end);
                    }
                }
                _ => return None,
            }
        }
        if styled {
            let end = body_end?;
            (source.get(end..)? == source.get(..opening)?).then_some(Some(end))
        } else {
            Some(None)
        }
    }

    pub(super) fn advance(
        &mut self,
        source: &str,
        lines: &mut [Line],
        line_index: usize,
    ) -> Option<usize> {
        // Leave the previous closing run in parser input. When its right flank
        // changes, the alternate rendering is already retained, not reparsed.
        let context = inline::left_context(source.get(..self.body_end)?);
        let prefix = format!("{}{context}", self.opening);
        let tail = source.get(self.body_end..)?;
        if tail.contains('\n')
            || (source.as_bytes().get(self.body_end.checked_sub(1)?) == Some(&b'`')
                && tail.starts_with('`'))
        {
            return None;
        }
        let probe = format!("{prefix}{tail}");
        #[cfg(test)]
        {
            work::copy(probe.len() + prefix.len());
            work::scan(probe.len() * 2 + tail.len());
        }
        let (events, _) = events::scoped(&probe, &References::default(), options(), 0);
        let closing = Self::shape(&probe, &events, self.kind, self.opening.len())?;
        let closed = closing.is_some();
        let body_end = closing.map_or(source.trim_end_matches([' ', '\t']).len(), |end| {
            self.body_end + end.saturating_sub(prefix.len())
        });
        if body_end < self.body_end {
            return None;
        }
        let body_probe_end = prefix.len() + body_end - self.body_end;
        Self::validate(&probe, &events, prefix.len()..body_probe_end)?;
        let line = lines.get_mut(line_index)?;
        let (plain, styled) = if self.closed {
            (&mut self.other, &mut *line)
        } else {
            (&mut *line, &mut self.other)
        };
        let raw_end = source.trim_end_matches([' ', '\t']).len();
        let plain_start = prefix.len() + self.raw_end - self.body_end;
        Self::append(
            plain,
            &probe,
            &events,
            plain_start..body_probe_end.max(plain_start),
            Style::default(),
        )?;
        if closed {
            plain.push(
                source.get(body_end.max(self.raw_end)..raw_end)?,
                Style::default(),
                None,
            );
        }
        Self::append(
            styled,
            &probe,
            &events,
            prefix.len()..body_probe_end,
            self.style,
        )?;
        if closed != self.closed {
            std::mem::swap(line, &mut self.other);
        }
        self.closed = closed;
        self.raw_end = raw_end;
        self.body_end = body_end;
        Some(line_index)
    }

    fn validate(
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        body: Range<usize>,
    ) -> Option<()> {
        for (event, range) in events {
            let start = range.start.max(body.start);
            let end = range.end.min(body.end);
            if start >= end {
                continue;
            }
            if let Event::Text(text) = event
                && source.get(range.clone()) == Some(text.as_ref())
                && inline::unstable_text(source, start..end)
            {
                return None;
            }
        }
        Some(())
    }

    fn append(
        line: &mut Line,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        body: Range<usize>,
        style: Style,
    ) -> Option<()> {
        for (event, range) in events {
            let start = range.start.max(body.start);
            let end = range.end.min(body.end);
            if start >= end {
                continue;
            }
            match event {
                Event::Text(text) if source.get(range.clone()) == Some(text.as_ref()) => {
                    line.push(source.get(start..end)?, style, None);
                }
                Event::Text(text) if start == range.start && end == range.end => {
                    line.push(text, style, None);
                }
                Event::Code(text) if start == range.start && end == range.end => {
                    line.push(
                        text,
                        Style {
                            foreground: Some(style::YELLOW),
                            ..style
                        },
                        None,
                    );
                }
                Event::Start(_) | Event::End(_) => {}
                _ => return None,
            }
        }
        Some(())
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> (usize, usize) {
        (
            self.other
                .spans
                .iter()
                .map(|span| span.text.capacity())
                .sum(),
            self.opening.capacity() + self.other.spans.capacity() * std::mem::size_of::<Span>(),
        )
    }
}

impl Markdown {
    pub(super) fn advance_flanking(&mut self) -> Option<usize> {
        let flanking = self.flanking.as_mut()?;
        if let Some(line) = flanking.advance(&self.source, &mut self.lines, self.stable_lines) {
            return Some(line);
        }
        self.flanking = None;
        if let Some(inline) = &mut self.inline {
            inline.rewind();
        }
        self.plain = false;
        None
    }
}
