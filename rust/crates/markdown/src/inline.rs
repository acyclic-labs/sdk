use super::*;

#[derive(Default, Clone, Copy)]
struct Mark {
    line: usize,
    visible: usize,
    spans: usize,
    bytes: usize,
}

// Content before this mark is retained only while the same span still crosses
// its synthetic opener. An unmatched opener or a changed outer span reopens the
// real paragraph. Delimiter runs inside styled text remain before the mark.
#[derive(Clone)]
struct Carry {
    prefix: String,
    kind: Option<TagEnd>,
}

#[derive(Default)]
pub(super) struct Checkpoint {
    offset: usize,
    mark: Mark,
    cost: usize,
    pub(super) valid: Range<usize>,
    openers: delimiters::State,
    carry: Option<Carry>,
}

#[derive(Default)]
pub(super) struct Inline {
    pub(super) checkpoint: Checkpoint,
    provisional: Option<Checkpoint>,
    start: usize,
}

impl Inline {
    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        let retained = |checkpoint: &Checkpoint| {
            checkpoint.openers.retained()
                + checkpoint
                    .carry
                    .as_ref()
                    .map_or(0, |carry| carry.prefix.capacity())
        };
        retained(&self.checkpoint) + self.provisional.as_ref().map_or(0, retained)
    }
    pub(super) fn visible_frontier(&self, stable: usize, fallback: usize) -> (usize, usize) {
        let mark = self.checkpoint.mark;
        (
            stable + mark.line,
            if mark.visible == 0 {
                fallback
            } else {
                stable + mark.visible
            },
        )
    }
}

impl Markdown {
    pub(super) fn advance_inline(&mut self, previous: usize) -> Option<usize> {
        let inline = self.inline.as_ref()?;
        let checkpoint = inline.provisional.as_ref().unwrap_or(&inline.checkpoint);
        let cost = checkpoint.cost;
        let base = checkpoint.offset;
        let mark = checkpoint.mark;
        let tail = self.source.get(base..).unwrap_or_default();
        let context = if let Some(carry) = &checkpoint.carry {
            carry.prefix.clone()
        } else if base == inline.start {
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
            left_context(self.source.get(..base).unwrap_or_default())
        };
        let prefix = checkpoint.openers.prefix(&context);
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
        let continues = |event: &Event<'_>, range: &Range<usize>| {
            range.start < sentinel
                && range.end > sentinel
                && checkpoint.carry.as_ref().is_some_and(|carry| {
                    range.start == sentinel - carry.prefix.len()
                        && match (carry.kind, event) {
                            (None, Event::Code(_)) => true,
                            (Some(kind), Event::Start(tag)) => tag.to_end() == kind,
                            _ => false,
                        }
                })
        };
        if checkpoint.carry.is_some()
            && !events.iter().any(|(event, range)| continues(event, range))
        {
            return None;
        }
        if !paragraph(&source, &events)
            || events.iter().any(|(event, range)| {
                matches!(event, Event::Start(tag) if !matches!(tag, Tag::Paragraph))
                    && range.start < sentinel
                    && range.end > sentinel
                    && !continues(event, range)
            })
        {
            return None;
        }
        let (boundary, mut provisional, openers, limit) = boundary(&source, &events, sentinel);
        let carry = carry(&source, &events, sentinel)
            .filter(|(end, _)| *end > provisional.unwrap_or(sentinel) && *end <= limit);
        if let Some((end, _)) = &carry {
            provisional = Some(*end);
        }
        let (cost, valid) = references::extend_expansion(
            cost,
            &checkpoint.valid,
            calls.iter().filter(|(end, _)| *end <= boundary),
        );
        let first = self.stable_lines + mark.line;
        let mut line = self
            .lines
            .get_mut(first)
            .map(std::mem::take)
            .unwrap_or_default();
        self.lines.truncate(first);
        line.spans.truncate(mark.spans);
        if let Some(span) = line.spans.last_mut() {
            span.text.truncate(mark.bytes);
        }
        let mut renderer = render::Renderer::inline(line);
        let mut committed = mark;
        let mut previous_mark = mark;
        let mut depth = 0usize;
        for (event, range) in &events {
            if range.end <= sentinel && !matches!(event, Event::Start(_) | Event::End(_)) {
                continue;
            }
            match event {
                Event::Code(text) if range.start < sentinel => {
                    // The retained code prefix contains one synthetic content byte.
                    renderer.event_in(
                        &Event::Code(text.get(1..).unwrap_or_default().into()),
                        Some(&self.references),
                    );
                }
                Event::Start(Tag::Paragraph) | Event::End(TagEnd::Paragraph) => continue,
                Event::Text(text) if source.get(range.clone()) == Some(text.as_ref()) => {
                    let start = sentinel.saturating_sub(range.start).min(text.len());
                    if let Some(cut) =
                        provisional.filter(|cut| *cut >= range.start + start && *cut < range.end)
                    {
                        let cut = cut - range.start;
                        renderer.event_in(
                            &Event::Text(text.get(start..cut).unwrap_or_default().into()),
                            Some(&self.references),
                        );
                        previous_mark = renderer.inline_mark(mark);
                        renderer.event_in(
                            &Event::Text(text.get(cut..).unwrap_or_default().into()),
                            Some(&self.references),
                        );
                    } else {
                        renderer.event_in(
                            &Event::Text(text.get(start..).unwrap_or_default().into()),
                            Some(&self.references),
                        );
                    }
                }
                Event::Text(text) if range.start < sentinel => {
                    let text = text.get(sentinel - range.start..).unwrap_or_default();
                    renderer.event_in(&Event::Text(text.into()), Some(&self.references));
                }
                _ => renderer.event_in(event, Some(&self.references)),
            }
            match event {
                Event::Start(_) => depth += 1,
                Event::End(_) => depth = depth.saturating_sub(1),
                _ => {}
            }
            if depth == 0 && range.end == boundary {
                committed = renderer.inline_mark(mark);
            }
            if carry.as_ref().is_some_and(|(end, _)| match event {
                Event::Code(_) => range.start < *end && *end < range.end,
                Event::Text(_) => range.end == *end,
                _ => false,
            }) {
                previous_mark = renderer.inline_mark(mark);
            }
            if depth == 0 && Some(range.end) == provisional {
                previous_mark = renderer.inline_mark(mark);
            }
        }
        #[cfg(test)]
        {
            let count = renderer.lines.len() + 1;
            work::copy(count * std::mem::size_of::<Line>());
            if self.lines.capacity() - self.lines.len() < count {
                work::copy(self.lines.len() * std::mem::size_of::<Line>());
            }
        }
        self.lines.reserve(renderer.lines.len() + 1);
        self.lines.append(&mut renderer.lines);
        self.lines.push(renderer.into_inline());
        self.inline = Some(Inline {
            checkpoint: Checkpoint {
                offset: base + boundary.saturating_sub(sentinel),
                mark: committed,
                cost,
                valid,
                openers: delimiters::State::compile(&openers, boundary),
                carry: (boundary == sentinel)
                    .then(|| checkpoint.carry.clone())
                    .flatten(),
            },
            provisional: provisional.map(|end| {
                let (cost, valid) = references::extend_expansion(
                    checkpoint.cost,
                    &checkpoint.valid,
                    calls.iter().filter(|(position, _)| *position <= end),
                );
                Checkpoint {
                    offset: base + end.saturating_sub(sentinel),
                    mark: previous_mark,
                    cost,
                    valid,
                    openers: delimiters::State::compile(&openers, end),
                    carry: carry.as_ref().map(|(_, carry)| carry.clone()).or_else(|| {
                        (end == sentinel)
                            .then(|| checkpoint.carry.clone())
                            .flatten()
                    }),
                }
            }),
            start: inline.start,
        });
        let fallback = if mark.visible == 0 {
            self.visibility.end
        } else {
            self.stable_lines + mark.visible
        };
        self.visible_lines = self.visible_end(first, self.lines.len(), fallback);
        let tail = source.get(sentinel..).unwrap_or_default();
        self.plain_links = tail.contains('[')
            && tail
                .rsplit_once(']')
                .is_none_or(|(_, rest)| rest.starts_with('('));
        self.plain_entity = tail
            .rsplit_once('&')
            .is_some_and(|(_, rest)| rest.chars().all(|c| c.is_ascii_alphanumeric() || c == '#'));
        self.plain_spaces = tail.bytes().rev().take_while(|byte| *byte == b' ').count();
        self.plain = tail.len() >= 8 && !tail.rsplit('\n').next().unwrap_or_default().trim_start_matches([' ', '\t']).starts_with('<') && !table_candidate(&source) && !block_pending(tail.rsplit('\n').next().unwrap_or_default()) && events.iter().rev().find(|(event, _)| !matches!(event, Event::End(_)))
            .is_some_and(|(event, range)| matches!(event, Event::Text(text) if text.chars().next_back().is_some_and(plain_character))
                && range.end > sentinel && range.end == source.trim_end_matches(' ').len());
        Some(first)
    }

    pub(super) fn begin_inline(
        &mut self,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        base: usize,
    ) {
        let content = source.trim_start_matches([' ', '\t']);
        if possible_definition(content)
            || !self.context.is_empty()
            || self.offset != base
            || !paragraph(source, events)
        {
            return;
        }
        self.inline = Some(Inline {
            checkpoint: Checkpoint {
                offset: base + source.len() - source.trim_start_matches([' ', '\t']).len(),
                mark: Mark::default(),
                cost: 0,
                valid: 0..usize::MAX,
                openers: delimiters::State::default(),
                carry: None,
            },
            provisional: None,
            start: base + source.len() - source.trim_start_matches([' ', '\t']).len(),
        });
        // Replay only this initial open tail; subsequent pushes retain its prefix.
        let plain = self.plain;
        let _ = self.advance_inline(self.source.len());
        self.plain = plain;
    }
}

fn carry(
    source: &str,
    events: &[(Event<'_>, Range<usize>)],
    sentinel: usize,
) -> Option<(usize, Carry)> {
    let mut style = None;
    let mut depth = 0usize;
    let mut candidate = None;
    let mut blocked = false;
    for (event, range) in events {
        match event {
            Event::Start(Tag::Paragraph) | Event::End(TagEnd::Paragraph) => {}
            Event::Start(tag) => {
                depth += 1;
                if depth == 1 {
                    blocked = false;
                    let marker = source.as_bytes().get(range.start);
                    let whole_run =
                        range.start == 0 || source.as_bytes().get(range.start - 1) != marker;
                    style = if whole_run {
                        match tag {
                            Tag::Emphasis => {
                                Some((tag.to_end(), &source[range.start..range.start + 1]))
                            }
                            Tag::Strong => {
                                Some((tag.to_end(), &source[range.start..range.start + 2]))
                            }
                            Tag::Strikethrough => None,
                            _ => None,
                        }
                    } else {
                        None
                    };
                    if whole_run && matches!(tag, Tag::Strikethrough) {
                        let count = source[range.start..]
                            .bytes()
                            .take_while(|byte| *byte == b'~')
                            .count();
                        style = Some((tag.to_end(), &source[range.start..range.start + count]));
                    }
                }
            }
            Event::End(_) => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    style = None;
                }
            }
            Event::Text(text)
                if depth == 1
                    && range.end > sentinel
                    && source.get(range.clone()) == Some(text.as_ref()) =>
            {
                blocked |= unstable_text(source, range.clone());
                if !blocked && let Some((kind, opening)) = style {
                    candidate = Some((
                        range.end,
                        Carry {
                            prefix: format!("{opening}{}", left_context(text)),
                            kind: Some(kind),
                        },
                    ));
                }
            }
            Event::Code(text) if depth == 0 => {
                let raw = source.get(range.clone())?;
                let count = raw.bytes().take_while(|byte| *byte == b'`').count();
                let end = range.end.checked_sub(count)?;
                let body = source.get(range.start + count..end)?;
                if count > 0
                    && end > sentinel
                    && body == text.as_ref()
                    && !body.starts_with(' ')
                    && !body.contains('\n')
                {
                    candidate = Some((
                        end,
                        Carry {
                            prefix: format!("{}x", &raw[..count]),
                            kind: None,
                        },
                    ));
                }
            }
            _ => {}
        }
    }
    candidate
}

fn unstable_text(source: &str, range: Range<usize>) -> bool {
    #[cfg(test)]
    work::scan(range.len());
    let mut cursor = range.start;
    for (index, byte) in source[range.clone()].bytes().enumerate() {
        let position = range.start + index;
        if position < cursor {
            continue;
        }
        match byte {
            b'*' | b'_' | b'~' => match delimiters::scan(source, position, 0) {
                Some((end, None)) if end < source.len() => cursor = end,
                _ => return true,
            },
            b'[' | b'`' | b'<' | b'&' | b'\\' => return true,
            _ => {}
        }
    }
    false
}

fn left_context(source: &str) -> String {
    let trimmed = source.trim_end_matches([' ', '\t']);
    if trimmed.ends_with('\n') {
        return format!("x\n{}", &source[trimmed.len()..]);
    }
    match source.chars().next_back() {
        Some(c) if c.is_whitespace() => "x ".into(),
        Some(c) if !c.is_ascii() => format!("x{c}"),
        Some(c) if !c.is_ascii_punctuation() => "x".into(),
        _ => "x.".into(),
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
        return left_context(source);
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
    match left_context(source).as_str() {
        "x " => prefix.push(' '),
        "x" => {}
        _ if prefix.ends_with('|') => {}
        context => {
            prefix.pop();
            prefix.push(context.chars().next_back().unwrap_or('.'));
        }
    }
    prefix
}

// Until content follows an empty marker it can still interrupt this paragraph.
fn block_pending(row: &str) -> bool {
    let row = row.trim_start_matches([' ', '\t']);
    // A one- or two-byte fence run is still paragraph text. Keep its line
    // boundary until another byte decides whether it interrupts the paragraph.
    if row.bytes().all(|byte| byte == b'~') || row.bytes().all(|byte| byte == b'`') {
        return true;
    }
    if row
        .bytes()
        .all(|byte| matches!(byte, b' ' | b'\t' | b'-' | b'_' | b'*'))
    {
        return true;
    }
    let marker = row.trim_start_matches(|c: char| c.is_ascii_digit());
    let digits = row.len() - marker.len();
    (1..=9).contains(&digits)
        && (marker.is_empty()
            || marker
                .strip_prefix(['.', ')'])
                .is_some_and(|rest| rest.bytes().all(|byte| matches!(byte, b' ' | b'\t'))))
        || row
            .strip_prefix(['-', '+', '*'])
            .is_some_and(|rest| rest.bytes().all(|byte| matches!(byte, b' ' | b'\t')))
}

fn paragraph(source: &str, events: &[(Event<'_>, Range<usize>)]) -> bool {
    matches!(events.first(), Some((Event::Start(Tag::Paragraph), _)))
        && events.iter().filter(|(event, _)| matches!(event, Event::Start(Tag::Paragraph))).count() == 1
        && !events.iter().any(|(event, _)| matches!(event, Event::Start(tag) if super::is_leaf(tag) && !matches!(tag, Tag::Paragraph)))
        && events.last().is_some_and(|(event, range)| matches!(event, Event::End(TagEnd::Paragraph))
            && source.get(range.end..).unwrap_or_default().bytes().all(|byte| matches!(byte, b' ' | b'\t')))
}

fn possible_definition(source: &str) -> bool {
    source.starts_with('[')
        && source
            .split_once(']')
            .is_none_or(|(_, rest)| rest.starts_with(':') || rest.chars().all(char::is_whitespace))
}

fn boundary(
    source: &str,
    events: &[(Event<'_>, Range<usize>)],
    sentinel: usize,
) -> (
    usize,
    Option<usize>,
    Vec<(usize, delimiters::Opener)>,
    usize,
) {
    let (prefix, row) = source.rsplit_once('\n').unwrap_or(("", source));
    let mut limit = if block_pending(row) {
        prefix.len()
    } else {
        source.len()
    };
    if table_candidate(source) {
        limit = limit.min(source.rfind('\n').unwrap_or(source.len()));
    }
    let mut brackets = Vec::new();
    let mut labels = Vec::new();
    let mut opaque_until = 0;
    let mut closed_until = 0;
    let (mut cursor, mut escaped) = (0, false);
    let mut quoted_end = 0;
    let mut style_until = 0;
    let mut openers = Vec::new();
    #[cfg(test)]
    work::scan(source.len() * 2);
    for (event, range) in events {
        if matches!(event, Event::Start(tag) if !matches!(tag, Tag::Paragraph)) {
            closed_until = closed_until.max(range.end);
        }
        match event {
            Event::Text(text) => {
                let opaque = range.end <= opaque_until;
                if source.get(range.clone()).unwrap_or_default() != text.as_ref() {
                    continue;
                }
                for byte in source
                    .as_bytes()
                    .get(cursor..range.start)
                    .unwrap_or_default()
                {
                    escaped = *byte == b'\\' && !escaped;
                }
                for (index, byte) in source
                    .get(range.clone())
                    .unwrap_or_default()
                    .bytes()
                    .enumerate()
                {
                    let offset = range.start + index;
                    let quoted = escaped;
                    escaped = byte == b'\\' && !escaped;
                    cursor = offset + 1;
                    if quoted {
                        quoted_end = offset + 1;
                        continue;
                    }
                    if matches!(byte, b'`' | b'<') {
                        // These higher-precedence openers can consume a closing
                        // delimiter or link bracket when a later chunk closes them.
                        limit = limit.min(offset);
                    }
                    if opaque {
                        continue;
                    }
                    match byte {
                        b'[' => brackets.push(offset),
                        b']' => {
                            if let Some(start) = brackets.pop()
                                && brackets.is_empty()
                            {
                                labels.push(start);
                            }
                        }
                        b'*' | b'_' | b'~' if offset >= closed_until && offset >= style_until => {
                            if let Some((end, opener)) =
                                delimiters::scan(source, offset, quoted_end)
                            {
                                style_until = end;
                                if let Some(opener) = opener {
                                    openers.push((end.min(range.end), opener));
                                }
                            } else {
                                limit = limit.min(offset);
                            }
                        }
                        b'`' | b'&' | b'<' | b'\\' => {
                            if offset >= closed_until {
                                limit = limit.min(offset);
                            }
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
    let mut provisional = None;
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
            && delimiters::safe_boundary(source, range.end)
        {
            // Every complete top-level event closes a checkpoint. At the input
            // edge retain its predecessor too: another byte can change delimiter
            // flanking or extend the closing run, regardless of the construct.
            provisional = (range.end == limit).then_some(boundary);
            if range.end == limit
                && let Event::Text(text) = event
                && source.get(range.clone()) == Some(text.as_ref())
            {
                let cut = delimiters::text_frontier(text, range.start).max(sentinel);
                if cut >= range.start && delimiters::safe_boundary(source, cut) {
                    provisional = Some(cut);
                }
            }
            // An escaped character is complete; never split its escape pair.
            if range.end == quoted_end {
                provisional = None;
            }
            boundary = range.end;
        }
    }
    (boundary, provisional, openers, limit)
}

impl render::Renderer {
    fn inline_mark(&self, base: Mark) -> Mark {
        let line = self.inline_line();
        let position = base.line + self.lines.len();
        let visible = if line.spans.is_empty() {
            self.lines
                .iter()
                .rposition(|line| {
                    #[cfg(test)]
                    work::visibility(1);
                    !line.spans.is_empty()
                })
                .map_or(base.visible, |index| base.line + index + 1)
        } else {
            position + 1
        };
        Mark {
            line: position,
            visible,
            spans: line.spans.len(),
            bytes: line.spans.last().map_or(0, |span| span.text.len()),
        }
    }
}
