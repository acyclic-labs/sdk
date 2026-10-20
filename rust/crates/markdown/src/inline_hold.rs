// A paragraph row of `=`, `-`, `_` or `*` markers and spaces can still turn
// the paragraph into a setext heading or end it before a thematic break; any
// other byte returns it to paragraph text. Feeding such a row to the inline
// engine would make each excursion discard and rebuild the whole open
// paragraph. Instead the engine stops at the row and the row is rendered from
// the engine's retained lines: shared paragraph runs under the heading style,
// the rule after a blank line, or the row's text after the paragraph's break.
// Markers at a row start follow a line break, so they can neither close an
// earlier delimiter nor change earlier inline output; the engine consumes the
// row unchanged once it stops being a candidate. Text and rule previews touch
// only the paragraph's last line, so only that line is saved until a heading
// preview restyles the whole paragraph.
use super::*;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Text,
    Rule,
    Heading(bool),
}

pub(super) struct Held {
    row: usize,
    read: usize,
    indent: usize,
    marker: Option<u8>,
    count: usize,
    // Bytes after the first marker.
    after: usize,
    // The first marker is followed by a space, as in a list item.
    listy: bool,
    // A space follows the first marker run.
    spaced: bool,
    // A marker follows a space.
    broken: bool,
    // The paragraph's previous row ends in a hard break.
    hard: bool,
    // The paragraph's first row ends in a hard break. Its first segment may
    // render no line, which a heading's marker would then occupy.
    first_break: bool,
    // The engine's first paragraph line and its visible paragraph lines.
    first: usize,
    lines: usize,
    // The engine's lines from `from`, restored before the engine resumes.
    from: usize,
    saved: Vec<Line>,
    shown: Option<Kind>,
}

impl Held {
    const fn new(row: usize) -> Self {
        Self {
            row,
            read: row,
            indent: 0,
            marker: None,
            count: 0,
            after: 0,
            listy: false,
            spaced: false,
            broken: false,
            hard: false,
            first_break: false,
            first: 0,
            lines: 0,
            from: 0,
            saved: Vec::new(),
            shown: None,
        }
    }

    // Consume the appended row bytes; false once the row is paragraph text or
    // another block, or a heading this preview cannot derive.
    fn scan(&mut self, source: &str) -> bool {
        self.consume(source) && !(self.first_break && matches!(self.kind(), Kind::Heading(_)))
    }

    fn consume(&mut self, source: &str) -> bool {
        let bytes = source.as_bytes().get(self.read..).unwrap_or_default();
        #[cfg(test)]
        work::scan(bytes.len());
        for &byte in bytes {
            match self.marker {
                None if byte == b' ' && self.indent < 3 => self.indent += 1,
                None if matches!(byte, b'=' | b'-' | b'_' | b'*') => {
                    self.marker = Some(byte);
                    self.count = 1;
                }
                Some(marker) if byte == b' ' => {
                    self.listy |= self.after == 0 && matches!(marker, b'-' | b'*');
                    self.spaced = true;
                    self.after += 1;
                }
                // `- -` and `* *` are list items; `= =` is text.
                Some(marker)
                    if byte == marker && !(self.spaced && (self.listy || marker == b'=')) =>
                {
                    self.broken |= self.spaced;
                    self.count += 1;
                    self.after += 1;
                }
                _ => return false,
            }
        }
        self.read = source.len();
        true
    }

    fn kind(&self) -> Kind {
        match self.marker {
            Some(b'=') => Kind::Heading(true),
            Some(b'-') if !self.broken => Kind::Heading(false),
            Some(_) if self.count >= 3 => Kind::Rule,
            _ => Kind::Text,
        }
    }

    // Save the engine's lines from `from` onward.
    fn save(&mut self, markdown: &mut Markdown, from: usize) -> Option<()> {
        let mut saved = Vec::with_capacity(self.from.checked_sub(from)?);
        for index in from..self.from {
            let line = markdown.lines.get_mut(index)?;
            saved.push(std::mem::take(line));
        }
        #[cfg(test)]
        work::copy(saved.len() * std::mem::size_of::<Line>());
        #[cfg(test)]
        if saved.capacity() - saved.len() < self.saved.len() {
            work::copy(saved.len() * std::mem::size_of::<Line>());
        }
        saved.append(&mut self.saved);
        self.saved = saved;
        self.from = from;
        Some(())
    }

    fn restore(&mut self, markdown: &mut Markdown) {
        markdown.lines.truncate(self.from);
        #[cfg(test)]
        work::copy(self.saved.len() * std::mem::size_of::<Line>());
        markdown.lines.append(&mut self.saved);
        markdown.visible_lines = self.first + self.lines;
    }

    // The engine's paragraph line at `index`, counted from its first line.
    fn line(&self, index: usize) -> Option<&Line> {
        self.saved.get((self.first + index).checked_sub(self.from)?)
    }

    // Render the held row; returns the first changed line.
    fn preview(&mut self, markdown: &mut Markdown) -> Option<usize> {
        let kind = self.kind();
        if self.shown == Some(kind) && kind != Kind::Text {
            return Some(markdown.visible_lines);
        }
        let heading = |kind: Kind| matches!(kind, Kind::Heading(_));
        if heading(kind) && self.from > self.first {
            self.save(markdown, self.first)?;
        }
        let last = self.first + self.lines - 1;
        let first = if heading(kind) || self.shown.is_some_and(heading) {
            self.first
        } else {
            last
        };
        self.shown = Some(kind);
        markdown.lines.truncate(first);
        #[cfg(test)]
        work::copy((last + 1 - first) * std::mem::size_of::<Line>());
        let style = Style::default();
        match kind {
            Kind::Heading(bold) => {
                let style = Style {
                    foreground: Some(style::CYAN),
                    bold,
                    ..Style::default()
                };
                for index in 0..self.lines {
                    let mut heading = Line::default();
                    if index == 0 {
                        heading.push("# ", style, None);
                    }
                    heading
                        .spans
                        .append_styled(self.line(index)?.spans.clone(), style);
                    markdown.lines.push(heading);
                }
            }
            Kind::Rule => {
                markdown.lines.push(self.line(self.lines - 1)?.clone());
                markdown.lines.push(Line::default());
                markdown.lines.push(render::rule());
            }
            Kind::Text => {
                let text = markdown
                    .source
                    .get(self.row..)
                    .unwrap_or_default()
                    .trim_matches(' ');
                #[cfg(test)]
                work::copy(text.len());
                let mut line = self.line(self.lines - 1)?.clone();
                if !text.is_empty() && self.hard {
                    markdown.lines.push(line);
                    line = Line::plain(text, style);
                } else if !text.is_empty() {
                    // A new run, so the engine's shared line is not materialized.
                    line.spans.push_content(
                        Span {
                            text: format!(" {text}").as_str().into(),
                            style,
                            link: None,
                        },
                        false,
                    );
                }
                markdown.lines.push(line);
            }
        }
        markdown.visible_lines = markdown.lines.len();
        Some(first)
    }
}

impl Markdown {
    pub(crate) fn advance_inline(
        &mut self,
        previous: usize,
        seed: Option<Seed<'_, '_>>,
    ) -> Option<usize> {
        let mut changed = usize::MAX;
        let mut previous = previous;
        if let Some(mut held) = self.inline.as_mut()?.held.take() {
            if held.scan(&self.source) {
                let first = held.preview(self)?;
                self.inline.as_mut()?.held = Some(held);
                return Some(first);
            }
            held.restore(self);
            changed = held.from;
            previous = previous.min(held.row);
        }
        if seed.is_some() {
            return self.advance_engine(previous, seed);
        }
        let held = self.held_row();
        #[cfg(test)]
        let held = held.filter(|_| !SKIP_HOLD.get());
        let Some(mut held) = held else {
            return self.advance_engine(previous, None).map(|c| c.min(changed));
        };
        let (row, classified) = (held.row, self.inline.as_ref()?.classified);
        // A backslash hard break changes the engine's preceding line, so this
        // preview cannot be derived from retained output. Reject it before
        // advancing a truncated prefix that would immediately be discarded.
        let hard = hard_break(&self.source, row - 1);
        if hard.is_none() {
            return self.advance_engine(previous, None).map(|c| c.min(changed));
        }
        if classified < row {
            // The engine consumes only the bytes before the held row.
            let tail = self.source.split_off(row);
            #[cfg(test)]
            work::copy(2 * tail.len());
            let advanced = self.advance_engine(previous.min(classified), None);
            self.source.push_str(&tail);
            changed = changed.min(advanced?);
        }
        let first = (self.stable_lines..self.visible_lines).find(|&index| {
            #[cfg(test)]
            work::scan(1);
            self.lines
                .get(index)
                .is_some_and(|line| !line.spans.is_empty())
        });
        let (Some(first), Some(hard)) = (first, hard) else {
            return self
                .advance_engine(previous.min(row), None)
                .map(|c| c.min(changed));
        };
        // The paragraph's visible lines end at its last nonempty line.
        let lines = (first..self.lines.len())
            .rev()
            .find(|&index| {
                #[cfg(test)]
                work::scan(1);
                self.lines
                    .get(index)
                    .is_some_and(|line| !line.spans.is_empty())
            })
            .map_or(0, |last| last + 1 - first);
        held.hard = hard;
        held.first = first;
        held.lines = lines;
        held.from = self.lines.len();
        held.save(self, first + lines - 1)?;
        changed = changed.min(held.preview(self)?);
        self.inline.as_mut()?.held = Some(held);
        Some(changed)
    }

    // The appended row that can still become a setext underline or a
    // thematic break, before the engine consumes it.
    fn held_row(&self) -> Option<Held> {
        let inline = self.inline.as_ref()?;
        let (start, classified) = (inline.start, inline.classified);
        let appended = self.source.get(classified..).unwrap_or_default();
        #[cfg(test)]
        work::scan(appended.len());
        let row = match appended.rfind('\n') {
            Some(at) => classified + at + 1,
            None if self.source.get(..classified)?.ends_with('\n') => classified,
            None => return None,
        };
        if row <= start || row >= self.source.len() || self.source.get(..row - 1)?.ends_with('\r') {
            return None;
        }
        let mut held = Held::new(row);
        if !held.consume(&self.source) {
            return None;
        }
        // Only the first row's end is needed; it never grows once ended.
        let first_row = self
            .source
            .get(start..row - 1)?
            .find('\n')
            .map_or(row - 1, |end| start + end);
        #[cfg(test)]
        work::scan(first_row - start);
        held.first_break = hard_break(&self.source, first_row) != Some(false);
        held.scan(&self.source).then_some(held)
    }
}
