use super::*;
#[path = "inline_classify.rs"]
mod classify;
#[path = "inline_hold.rs"]
mod hold;

// Events and expansion calls already parsed from a classification base.
pub(super) type Seed<'s, 'e> = (usize, &'s [(Event<'e>, Range<usize>)], &'s [(usize, usize)]);

#[derive(Default)]
pub(super) struct Checkpoint {
    offset: usize,
    cost: usize,
    pub(super) valid: Range<usize>,
}

#[derive(Default)]
pub(super) struct Inline {
    pub(super) checkpoint: Checkpoint,
    start: usize,
    scanner: inline_scan::Scanner,
    classified: usize,
    rows: classify::Rows,
    charges: Vec<(usize, usize, Range<usize>)>,
    code: Option<Range<usize>>,
    held: Option<hold::Held>,
}

impl Inline {
    fn classification(&mut self, source: &str, references: &References) -> Option<(usize, bool)> {
        let appended = source.get(self.classified..)?;
        let destination = self.scanner.prepare_classification(source, self.start);
        // Only a byte that may close an angle opener can reinterpret it.
        // Openers before the checkpoint are escaped or settled; a live one
        // holds the checkpoint behind it.
        let reopen = self
            .scanner
            .feed_angles(source)
            .map(|opener| opener.max(self.checkpoint.offset));
        let foreign = destination
            || reopen.is_some()
            || ((!references.committed.is_empty() || !references.pending.is_empty())
                && appended.contains(']'));
        let mut base = if foreign {
            self.scanner.classification_resume(self.classified, reopen)
        } else {
            self.classified
        };
        let row = self.rows.push(source);
        // A blank row ends a paragraph even when an inline construct remains
        // provisional. Hand it directly to the block parser; classifying it
        // here would allocate events that the fallback immediately discards.
        if self.rows.closed() {
            return None;
        }
        let classify = foreign || row.is_some() || appended.contains('\n');
        #[cfg(test)]
        let classify = if REPLAY_CLASSIFIER.get() {
            base = self.start;
            true
        } else {
            classify
        };
        if let Some(row) = row {
            base = base.min(row);
        }
        // A row can begin inside a multiline code span; reclassifying the row
        // can end the paragraph before the span closes, so classify from the
        // span's start.
        #[cfg(test)]
        let skip_code = SKIP_CODE.get();
        #[cfg(not(test))]
        let skip_code = false;
        if let Some(code) = self.code.as_ref().filter(|_| !skip_code)
            && code.start < base
            && base < code.end
        {
            base = code.start;
        }
        if classify {
            base = self.scanner.classification_boundary(base);
        }
        Some((base, classify))
    }

    fn classification_end(&self, length: usize, base: usize, classify: bool) -> usize {
        // EOF tick runs remain provisional in the native scanner. Exclude them
        // from foreign-scope classification so their temporary code projection
        // cannot hide label syntax needed when the run extends.
        let end = if classify {
            self.scanner.classification_end(length).max(base)
        } else {
            length
        };
        // A row that can still interrupt is decided by all of its bytes: an
        // EOF tick run is the content that makes `1. ` a list item.
        let undecided = self.rows.undecided();
        #[cfg(test)]
        let undecided = undecided.filter(|_| !SKIP_UNDECIDED.get());
        if self.rows.inline_at(end) && undecided.is_none() {
            end
        } else {
            length
        }
    }

    fn record_charges(
        &mut self,
        calls: &[(usize, usize)],
        base: usize,
        sentinel: usize,
        cost: usize,
        valid: Range<usize>,
    ) -> (usize, Range<usize>) {
        let (mut cost, mut valid) = (cost, valid);
        for (end, charge) in calls.iter().filter(|(end, _)| *end >= sentinel) {
            (cost, valid) =
                references::extend_expansion(cost, &valid, std::iter::once(&(*end, *charge)));
            #[cfg(test)]
            work::copy(
                std::mem::size_of::<(usize, usize, Range<usize>)>()
                    * (1 + if self.charges.len() == self.charges.capacity() {
                        self.charges.len()
                    } else {
                        0
                    }),
            );
            self.charges
                .push((base + end.saturating_sub(sentinel), cost, valid.clone()));
        }
        (cost, valid)
    }
    #[cfg(test)]
    pub(super) fn retained(
        &self,
        nodes: &mut HashSet<usize>,
        texts: &mut HashSet<usize>,
        links: &mut HashSet<*const u8>,
    ) -> (usize, usize) {
        let (payload, metadata) = self.scanner.glyph_retained(nodes, texts, links);
        (
            payload,
            metadata
                + self.scanner.retained()
                + self.charges.capacity() * std::mem::size_of::<(usize, usize, Range<usize>)>(),
        )
    }
    fn update_code(
        &mut self,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        base: usize,
        sentinel: usize,
    ) {
        let code = events
            .iter()
            .filter(|(event, range)| matches!(event, Event::Code(_)) && range.start >= sentinel)
            .map(|(_, range)| base + range.start - sentinel..base + range.end - sentinel)
            .next_back()
            .filter(|range| {
                #[cfg(test)]
                work::scan(range.len());
                source
                    .get(range.clone())
                    .is_some_and(|code| code.contains('\n'))
            });
        self.code = code.or(self.code.take().filter(|code| code.start < base));
    }

    pub(super) fn plain(&self) -> bool {
        self.held.is_none() && self.scanner.plain()
    }
    pub(super) fn visible_frontier(&self, stable: usize, fallback: usize) -> (usize, usize) {
        (stable, fallback)
    }
}

fn inline_paragraph(source: &str, events: &[(Event<'_>, Range<usize>)], sentinel: usize) -> bool {
    events
        .first()
        .is_none_or(|(_, range)| range.start <= sentinel)
        && paragraph(source, events)
        && !events.iter().any(|(event, range)| {
            matches!(event, Event::Start(tag) if !matches!(tag, Tag::Paragraph))
                && range.start < sentinel
                && range.end > sentinel
        })
}

impl Markdown {
    fn inline_source<'a>(
        &'a self,
        inline: &Inline,
        base: usize,
        end: usize,
        previous: usize,
        classify: bool,
    ) -> (std::borrow::Cow<'a, str>, usize) {
        let tail = self.source.get(base..end).unwrap_or_default();

        let context = if !classify || base == inline.start {
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
        let mut prefix = context;
        #[cfg(test)]
        if classify {
            work::scan(tail.len());
        }
        if classify
            && tail.contains("\\\n")
            && inline.scanner.expanded_before(base)
            && !inline.scanner.expands_after(base)
        {
            // Insert inside an existing cell without introducing a new row.
            let at = prefix
                .char_indices()
                .find(|(_, c)| *c != '|' && !c.is_whitespace())
                .map(|(at, _)| at)
                .or_else(|| prefix.find('|').map(|at| at + 1))
                .unwrap_or(0);
            prefix.insert_str(at, "`x`");
        }
        let sentinel = prefix.len();
        let source = if prefix.is_empty() {
            std::borrow::Cow::Borrowed(tail)
        } else {
            #[cfg(test)]
            work::copy(prefix.len() + tail.len());
            std::borrow::Cow::Owned(format!("{prefix}{tail}"))
        };
        #[cfg(test)]
        work::copy(prefix.len());
        (source, sentinel)
    }
    fn advance_engine(&mut self, previous: usize, seed: Option<Seed<'_, '_>>) -> Option<usize> {
        let mut inline = self.inline.take()?;
        let (base, classify) = inline.classification(&self.source, &self.references)?;
        while inline.charges.last().is_some_and(|(end, _, _)| *end > base) {
            #[cfg(test)]
            work::scan(1);
            inline.charges.pop();
        }
        let (cost, interval) = inline
            .charges
            .last()
            .map_or((0, 0..usize::MAX), |(_, cost, valid)| {
                (*cost, valid.clone())
            });
        let end = inline.classification_end(self.source.len(), base, classify);
        let (source, sentinel) = self.inline_source(&inline, base, end, previous, classify);
        let seed = seed.filter(|(seed_base, _, _)| *seed_base == base && end == self.source.len());
        let (events, calls) = if classify {
            if let Some((_, events, calls)) = seed {
                (
                    std::borrow::Cow::Borrowed(events),
                    std::borrow::Cow::Borrowed(calls),
                )
            } else {
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
                (
                    std::borrow::Cow::Owned(events),
                    std::borrow::Cow::Owned(calls),
                )
            }
        } else {
            (
                std::borrow::Cow::Borrowed(&[][..]),
                std::borrow::Cow::Borrowed(&[][..]),
            )
        };
        if classify && !inline_paragraph(&source, &events, sentinel) {
            return None;
        }
        if classify {
            inline.update_code(&self.source, &events, base, sentinel);
        }
        inline.scanner.frontier(
            inline_scan::Slice {
                original: &self.source,
                parsed: &source,
                start: inline.start,
                base,
                sentinel,
                boundary: sentinel,
                classified: classify,
            },
            &events,
            &self.references,
        );
        let frontier = inline.scanner.neutral_frontier();
        let boundary = if classify && frontier >= base {
            base + boundary(&source, &events, sentinel, frontier - base + sentinel)
                .saturating_sub(sentinel)
        } else {
            frontier
        };
        let boundary = inline.scanner.checkpoint(boundary);
        let (cost, valid) = inline.record_charges(&calls, base, sentinel, cost, interval);
        let changed = if previous < self.source.len() {
            let (changed, visible) = inline.scanner.render_tree(
                &self.source,
                inline.start,
                &mut self.lines,
                self.stable_lines,
                Some(&mut self.spare),
            )?;
            self.visible_lines = if visible == 0 {
                self.visibility.end
            } else {
                self.stable_lines + visible
            };
            self.plain = false;
            changed
        } else {
            inline.scanner.seed_input(&self.source, inline.start);
            // The block parser has just rendered this initial tail. Seed only
            // the retained state; its first projection will replace that tail.
            self.stable_lines
        };
        let offset = inline.checkpoint.offset.max(boundary);
        inline.scanner.permanent = inline.rows.permanent();
        inline.scanner.discard_before(offset);
        inline.checkpoint.offset = offset;
        inline.checkpoint.cost = cost;
        inline.checkpoint.valid = valid;
        inline.classified = self.source.len();
        self.inline = Some(inline);
        Some(changed)
    }

    pub(super) fn begin_inline(
        &mut self,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        calls: &[(usize, usize)],
        base: usize,
    ) {
        let paragraph_start = events.first().map_or(0, |(_, range)| range.start);
        let body = source.get(paragraph_start..).unwrap_or_default();
        let content = body.trim_start_matches([' ', '\t']);
        if !self.context.is_empty() || self.offset != base || !paragraph(source, events) {
            return;
        }
        let start = base + paragraph_start + body.len() - content.len();
        self.inline = Some(Inline {
            checkpoint: Checkpoint {
                offset: start,
                cost: 0,
                valid: 0..usize::MAX,
            },
            start,
            scanner: inline_scan::Scanner::new(&self.source, start),
            classified: start,
            rows: classify::Rows::at(start),
            charges: Vec::new(),
            code: None,
            held: None,
        });
        // Replay only this initial open tail; subsequent pushes retain its prefix.
        let plain = self.plain;
        let _ = self.advance_inline(self.source.len(), Some((base, events, calls)));
        self.plain = plain;
    }
}

pub(super) fn left_context(source: &str) -> String {
    match source.chars().next_back() {
        Some('\n') => "x\n".into(),
        Some(c) if c.is_whitespace() => "x ".into(),
        Some(c) if !c.is_ascii() => format!("x{c}"),
        Some(c) if !c.is_ascii_punctuation() => "x".into(),
        _ => ".".into(),
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

fn boundary(
    source: &str,
    events: &[(Event<'_>, Range<usize>)],
    sentinel: usize,
    frontier: usize,
) -> usize {
    let (prefix, row) = source.rsplit_once('\n').unwrap_or(("", source));
    let mut limit = frontier;
    if block_pending(row) {
        limit = limit.min(prefix.len());
    }
    if table_candidate(source) {
        limit = limit.min(source.rfind('\n').unwrap_or(source.len()));
    }
    limit = limit.min(source.trim_end_matches([' ', '\t']).len());
    let mut depth = 0usize;
    let mut boundary = sentinel;
    for (event, range) in events {
        match event {
            Event::Start(Tag::Paragraph) | Event::End(TagEnd::Paragraph) => continue,
            Event::Start(_) => depth += 1,
            Event::End(_) => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 {
            if range.end <= limit && range.end > sentinel {
                boundary = range.end;
            } else if let Event::Text(text) = event
                && source.get(range.clone()) == Some(text.as_ref())
                && range.start <= limit
                && range.end > limit
                && limit > sentinel
            {
                boundary = limit;
            }
        }
    }
    boundary
}

// Negative control: feed rule and setext candidate rows to the engine.
#[cfg(test)]
thread_local! { pub(crate) static SKIP_HOLD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(test)]
thread_local! { pub(crate) static SKIP_CODE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
// Negative control: treat rows that can still interrupt as decided.
#[cfg(test)]
thread_local! { pub(crate) static SKIP_UNDECIDED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(test)]
thread_local! { pub(super) static REPLAY_CLASSIFIER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
