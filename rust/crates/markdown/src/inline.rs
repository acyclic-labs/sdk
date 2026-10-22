use super::*;
use delimiters::Candidate;
use std::rc::Rc;

#[derive(Default, Clone, Copy)]
struct Mark {
    line: usize,
    visible: usize,
    spans: usize,
    bytes: usize,
}

#[derive(Default)]
pub(super) struct Checkpoint {
    offset: usize,
    mark: Mark,
    cost: usize,
    pub(super) valid: Range<usize>,
    openers: delimiters::State,
    reopen: Reopen,
    // Parallel to `openers`: where each opener's run starts in the source, and
    // a checkpoint there to resume from when a later closer matches it.
    fallbacks: Vec<Fallback>,
    // The reopened span's start, to resume from when it no longer closes.
    restart: Option<Rc<Checkpoint>>,
}

// Checkpoints held only as fallbacks keep the run starts but no checkpoints of
// their own, so the retained set stays bounded by the opener classes.
#[derive(Clone, Default)]
struct Fallback {
    start: Option<usize>,
    checkpoint: Option<Rc<Checkpoint>>,
}

// A span closed at the input edge whose checkpoint sits inside it, at the
// start of the run that currently closes it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Reopen {
    #[default]
    None,
    // The opening backtick run length.
    Code(usize),
    // An emphasis-like span; its opener is the state's reopened opener.
    Emphasis,
    Strong,
    Strikethrough,
}

impl Reopen {
    fn span(tag: &Tag<'_>) -> Self {
        match tag {
            Tag::Emphasis => Self::Emphasis,
            Tag::Strong => Self::Strong,
            Tag::Strikethrough => Self::Strikethrough,
            _ => Self::None,
        }
    }
}

#[derive(Default)]
pub(super) struct Inline {
    pub(super) checkpoint: Rc<Checkpoint>,
    provisional: Option<Rc<Checkpoint>>,
    start: usize,
}

impl Inline {
    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        let mut seen = HashSet::new();
        let mut pending: Vec<&Rc<Checkpoint>> = vec![&self.checkpoint];
        pending.extend(&self.provisional);
        let mut total = 0;
        while let Some(checkpoint) = pending.pop() {
            if !seen.insert(Rc::as_ptr(checkpoint)) {
                continue;
            }
            total += std::mem::size_of::<Checkpoint>()
                + checkpoint.openers.retained()
                + checkpoint.fallbacks.capacity() * std::mem::size_of::<Fallback>();
            pending.extend(
                checkpoint
                    .fallbacks
                    .iter()
                    .filter_map(|fallback| fallback.checkpoint.as_ref()),
            );
            pending.extend(&checkpoint.restart);
        }
        total
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

// One reparse of the open paragraph from a checkpoint.
struct Parse<'a> {
    source: &'a str,
    events: &'a [(Event<'static>, Range<usize>)],
    calls: &'a [(usize, usize)],
    sentinel: usize,
    runs: &'a [Range<usize>],
}

// Why a reparse does not continue its checkpoint's prefix.
enum Failure {
    // A later closer matched this synthetic opener.
    Opener(usize),
    // The reopened span no longer closes where it resumed.
    Restart,
    Block,
}

impl Markdown {
    pub(super) fn advance_inline(&mut self, previous: usize) -> Option<usize> {
        let inline = self.inline.as_ref()?;
        let resumed = Rc::clone(inline.provisional.as_ref().unwrap_or(&inline.checkpoint));
        let start = inline.start;
        let budget = self
            .source
            .len()
            .max(100_000)
            .saturating_sub(self.reference_cost);
        let mut attempt = Rc::clone(&resumed);
        loop {
            let context = self.reparse_context(attempt.offset, start, previous);
            let (prefix, runs, reopened) = resume_prefix(&attempt, &context);
            let sentinel = prefix.len();
            let tail = self.source.get(attempt.offset..).unwrap_or_default();
            let source = format!("{prefix}{tail}");
            #[cfg(test)]
            work::copy(source.len() + prefix.len());
            let (events, calls) = events::scoped(
                &source,
                &self.references,
                options(),
                budget.saturating_sub(attempt.cost),
            );
            if !paragraph(&source, &events) {
                return None;
            }
            let failure = match resumes(&events, sentinel, attempt.reopen, reopened, &runs) {
                Ok(()) => {
                    let parse = Parse {
                        source: &source,
                        events: &events,
                        calls: &calls,
                        sentinel,
                        runs: &runs,
                    };
                    return Some(self.commit_inline(&parse, &attempt, &resumed, start));
                }
                Err(failure) => failure,
            };
            // A later closer reinterprets source after the checkpoint before
            // what it matched; reparse once from there. That source is restyled.
            if !Rc::ptr_eq(&attempt, &resumed) {
                return None;
            }
            attempt = match failure {
                Failure::Opener(index) => resumed
                    .fallbacks
                    .get(index)
                    .and_then(|fallback| fallback.checkpoint.clone()),
                Failure::Restart => resumed.restart.clone(),
                Failure::Block => None,
            }
            .filter(|checkpoint| checkpoint.valid.contains(&budget))?;
        }
    }

    fn commit_inline(
        &mut self,
        parse: &Parse<'_>,
        attempt: &Rc<Checkpoint>,
        resumed: &Checkpoint,
        start: usize,
    ) -> usize {
        let Parse {
            source,
            events,
            sentinel,
            ..
        } = *parse;
        let (boundary, cut, top) = boundary(source, events, sentinel);
        let builder = Builder {
            parse,
            attempt,
            resumed,
        };
        // A checkpoint at the sentinel is the one it resumed from: a reopened
        // span consumes its synthetic opener, so no scan rebuilds it.
        let provisional = cut
            .as_ref()
            .filter(|cut| cut.offset != sentinel || cut.reopen != Reopen::None)
            .map(|cut| {
                let (candidates, marked) = cut.candidates(&top);
                Pending::new(cut.offset, candidates, marked, cut.reopen)
            });
        let committed =
            (boundary != sentinel).then(|| Pending::new(boundary, top, None, Reopen::None));
        let mut cuts = vec![boundary];
        cuts.extend(cut.as_ref().map(|cut| cut.offset));
        cuts.extend(cut.as_ref().and_then(|cut| cut.restart(sentinel)));
        for pending in committed.iter().chain(&provisional) {
            cuts.extend(pending.fresh(source, sentinel));
        }
        cuts.sort_unstable();
        let mark = attempt.mark;
        let first = self.stable_lines + mark.line;
        let mut renderer = render::Renderer::inline(self.reopen_line(first, mark));
        let marks = replay(
            &mut renderer,
            (source, events),
            &self.references,
            sentinel,
            (&cuts, mark),
        );
        #[cfg(test)]
        {
            let count = renderer.lines.len() + 1;
            work::copy(count * std::mem::size_of::<Line>());
            work::grow(count * std::mem::size_of::<Line>());
        }
        self.lines.reserve(renderer.lines.len() + 1);
        self.lines.append(&mut renderer.lines);
        self.lines.push(renderer.into_inline());
        let marks = Marks {
            cuts: &cuts,
            marks: &marks,
        };
        let checkpoint = committed.map_or_else(
            || Rc::clone(attempt),
            |pending| Rc::new(builder.build(pending, &marks, None)),
        );
        let provisional = cut.map(|cut| match provisional {
            Some(pending) => {
                let restart = builder.restart(&cut, &pending, &marks);
                Rc::new(builder.build(pending, &marks, restart))
            }
            None => Rc::clone(attempt),
        });
        self.inline = Some(Inline {
            checkpoint,
            provisional,
            start,
        });
        let fallback = if mark.visible == 0 {
            self.visibility.end
        } else {
            self.stable_lines + mark.visible
        };
        self.visible_lines = self.visible_end(first, self.lines.len(), fallback);
        self.settle_plain(source, events, sentinel);
        first
    }

    // Left context standing in for source before `base`. After a line break
    // only the row's column structure matters for a table interrupt.
    fn reparse_context(&self, base: usize, start: usize, previous: usize) -> String {
        let before = self.source.get(..base).unwrap_or_default();
        if base == start {
            String::new()
        } else if self
            .source
            .get(previous..)
            .unwrap_or_default()
            .contains('\n')
            || self.source.get(base..).unwrap_or_default().contains('\n')
        {
            row_prefix(before)
        } else {
            left_context(before)
        }
    }

    // Take the display line at `first` back to the checkpoint's span and byte.
    fn reopen_line(&mut self, first: usize, mark: Mark) -> Line {
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
        line
    }

    // Decide whether the next plain delta can extend the last span directly.
    fn settle_plain(
        &mut self,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        sentinel: usize,
    ) {
        let tail = source.get(sentinel..).unwrap_or_default();
        self.plain_links = tail.contains('[')
            && tail
                .rsplit_once(']')
                .is_none_or(|(_, rest)| rest.starts_with('('));
        self.plain_entity = tail
            .rsplit_once('&')
            .is_some_and(|(_, rest)| rest.chars().all(|c| c.is_ascii_alphanumeric() || c == '#'));
        self.plain_spaces = tail.bytes().rev().take_while(|byte| *byte == b' ').count();
        self.plain = tail.len() >= 8 && !tail.rsplit('\n').next().unwrap_or_default().trim_start_matches([' ', '\t']).starts_with('<') && !table_candidate(source) && !block_pending(tail.rsplit('\n').next().unwrap_or_default()) && events.iter().rev().find(|(event, _)| !matches!(event, Event::End(_)))
            .is_some_and(|(event, range)| matches!(event, Event::Text(text) if text.chars().next_back().is_some_and(plain_character))
                && range.end > sentinel && range.end == source.trim_end_matches(' ').len());
    }

    pub(super) fn begin_inline(
        &mut self,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        base: usize,
    ) {
        let content = source.trim_start_matches([' ', '\t']);
        if possible_definition(content)
            || !self.references.pending.is_empty()
            || !self.context.is_empty()
            || self.offset != base
            || !paragraph(source, events)
        {
            return;
        }
        let start = base + source.len() - content.len();
        self.inline = Some(Inline {
            checkpoint: Rc::new(Checkpoint {
                offset: start,
                valid: 0..usize::MAX,
                ..Checkpoint::default()
            }),
            provisional: None,
            start,
        });
        // Replay only this initial open tail; subsequent pushes retain its prefix.
        let plain = self.plain;
        let _ = self.advance_inline(self.source.len());
        self.plain = plain;
    }
}

// The synthetic prefix resuming at a checkpoint, each synthetic opener's run
// in it, and where the reopened span's opener starts.
fn resume_prefix(
    checkpoint: &Checkpoint,
    context: &str,
) -> (String, Vec<Range<usize>>, Option<usize>) {
    let (mut prefix, runs) = checkpoint.openers.prefix(context);
    let mut reopened = checkpoint
        .openers
        .reopen()
        .and_then(|index| runs.get(index))
        .map(|run| run.start);
    if let Reopen::Code(run) = checkpoint.reopen {
        // Reopen the code span: its opener and a stand-in for the committed
        // content byte keep the old closing run a separate run.
        reopened = Some(prefix.len());
        prefix.extend(std::iter::repeat_n('`', run));
        prefix.push('x');
    }
    (prefix, runs, reopened)
}

// A checkpoint to build once the replay has marked its display position: the
// openers among `candidates` before `end`, inside a span reopened at `marked`.
struct Pending {
    end: usize,
    candidates: Vec<Candidate>,
    marked: Option<usize>,
    reopen: Reopen,
    state: delimiters::State,
    kept: Vec<usize>,
}

impl Pending {
    fn new(end: usize, candidates: Vec<Candidate>, marked: Option<usize>, reopen: Reopen) -> Self {
        let (state, kept) = delimiters::State::compile(&candidates, end, marked);
        Self {
            end,
            candidates,
            marked,
            reopen,
            state,
            kept,
        }
    }

    // Run starts of kept openers parsed in this reparse, where fallbacks go.
    fn fresh<'a>(&'a self, source: &'a str, sentinel: usize) -> impl Iterator<Item = usize> + 'a {
        self.kept
            .iter()
            .filter_map(|index| self.candidates.get(*index)?.start)
            .filter(move |start| *start >= sentinel && delimiters::safe_boundary(source, *start))
    }
}

struct Marks<'a> {
    cuts: &'a [usize],
    marks: &'a [Mark],
}

impl Marks<'_> {
    fn at(&self, cut: usize) -> Mark {
        let index = self.cuts.partition_point(|position| *position < cut);
        self.marks.get(index).copied().unwrap_or_default()
    }
}

struct Builder<'a> {
    parse: &'a Parse<'a>,
    attempt: &'a Checkpoint,
    resumed: &'a Checkpoint,
}

impl Builder<'_> {
    fn build(
        &self,
        pending: Pending,
        marks: &Marks<'_>,
        restart: Option<Rc<Checkpoint>>,
    ) -> Checkpoint {
        let fallbacks = pending
            .kept
            .iter()
            .filter_map(|index| {
                let candidate = pending.candidates.get(*index)?;
                Some(if Some(*index) == pending.marked {
                    Fallback {
                        start: self.absolute(candidate.start),
                        checkpoint: restart.clone(),
                    }
                } else {
                    self.fallback(candidate, &pending, marks)
                })
            })
            .collect();
        let mark = marks.at(pending.end);
        self.checkpoint(
            pending.end,
            (pending.state, pending.reopen),
            mark,
            fallbacks,
            restart,
        )
    }

    fn checkpoint(
        &self,
        end: usize,
        (openers, reopen): (delimiters::State, Reopen),
        mark: Mark,
        fallbacks: Vec<Fallback>,
        restart: Option<Rc<Checkpoint>>,
    ) -> Checkpoint {
        let (cost, valid) = references::extend_expansion(
            self.attempt.cost,
            &self.attempt.valid,
            self.parse
                .calls
                .iter()
                .filter(|(position, _)| *position <= end),
        );
        #[cfg(test)]
        work::grow(fallbacks.len() * std::mem::size_of::<Fallback>());
        Checkpoint {
            offset: self.attempt.offset + end - self.parse.sentinel,
            mark,
            cost,
            valid,
            openers,
            reopen,
            fallbacks,
            restart,
        }
    }

    // A fallback checkpoint resuming at `end` with the openers among
    // `candidates` before it. It records its openers' run starts only.
    fn shallow(
        &self,
        end: usize,
        (candidates, marked): (&[Candidate], Option<usize>),
        reopen: Reopen,
        mark: Mark,
    ) -> Rc<Checkpoint> {
        let (state, kept) = delimiters::State::compile(candidates, end, marked);
        let reopen = if state.reopen().is_some() {
            reopen
        } else {
            Reopen::None
        };
        let fallbacks = kept
            .iter()
            .filter_map(|index| candidates.get(*index))
            .map(|candidate| Fallback {
                start: self.absolute(candidate.start),
                checkpoint: None,
            })
            .collect();
        Rc::new(self.checkpoint(end, (state, reopen), mark, fallbacks, None))
    }

    // The source position of a reparse position: a synthetic run stands for
    // the opener its checkpoint recorded.
    fn absolute(&self, position: Option<usize>) -> Option<usize> {
        let position = position?;
        if position >= self.parse.sentinel {
            return Some(self.attempt.offset + position - self.parse.sentinel);
        }
        let index = self
            .parse
            .runs
            .iter()
            .position(|run| run.start == position)?;
        self.attempt.fallbacks.get(index)?.start
    }

    fn fallback(&self, candidate: &Candidate, pending: &Pending, marks: &Marks<'_>) -> Fallback {
        let start = self.absolute(candidate.start);
        let checkpoint = match candidate.start {
            Some(position) if position < self.parse.sentinel => self.inherited(position, start),
            Some(position) if delimiters::safe_boundary(self.parse.source, position) => {
                Some(self.shallow(
                    position,
                    (&pending.candidates, pending.marked),
                    pending.reopen,
                    marks.at(position),
                ))
            }
            _ => None,
        };
        Fallback { start, checkpoint }
    }

    // A synthetic opener keeps its fallback. After a retry the attempt is a
    // fallback itself, so look the opener up in the checkpoint it came from.
    fn inherited(&self, position: usize, start: Option<usize>) -> Option<Rc<Checkpoint>> {
        self.parse
            .runs
            .iter()
            .position(|run| run.start == position)
            .and_then(|index| self.attempt.fallbacks.get(index))
            .and_then(|fallback| fallback.checkpoint.clone())
            .or_else(|| {
                self.resumed
                    .fallbacks
                    .iter()
                    .find(|fallback| start.is_some() && fallback.start == start)
                    .and_then(|fallback| fallback.checkpoint.clone())
            })
    }

    // The checkpoint at the start of the span a cut reopens.
    fn restart(&self, cut: &Cut, pending: &Pending, marks: &Marks<'_>) -> Option<Rc<Checkpoint>> {
        let start = cut.start?;
        if start < self.parse.sentinel {
            return self
                .attempt
                .restart
                .clone()
                .or_else(|| self.resumed.restart.clone());
        }
        if !delimiters::safe_boundary(self.parse.source, start) {
            return None;
        }
        let top = pending
            .candidates
            .get(..pending.marked.unwrap_or(pending.candidates.len()))?;
        Some(self.shallow(start, (top, None), Reopen::None, marks.at(start)))
    }
}

// Whether the reparse continues the checkpoint's prefix. Only the reopened
// span may cross the sentinel, and it must still close: a later run that
// leaves it unclosed makes its opener literal.
fn resumes(
    events: &[(Event<'_>, Range<usize>)],
    sentinel: usize,
    reopen: Reopen,
    reopened: Option<usize>,
    runs: &[Range<usize>],
) -> Result<(), Failure> {
    let mut found = false;
    for (event, range) in events {
        let crossing = range.start < sentinel && range.end > sentinel;
        let span = match event {
            Event::Start(Tag::Paragraph) => continue,
            Event::Start(tag) => Reopen::span(tag),
            Event::Code(_) if crossing => Reopen::Code(range.end - range.start),
            _ => continue,
        };
        let expected = Some(range.start) == reopened
            && match (span, reopen) {
                (Reopen::Code(_), Reopen::Code(_)) => true,
                (span, reopen) => span == reopen && span != Reopen::None,
            };
        if expected && crossing {
            found = true;
        } else if expected {
            return Err(Failure::Restart);
        } else if crossing {
            return Err(
                match runs.iter().position(|run| run.contains(&range.start)) {
                    Some(index) if runs.get(index).map(|run| run.start) == reopened => {
                        Failure::Restart
                    }
                    Some(index) => Failure::Opener(index),
                    None => Failure::Block,
                },
            );
        }
    }
    if found || reopen == Reopen::None {
        Ok(())
    } else {
        Err(Failure::Restart)
    }
}

// Mark every remaining cut before `until`.
fn mark_before(
    renderer: &render::Renderer,
    (cuts, base): (&[usize], Mark),
    marks: &mut Vec<Mark>,
    until: usize,
) {
    if cuts.get(marks.len()).is_some_and(|cut| *cut < until) {
        let mark = renderer.inline_mark(base);
        while cuts.get(marks.len()).is_some_and(|cut| *cut < until) {
            marks.push(mark);
        }
    }
}

// Render the reparsed suffix after `sentinel` and mark each sorted cut in
// display coordinates: where the output covers exactly the source before it.
fn replay(
    renderer: &mut render::Renderer,
    (source, events): (&str, &[(Event<'_>, Range<usize>)]),
    references: &References,
    sentinel: usize,
    (cuts, base): (&[usize], Mark),
) -> Vec<Mark> {
    let mut marks = Vec::with_capacity(cuts.len());
    for (event, range) in events {
        // The left context, such as a line break, was rendered before.
        if matches!(
            event,
            Event::Start(Tag::Paragraph) | Event::End(TagEnd::Paragraph)
        ) || !matches!(event, Event::Start(_) | Event::End(_)) && range.end <= sentinel
        {
            continue;
        }
        match event {
            // A cut in a span's closing run follows all of its content, before
            // the style its end restores.
            Event::End(_) => mark_before(renderer, (cuts, base), &mut marks, range.end),
            _ => mark_before(renderer, (cuts, base), &mut marks, range.start + 1),
        }
        match event {
            Event::Text(text) if source.get(range.clone()) == Some(text.as_ref()) => {
                let mut from = sentinel.saturating_sub(range.start).min(text.len());
                while let Some(cut) = cuts
                    .get(marks.len())
                    .map(|cut| cut.saturating_sub(range.start).max(from))
                    .filter(|cut| *cut < text.len())
                {
                    if let Some(piece) = text.get(from..cut).filter(|piece| !piece.is_empty()) {
                        renderer.event_in(&Event::Text(piece.into()), Some(references));
                    }
                    marks.push(renderer.inline_mark(base));
                    from = cut;
                }
                if let Some(piece) = text.get(from..).filter(|piece| !piece.is_empty()) {
                    renderer.event_in(&Event::Text(piece.into()), Some(references));
                }
            }
            Event::Text(text) if range.start < sentinel => {
                let text = text.get(sentinel - range.start..).unwrap_or_default();
                renderer.event_in(&Event::Text(text.into()), Some(references));
            }
            // A reopened code span: its first content byte stands in for
            // content already rendered before the checkpoint.
            Event::Code(text) if range.start < sentinel => {
                if let Some(text) = text.get(1..).filter(|text| !text.is_empty()) {
                    renderer.event_in(&Event::Code(text.into()), Some(references));
                }
            }
            _ => renderer.event_in(event, Some(references)),
        }
        // A cut inside a code span follows the whole span.
        if matches!(event, Event::Code(_)) {
            mark_before(renderer, (cuts, base), &mut marks, range.end);
        }
    }
    mark_before(renderer, (cuts, base), &mut marks, usize::MAX);
    marks
}

fn left_context(source: &str) -> String {
    let trimmed = source.trim_end_matches([' ', '\t']);
    if trimmed.ends_with('\n') {
        return format!("x\n{}", source.get(trimmed.len()..).unwrap_or_default());
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
    // An HTML block opener interrupts a paragraph once its tag name or comment,
    // CDATA or processing-instruction marker completes (`</di` then `v`). Until
    // a byte outside those markers settles the row, it can still become one.
    if row.starts_with('<')
        && row.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'<' | b'/' | b'!' | b'?' | b'-' | b'[')
        })
    {
        return true;
    }
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
) -> (usize, Option<Cut>, Vec<Candidate>) {
    let mut scan = LimitScan::new(source);
    #[cfg(test)]
    work::scan(source.len() * 2);
    for (event, range) in events {
        scan.event(event, range);
    }
    let (boundary, provisional) = select_boundary(source, events, sentinel, &scan);
    (boundary, provisional, scan.openers)
}

// The earliest source offset a later delta can still reinterpret: an open
// bracket, a recent label, an unfinished delimiter run or a pending opener.
struct LimitScan<'a> {
    source: &'a str,
    limit: usize,
    brackets: Vec<usize>,
    labels: Vec<usize>,
    opaque_until: usize,
    closed_until: usize,
    cursor: usize,
    escaped: bool,
    quoted_end: usize,
    style_until: usize,
    openers: Vec<Candidate>,
    depth: usize,
    // The start of the last top-level span, and the opener runs among its
    // direct children; none when one could not be classified.
    inner: Option<(usize, Vec<Candidate>)>,
    // The end of the last span nested in a top-level span.
    inner_closed: usize,
}

impl<'a> LimitScan<'a> {
    fn new(source: &'a str) -> Self {
        let (prefix, row) = source.rsplit_once('\n').unwrap_or(("", source));
        let mut limit = if block_pending(row) {
            prefix.len()
        } else {
            source.len()
        };
        if table_candidate(source) {
            limit = limit.min(source.rfind('\n').unwrap_or(source.len()));
        }
        Self {
            source,
            limit,
            brackets: Vec::new(),
            labels: Vec::new(),
            opaque_until: 0,
            closed_until: 0,
            cursor: 0,
            escaped: false,
            quoted_end: 0,
            style_until: 0,
            openers: Vec::new(),
            depth: 0,
            inner: None,
            inner_closed: 0,
        }
    }

    fn event(&mut self, event: &Event<'_>, range: &Range<usize>) {
        match event {
            Event::Start(tag) if !matches!(tag, Tag::Paragraph) => {
                self.closed_until = self.closed_until.max(range.end);
                if self.depth == 0 {
                    self.inner = Some((range.start, Vec::new()));
                } else {
                    self.inner_closed = self.inner_closed.max(range.end);
                }
                self.depth += 1;
            }
            Event::End(tag) if *tag != TagEnd::Paragraph => {
                self.depth = self.depth.saturating_sub(1);
            }
            _ => {}
        }
        match event {
            Event::Text(text) => self.text(text, range),
            Event::Start(Tag::Link { link_type, .. } | Tag::Image { link_type, .. }) => {
                if matches!(event, Event::Start(Tag::Link { .. })) {
                    // A successful inner link deactivates enclosing link brackets.
                    // Image brackets remain active: image labels can contain links.
                    let source = self.source;
                    self.brackets.retain(|start: &usize| {
                        source.as_bytes().get(start.saturating_sub(1)) == Some(&b'!')
                    });
                }
                if matches!(
                    link_type,
                    pulldown_cmark::LinkType::Inline
                        | pulldown_cmark::LinkType::Autolink
                        | pulldown_cmark::LinkType::Email
                ) {
                    self.opaque_until = range.end;
                } else {
                    self.labels.push(range.start);
                }
            }
            _ => {}
        }
    }

    fn text(&mut self, text: &str, range: &Range<usize>) {
        let opaque = range.end <= self.opaque_until;
        let raw = self.source.get(range.clone()).unwrap_or_default();
        if raw != text {
            return;
        }
        for byte in self
            .source
            .as_bytes()
            .get(self.cursor..range.start)
            .unwrap_or_default()
        {
            self.escaped = *byte == b'\\' && !self.escaped;
        }
        for (index, byte) in raw.bytes().enumerate() {
            let offset = range.start + index;
            let quoted = self.escaped;
            self.escaped = byte == b'\\' && !self.escaped;
            self.cursor = offset + 1;
            if quoted {
                self.quoted_end = offset + 1;
                continue;
            }
            if matches!(byte, b'`' | b'<') {
                // These higher-precedence openers can consume a closing
                // delimiter or link bracket when a later chunk closes them.
                self.limit = self.limit.min(offset);
            }
            if !opaque {
                self.byte(byte, offset, range.end);
            }
        }
    }

    fn byte(&mut self, byte: u8, offset: usize, text_end: usize) {
        match byte {
            b'[' => self.brackets.push(offset),
            b']' => {
                if let Some(start) = self.brackets.pop()
                    && self.brackets.is_empty()
                {
                    self.labels.push(start);
                }
            }
            b'*' | b'_' | b'~' if offset >= self.closed_until && offset >= self.style_until => {
                if let Some((end, opener)) = delimiters::scan(self.source, offset, self.quoted_end)
                {
                    self.style_until = end;
                    if let Some(opener) = opener {
                        let start = end - opener.count();
                        self.openers.push(Candidate {
                            end: end.min(text_end),
                            start: (start >= self.closed_until).then_some(start),
                            opener,
                        });
                    }
                } else {
                    self.limit = self.limit.min(offset);
                }
            }
            b'*' | b'_' | b'~' if self.depth == 1 && offset >= self.style_until => {
                match delimiters::scan(self.source, offset, self.quoted_end) {
                    Some((end, opener)) => {
                        self.style_until = end;
                        if let (Some(opener), Some((_, inner))) = (opener, self.inner.as_mut()) {
                            let start = end - opener.count();
                            inner.push(Candidate {
                                end: end.min(text_end),
                                start: (start >= self.inner_closed).then_some(start),
                                opener,
                            });
                        }
                    }
                    None => self.inner = None,
                }
            }
            b'`' | b'&' | b'<' | b'\\' if offset >= self.closed_until => {
                self.limit = self.limit.min(offset);
            }
            _ => {}
        }
    }

    fn limit(&self) -> usize {
        let mut limit = self.limit;
        if let Some(start) = self.brackets.first() {
            limit = limit.min(*start);
        }
        if let Some(start) = self.labels.get(self.labels.len().saturating_sub(2)) {
            limit = limit.min(*start);
        }
        limit
    }
}

// The last complete top-level event before `limit` commits; at the input edge
// its predecessor (or a safe text frontier) remains a provisional checkpoint.
fn select_boundary(
    source: &str,
    events: &[(Event<'_>, Range<usize>)],
    sentinel: usize,
    scan: &LimitScan<'_>,
) -> (usize, Option<Cut>) {
    let (limit, quoted_end) = (scan.limit(), scan.quoted_end);
    let mut depth = 0usize;
    let mut boundary = sentinel;
    let mut provisional = None;
    for (event, range) in events {
        match event {
            // A line break ends at the next line's start, where a resumed
            // reparse would keep the indentation the paragraph strips.
            Event::Start(Tag::Paragraph)
            | Event::End(TagEnd::Paragraph)
            | Event::SoftBreak
            | Event::HardBreak => {
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
            provisional = (range.end == limit).then_some(Cut::at(boundary));
            if range.end == limit
                && let Event::Text(text) = event
                && source.get(range.clone()) == Some(text.as_ref())
            {
                let cut = delimiters::text_frontier(text, range.start).max(sentinel);
                if cut >= range.start && delimiters::safe_boundary(source, cut) {
                    provisional = Some(Cut::at(cut));
                }
            }
            if range.end == limit
                && let Some(cut) = match event {
                    Event::Code(_) => code_frontier(source, range, sentinel),
                    Event::End(tag) => span_frontier(source, *tag, range, scan),
                    _ => None,
                }
            {
                provisional = Some(cut);
            }
            // An escaped character is complete; never split its escape pair.
            if range.end == quoted_end {
                provisional = None;
            }
            boundary = range.end;
        }
    }
    (boundary, provisional)
}

// A provisional resume offset. Inside a reopened span (starting at `start`)
// it also carries the span's opener and the unmatched openers among its
// children.
struct Cut {
    offset: usize,
    start: Option<usize>,
    reopen: Reopen,
    openers: Vec<Candidate>,
}

impl Cut {
    fn at(offset: usize) -> Self {
        Self {
            offset,
            start: None,
            reopen: Reopen::None,
            openers: Vec::new(),
        }
    }

    // Top-level openers all precede a reopened span, so they come first. The
    // span's own opener follows them.
    fn candidates(&self, top: &[Candidate]) -> (Vec<Candidate>, Option<usize>) {
        #[cfg(test)]
        work::copy((top.len() + self.openers.len()) * std::mem::size_of::<Candidate>());
        let marked = (!self.openers.is_empty()).then_some(top.len());
        ([top, &self.openers].concat(), marked)
    }

    // A reopened span's start parsed in this reparse, where its restart goes.
    fn restart(&self, sentinel: usize) -> Option<usize> {
        self.start.filter(|start| *start >= sentinel)
    }
}

// An emphasis-like span closed at the input edge resumes inside it too. A
// later run can only extend its closer or match it elsewhere, so its opener
// and its children's unmatched openers stand in for its committed content.
// Delimiter runs longer than the span consumes leave ambiguous run lengths;
// an inner opener in the opener's class would replace it in the state.
fn span_frontier(
    source: &str,
    tag: TagEnd,
    range: &Range<usize>,
    scan: &LimitScan<'_>,
) -> Option<Cut> {
    let (reopen, counts) = match tag {
        TagEnd::Emphasis => (Reopen::Emphasis, 1..2),
        TagEnd::Strong => (Reopen::Strong, 2..3),
        TagEnd::Strikethrough => (Reopen::Strikethrough, 1..3),
        _ => return None,
    };
    let (start, inner) = scan.inner.as_ref()?;
    let (end, opener) = delimiters::scan(source, range.start, scan.quoted_end)?;
    let opener = opener?;
    let count = opener.count();
    let close = range.end.checked_sub(count)?;
    let byte = opener.byte();
    let exact = *start == range.start
        && end == range.start + count
        && counts.contains(&count)
        && close > end
        && source
            .as_bytes()
            .get(close..range.end)?
            .iter()
            .all(|next| *next == byte)
        && source.as_bytes().get(close - 1) != Some(&byte);
    if !exact
        || inner
            .iter()
            .any(|inner| inner.opener.class() == opener.class())
    {
        return None;
    }
    let mut openers = Vec::with_capacity(inner.len() + 1);
    openers.push(Candidate {
        end,
        start: Some(range.start),
        opener,
    });
    openers.extend(inner.iter().copied());
    Some(Cut {
        offset: close,
        start: Some(range.start),
        reopen,
        openers,
    })
}

// A code span closed at the input edge stays open to its content: a later
// backtick only lengthens the closing run. Resume at that run's start unless
// one space could be stripped from both content ends once the span grows.
fn code_frontier(source: &str, range: &Range<usize>, sentinel: usize) -> Option<Cut> {
    let raw = source.get(range.clone())?;
    let run = raw.bytes().take_while(|byte| *byte == b'`').count();
    let first = raw.as_bytes().get(run)?;
    (range.start < sentinel || !matches!(first, b' ' | b'\n' | b'\r')).then_some(Cut {
        offset: range.end.checked_sub(run)?,
        start: Some(range.start),
        reopen: Reopen::Code(run),
        openers: Vec::new(),
    })
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
