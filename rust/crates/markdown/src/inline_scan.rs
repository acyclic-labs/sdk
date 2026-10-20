use super::*;
#[cfg(test)]
pub(crate) use forest::glyphs::events::KEEP_KEYS;
#[path = "inline_angles.rs"]
mod angles;
#[path = "inline_bind.rs"]
mod bind;
#[path = "inline_compact.rs"]
mod compact;
#[path = "inline_delimiters.rs"]
mod delimiters;
#[path = "inline_forest.rs"]
mod forest;
#[path = "inline_frontier.rs"]
mod frontier;
#[path = "inline_resume.rs"]
mod resume;
#[path = "inline_state.rs"]
mod state;
#[path = "inline_tokens.rs"]
mod tokens;
use state::State;
use tokens::{Kind, Tokens};

#[derive(Clone, Copy)]
pub(super) struct Slice<'a> {
    pub(super) original: &'a str,
    pub(super) parsed: &'a str,
    pub(super) start: usize,
    pub(super) base: usize,
    pub(super) sentinel: usize,
    pub(super) boundary: usize,
    pub(super) classified: bool,
}

#[derive(Default)]
pub(super) struct Scanner {
    tokens: Tokens,
    angles: angles::Angles,
    capsules: Vec<Range<usize>>,
    state: State,
    render: forest::Cache,
    syntax: Syntax,
    origin: usize,
    pub(super) permanent: bool,
    folded_structures: usize,
    processed: usize,
    projected: usize,
    fed: usize,
    discarded: usize,
    frozen: usize,
    frontier: usize,
    pending_break: Option<usize>,
    pending_after_break: bool,
    first_expansion: Option<usize>,
    line_content: bool,
    #[cfg(test)]
    replay: bool,
    #[cfg(test)]
    fed_bytes: usize,
}

struct Scope {
    next: usize,
    events: Range<usize>,
    range: Range<usize>,
    close: usize,
}

#[derive(Default)]
struct Syntax {
    outer: Vec<Scope>,
    hard_breaks: HashSet<usize>,
}

impl Syntax {
    fn new(
        events: &[(Event<'_>, Range<usize>)],
        parsed: &str,
        tokens: &Tokens,
        base: usize,
        sentinel: usize,
        discarded: usize,
        frozen: usize,
    ) -> Self {
        let mut syntax = Self::default();
        let mut links = Vec::<usize>::new();
        for (event_index, (event, range)) in events.iter().enumerate() {
            if base + range.end.saturating_sub(sentinel) <= frozen {
                continue;
            }
            #[cfg(test)]
            work::scan(1);
            if matches!(event, Event::HardBreak) {
                #[cfg(test)]
                work::copy(
                    std::mem::size_of::<usize>()
                        * (1 + if syntax.hard_breaks.len() == syntax.hard_breaks.capacity() {
                            syntax.hard_breaks.len()
                        } else {
                            0
                        }),
                );
                syntax.hard_breaks.insert(range.start);
            }
            let mut consumed = range.clone();
            match event {
                Event::Start(Tag::Link { link_type, .. } | Tag::Image { link_type, .. }) => {
                    // The reported range excludes the collapsed suffix. Use the
                    // lexer's source coverage, including first-pass escapes.
                    if matches!(
                        link_type,
                        pulldown_cmark::LinkType::Collapsed
                            | pulldown_cmark::LinkType::CollapsedUnknown
                    ) && let Some(end) =
                        tokens.collapsed_end(base + range.end.saturating_sub(sentinel), discarded)
                    {
                        consumed.end = sentinel + end.saturating_sub(base);
                    }
                    #[cfg(test)]
                    work::copy(
                        std::mem::size_of::<usize>()
                            * (1 + if links.len() == links.capacity() {
                                links.len()
                            } else {
                                0
                            }),
                    );
                    let index = syntax.outer.len();
                    syntax.scope(Scope {
                        next: index + 1,
                        events: event_index..event_index + 1,
                        range: consumed.clone(),
                        close: 0,
                    });
                    links.push(index);
                }
                Event::End(TagEnd::Link | TagEnd::Image) => {
                    let next = syntax.outer.len();
                    if let Some(frame) = links.pop()
                        && let Some(scope) = syntax.outer.get_mut(frame)
                    {
                        scope.events.end = event_index + 1;
                        scope.next = next;
                        if matches!(events.get(scope.events.start).map(|(event, _)| event),
                            Some(Event::Start(Tag::Link { link_type, .. } | Tag::Image { link_type, .. }))
                            if !matches!(link_type, pulldown_cmark::LinkType::Autolink | pulldown_cmark::LinkType::Email))
                        {
                            let end = if event_index > scope.events.start + 1 {
                                events
                                    .get(event_index - 1)
                                    .map_or(scope.range.start, |(_, range)| range.end)
                            } else {
                                scope.range.start
                            };
                            scope.close = parsed
                                .get(end..scope.range.end)
                                .and_then(|tail| tail.find(']'))
                                .map_or(0, |offset| {
                                    #[cfg(test)]
                                    work::scan(offset + 1);
                                    end + offset + 1
                                });
                        }
                    }
                }
                _ => {}
            }
            if matches!(event, Event::InlineHtml(_) | Event::Html(_)) {
                syntax.scope(Scope {
                    next: syntax.outer.len() + 1,
                    events: event_index..event_index + 1,
                    range: range.clone(),
                    close: 0,
                });
            }
        }
        syntax
    }

    fn scope(&mut self, scope: Scope) {
        #[cfg(test)]
        work::copy(
            std::mem::size_of::<Scope>()
                * (1 + if self.outer.len() == self.outer.capacity() {
                    self.outer.len()
                } else {
                    0
                }),
        );
        self.outer.push(scope);
    }

    fn absolute(&mut self, base: usize, sentinel: usize) {
        #[cfg(test)]
        work::copy(self.hard_breaks.len() * std::mem::size_of::<usize>());
        self.hard_breaks = self
            .hard_breaks
            .drain()
            .map(|position| {
                #[cfg(test)]
                work::scan(1);
                base + position.saturating_sub(sentinel)
            })
            .collect();
        for scope in &mut self.outer {
            if scope.close != 0 {
                scope.close = base + scope.close.saturating_sub(sentinel);
            }
        }
        for range in self.outer.iter_mut().map(|scope| &mut scope.range) {
            #[cfg(test)]
            work::scan(1);
            *range = base + range.start.saturating_sub(sentinel)
                ..base + range.end.saturating_sub(sentinel);
        }
    }

    fn ranges(&self) -> impl Iterator<Item = &Range<usize>> {
        let mut index = 0;
        std::iter::from_fn(move || {
            let scope = self.outer.get(index)?;
            index = scope.next;
            Some(&scope.range)
        })
    }

    fn changed_before(
        &self,
        previous: &Self,
        base: usize,
        end: usize,
        capsules: &[Range<usize>],
    ) -> Option<usize> {
        let mut old = previous.ranges().peekable();
        for range in self.ranges() {
            #[cfg(test)]
            work::scan(1);
            let range = range.start.max(base)..range.end;
            if range.start >= end || range.is_empty() {
                continue;
            }
            while old.peek().is_some_and(|old| old.end < range.end) {
                old.next();
                #[cfg(test)]
                work::scan(1);
            }
            if old.peek().is_some_and(|old| **old == range) {
                continue;
            }
            if capsules.last().is_some_and(|scope| scope.end >= range.end) {
                let index = capsules.partition_point(|old| {
                    #[cfg(test)]
                    work::scan(1);
                    old.end < range.end
                });
                if capsules
                    .get(index)
                    .is_some_and(|scope| scope.start <= range.start && range.end <= scope.end)
                {
                    continue;
                }
            }
            return Some(range.start);
        }
        None
    }
}

impl Scanner {
    pub(super) fn new(source: &str, start: usize) -> Self {
        Self {
            tokens: Tokens::at(
                start,
                source
                    .get(..start)
                    .and_then(|text| text.chars().next_back()),
            ),
            origin: start,
            processed: start,
            frozen: start,
            frontier: start,
            angles: angles::Angles::at(start),
            ..Self::default()
        }
    }

    /// Consume appended bytes in the angle recognizer. Returns the earliest
    /// opener whose construct they may close.
    pub(super) fn feed_angles(&mut self, source: &str) -> Option<usize> {
        let closed = self.angles.feed(source);
        self.state.angle = self.angles.floor();
        closed
    }

    #[cfg(test)]
    pub(super) fn retained(&self) -> usize {
        self.tokens.retained()
            + self.capsules.capacity() * std::mem::size_of::<Range<usize>>()
            + self.state.retained()
            + self.syntax.outer.capacity() * std::mem::size_of::<Scope>()
            + self.syntax.hard_breaks.capacity() * std::mem::size_of::<usize>()
    }

    pub(super) fn plain(&self) -> bool {
        self.state.neutral() && self.tokens.neutral()
    }

    pub(super) fn expanded_before(&self, position: usize) -> bool {
        self.first_expansion.is_some_and(|first| first < position)
    }

    pub(super) fn seed_input(&mut self, source: &str, start: usize) {
        self.render.seed_input(source, start);
    }

    pub(super) fn prepare_classification(&mut self, source: &str, start: usize) -> bool {
        self.tokens.push(source);
        self.render.track_destinations(source, start)
            && self
                .render
                .completed_destinations()
                .any(|start| start >= self.processed || self.state.destination_possible(start))
    }

    pub(super) fn classification_end(&self, end: usize) -> usize {
        self.tokens
            .preview()
            .filter(|token| matches!(token.kind, Kind::Run { byte: b'`', .. } | Kind::Escape('`')))
            .map_or(end, |token| token.range.start)
    }

    pub(super) fn expands_after(&self, start: usize) -> bool {
        self.tokens.expands_after(start)
    }

    pub(super) fn classification_boundary(&self, mut position: usize) -> usize {
        if let Some(range) = self.tokens.covering(position) {
            position = range.start;
        }
        if let Some(range) = self.state.forest.scope_covering(self.projected, position)
            && range.start < position
            && position < range.end
        {
            position = range.start;
        }
        if self
            .capsules
            .last()
            .is_none_or(|scope| scope.end <= position)
        {
            return position;
        }
        let (mut low, mut high) = (0, self.capsules.len());
        while low < high {
            #[cfg(test)]
            work::scan(1);
            let mid = low + (high - low) / 2;
            if self
                .capsules
                .get(mid)
                .is_some_and(|scope| scope.end <= position)
            {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        self.capsules
            .get(low)
            .filter(|scope| scope.start < position)
            .map_or(position, |scope| scope.start)
    }

    pub(super) fn reopen_start(&self, end: usize, closed: usize) -> usize {
        self.classification_boundary(self.state.reopen_start(end, closed))
    }

    pub(super) fn discard_before(&mut self, position: usize) {
        if position > self.frozen {
            self.render.freeze(position);
        }
        self.frozen = self.frozen.max(position);
        let before = self.tokens.ready.len();
        self.tokens.discard_before(position);
        let removed = before - self.tokens.ready.len();
        self.discarded += removed;
        self.fed = self.fed.saturating_sub(removed);
    }

    pub(super) fn render_tree(
        &mut self,
        source: &str,
        start: usize,
        lines: &mut crate::line_buffer::Buffer,
        first: usize,
        spare: Option<&mut crate::inline_views::Spare>,
    ) -> Option<(usize, usize)> {
        if self.state.invalid {
            return None;
        }
        #[cfg(test)]
        let rendering = work::rendering(true);
        let result = self.render.project(
            &mut self.state.forest,
            self.projected,
            &forest::glyphs::events::projection::Projection {
                source,
                visible: start..source.len(),
                hard_breaks: &self.syntax.hard_breaks,
            },
            lines,
            first,
            spare,
        );
        #[cfg(test)]
        work::rendering(rendering);
        result
    }

    #[cfg(test)]
    pub(super) fn glyph_retained(
        &self,
        nodes: &mut HashSet<usize>,
        texts: &mut HashSet<usize>,
        links: &mut HashSet<*const u8>,
    ) -> (usize, usize) {
        self.render.retained(nodes, texts, links)
    }

    pub(super) fn neutral_frontier(&self) -> usize {
        self.pending_break
            .map_or(self.frontier, |start| self.frontier.min(start))
    }

    pub(super) fn checkpoint(&self, position: usize) -> usize {
        if position == self.frontier {
            return position;
        }
        let mut boundary = self.classification_boundary(position);
        if let Some(range) = self.tokens.covering(position) {
            boundary = boundary.min(range.start);
        }
        if let Some(range) = self.state.forest.scope_covering(self.projected, position)
            && range.start < position
            && position < range.end
        {
            boundary = boundary.min(range.start);
        }
        boundary
    }
}

#[cfg(test)]
#[path = "../tests/inline_scan.rs"]
mod tests;
