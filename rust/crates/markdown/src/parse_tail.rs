use super::*;
use std::iter::Peekable;

type Checkpoint = (usize, usize, render::Renderer, Vec<Container>);
type Leaf = (usize, usize, render::Renderer, usize, bool);
type Candidate = (Range<usize>, Range<usize>, render::Renderer, Range<usize>);

struct Tail<'a> {
    source: &'a str,
    events: &'a [(Event<'static>, Range<usize>)],
    base: usize,
    prefix: usize,
    history: usize,
    out: render::Renderer,
    stack: Vec<Container>,
    candidate: Option<Checkpoint>,
    leaf: Option<Leaf>,
    caches: Vec<Candidate>,
    synthetic_leaf: bool,
    overridden: bool,
    overrides: Peekable<std::vec::IntoIter<(usize, Vec<Event<'static>>)>>,
}

impl Tail<'_> {
    fn start(
        &mut self,
        markdown: &mut Markdown,
        index: usize,
        tag: &Tag<'_>,
        range: &Range<usize>,
    ) -> bool {
        if is_leaf(tag) {
            let closed =
                stream_code::closed(self.source, self.events.get(index..).unwrap_or_default());
            self.leaf = Some((
                range.start,
                self.history + self.out.lines.len(),
                self.out.checkpoint(),
                index,
                closed && self.source.as_bytes().get(range.end.saturating_sub(1)) == Some(&b'\n'),
            ));
            if !closed
                && markdown.start_fence(
                    self.source,
                    (self.base, self.prefix),
                    self.events.get(index..).unwrap_or_default(),
                    &mut self.out,
                    &self.stack,
                )
            {
                return true;
            }
            if let Some((_, events)) = self.overrides.next_if(|(begin, _)| *begin == index) {
                for event in events {
                    self.out.event_in(&event, Some(&markdown.references));
                }
                self.overridden = true;
            }
        }
        self.stack.push(container(self.source, range.start, tag));
        false
    }

    fn close(&mut self, index: usize, end: &TagEnd, range: &Range<usize>, next_block: bool) {
        let closing = if self
            .stack
            .last()
            .is_some_and(|container| container.end == *end)
        {
            self.stack.pop()
        } else {
            None
        };
        let leaf_end = is_leaf_end(*end);
        if leaf_end {
            if self.overridden {
                self.out.flush();
            }
            self.overridden = false;
        }
        let closed = self.leaf.as_ref().is_some_and(|leaf| leaf.4);
        if leaf_end && let Some((start, first, state, begin, _)) = self.leaf.take() {
            self.caches.push((
                start..range.end,
                first..self.history + self.out.lines.len(),
                state,
                begin..index + 1,
            ));
        }
        if self.stack.is_empty() {
            self.out.lines.push(Line::default());
        }
        if inline_end(*end) {
            return;
        }
        let sibling = *end == TagEnd::Item
            && self.events.get(index + 1).is_some_and(|(event, next)| {
                matches!(event, Event::Start(Tag::Item))
                    && context::item_marker_complete(self.source, next.start)
            });
        if sibling
            && let Some(item) = closing
            && let Some(list) = self.stack.last_mut()
        {
            list.opening = item.opening;
            list.opening.truncate(list.opening.len().saturating_sub(2));
            list.opening.push('\n');
        }
        let sealed = sibling
            || closed
            || sealed(self.source, range.end, *end, next_block)
            || (*end == TagEnd::HtmlBlock && context::html_closed(self.source, range, &self.stack));
        if range.end >= self.prefix
            && (leaf_end || sibling || (self.stack.is_empty() && next_block))
            && sealed
        {
            self.candidate = Some((
                self.base + range.end - self.prefix,
                self.history + self.out.lines.len(),
                self.out.checkpoint(),
                self.stack.clone(),
            ));
        }
    }

    fn rule(&mut self, event: &Event<'_>, range: &Range<usize>) {
        // Standalone block events have no closing tag. A completed rule
        // line is just as final as a closed paragraph or heading.
        if matches!(event, Event::Rule) {
            let end = if self.source.as_bytes().get(range.end.saturating_sub(1)) == Some(&b'\n') {
                Some(range.end)
            } else {
                (self.source.as_bytes().get(range.end) == Some(&b'\n')).then_some(range.end + 1)
            };
            #[cfg(test)]
            let end = end.filter(|_| !SKIP_RULE_CHECKPOINT.get());
            if let Some(end) = end.filter(|end| *end >= self.prefix) {
                self.candidate = Some((
                    self.base + end - self.prefix,
                    self.history + self.out.lines.len(),
                    self.out.checkpoint(),
                    self.stack.clone(),
                ));
            }
        }
    }

    fn push(
        &mut self,
        markdown: &mut Markdown,
        index: usize,
        event: &Event<'_>,
        range: &Range<usize>,
        next_block: bool,
    ) -> bool {
        if matches!(event, Event::End(TagEnd::Item))
            && range.end <= self.prefix
            && self
                .stack
                .last()
                .is_none_or(|container| container.end != TagEnd::Item)
        {
            return false;
        }
        let html_end = matches!(event, Event::End(TagEnd::HtmlBlock))
            && self
                .stack
                .last()
                .is_some_and(|container| container.end == TagEnd::HtmlBlock);
        if html_end {
            self.synthetic_leaf = false;
        } else if context_event(event, range, self.prefix, &mut self.synthetic_leaf) {
            return false;
        }
        if let Event::Start(tag) = event
            && self.start(markdown, index, tag, range)
        {
            return true;
        }
        if !self.overridden {
            self.out.event_in(event, Some(&markdown.references));
        }
        if matches!(event, Event::Html(_))
            && range.end > self.prefix
            && self.source.as_bytes().get(range.end.saturating_sub(1)) == Some(&b'\n')
            && self
                .stack
                .last()
                .is_some_and(|container| container.end == TagEnd::HtmlBlock)
        {
            self.candidate = Some((
                self.base + range.end - self.prefix,
                self.history + self.out.lines.len(),
                self.out.checkpoint(),
                self.stack.clone(),
            ));
        }
        if let Event::End(end) = event {
            self.close(index, end, range, next_block);
        } else if self.stack.is_empty() {
            self.out.flush();
            self.out.lines.push(Line::default());
        }
        self.rule(event, range);
        false
    }
}

impl Markdown {
    pub(super) fn parse_tail(&mut self) -> bool {
        let base = self.offset;
        let prefix = context_prefix(&self.context);
        let source = format!("{prefix}{}", self.source.get(base..).unwrap_or_default());
        let (events, calls, overrides) = self.parse_frontier(&source);
        self.seal_leading_definitions(base, &source, &events);
        let following = self.following_blocks(&source, &events, (base, prefix.len()));
        let mut tail = Tail {
            source: &source,
            events: &events,
            base,
            prefix: prefix.len(),
            history: self.lines.len(),
            out: self.state.checkpoint(),
            stack: self.context.clone(),
            candidate: None,
            leaf: None,
            caches: Vec::new(),
            synthetic_leaf: false,
            overridden: false,
            overrides: overrides.into_iter().peekable(),
        };
        for (index, (event, range)) in events.iter().enumerate() {
            if tail.push(
                self,
                index,
                event,
                range,
                following.get(index).copied().unwrap_or_default(),
            ) {
                self.cache_frontier(&source, &events, &calls, tail.caches, (base, prefix.len()));
                return self.advance_fence();
            }
        }
        tail.out.flush();
        self.lines.extend(tail.out.lines);
        self.finish_tail(tail.candidate);
        self.cache_frontier(&source, &events, &calls, tail.caches, (base, prefix.len()));
        self.finish_definitions(&source, &events, (base, prefix.len()));
        self.rule = events.last().and_then(|(event, range)| {
            if !matches!(event, Event::Rule) || !self.context.is_empty() || source.ends_with('\n') {
                return None;
            }
            source
                .get(range.clone())?
                .bytes()
                .find(|byte| matches!(byte, b'*' | b'-' | b'_'))
        });
        self.begin_inline(&source, &events, &calls, base);
        self.begin_table(&source, &events, (base, prefix.len()));
        false
    }
}
