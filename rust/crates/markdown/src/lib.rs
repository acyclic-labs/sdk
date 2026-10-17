//! Streaming Markdown as styled, unwrapped lines. Feed one instance per text
//! stream; keep viewport layout and task/query ownership in the consuming client.

mod code;
mod context;
mod delimiters;
mod events;
mod inline;
mod reference_render;
mod references;
mod render;
mod stream_code;
mod stream_table;
mod style;
mod table;
mod visibility;
#[cfg(test)]
mod work;

use context::{container, context_event, context_prefix};
use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag, TagEnd};
use references::{Key, References, Template, normalize};
use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};
pub use style::{Color, Line, Span, Style};

/// Disposable incremental presentation of one append-only Markdown stream.
/// Completed leaves and fence lines remain behind a parser checkpoint, including
/// leaves inside containers. Reference changes parse only dependent blocks.
#[derive(Default)]
pub struct Markdown {
    source: String,
    offset: usize,
    stable_lines: usize,
    visibility: visibility::Visibility,
    visible_lines: usize,
    lines: Vec<Line>,
    state: render::Renderer,
    context: Vec<Container>,
    references: References,
    dependencies: HashMap<Key, Vec<usize>>,
    cached: Vec<Cached>,
    fence: Option<Fence>,
    inline: Option<inline::Inline>,
    table: Option<stream_table::Stream>,
    reference_cost: usize,
    reference_limit: usize,
    plain: bool,
    plain_links: bool,
    plain_entity: bool,
    plain_heading: bool,
    plain_spaces: usize,
    cr: bool,
    #[cfg(test)]
    parsed_bytes: usize,
    #[cfg(test)]
    reference_cache_visits: usize,
    #[cfg(test)]
    reference_moved_lines: usize,
    #[cfg(test)]
    boundary_work: usize,
    #[cfg(test)]
    visible_work: std::cell::Cell<usize>,
}

#[derive(Clone)]
struct Container {
    end: TagEnd,
    opening: String,
    continuation: String,
}

struct Cached {
    template: Template,
    state: render::Renderer,
    lines: Range<usize>,
    cost: usize,
    cumulative: usize,
    prefix: usize,
    budget: Range<usize>,
}

impl Cached {
    fn new(
        source: &str,
        mut range: Range<usize>,
        lines: Range<usize>,
        state: render::Renderer,
        events: &[(Event<'_>, Range<usize>)],
    ) -> Option<Self> {
        if matches!(
            events.first(),
            Some((Event::Start(Tag::CodeBlock(_) | Tag::HtmlBlock), _))
        ) {
            return None;
        }
        // Loose-list paragraph ranges can begin after the task marker even
        // though its event belongs to the paragraph. Keep it in the template.
        for (event, marker) in events {
            if matches!(event, Event::TaskListMarker(_)) {
                range.start = range.start.min(marker.start);
            }
        }
        let quotes = source
            .get(line_start(source, range.start)..range.start)
            .unwrap_or_default()
            .bytes()
            .filter(|byte| *byte == b'>')
            .count();
        let raw = source.get(range).unwrap_or_default();
        if !raw.contains('[') {
            return None;
        }
        Some(Self {
            template: Template::new(raw, quotes, events),
            state,
            lines,
            cost: 0,
            cumulative: 0,
            prefix: 0,
            budget: 0..usize::MAX,
        })
    }
}

struct Fence {
    indented: bool,
    pending: String,
    marker: u8,
    count: usize,
    indent: usize,
    opening: String,
    parser_opening: String,
    code: code::Code,
}

impl Markdown {
    /// Create a stream with tables bounded to this many display columns.
    /// Zero selects one column; the default presentation uses 80 columns.
    /// Recreate the presentation from its source when the viewport width changes.
    pub fn with_width(width: u16) -> Self {
        Self {
            state: render::Renderer::with_width(width),
            ..Self::default()
        }
    }

    /// Append a UTF-8 text delta and return the first changed display line.
    /// A consumer can truncate its presentation there and replace the suffix
    /// from [`Self::lines`]. Empty deltas leave the presentation unchanged.
    pub fn push(&mut self, delta: &str) -> usize {
        let old_len = self.visible_lines;
        if delta.is_empty() {
            return old_len;
        }
        let previous = self.source.len();
        self.append(delta);
        if self.source.len() == previous {
            return old_len;
        }
        let budget_line = self.advance_budget();
        let appended = self.source.get(previous..).unwrap_or_default();
        if self.plain
            && plain_text(appended)
            && !(self.plain_links && appended.contains(['(', ')']))
            && !(self.plain_entity && appended.contains(';'))
            && !(self.plain_heading && appended.contains('#'))
        {
            self.plain_entity &= appended
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '#');
            if let Some(line) = self.lines.get_mut(self.visible_lines.saturating_sub(1)) {
                let visible = appended.trim_end_matches(' ');
                if visible.is_empty() {
                    self.plain_spaces += appended.len();
                } else {
                    if let Some(span) = line.spans.last_mut() {
                        #[cfg(test)]
                        {
                            work::copy(self.plain_spaces + visible.len());
                            if self.plain_spaces > span.text.capacity() - span.text.len() {
                                work::copy(span.text.len());
                            }
                        }
                        span.text
                            .extend(std::iter::repeat_n(' ', self.plain_spaces));
                        #[cfg(test)]
                        if visible.len() > span.text.capacity() - span.text.len() {
                            work::copy(span.text.len());
                        }
                        span.text.push_str(visible);
                    }
                    self.plain_spaces = appended.len() - visible.len();
                }
            }
            return self.visible_lines.saturating_sub(1).min(budget_line);
        }
        // Trailing horizontal whitespace has no display effect in paragraphs or
        // tables. Keep it in source until a non-whitespace byte settles the row.
        if (self.inline.is_some() || self.table.is_some())
            && appended.bytes().all(|byte| matches!(byte, b' ' | b'\t'))
        {
            self.plain &= !appended.contains('\t');
            return old_len.min(self.visible_lines).min(budget_line);
        }
        if let Some(changed) = self.advance_inline(previous) {
            return changed
                .min(old_len)
                .min(self.visible_lines)
                .min(budget_line);
        }
        if let Some(changed) = self.advance_table() {
            return changed
                .min(old_len)
                .min(self.visible_lines)
                .min(budget_line);
        }
        self.table = None;
        self.inline = None;
        self.plain = false;
        let mut changed = self.stable_lines.min(old_len).min(budget_line);
        self.lines.truncate(self.stable_lines);
        if self.fence.is_some() && !self.advance_fence() {
            self.finish_visible();
            return changed.min(self.visible_lines);
        }
        loop {
            let tail = self.source.get(self.offset..).unwrap_or_default();
            let prefix = context_prefix(&self.context);
            let source = format!("{prefix}{tail}");
            #[cfg(test)]
            work::copy(source.len());
            let keys = self.references.update(&source, self.offset, prefix.len());
            changed = changed.min(self.resolve(&keys));
            if !self.parse_tail() {
                break;
            }
        }
        self.finish_visible();
        changed.min(self.visible_lines)
    }

    /// Current unwrapped lines. A partial delimiter remains visible until it
    /// forms valid Markdown; no input is hidden waiting for a future delta.
    pub fn lines(&self) -> &[Line] {
        self.lines.get(..self.visible_lines).unwrap_or_default()
    }

    fn visible_end(&self, start: usize, end: usize, fallback: usize) -> usize {
        self.lines
            .get(start..end)
            .unwrap_or_default()
            .iter()
            .rposition(|line| {
                #[cfg(test)]
                self.visible_work.set(self.visible_work.get() + 1);
                !line.spans.is_empty()
            })
            .map_or(fallback, |index| start + index + 1)
    }

    fn finish_visible(&mut self) {
        let (start, fallback) = self
            .inline
            .as_ref()
            .map_or((self.stable_lines, self.visibility.end), |inline| {
                inline.visible_frontier(self.stable_lines, self.visibility.end)
            });
        self.visible_lines = self.visible_end(start, self.lines.len(), fallback);
    }

    fn index_visible(&mut self, index: usize) {
        #[cfg(test)]
        self.visible_work.set(self.visible_work.get() + 1);
        self.visibility.set(
            index,
            self.lines
                .get(index)
                .is_some_and(|line| !line.spans.is_empty()),
        );
    }

    fn set_stable_lines(&mut self, length: usize) {
        for index in self.stable_lines..length {
            self.index_visible(index);
        }
        self.stable_lines = length;
    }

    /// Recover the Markdown source with CRLF and CR normalized to LF.
    pub fn into_source(self) -> String {
        self.source
    }

    /// Discard this presentation when a query resets or selects another turn.
    pub fn clear(&mut self) {
        *self = Self::with_width(self.state.render_width());
    }

    fn append(&mut self, delta: &str) {
        #[cfg(test)]
        work::copy(delta.len());
        if !self.cr && !delta.contains('\r') {
            #[cfg(test)]
            if delta.len() > self.source.capacity() - self.source.len() {
                work::grow_source(self.source.len());
            }
            self.source.push_str(delta);
            return;
        }
        for character in delta.chars() {
            if character == '\n' && self.cr {
                self.cr = false;
                continue;
            }
            self.cr = character == '\r';
            self.source.push(if self.cr { '\n' } else { character });
        }
    }

    fn following_blocks(
        &mut self,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
    ) -> Vec<bool> {
        #[cfg(test)]
        {
            // Charge the full newline-search input and each reverse-sweep event.
            self.boundary_work += source.len() + events.len();
        }
        let newline = source.rfind('\n');
        let mut next = false;
        let mut following = vec![false; events.len()];
        for ((event, range), following) in events.iter().rev().zip(following.iter_mut().rev()) {
            *following = next;
            if matches!(event, Event::Start(_)) {
                next = newline.is_some_and(|newline| range.start <= newline);
            }
        }
        following
    }

    fn parse_tail(&mut self) -> bool {
        let base = self.offset;
        let prefix = context_prefix(&self.context);
        let source = format!("{prefix}{}", self.source.get(base..).unwrap_or_default());
        let events = self.parse_frontier(&source);
        self.seal_leading_definitions(base, &source, &events);
        let following_blocks = self.following_blocks(&source, &events);
        let mut out = self.state.checkpoint();
        let mut stack = self.context.clone();
        let mut candidate = None;
        let mut leaf = None;
        let mut caches: Vec<(usize, Cached)> = Vec::new();
        let mut cost = self.reference_cost;
        let mut synthetic_leaf = false;
        for (index, (event, range)) in events.iter().enumerate() {
            let html_end = matches!(event, Event::End(TagEnd::HtmlBlock))
                && stack
                    .last()
                    .is_some_and(|container| container.end == TagEnd::HtmlBlock);
            if html_end {
                synthetic_leaf = false;
            } else if context_event(event, range, prefix.len(), &mut synthetic_leaf) {
                continue;
            }
            if let Event::Start(tag) = event {
                if is_leaf(tag) {
                    let closed =
                        stream_code::closed(&source, events.get(index..).unwrap_or_default());
                    leaf = Some((
                        range.start,
                        self.lines.len() + out.lines.len(),
                        out.checkpoint(),
                        index,
                        closed
                            && source.as_bytes().get(range.end.saturating_sub(1)) == Some(&b'\n'),
                    ));
                    if !closed
                        && self.start_fence(
                            &source,
                            (base, prefix.len()),
                            events.get(index..).unwrap_or_default(),
                            &mut out,
                            &stack,
                        )
                    {
                        for (end, cache) in caches {
                            if base + end.saturating_sub(prefix.len()) <= self.offset {
                                self.cache(cache);
                            }
                        }
                        return self.advance_fence();
                    }
                }
                stack.push(container(&source, range.start, tag));
            }
            out.event_in(event, Some(&self.references));
            if matches!(event, Event::Html(_))
                && range.end > prefix.len()
                && source.as_bytes().get(range.end.saturating_sub(1)) == Some(&b'\n')
                && stack
                    .last()
                    .is_some_and(|container| container.end == TagEnd::HtmlBlock)
            {
                candidate = Some((
                    base + range.end - prefix.len(),
                    self.lines.len() + out.lines.len(),
                    out.checkpoint(),
                    stack.clone(),
                ));
            }
            if let Event::End(end) = event {
                if stack.last().is_some_and(|container| container.end == *end) {
                    stack.pop();
                }
                let leaf_end = is_leaf_end(*end);
                let closed = leaf.as_ref().is_some_and(|leaf| leaf.4);
                if leaf_end
                    && let Some((start, first, state, begin, _)) = leaf.take()
                    && let Some(mut cache) = Cached::new(
                        &source,
                        start..range.end,
                        first..self.lines.len() + out.lines.len(),
                        state,
                        events.get(begin..=index).unwrap_or_default(),
                    )
                {
                    self.render_cache(&mut cache, &mut out, &mut cost);
                    caches.push((range.end, cache));
                }
                if stack.is_empty() {
                    out.lines.push(Line::default());
                }
                if inline_end(*end) {
                    continue;
                }
                let next_block = following_blocks.get(index).copied().unwrap_or_default();
                let sealed = closed
                    || sealed(&source, range.end, *end, next_block)
                    || (*end == TagEnd::HtmlBlock && context::html_closed(&source, range, &stack));
                if range.end >= prefix.len()
                    && (leaf_end || (stack.is_empty() && next_block))
                    && sealed
                {
                    candidate = Some((
                        base + range.end - prefix.len(),
                        self.lines.len() + out.lines.len(),
                        out.checkpoint(),
                        stack.clone(),
                    ));
                }
            } else if stack.is_empty() {
                out.flush();
                out.lines.push(Line::default());
            }
        }
        out.flush();
        self.lines.extend(out.lines);
        self.finish_tail(candidate, caches, (base, prefix.len()));
        self.finish_definitions(&source, &events, (base, prefix.len()));
        self.begin_inline(&source, &events, base);
        self.begin_table(&source, &events, base);
        false
    }

    fn seal_leading_definitions(
        &mut self,
        base: usize,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
    ) {
        if self.context.is_empty()
            && let Some((_, first)) = events.first()
            && source
                .get(..first.start)
                .is_some_and(|prefix| prefix.ends_with("\n\n"))
        {
            self.offset = base + first.start;
            self.references.commit(self.offset);
        }
    }

    fn finish_tail(
        &mut self,
        candidate: Option<(usize, usize, render::Renderer, Vec<Container>)>,
        caches: Vec<(usize, Cached)>,
        (base, prefix): (usize, usize),
    ) {
        if let Some((offset, lines, state, context)) = candidate {
            self.offset = offset;
            self.set_stable_lines(lines);
            self.state = state;
            self.context = context;
            for (end, cache) in caches {
                if base + end.saturating_sub(prefix) <= offset && !cache.template.keys.is_empty() {
                    self.cache(cache);
                }
            }
        }
    }

    fn finish_definitions(
        &mut self,
        source: &str,
        events: &[(Event<'_>, Range<usize>)],
        (base, prefix): (usize, usize),
    ) {
        // Definition-only prefixes have no display events. Seal definitions
        // terminated by a blank line without retaining an ever-growing suffix.
        let tail = self.source.get(self.offset..).unwrap_or_default();
        if events.is_empty() && tail.ends_with("\n\n") {
            self.offset = self.source.len();
        } else if events.is_empty() {
            self.offset = self.references.frontier(self.offset);
        }
        if self.context.is_empty() {
            self.offset += self
                .source
                .get(self.offset..)
                .unwrap_or_default()
                .split_inclusive('\n')
                .take_while(|row| {
                    #[cfg(test)]
                    work::scan(row.len() * 2);
                    row.ends_with('\n')
                        && row.bytes().all(|byte| matches!(byte, b' ' | b'\t' | b'\n'))
                })
                .map(str::len)
                .sum::<usize>();
        }
        self.references.commit(self.offset);
        let tail = self.source.get(self.offset..).unwrap_or_default();
        let frontier = self.offset.saturating_sub(base) + prefix;
        let mut leaves = events.iter().filter_map(|(event, range)| match event {
            Event::Start(tag) if is_leaf(tag) && range.start >= frontier => Some((tag, range)),
            _ => None,
        });
        let last_leaf = leaves.next();
        self.plain_heading = matches!(last_leaf, Some((Tag::Heading { .. }, _)));
        let link_end = events
            .iter()
            .filter_map(|(event, range)| {
                matches!(event, Event::End(TagEnd::Link | TagEnd::Image)).then_some(range.end)
            })
            .max()
            .unwrap_or(frontier);
        let inline_tail = source.get(link_end..).unwrap_or_default();
        self.plain_links = inline_tail.contains('[')
            && inline_tail
                .rsplit_once(']')
                .is_none_or(|(_, rest)| rest.starts_with('('));
        self.plain_entity = tail.rsplit_once('&').is_some_and(|(_, suffix)| {
            suffix
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '#')
        });
        let only_inline_leaf = matches!(last_leaf, Some((Tag::Paragraph | Tag::Heading { .. }, range))
            if !definition_candidate(source.get(range.start..).unwrap_or_default()))
            && leaves.next().is_none();
        self.plain = tail.len() >= 8
            // An unfinished block marker (for example `1.` on a new line)
            // can still become a list when a space arrives.
            && tail.rsplit('\n').next().is_some_and(|row| !row.trim_start_matches([' ', '\t']).starts_with('<') && row.chars().any(char::is_alphabetic))
            && tail.chars().next_back().is_some_and(plain_character)
            && only_inline_leaf
            && events
                .iter()
                .rev()
                .find(|(event, _)| !matches!(event, Event::End(_)))
                .is_some_and(|(event, range)| {
                    matches!(event, Event::Text(text) if text.chars().next_back().is_some_and(plain_character))
                        && range.end == source.trim_end_matches(' ').len()
                });
        self.plain_spaces = tail.bytes().rev().take_while(|byte| *byte == b' ').count();
    }

    fn cache(&mut self, mut cache: Cached) {
        if cache.template.keys.is_empty() {
            return;
        }
        cache.prefix = self.reference_cost;
        self.reference_cost = self.reference_cost.saturating_add(cache.cost);
        cache.cumulative = self.reference_cost;
        let index = self.cached.len();
        for key in &cache.template.keys {
            self.dependencies
                .entry(key.clone())
                .or_default()
                .push(index);
        }
        self.cached.push(cache);
    }
}

fn plain_text(text: &str) -> bool {
    text.chars().all(plain_character)
}

fn definition_candidate(source: &str) -> bool {
    source.starts_with('[')
        && source
            .rsplit_once(']')
            .is_some_and(|(_, rest)| rest.starts_with(':'))
}

fn plain_character(character: char) -> bool {
    // These characters can complete or reopen inline syntax. Other printable
    // punctuation is literal inside an established paragraph or heading.
    !character.is_ascii()
        || (character.is_ascii_graphic()
            && !matches!(
                character,
                '*' | '_' | '~' | '`' | '[' | ']' | '<' | '>' | '&' | '\\'
            ))
        || character == ' '
}

fn is_leaf(tag: &Tag<'_>) -> bool {
    matches!(
        tag,
        Tag::Paragraph | Tag::Heading { .. } | Tag::CodeBlock(_) | Tag::Table(_) | Tag::HtmlBlock
    )
}

fn inline_end(tag: TagEnd) -> bool {
    matches!(
        tag,
        TagEnd::Emphasis | TagEnd::Strong | TagEnd::Strikethrough | TagEnd::Link | TagEnd::Image
    )
}

fn is_leaf_end(tag: TagEnd) -> bool {
    matches!(
        tag,
        TagEnd::Paragraph
            | TagEnd::Heading(_)
            | TagEnd::CodeBlock
            | TagEnd::Table
            | TagEnd::HtmlBlock
    )
}

fn line_start(source: &str, position: usize) -> usize {
    source
        .get(..position)
        .and_then(|text| text.rfind('\n'))
        .map_or(0, |position| position + 1)
}

fn sealed(source: &str, end: usize, tag: TagEnd, next_complete: bool) -> bool {
    let rest = source.get(end..).unwrap_or_default();
    if tag == TagEnd::Table {
        let rest = if source.as_bytes().get(end.saturating_sub(1)) == Some(&b'\n') {
            rest
        } else {
            rest.strip_prefix('\n').unwrap_or(rest)
        };
        return blank_line(rest) || next_complete;
    }
    if blank_line(rest) {
        return true;
    }
    next_complete
}

fn blank_line(source: &str) -> bool {
    source.split_once('\n').is_some_and(|(line, _)| {
        line.chars()
            .all(|character| character == '>' || character.is_ascii_whitespace())
    })
}

fn options() -> Options {
    Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS
}

#[cfg(test)]
fn blocks<'a>(
    events: impl Iterator<Item = (Event<'a>, Range<usize>)>,
) -> Vec<(Range<usize>, Vec<Event<'a>>)> {
    let mut blocks = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    let mut block = Vec::new();
    for (event, range) in events {
        if depth == 0 {
            start = range.start;
        }
        match &event {
            Event::Start(_) => depth += 1,
            Event::End(_) => depth = depth.saturating_sub(1),
            _ => {}
        }
        block.push(event);
        if depth == 0 {
            blocks.push((start..range.end, std::mem::take(&mut block)));
        }
    }
    blocks
}

#[cfg(test)]
#[path = "../tests/full_parse.rs"]
mod tests;
