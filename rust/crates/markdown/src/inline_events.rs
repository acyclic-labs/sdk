use super::Input;
#[cfg(test)]
use super::REPLAY;
use crate::{Event, Range, References, Span, Spans, Style, Tag, TagEnd};
#[cfg(test)]
use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};

#[path = "inline_labels.rs"]
mod labels;
#[path = "inline_rows.rs"]
mod rows;
use rows::Output;
#[path = "inline_projection.rs"]
pub(in crate::inline_scan) mod projection;
#[cfg(test)]
pub(super) use rows::RESCAN_VISIBLE;

#[derive(Hash, PartialEq, Eq)]
enum Key {
    Leaf(u8, usize, usize),
    #[cfg(test)]
    Join(usize, usize),
    Mask(usize, u64, usize),
    Suffix(usize, usize),
}

pub(in crate::inline_scan) struct Cache {
    input: Input,
    destinations: labels::destinations::Index,
    keys: HashMap<Key, (usize, usize)>,
    first_key_end: Option<usize>,
    nodes: Vec<Vec<Output>>,
    page_size: usize,
    length: usize,
    targets: HashMap<(usize, usize), Arc<str>>,
    atoms: HashMap<(usize, usize), (usize, bool)>,
    last: usize,
    last_join: Option<(usize, usize, usize)>,
    alternate: Option<(usize, usize)>,
    rendered: bool,
    #[cfg(test)]
    committed: usize,
    epoch: usize,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            input: Input::default(),
            destinations: labels::destinations::Index::default(),
            keys: HashMap::new(),
            first_key_end: None,
            nodes: Vec::new(),
            page_size: {
                #[cfg(test)]
                if crate::work::EAGER_RENDER_PAGE.get() {
                    super::super::PAGE
                } else {
                    2
                }
                #[cfg(not(test))]
                {
                    2
                }
            },
            length: 0,
            targets: HashMap::new(),
            atoms: HashMap::new(),
            last: 0,
            last_join: None,
            alternate: None,
            rendered: false,
            #[cfg(test)]
            committed: 0,
            epoch: 1,
        }
    }
}

struct Frame {
    body: usize,
    style: Style,
    link: Option<Arc<str>>,
}

fn style_key(style: Style) -> u64 {
    let flags = u64::from(style.bold)
        | u64::from(style.italic) << 1
        | u64::from(style.dim) << 2
        | u64::from(style.underline) << 3
        | u64::from(style.strikethrough) << 4;
    style.foreground.map_or(flags, |crate::Color(r, g, b)| {
        flags | 1 << 5 | u64::from(r) << 8 | u64::from(g) << 16 | u64::from(b) << 24
    })
}

fn text_output(text: crate::Text, style: Style) -> Output {
    let mut spans = Spans::default();
    if !text.is_empty() {
        spans.push_content(
            Span {
                text,
                style,
                link: None,
            },
            false,
        );
        spans.freeze();
    }
    Output::text(spans)
}

impl Cache {
    // Geometrically sized pages never move stored outputs. The directory has
    // logarithmically many entries, so growing it cannot copy a linear-sized
    // history of page handles at a single push boundary.
    fn slot(&self, id: usize) -> (usize, usize) {
        let page = (id / self.page_size + 1).ilog2() as usize;
        let start = self.page_size * ((1usize << page) - 1);
        (page, id - start)
    }

    fn output(&self, id: usize) -> Option<&Output> {
        let (page, offset) = self.slot(id);
        self.nodes.get(page)?.get(offset)
    }

    fn store(&mut self, output: Output) -> usize {
        #[cfg(test)]
        crate::work::scan(1);
        let id = self.length;
        let (page, offset) = self.slot(id);
        if page == self.nodes.len() {
            #[cfg(test)]
            if self.nodes.len() == self.nodes.capacity() {
                crate::work::copy(self.nodes.len() * std::mem::size_of::<Vec<Output>>());
            }
            self.nodes.push(Vec::with_capacity(self.page_size << page));
        }
        if let Some(page) = self.nodes.get_mut(page) {
            debug_assert_eq!(offset, page.len());
            page.push(output);
        }
        self.length += 1;
        id
    }

    fn insert(&mut self, key: Key, output: Output) -> usize {
        let id = self.store(output);
        let end = self.input.origin.unwrap_or(0) + self.input.raw.len();
        self.first_key_end = Some(self.first_key_end.map_or(end, |old| old.min(end)));
        self.keys.insert(key, (id, end));
        id
    }

    fn join(&mut self, left: usize, right: usize) -> Option<usize> {
        if left == 0 {
            return Some(right);
        }
        if right == 0 {
            return Some(left);
        }
        #[cfg(test)]
        if crate::work::CACHE_JOINS.get()
            && let Some(&(id, _)) = self.keys.get(&Key::Join(left, right))
        {
            return Some(id);
        }
        if let Some((old_left, old_right, id)) = self.last_join
            && (left, right) == (old_left, old_right)
        {
            return Some(id);
        }
        let mut output = self.output(left)?.clone();
        output.append(self.output(right)?.clone());
        // The forest already owns every compiled join. Keep only the immediate
        // full/trimmed repeat, rather than a second index over its whole history.
        #[cfg(test)]
        let id = if crate::work::CACHE_JOINS.get() {
            self.insert(Key::Join(left, right), output)
        } else {
            self.store(output)
        };
        #[cfg(not(test))]
        let id = self.store(output);
        self.last_join = Some((left, right, id));
        Some(id)
    }

    fn mask(&mut self, body: usize, style: Style, link: Option<Arc<str>>) -> Option<usize> {
        if style == Style::default() && link.is_none() {
            return Some(body);
        }
        let key = Key::Mask(
            body,
            style_key(style),
            link.as_ref()
                .map_or(0, |link| Arc::as_ptr(link).cast::<u8>() as usize),
        );
        if let Some(&(id, _)) = self.keys.get(&key) {
            return Some(id);
        }
        let output = self.output(body)?.clone().masked(style, link);
        Some(self.insert(key, output))
    }

    pub(in crate::inline_scan) fn freeze(&mut self, position: usize) {
        #[cfg(test)]
        if KEEP_KEYS.get() {
            return;
        }
        let eligible = self.first_key_end.is_some_and(|end| end <= position);
        if eligible && !self.keys.is_empty() {
            #[cfg(test)]
            crate::work::scan(self.keys.capacity());
            // Forest nodes own compiled output handles. Keep operation keys
            // created after this boundary, without moving the surviving tail.
            self.first_key_end = None;
            self.keys.retain(|_, (_, end)| {
                if *end <= position {
                    return false;
                }
                self.first_key_end = Some(self.first_key_end.map_or(*end, |old| old.min(*end)));
                true
            });
            // Reuse the open block's high-water allocation. Compaction would
            // re-copy live keys at each checkpoint; the block owns this map
            // and releases it when it closes.
            if self.keys.is_empty() {
                self.keys = HashMap::new();
            }
        }
    }

    pub(in crate::inline_scan) fn seed_input(&mut self, source: &str, start: usize) {
        self.input.feed(source, start);
    }

    pub(in crate::inline_scan) fn completed_destinations(
        &self,
    ) -> impl Iterator<Item = usize> + '_ {
        self.destinations.completions()
    }

    pub(in crate::inline_scan) fn track_destinations(
        &mut self,
        source: &str,
        origin: usize,
    ) -> bool {
        let previous = self.destinations.completed();
        self.destinations.push(source, origin);
        self.destinations.completed() != previous
    }

    fn target(
        &mut self,
        range: &Range<usize>,
        url: &str,
        id: &str,
        references: &References,
    ) -> Arc<str> {
        if !id.is_empty()
            && let Some((target, _, _)) = references.get(&crate::normalize(id))
        {
            return target.clone();
        }
        self.targets
            .entry((range.start, range.end))
            .or_insert_with(|| {
                #[cfg(test)]
                crate::work::copy(url.len());
                Arc::from(url)
            })
            .clone()
    }

    fn raw(&self, source: &str, range: Range<usize>, style: Style) -> Option<Output> {
        let mut result = Output::default();
        let mut start = range.start;
        for end in self.input.newlines(range.clone())? {
            result.append(text_output(self.input.text(source, start..end)?, style));
            result.append(Output::newline(true));
            start = end + 1;
        }
        result.append(text_output(
            self.input.text(source, start..range.end)?,
            style,
        ));
        Some(result)
    }

    fn value(
        &mut self,
        kind: u8,
        range: Range<usize>,
        value: &str,
        source: &crate::inline_scan::Slice<'_>,
        style: Style,
    ) -> Option<usize> {
        let key = Key::Leaf(kind, range.start, range.end);
        if let Some(&(id, _)) = self.keys.get(&key) {
            return Some(id);
        }
        let begin = source.parsed.as_ptr() as usize;
        let value_begin = value.as_ptr() as usize;
        let raw_offset = value_begin.checked_sub(begin);
        let output = if let Some(offset) = raw_offset.filter(|&offset| {
            offset <= source.parsed.len() && value.len() <= source.parsed.len() - offset
        }) {
            let offset = source.base + offset.checked_sub(source.sentinel)?;
            self.raw(source.original, offset..offset + value.len(), style)?
        } else if kind == 1 {
            let raw = source.original.get(range.clone())?;
            let width = raw.bytes().take_while(|&byte| byte == b'`').count();
            if width == 0 || width * 2 > range.len() {
                return None;
            }
            #[cfg(test)]
            crate::work::scan(width);
            text_output(
                self.input
                    .code(range.start + width..range.end - width, source.original)?,
                style,
            )
        } else {
            let mut output = Output::default();
            #[cfg(test)]
            crate::work::scan(value.len());
            for part in value.split_inclusive(['\r', '\n']) {
                output.append(text_output(
                    part.trim_end_matches(['\r', '\n']).into(),
                    style,
                ));
                if part.ends_with(['\r', '\n']) {
                    output.append(Output::newline(true));
                }
            }
            output
        };
        Some(self.insert(key, output))
    }

    #[cfg(test)]
    pub(in crate::inline_scan) fn render(
        &mut self,
        source: crate::inline_scan::Slice<'_>,
        events: &[(Event<'_>, Range<usize>)],
        references: &References,
        lines: &mut crate::line_buffer::Buffer,
        first: usize,
    ) -> Option<(usize, usize)> {
        #[cfg(test)]
        if REPLAY.get() {
            let epoch = self.epoch.checked_add(1)?;
            *self = Self::default();
            self.epoch = epoch;
        }
        self.input.feed(source.original, source.start);
        if self.nodes.is_empty() {
            self.store(Output::default());
        }
        let (current, next_committed) = self.reduce(&source, events, references, self.committed)?;
        let changed = self
            .output(current)?
            .update(self.output(self.last)?, lines, first);
        let visible = self.output(current)?.visible();
        self.last = current;
        self.committed = next_committed;
        Some((changed, visible))
    }

    fn open_frame(
        &mut self,
        tag: &Tag<'_>,
        range: &Range<usize>,
        references: &References,
    ) -> Option<Frame> {
        let (style, link) = match tag {
            Tag::Paragraph => (Style::default(), None),
            Tag::Emphasis => (
                Style {
                    italic: true,
                    ..Style::default()
                },
                None,
            ),
            Tag::Strong => (
                Style {
                    bold: true,
                    ..Style::default()
                },
                None,
            ),
            Tag::Strikethrough => (
                Style {
                    dim: true,
                    strikethrough: true,
                    ..Style::default()
                },
                None,
            ),
            Tag::Link { dest_url, id, .. } | Tag::Image { dest_url, id, .. } => (
                Style {
                    foreground: Some(crate::style::BLUE),
                    underline: true,
                    ..Style::default()
                },
                Some(self.target(range, dest_url, id, references)),
            ),
            _ => return None,
        };
        Some(Frame {
            body: 0,
            style,
            link,
        })
    }

    fn literal_event(
        &mut self,
        event: &Event<'_>,
        range: Range<usize>,
        source: &crate::inline_scan::Slice<'_>,
    ) -> Option<usize> {
        Some(match event {
            Event::Code(text) => self.value(
                1,
                range,
                text,
                source,
                Style {
                    foreground: Some(crate::style::YELLOW),
                    ..Style::default()
                },
            )?,
            Event::InlineHtml(text)
            | Event::Html(text)
            | Event::InlineMath(text)
            | Event::DisplayMath(text) => self.value(2, range, text, source, Style::default())?,
            Event::SoftBreak => self.value(3, range, " ", source, Style::default())?,
            Event::HardBreak => {
                let key = Key::Leaf(4, range.start, range.end);
                if let Some(&(id, _)) = self.keys.get(&key) {
                    id
                } else {
                    self.insert(key, Output::newline(false))
                }
            }
            Event::FootnoteReference(text) => {
                self.value(5, range, &format!("[{text}]"), source, Style::default())?
            }
            _ => return None,
        })
    }

    fn reduce(
        &mut self,
        source: &crate::inline_scan::Slice<'_>,
        events: &[(Event<'_>, Range<usize>)],
        references: &References,
        initial: usize,
    ) -> Option<(usize, usize)> {
        let mut frames = vec![Frame {
            body: initial,
            style: Style::default(),
            link: None,
        }];
        let mut committed = None;
        for (event, relative) in events {
            #[cfg(test)]
            crate::work::scan(1);
            if matches!(
                event,
                Event::Start(Tag::Paragraph) | Event::End(TagEnd::Paragraph)
            ) || relative.end <= source.sentinel
            {
                continue;
            }
            if frames.len() == 1 && relative.start == source.boundary {
                committed = Some(frames.last()?.body);
            }
            let range = source.base + relative.start.saturating_sub(source.sentinel)
                ..source.base + relative.end.saturating_sub(source.sentinel);
            let leaf = match event {
                Event::Start(tag) => {
                    frames.push(self.open_frame(tag, &range, references)?);
                    continue;
                }
                Event::End(
                    TagEnd::Paragraph
                    | TagEnd::Emphasis
                    | TagEnd::Strong
                    | TagEnd::Strikethrough
                    | TagEnd::Link
                    | TagEnd::Image,
                ) => {
                    if frames.len() <= 1 {
                        return None;
                    }
                    let frame = frames.pop()?;
                    self.mask(frame.body, frame.style, frame.link)?
                }
                Event::Text(text) => {
                    let trim = source.sentinel.saturating_sub(relative.start);
                    let text = text.get(trim..)?;
                    let begin = relative.start.max(source.sentinel);
                    let cut = source.boundary.saturating_sub(begin);
                    if frames.len() == 1
                        && cut > 0
                        && cut < text.len()
                        && source.parsed.get(begin..relative.end).is_some_and(|raw| {
                            raw.as_ptr() == text.as_ptr() && raw.len() == text.len()
                        })
                    {
                        let left = self.value(
                            0,
                            range.start..range.start + cut,
                            text.get(..cut)?,
                            source,
                            Style::default(),
                        )?;
                        let frame = frames.last_mut()?;
                        frame.body = self.join(frame.body, left)?;
                        committed = Some(frame.body);
                        self.value(
                            0,
                            range.start + cut..range.end,
                            text.get(cut..)?,
                            source,
                            Style::default(),
                        )?
                    } else {
                        self.value(0, range, text, source, Style::default())?
                    }
                }
                _ => self.literal_event(event, range, source)?,
            };
            let current = frames.last_mut()?;
            current.body = self.join(current.body, leaf)?;
            if frames.len() == 1 && relative.end == source.boundary {
                committed = Some(frames.last()?.body);
            }
        }
        if frames.len() != 1 {
            return None;
        }
        let current = frames.pop()?.body;
        let next_committed = if source.boundary > source.sentinel {
            committed?
        } else {
            initial
        };
        Some((current, next_committed))
    }

    #[cfg(test)]
    pub(in crate::inline_scan) fn retained(
        &self,
        nodes: &mut HashSet<usize>,
        texts: &mut HashSet<usize>,
        links: &mut HashSet<*const u8>,
    ) -> (usize, usize) {
        let (mut payload, mut metadata) = self.input.retained(texts);
        metadata += self.nodes.capacity() * std::mem::size_of::<Vec<Output>>()
            + self
                .nodes
                .iter()
                .map(|page| page.capacity() * std::mem::size_of::<Output>())
                .sum::<usize>()
            + self.keys.capacity() * std::mem::size_of::<(Key, (usize, usize))>()
            + self.targets.capacity() * std::mem::size_of::<((usize, usize), Arc<str>)>()
            + self.atoms.capacity() * std::mem::size_of::<((usize, usize), (usize, bool))>();
        for target in self.targets.values() {
            if links.insert(Arc::as_ptr(target).cast::<u8>()) {
                payload += target.len();
            }
        }
        for output in self.nodes.iter().flat_map(|page| page.iter()) {
            for spans in output.spans() {
                let (bytes, size) = spans.retained(nodes, texts, links);
                payload += bytes;
                metadata += size;
            }
            let (bytes, size) = output.middle.retained(nodes, texts, links);
            payload += bytes;
            metadata += size;
        }
        let (bytes, size) = self.destinations.retained(links);
        (payload + bytes, metadata + size)
    }
}

#[cfg(test)]
thread_local! { pub(crate) static KEEP_KEYS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
