use crate::{Span, Style};
#[cfg(any(test, feature = "fuzz-support"))]
use std::collections::HashSet;
use std::{fmt, sync::Arc};

#[path = "spans_view.rs"]
mod view;
pub use view::{SpanRef, TextRef};

/// A snapshot of ordered styled runs with shared immutable prefixes.
#[derive(Default)]
pub struct Spans {
    value: Option<Arc<Contents>>,
}

#[derive(Default)]
struct Contents {
    prefix: Option<Arc<Node>>,
    stop: Option<Arc<Node>>,
    tail: Option<Box<Span>>,
    forced: bool,
}

struct Node {
    previous: Option<Arc<Contents>>,
    runs: usize,
    part: Part,
}

enum Part {
    Span(Box<Span>, bool),
    Group(Spans, Style, Option<Arc<str>>, bool),
}

impl Spans {
    fn data(&self) -> &Contents {
        static EMPTY: Contents = Contents {
            prefix: None,
            stop: None,
            tail: None,
            forced: false,
        };
        self.value.as_deref().unwrap_or(&EMPTY)
    }

    fn edit(&mut self) -> &mut Contents {
        Arc::make_mut(self.value.get_or_insert_with(Arc::default))
    }

    fn from_data(data: Contents) -> Self {
        Self {
            value: Some(Arc::new(data)),
        }
    }

    /// Visit canonical styled runs, retaining explicit link boundaries.
    pub fn iter(&self) -> view::Iter<'_> {
        view::Iter::new(self)
    }

    /// Number of canonical styled runs.
    pub fn len(&self) -> usize {
        #[cfg(test)]
        if STALE_COUNTS.get() {
            return self.raw_count();
        }
        self.iter().count()
    }

    /// Whether the sequence has no runs.
    pub fn is_empty(&self) -> bool {
        self.raw_count() == 0
    }

    fn raw_count(&self) -> usize {
        self.data().prefix.as_ref().map_or(0, |node| node.runs)
            - self.data().stop.as_ref().map_or(0, |node| node.runs)
            + usize::from(self.data().tail.is_some())
    }

    /// The first canonical run.
    pub fn first(&self) -> Option<SpanRef<'_>> {
        if self.data().prefix.is_none() {
            return self.data().tail.as_ref().map(|tail| {
                let mut span = SpanRef::borrowed(tail);
                span.boundary = self.data().forced;
                span
            });
        }
        self.iter().next()
    }

    /// The last canonical run.
    pub fn last(&self) -> Option<SpanRef<'_>> {
        self.iter().last()
    }

    /// Mutate the final run; shared prefixes remain unchanged.
    pub fn last_mut(&mut self) -> Option<&mut Span> {
        if self.is_empty() {
            return None;
        }
        if self.data().prefix.is_some() {
            *self = self.materialized(usize::MAX);
        }
        self.edit().tail.as_deref_mut()
    }

    pub(crate) fn append_text(&mut self, spaces: usize, text: &str) {
        if let Some(last) = self.last_piece_mut() {
            last.text.extend(std::iter::repeat_n(' ', spaces));
            last.text.push_str(text);
        }
    }

    pub(crate) fn append_last(
        &mut self,
        text: &str,
        style: Style,
        link: Option<&Arc<str>>,
    ) -> bool {
        // A shared group stays shared: the caller appends a mergeable run,
        // which iterates as the same canonical runs.
        if self.data().tail.is_none()
            && self
                .data()
                .prefix
                .as_deref()
                .is_some_and(|node| matches!(node.part, Part::Group(..)))
        {
            return false;
        }
        if let Some(last) = self.last_piece_mut()
            && last.style == style
            && match (&last.link, link) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            }
        {
            last.text.push_str(text);
            true
        } else {
            false
        }
    }

    fn last_piece_mut(&mut self) -> Option<&mut Span> {
        if self.is_empty() {
            return None;
        }
        if self.data().tail.is_none()
            && let Some(node) = &self.data().prefix
        {
            match &node.part {
                Part::Span(span, forced) => {
                    #[cfg(test)]
                    crate::work::copy(std::mem::size_of::<Span>());
                    let (span, forced, prefix) = (
                        span.clone(),
                        *forced,
                        node.previous.as_ref().and_then(|data| data.prefix.clone()),
                    );
                    let data = self.edit();
                    data.tail = Some(span);
                    data.forced = forced;
                    data.prefix = prefix;
                }
                Part::Group(..) => *self = self.materialized(usize::MAX),
            }
        }
        self.edit().tail.as_deref_mut()
    }

    /// Append a distinct run, preserving its boundary.
    pub fn push(&mut self, span: Span) {
        self.push_content(span, true);
    }

    pub(crate) fn push_content(&mut self, span: Span, forced: bool) {
        self.flush_tail();
        let shared = self
            .value
            .as_ref()
            .is_some_and(|value| Arc::strong_count(value) > 1);
        #[cfg(test)]
        let shared = shared && !COPY_SHARED_TAIL.get();
        if shared {
            // Only the immutable cursors survive this append. Construct the
            // new tail directly instead of copying a Contents value whose
            // tail and boundary flag would immediately be overwritten.
            let old = self.data();
            #[cfg(test)]
            crate::work::copy(
                std::mem::size_of::<Arc<Node>>()
                    * (usize::from(old.prefix.is_some()) + usize::from(old.stop.is_some())),
            );
            let data = Contents {
                prefix: old.prefix.clone(),
                stop: old.stop.clone(),
                tail: Some(Box::new(span)),
                forced,
            };
            *self = Self::from_data(data);
            return;
        }
        let data = self.edit();
        data.tail = Some(Box::new(span));
        data.forced = forced;
    }

    fn flush_tail(&mut self) {
        if self.data().tail.is_none() {
            return;
        }
        let shared = self
            .value
            .as_ref()
            .is_some_and(|value| Arc::strong_count(value) > 1);
        #[cfg(test)]
        let shared = shared && !COPY_SHARED_TAIL.get();
        if shared {
            #[cfg(test)]
            crate::work::scan(1);
            // A snapshot already owns this tail. Retain its complete contents
            // as an immutable group instead of cloning them to take the tail.
            let runs = self.raw_count();
            let child = std::mem::take(self);
            *self = Self::from_data(Contents {
                prefix: Some(Arc::new(Node {
                    previous: None,
                    runs,
                    part: Part::Group(child, Style::default(), None, false),
                })),
                stop: None,
                tail: None,
                forced: false,
            });
            return;
        }
        let Some(span) = self.edit().tail.take() else {
            return;
        };
        let runs = self.data().prefix.as_ref().map_or(0, |node| node.runs) + 1;
        #[cfg(test)]
        crate::work::scan(1);
        if self.data().prefix.is_none() {
            let data = self.edit();
            data.prefix = Some(Arc::new(Node {
                runs,
                previous: None,
                part: Part::Span(span, data.forced),
            }));
            return;
        }
        #[cfg(test)]
        if self.data().stop.is_some() {
            crate::work::copy(std::mem::size_of::<Arc<Node>>());
        }
        let stop = self.data().stop.clone();
        let forced = self.data().forced;
        let previous = self.value.take();
        *self = Self::from_data(Contents {
            prefix: Some(Arc::new(Node {
                runs,
                previous,
                part: Part::Span(span, forced),
            })),
            stop,
            tail: None,
            forced: false,
        });
    }

    pub(crate) fn freeze(&mut self) {
        self.flush_tail();
    }

    // Both values are frozen cursors in the same immutable sibling list.
    // The inline forest owns and validates their ancestry before constructing
    // the view; no traversal or copying of the enclosed children is needed.
    pub(crate) fn suffix_after(&self, start: &Self) -> Self {
        debug_assert!(self.data().tail.is_none() && start.data().tail.is_none());
        let result = Self::from_data(Contents {
            prefix: self.data().prefix.clone(),
            stop: start
                .data()
                .prefix
                .clone()
                .or_else(|| self.data().stop.clone()),
            tail: None,
            forced: false,
        });
        if result.is_empty() {
            Self::default()
        } else {
            result
        }
    }

    /// Remove every run from this snapshot.
    pub fn clear(&mut self) {
        *self = Self::default();
    }

    /// Retain the first `length` canonical runs.
    pub fn truncate(&mut self, length: usize) {
        if length == 0 {
            self.clear();
        } else if self.iter().take(length.saturating_add(1)).count() > length {
            *self = self.materialized(length);
        }
    }

    fn materialized(&self, length: usize) -> Self {
        let mut spans = Self::default();
        for span in self.iter().take(length) {
            let forced = span.boundary;
            spans.push_content(span.owned(), forced);
        }
        spans
    }

    /// Materialize owned runs when a caller needs to edit the whole sequence.
    pub fn to_vec(&self) -> Vec<Span> {
        let mut result = Vec::new();
        for span in self.iter() {
            #[cfg(test)]
            if result.len() == result.capacity() {
                crate::work::copy(result.len() * std::mem::size_of::<Span>());
            }
            result.push(span.owned());
        }
        result
    }

    // Append shared runs under `style`, each starting a distinct run as
    // `push` would.
    pub(crate) fn append_boundary(&mut self, other: Spans, style: Style) {
        self.group(other, style, None, true);
    }

    pub(crate) fn append_shared(&mut self, other: Spans) {
        self.group(other, Style::default(), None, true);
    }

    /// Append runs under an enclosing style without copying them.
    pub(crate) fn append_styled(&mut self, other: Spans, style: Style) {
        self.group(other, style, None, false);
    }

    /// Apply an enclosing style without copying its child runs.
    pub fn styled(self, style: Style) -> Self {
        let mut spans = Self::default();
        spans.group(self, style, None, false);
        spans
    }

    pub(crate) fn masked(self, style: Style, link: Option<Arc<str>>) -> Self {
        if style == Style::default() && link.is_none() {
            return self;
        }
        let mut spans = Self::default();
        let boundary = link.is_some();
        spans.group(self, style, link, boundary);
        spans
    }

    pub(crate) fn same_snapshot(&self, other: &Self) -> bool {
        fn same<T>(left: &Option<Arc<T>>, right: &Option<Arc<T>>) -> bool {
            match (left, right) {
                (None, None) => true,
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                _ => false,
            }
        }
        same(&self.value, &other.value)
            || (self.data().tail.is_none()
                && other.data().tail.is_none()
                && same(&self.data().prefix, &other.data().prefix)
                && same(&self.data().stop, &other.data().stop))
    }

    /// Concatenate parsed content, coalescing equal styles at the boundary.
    pub fn concat(&mut self, other: Spans) {
        self.group(other, Style::default(), None, false);
    }

    fn group(&mut self, other: Spans, style: Style, link: Option<Arc<str>>, forced: bool) {
        if !forced
            && style == Style::default()
            && link.is_none()
            && self.data().prefix.is_none()
            && self.data().tail.is_none()
        {
            *self = other;
            return;
        }
        if other.data().prefix.is_none() && other.data().tail.is_none() && link.is_none() {
            return;
        }
        self.flush_tail();
        #[cfg(test)]
        crate::work::scan(1);
        let runs = self.data().prefix.as_ref().map_or(0, |node| node.runs) + other.raw_count();
        #[cfg(test)]
        if self.data().stop.is_some() {
            crate::work::copy(std::mem::size_of::<Arc<Node>>());
        }
        let stop = self.data().stop.clone();
        let previous = self.value.take();
        *self = Self::from_data(Contents {
            prefix: Some(Arc::new(Node {
                runs,
                previous,
                part: Part::Group(other, style, link, forced),
            })),
            stop,
            tail: None,
            forced: false,
        });
    }

    /// Count shared run nodes, text backing and link targets once per address.
    #[cfg(any(test, feature = "fuzz-support"))]
    pub fn retained(
        &self,
        nodes: &mut HashSet<usize>,
        texts: &mut HashSet<usize>,
        links: &mut HashSet<*const u8>,
    ) -> (usize, usize) {
        let mut pending = vec![self];
        let (mut payload, mut metadata) = (0, 0);
        while let Some(spans) = pending.pop() {
            if let Some(data) = &spans.value {
                if !nodes.insert(Arc::as_ptr(data) as usize) {
                    continue;
                }
                metadata += std::mem::size_of::<Contents>() + 2 * std::mem::size_of::<usize>();
            }
            if let Some(tail) = &spans.data().tail {
                let (bytes, size) = retained_span(tail, texts, links);
                payload += bytes;
                metadata += size;
            }
            let mut current = spans.data().prefix.as_ref();
            while let Some(node) = current {
                if !nodes.insert(Arc::as_ptr(node) as usize) {
                    break;
                }
                metadata += std::mem::size_of::<Node>() + 2 * std::mem::size_of::<usize>();
                match &node.part {
                    Part::Span(span, _) => {
                        let (bytes, size) = retained_span(span, texts, links);
                        payload += bytes;
                        metadata += size;
                    }
                    Part::Group(child, _, link, _) => {
                        if let Some(link) = link
                            && links.insert(Arc::as_ptr(link).cast::<u8>())
                        {
                            payload += link.len();
                        }
                        pending.push(child);
                    }
                }
                if let Some(data) = &node.previous {
                    if !nodes.insert(Arc::as_ptr(data) as usize) {
                        break;
                    }
                    metadata += std::mem::size_of::<Contents>() + 2 * std::mem::size_of::<usize>();
                    debug_assert!(data.tail.is_none());
                    current = data.prefix.as_ref();
                } else {
                    current = None;
                }
            }
        }
        (payload, metadata)
    }
}

#[cfg(any(test, feature = "fuzz-support"))]
fn retained_span(
    span: &Span,
    texts: &mut HashSet<usize>,
    links: &mut HashSet<*const u8>,
) -> (usize, usize) {
    let (mut payload, metadata) = span.text.retained(texts);
    if let Some(link) = &span.link
        && links.insert(Arc::as_ptr(link).cast::<u8>())
    {
        payload += link.len();
    }
    (payload, metadata + std::mem::size_of::<Span>())
}

impl Clone for Spans {
    fn clone(&self) -> Self {
        #[cfg(test)]
        if COPY_PREFIX.get() {
            return self.iter().map(SpanRef::owned).collect();
        }
        let Some(value) = &self.value else {
            return Self::default();
        };
        #[cfg(test)]
        crate::work::copy(std::mem::size_of::<Self>());
        Self {
            value: Some(Arc::clone(value)),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        #[cfg(test)]
        if COPY_PREFIX.get() {
            *self = source.clone();
            return;
        }
        #[cfg(test)]
        crate::work::copy(std::mem::size_of::<Self>());
        self.value.clone_from(&source.value);
    }
}

impl Clone for Contents {
    fn clone(&self) -> Self {
        #[cfg(test)]
        crate::work::copy(
            std::mem::size_of::<Self>()
                + if self.tail.is_some() {
                    std::mem::size_of::<Span>()
                } else {
                    0
                },
        );
        Self {
            prefix: self.prefix.clone(),
            stop: self.stop.clone(),
            tail: self.tail.clone(),
            forced: self.forced,
        }
    }
}

impl Drop for Contents {
    fn drop(&mut self) {
        let mut current = self.prefix.take();
        let mut pending: Vec<_> = self.stop.take().into_iter().collect();
        while let Some(node) = current.take().or_else(|| pending.pop()) {
            let Some(mut node) = Arc::into_inner(node) else {
                continue;
            };
            #[cfg(test)]
            crate::work::scan(1);
            if let Some(mut data) = node.previous.take().and_then(Arc::into_inner) {
                current = data.prefix.take();
                pending.extend(data.stop.take());
            }
            if let Part::Group(child, _, _, _) = &mut node.part
                && let Some(mut data) = child.value.take().and_then(Arc::into_inner)
            {
                pending.extend(data.prefix.take());
                pending.extend(data.stop.take());
            }
        }
    }
}

impl From<Vec<Span>> for Spans {
    fn from(spans: Vec<Span>) -> Self {
        spans.into_iter().collect()
    }
}
impl FromIterator<Span> for Spans {
    fn from_iter<T: IntoIterator<Item = Span>>(spans: T) -> Self {
        let mut result = Self::default();
        result.extend(spans);
        result
    }
}
impl Extend<Span> for Spans {
    fn extend<T: IntoIterator<Item = Span>>(&mut self, spans: T) {
        for span in spans {
            self.push(span);
        }
    }
}
impl<'a> IntoIterator for &'a Spans {
    type Item = SpanRef<'a>;
    type IntoIter = view::Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl IntoIterator for Spans {
    type Item = Span;
    type IntoIter = std::vec::IntoIter<Span>;
    fn into_iter(self) -> Self::IntoIter {
        self.to_vec().into_iter()
    }
}
impl PartialEq for Spans {
    fn eq(&self, other: &Self) -> bool {
        self.same_snapshot(other) || self.iter().eq(other.iter())
    }
}
impl Eq for Spans {}
impl fmt::Debug for Spans {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

fn overlay(inner: Style, outer: Style) -> Style {
    Style {
        foreground: inner.foreground.or(outer.foreground),
        bold: inner.bold || outer.bold,
        italic: inner.italic || outer.italic,
        dim: inner.dim || outer.dim,
        underline: inner.underline || outer.underline,
        strikethrough: inner.strikethrough || outer.strikethrough,
    }
}

#[cfg(test)]
thread_local! { static STALE_COUNTS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; static COPY_PREFIX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(test)]
thread_local! { pub(crate) static COPY_SHARED_TAIL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/spans.rs"]
mod tests;
