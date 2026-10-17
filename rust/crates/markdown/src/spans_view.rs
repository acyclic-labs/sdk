use super::{Node, Part, Spans, overlay};
use crate::{Span, Style, Text};
#[cfg(any(test, feature = "fuzz-support"))]
use std::collections::HashSet;
use std::{borrow::Cow, fmt, sync::Arc};

/// Borrowed text of one canonical styled run.
pub enum TextRef<'a> {
    /// One shared text value.
    One(&'a Text),
    /// Adjacent text values whose enclosing styles became equal.
    Many(Vec<&'a Text>),
}

impl<'a> TextRef<'a> {
    /// The visible UTF-8 byte length.
    pub fn len(&self) -> usize {
        match self {
            Self::One(text) => text.len(),
            Self::Many(parts) => parts.iter().map(|text| text.len()).sum(),
        }
    }
    /// Whether the text is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Borrow contiguous text or join shared pieces for display.
    pub fn as_str(&self) -> Cow<'a, str> {
        match self {
            Self::One(text) => (*text).as_str(),
            Self::Many(parts) => {
                let length = self.len();
                #[cfg(test)]
                crate::work::copy(length);
                let mut text = String::with_capacity(length);
                for part in parts {
                    text.push_str(&part.as_str());
                }
                Cow::Owned(text)
            }
        }
    }
    /// Whether any given character occurs.
    pub fn contains<const N: usize>(&self, characters: [char; N]) -> bool {
        match self {
            Self::One(text) => text.contains(characters),
            Self::Many(parts) => parts.iter().any(|text| text.contains(characters)),
        }
    }
    /// Count shared backing text once per address.
    #[cfg(any(test, feature = "fuzz-support"))]
    pub fn retained(&self, seen: &mut HashSet<usize>) -> (usize, usize) {
        let mut result = (0, 0);
        match self {
            Self::One(text) => result = text.retained(seen),
            Self::Many(parts) => {
                for part in parts {
                    let (payload, metadata) = part.retained(seen);
                    result.0 += payload;
                    result.1 += metadata;
                }
            }
        }
        result
    }
    pub(super) fn owned(&self) -> Text {
        match self {
            Self::One(text) => (*text).clone(),
            Self::Many(parts) => parts.iter().map(|text| (*text).clone()).collect(),
        }
    }
    fn extend(&mut self, other: Self) {
        if let Self::One(left) = self {
            #[cfg(test)]
            crate::work::copy(std::mem::size_of::<&Text>());
            *self = Self::Many(vec![*left]);
        }
        if let Self::Many(parts) = self {
            for part in other.parts() {
                #[cfg(test)]
                if parts.len() == parts.capacity() {
                    crate::work::copy(parts.len() * std::mem::size_of::<&Text>());
                }
                parts.push(part);
            }
        }
    }
    fn parts(self) -> impl Iterator<Item = &'a Text> {
        let (one, many) = match self {
            Self::One(text) => (Some(text), Vec::new()),
            Self::Many(parts) => (None, parts),
        };
        one.into_iter().chain(many)
    }
    #[cfg(test)]
    pub(crate) fn allocation(&self) -> Option<*const u8> {
        match self {
            Self::One(text) => text.allocation(),
            Self::Many(parts) => parts.first().and_then(|text| text.allocation()),
        }
    }
}

impl PartialEq for TextRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.owned() == other.owned()
    }
}
impl Eq for TextRef<'_> {}
impl PartialEq<str> for TextRef<'_> {
    fn eq(&self, other: &str) -> bool {
        self.owned() == other
    }
}
impl PartialEq<&str> for TextRef<'_> {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}
impl fmt::Debug for TextRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.as_str(), f)
    }
}

/// A styled run borrowing its text from an immutable snapshot.
#[derive(Debug)]
pub struct SpanRef<'a> {
    /// Visible text.
    pub text: TextRef<'a>,
    /// Combined presentation attributes.
    pub style: Style,
    /// Link destination.
    pub link: Option<Arc<str>>,
    pub(super) boundary: bool,
}

impl<'a> SpanRef<'a> {
    pub(super) fn borrowed(span: &'a Span) -> Self {
        Self {
            text: TextRef::One(&span.text),
            style: span.style,
            link: span.link.clone(),
            boundary: false,
        }
    }
    /// Retain an owned run without copying its text bytes.
    pub fn owned(self) -> Span {
        #[cfg(test)]
        crate::work::copy(std::mem::size_of::<Span>());
        Span {
            text: self.text.owned(),
            style: self.style,
            link: self.link,
        }
    }
}

enum Task<'a> {
    Sequence(&'a Spans, Style, Option<&'a Arc<str>>),
    Prefix(&'a Node, Option<&'a Node>, Style, Option<&'a Arc<str>>),
    Part(&'a Part, Style, Option<&'a Arc<str>>),
    Tail(&'a Span, bool, Style, Option<&'a Arc<str>>),
    Boundary,
    AfterLink(bool),
}

/// Iterator over canonical styled runs in a snapshot.
pub struct Iter<'a> {
    tasks: Vec<Task<'a>>,
    buffered: Option<SpanRef<'a>>,
    boundary: bool,
}

impl<'a> Iter<'a> {
    pub(super) fn new(spans: &'a Spans) -> Self {
        // A prefix chain is walked newest-first, so it parks one task per
        // node before yielding the oldest run. Size the stack for the chain
        // up front instead of regrowing it while the walk descends.
        let mut tasks = Vec::with_capacity(spans.raw_count() + 2);
        tasks.push(Task::Sequence(spans, Style::default(), None));
        Self {
            tasks,
            buffered: None,
            boundary: false,
        }
    }
    fn push(&mut self, task: Task<'a>) {
        #[cfg(test)]
        if self.tasks.len() == self.tasks.capacity() {
            crate::work::copy(self.tasks.len() * std::mem::size_of::<Task<'_>>());
        }
        self.tasks.push(task);
    }
    fn raw(&mut self) -> Option<(SpanRef<'a>, bool)> {
        while let Some(task) = self.tasks.pop() {
            #[cfg(test)]
            crate::work::scan(1);
            match task {
                Task::Sequence(spans, style, link) => {
                    if let Some(tail) = &spans.data().tail {
                        self.push(Task::Tail(tail, spans.data().forced, style, link));
                    }
                    if let Some(prefix) = spans.data().prefix.as_deref() {
                        self.push(Task::Prefix(
                            prefix,
                            spans.data().stop.as_deref(),
                            style,
                            link,
                        ));
                    }
                }
                Task::Prefix(node, stop, style, link) => {
                    if stop.is_some_and(|stop| std::ptr::eq(node, stop)) {
                        continue;
                    }
                    self.push(Task::Part(&node.part, style, link));
                    if let Some(previous) = node
                        .previous
                        .as_deref()
                        .and_then(|data| data.prefix.as_deref())
                    {
                        self.push(Task::Prefix(previous, stop, style, link));
                    }
                }
                Task::Part(Part::Span(span, forced), style, link) => {
                    self.push(Task::Tail(span, *forced, style, link));
                }
                Task::Part(Part::Group(child, mask, target, forced), style, link) => {
                    if target.is_some() {
                        self.push(Task::AfterLink(link.is_some()));
                    }
                    self.push(Task::Sequence(
                        child,
                        overlay(*mask, style),
                        target.as_ref().or(link),
                    ));
                    if *forced {
                        self.push(Task::Boundary);
                    }
                }
                Task::Boundary => self.boundary = true,
                Task::AfterLink(linked) => self.boundary = linked,
                Task::Tail(span, forced, style, link) => {
                    let mut span = SpanRef::borrowed(span);
                    span.style = overlay(span.style, style);
                    span.link = span.link.or_else(|| link.cloned());
                    span.boundary = forced | std::mem::take(&mut self.boundary);
                    let boundary = span.boundary;
                    return Some((span, boundary));
                }
            }
        }
        None
    }
}

impl PartialEq for SpanRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text && self.style == other.style && self.link == other.link
    }
}
impl Eq for SpanRef<'_> {}

impl<'a> Iterator for Iter<'a> {
    type Item = SpanRef<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut current = self
            .buffered
            .take()
            .or_else(|| self.raw().map(|(span, _)| span))?;
        while let Some((next, forced)) = self.raw() {
            let same_link = match (&current.link, &next.link) {
                (None, None) => true,
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                _ => false,
            };
            if !forced && current.style == next.style && same_link {
                current.text.extend(next.text);
            } else {
                self.buffered = Some(next);
                break;
            }
        }
        Some(current)
    }
}
