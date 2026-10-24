use pulldown_cmark::{BrokenLink, Event, Parser, Tag, TagEnd};
use std::ops::Range;

type OwnedEvents = Vec<(Event<'static>, Range<usize>)>;
type Charges = Vec<(usize, usize)>;

fn frontier(source: &str, references: &crate::References) -> (OwnedEvents, Option<Charges>) {
    acyclic_markdown_work::record!(scan(source.len()));
    #[cfg(test)]
    crate::references::PARSED_BYTES.with(|work| work.set(work.get() + source.len()));
    let mut calls = Vec::new();
    let mut callback = |link: BrokenLink<'_>| {
        references
            .get(&crate::normalize(&link.reference))
            .map(|(destination, title, _)| {
                calls.push((link.span.end, destination.len() + title.len()));
                (destination.as_ref().into(), title.as_ref().into())
            })
    };
    let parser =
        Parser::new_with_broken_link_callback(source, crate::options(), Some(&mut callback));
    let local_definitions = parser.reference_definitions().iter().next().is_some();
    let events: Vec<_> = parser
        .into_offset_iter()
        .map(|(mut event, range)| {
            if let Event::Start(
                Tag::Link {
                    id,
                    dest_url,
                    title,
                    ..
                }
                | Tag::Image {
                    id,
                    dest_url,
                    title,
                    ..
                },
            ) = &mut event
                && !id.is_empty()
                && references.get(&crate::normalize(id)).is_some()
            {
                *dest_url = "".into();
                *title = "".into();
            }
            acyclic_markdown_work::record!(node());
            #[cfg(test)]
            crate::work::event(&event);
            (event.into_static(), range)
        })
        .collect();
    let events = paragraphs(source, events);
    // Local definitions bypass the callback; a saturated parser budget may
    // suppress later attempts. Those frontiers still use scoped leaf rendering.
    let reusable = !local_definitions
        && calls.iter().map(|(_, cost)| cost).sum::<usize>() < source.len().max(100_000);
    (events, reusable.then_some(calls))
}

// Normalized leaf templates contain no definitions. Charge reference expansion
// against the document allowance, rather than restarting it for each leaf.
pub(crate) fn scoped(
    source: &str,
    references: &crate::References,
    options: pulldown_cmark::Options,
    mut budget: usize,
) -> (OwnedEvents, Charges) {
    acyclic_markdown_work::record!(scan(source.len()));
    #[cfg(test)]
    crate::references::PARSED_BYTES.with(|work| work.set(work.get() + source.len()));
    let mut calls = Vec::new();
    let mut exhausted = false;
    let mut callback = |link: BrokenLink<'_>| {
        if budget == 0 {
            // No allowance remains. Keep a sentinel for the deferred suffix;
            // a larger document budget will reopen only this leaf.
            if !exhausted {
                calls.push((link.span.end, usize::MAX));
                exhausted = true;
            }
            return None;
        }
        let (destination, title, _) = references.get(&crate::normalize(&link.reference))?;
        let cost = destination.len() + title.len();
        budget = budget.saturating_sub(cost);
        calls.push((link.span.end, cost));
        // The real lengths were charged above. Rendering obtains their shared
        // handles by id; do not also restart the parser's per-input allowance.
        Some(("".into(), "".into()))
    };
    let events = Parser::new_with_broken_link_callback(source, options, Some(&mut callback))
        .into_offset_iter()
        .map(|(event, range)| {
            acyclic_markdown_work::record!(node());
            #[cfg(test)]
            crate::work::event(&event);
            (event.into_static(), range)
        })
        .collect();
    #[cfg(test)]
    crate::work::copy(calls.len() * std::mem::size_of::<(usize, usize)>() * 2);
    (events, calls)
}

// The parser suppresses paragraph events in tight list items. Give those inline
// runs the same checkpoint boundaries as paragraphs in loose lists.
pub(crate) fn paragraphs<'a>(
    source: &str,
    events: Vec<(Event<'a>, Range<usize>)>,
) -> Vec<(Event<'a>, Range<usize>)> {
    // Only tight list items need synthesized paragraph boundaries. Keep the
    // original allocation for ordinary leaves and potentially very wide tables.
    if !events
        .iter()
        .any(|(event, _)| matches!(event, Event::Start(Tag::Item)))
    {
        return events;
    }
    let mut result = Vec::with_capacity(events.len());
    let mut stack = Vec::new();
    let mut events = events.into_iter().peekable();
    while let Some((event, range)) = events.next() {
        if stack.last() == Some(&TagEnd::Item) && inline(&event) {
            let paragraph = result.len();
            let mut end = range.end;
            result.push((Event::Start(Tag::Paragraph), range.clone()));
            result.push((event, range.clone()));
            while events.peek().is_some_and(|(event, _)| inline(event)) {
                if let Some((event, range)) = events.next() {
                    end = range.end;
                    result.push((event, range));
                }
            }
            let suffix = source.get(end..).unwrap_or_default();
            let spaces = suffix
                .bytes()
                .take_while(|byte| matches!(byte, b' ' | b'\t'))
                .count();
            end += spaces;
            if source.as_bytes().get(end) == Some(&b'\n') {
                end += 1;
            }
            let range = range.start..end;
            if let Some((_, opening)) = result.get_mut(paragraph) {
                *opening = range.clone();
            }
            result.push((Event::End(TagEnd::Paragraph), range));
            continue;
        }
        match &event {
            Event::Start(tag) => stack.push(tag.to_end()),
            Event::End(_) => {
                stack.pop();
            }
            _ => {}
        }
        result.push((event, range));
    }
    result
}

fn inline(event: &Event<'_>) -> bool {
    match event {
        Event::Start(tag) => {
            !crate::is_leaf(tag) && !matches!(tag, Tag::List(_) | Tag::Item | Tag::BlockQuote(_))
        }
        Event::End(tag) => matches!(
            tag,
            TagEnd::Emphasis
                | TagEnd::Strong
                | TagEnd::Strikethrough
                | TagEnd::Link
                | TagEnd::Image
        ),
        Event::Rule => false,
        _ => true,
    }
}

impl crate::Markdown {
    pub(super) fn parse_frontier(&mut self, source: &str) -> (OwnedEvents, Option<Charges>) {
        #[cfg(test)]
        {
            crate::work::copy(source.len());
            self.parsed_bytes += source.len();
        }
        frontier(source, &self.references)
    }
}

// A frontier can reuse its single parse only while every reference attempt fits
// the remaining document allowance. Otherwise leaves use their scoped budgets.
pub(super) struct FrontierReferences {
    calls: Option<Charges>,
    consumed: usize,
    pub(super) cost: usize,
}

impl FrontierReferences {
    pub(super) fn new(calls: Option<Charges>, cost: usize, length: usize) -> Self {
        let reusable = calls.as_ref().is_some_and(|calls| {
            calls.iter().map(|(_, cost)| cost).sum::<usize>()
                < length.max(100_000).saturating_sub(cost)
        });
        Self {
            calls: calls.filter(|_| reusable),
            consumed: 0,
            cost,
        }
    }

    pub(super) fn reuse(&mut self, cache: &mut crate::Cached, end: usize) -> bool {
        let Some(calls) = &mut self.calls else {
            return false;
        };
        let remaining = calls.get(self.consumed..).unwrap_or_default();
        let length = remaining
            .iter()
            .take_while(|(position, _)| *position <= end)
            .count();
        let (used, interval) = crate::references::expansion(remaining.iter().take(length));
        self.consumed += length;
        cache.cost = used;
        cache.budget = interval;
        self.cost = self.cost.saturating_add(used);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_transfers_owned_events_without_copying_text() {
        for source in [
            "a sufficiently long owned paragraph without a list\n",
            "- a sufficiently long owned paragraph inside a tight list\n- next\n",
        ] {
            let events: Vec<_> = Parser::new(source)
                .into_offset_iter()
                .map(|(event, range)| (event.into_static(), range))
                .collect();
            let allocation = events.as_ptr();
            let text: Vec<_> = events
                .iter()
                .filter_map(|(event, _)| match event {
                    Event::Text(text) if text.len() > 32 => Some(text.as_ptr()),
                    _ => None,
                })
                .collect();
            assert!(!text.is_empty());
            let normalized = paragraphs(source, events);
            if !source.starts_with('-') {
                assert_eq!(normalized.as_ptr(), allocation);
            }
            let retained: Vec<_> = normalized
                .iter()
                .filter_map(|(event, _)| match event {
                    Event::Text(text) if text.len() > 32 => Some(text.as_ptr()),
                    _ => None,
                })
                .collect();
            assert_eq!(retained, text);
        }
    }
}
