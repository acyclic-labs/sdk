use pulldown_cmark::{BrokenLink, Event, Parser, Tag, TagEnd};
use std::ops::Range;

type OwnedEvents = Vec<(Event<'static>, Range<usize>)>;
type Charges = Vec<(usize, usize)>;
pub(super) type Overrides = Vec<(usize, Vec<Event<'static>>)>;

// Normalized leaf templates contain no definitions. Charge reference expansion
// against the document allowance, rather than restarting it for each leaf.
pub(crate) fn scoped<'a>(
    source: &'a str,
    references: &crate::References,
    options: pulldown_cmark::Options,
    mut budget: usize,
) -> (Vec<(Event<'a>, Range<usize>)>, Charges) {
    #[cfg(test)]
    crate::references::PARSED_BYTES.with(|work| work.set(work.get() + source.len()));
    let mut calls = Vec::new();
    let mut callback = |link: BrokenLink<'_>| {
        if budget == 0 {
            // No allowance remains. Keep a sentinel for the deferred suffix;
            // a larger document budget will reopen only this leaf.
            calls.push((link.span.end, usize::MAX));
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
        .inspect(|(_event, _)| {
            #[cfg(test)]
            crate::work::event(_event);
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
    events: &[(Event<'a>, Range<usize>)],
) -> Vec<(Event<'a>, Range<usize>)> {
    let mut result = Vec::new();
    let mut stack = Vec::new();
    let mut index = 0;
    while let Some((event, range)) = events.get(index) {
        if stack.last() == Some(&TagEnd::Item) && inline(event) {
            let start = index;
            while events.get(index).is_some_and(|(event, _)| inline(event)) {
                index += 1;
            }
            let mut end = events
                .get(index.saturating_sub(1))
                .map_or(range.end, |(_, range)| range.end);
            let suffix = source.get(end..).unwrap_or_default();
            let spaces = suffix
                .bytes()
                .take_while(|byte| matches!(byte, b' ' | b'\t'))
                .count();
            end += spaces;
            if source.as_bytes().get(end) == Some(&b'\n') {
                end += 1;
            }
            // An escaped first character's text begins after its backslash.
            let begin = range.start
                - usize::from(source.as_bytes().get(range.start.wrapping_sub(1)) == Some(&b'\\'));
            let range = begin..end;
            result.push((Event::Start(Tag::Paragraph), range.clone()));
            result.extend_from_slice(events.get(start..index).unwrap_or_default());
            result.push((Event::End(TagEnd::Paragraph), range));
            continue;
        }
        match event {
            Event::Start(tag) => stack.push(tag.to_end()),
            Event::End(_) => {
                stack.pop();
            }
            _ => {}
        }
        result.push((event.clone(), range.clone()));
        index += 1;
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
    pub(super) fn parse_frontier(&mut self, source: &str) -> (OwnedEvents, Charges, Overrides) {
        #[cfg(test)]
        {
            crate::work::copy(source.len());
            self.parsed_bytes += source.len();
        }
        let (events, calls) = scoped(
            source,
            &self.references,
            crate::options(),
            self.source
                .len()
                .max(100_000)
                .saturating_sub(self.reference_cost),
        );
        let events: OwnedEvents = events
            .into_iter()
            .map(|(event, range)| (event.into_static(), range))
            .collect();
        let events = if events
            .iter()
            .any(|(event, _)| matches!(event, Event::Start(Tag::Item)))
        {
            paragraphs(source, &events)
        } else {
            events
        };
        #[cfg(test)]
        let unchecked = UNCHECKED_LOCAL_BUDGET.get();
        #[cfg(not(test))]
        let unchecked = false;
        if self.references.pending.is_empty() || unchecked {
            return (events, calls, Vec::new());
        }
        // Definitions in this parse bypass the broken-link callback. Resolve
        // each reference-bearing leaf without definitions so every attempt
        // consumes the same remaining document allowance as cached history.
        let mut overrides = Vec::new();
        let mut charges = Vec::new();
        let mut budget = self
            .source
            .len()
            .max(100_000)
            .saturating_sub(self.reference_cost);
        for (index, (event, range)) in events.iter().enumerate() {
            let Event::Start(tag) = event else {
                continue;
            };
            if !crate::is_leaf(tag) {
                continue;
            }
            let mut end = index;
            while end < events.len() {
                #[cfg(test)]
                crate::work::scan(1);
                if events.get(end).is_some_and(
                    |(event, _)| matches!(event, Event::End(tag) if crate::is_leaf_end(*tag)),
                ) {
                    break;
                }
                end += 1;
            }
            let Some(cache) = crate::Cached::new(
                source,
                range.clone(),
                0..0,
                crate::render::Renderer::default(),
                events.get(index..=end).unwrap_or_default(),
            ) else {
                continue;
            };
            let (resolved, calls) = cache.template.resolved(&self.references, budget);
            for (_, cost) in calls {
                budget = budget.saturating_sub(cost);
                charges.push((range.end, cost));
            }
            overrides.push((index, resolved));
        }
        (events, charges, overrides)
    }
}

#[cfg(test)]
thread_local! { pub(super) static UNCHECKED_LOCAL_BUDGET: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
