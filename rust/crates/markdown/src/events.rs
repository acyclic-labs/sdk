use pulldown_cmark::{BrokenLink, Event, Parser, Tag, TagEnd};
use std::ops::Range;

pub(crate) fn parse<'a>(
    source: &'a str,
    references: &crate::References,
) -> Vec<(Event<'a>, Range<usize>)> {
    #[cfg(test)]
    crate::references::PARSED_BYTES.with(|work| work.set(work.get() + source.len()));
    let mut callback = |link: BrokenLink<'_>| {
        references
            .get(&crate::normalize(&link.reference))
            .map(|_| ("".into(), "".into()))
    };
    let events: Vec<_> =
        Parser::new_with_broken_link_callback(source, crate::options(), Some(&mut callback))
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
                (event, range)
            })
            .collect();
    if events
        .iter()
        .any(|(event, _)| matches!(event, Event::Start(Tag::Item)))
    {
        paragraphs(source, &events)
    } else {
        events
    }
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
            let range = range.start..end;
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
