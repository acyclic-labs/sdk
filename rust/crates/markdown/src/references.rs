use pulldown_cmark::{BrokenLink, Event, Parser, Tag, TagEnd};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub(crate) type Key = unicase::UniCase<String>;
type Definition = (Arc<str>, Arc<str>, usize);

// All callers receive labels already normalized by pulldown-cmark. In particular,
// its ASCII whitespace rules must not be applied again as Unicode whitespace.
pub(crate) fn normalize(label: &str) -> Key {
    #[cfg(test)]
    acyclic_markdown_work::work::copy(label.len());
    unicase::UniCase::new(label.to_owned())
}

#[derive(Default)]
#[cfg_attr(test, derive(Clone))]
pub(crate) struct References {
    pub(super) committed: HashMap<Key, Definition>,
    pub(super) pending: HashMap<Key, Definition>,
    #[cfg(test)]
    pub(crate) work: usize,
}

impl References {
    pub(crate) fn frontier(&self, offset: usize) -> usize {
        self.pending
            .values()
            .map(|definition| definition.2)
            .max()
            .unwrap_or(offset)
    }

    pub(crate) fn get(&self, key: &Key) -> Option<&Definition> {
        self.committed.get(key).or_else(|| self.pending.get(key))
    }

    pub(crate) fn update(&mut self, source: &str, offset: usize, prefix: usize) -> HashSet<Key> {
        // Every definition opens with a `[` label: without one the tail has none.
        if !source.contains('[') {
            acyclic_markdown_work::record!(scan(source.len()));
            #[cfg(test)]
            acyclic_markdown_work::work::scan(source.len());
            return self.replace_pending(HashMap::new());
        }
        acyclic_markdown_work::record!(scan(source.len()));
        #[cfg(test)]
        acyclic_markdown_work::work::parsed(source.len());
        let parser = Parser::new_ext(source, crate::options());
        let pending: HashMap<_, _> = parser
            .reference_definitions()
            .iter()
            .map(|(key, value)| {
                acyclic_markdown_work::record!(node());
                #[cfg(test)]
                acyclic_markdown_work::work::copy(
                    value.dest.len() + value.title.as_ref().map_or(0, |title| title.len()),
                );
                (
                    normalize(key),
                    (
                        Arc::from(value.dest.as_ref()),
                        Arc::from(value.title.as_deref().unwrap_or_default()),
                        offset + value.span.start.saturating_sub(prefix),
                    ),
                )
            })
            .collect();
        self.replace_pending(pending)
    }

    fn replace_pending(&mut self, pending: HashMap<Key, Definition>) -> HashSet<Key> {
        #[cfg(test)]
        {
            self.work += self.pending.len() + pending.len();
        }
        let mut changed = HashSet::new();
        for key in self.pending.keys().chain(pending.keys()) {
            if !self.committed.contains_key(key) && self.pending.get(key) != pending.get(key) {
                changed.insert(key.clone());
            }
        }
        self.pending = pending;
        changed
    }

    pub(crate) fn commit(&mut self, offset: usize) {
        #[cfg(test)]
        {
            self.work += self.pending.len();
        }
        self.pending.retain(|key, value| {
            if value.2 < offset {
                self.committed
                    .entry(key.clone())
                    .or_insert_with(|| value.clone());
                false
            } else {
                true
            }
        });
    }
}

// A reference-bearing block retains source, not alternate parses. Dependencies
// are collected with every reference unresolved, so no resolved link can hide
// an inner or following candidate. A changed definition parses each dependent
// block once with the actual complete definition map.
#[cfg_attr(test, derive(Clone))]
pub(crate) struct Template {
    pub(super) source: String,
    paragraph: bool,
    task: bool,
    pub(crate) keys: HashSet<Key>,
}

impl Template {
    pub(crate) fn new(
        source: &str,
        quotes: usize,
        events: &[(Event<'_>, std::ops::Range<usize>)],
    ) -> Option<Self> {
        let paragraph = matches!(events.first(), Some((Event::Start(Tag::Paragraph), _)));
        let task = events
            .iter()
            .any(|(event, _)| matches!(event, Event::TaskListMarker(_)));
        let mut normalized = String::new();
        if task {
            normalized.push_str("- ");
        }
        for (index, mut line) in source.split_inclusive('\n').enumerate() {
            if index > 0 {
                for _ in 0..quotes {
                    let trimmed = line.trim_start_matches([' ', '\t']);
                    if let Some(rest) = trimmed.strip_prefix('>') {
                        line = rest.strip_prefix([' ', '\t']).unwrap_or(rest);
                    } else {
                        break;
                    }
                }
            }
            normalized.push_str(line);
        }
        #[cfg(test)]
        acyclic_markdown_work::work::copy(normalized.len());
        let mut keys = HashSet::new();
        let mut callback = |link: BrokenLink<'_>| {
            keys.insert(normalize(&link.reference));
            None
        };
        acyclic_markdown_work::record!(scan(normalized.len()));
        #[cfg(test)]
        acyclic_markdown_work::work::parsed(normalized.len());
        let mut first = None;
        for event in Parser::new_with_broken_link_callback(
            &normalized,
            options(paragraph),
            Some(&mut callback),
        ) {
            acyclic_markdown_work::record!(node());
            if first.is_none() {
                first = match event {
                    Event::Start(tag) => Some(tag.to_end()),
                    _ => None,
                };
            }
        }
        // A block that is a lazy continuation of a preceding definition or
        // paragraph is not a standalone block: reparsing its raw source can
        // change its block structure. Keep a template only when the reparse
        // starts with the same block as the original events.
        let original = events.first().and_then(|(event, _)| match event {
            Event::Start(tag) => Some(tag.to_end()),
            _ => None,
        });
        // A task template prepends a list marker, so its reparse starts with
        // the synthetic list rather than the original paragraph.
        let matches_original = if task {
            matches!(first, Some(TagEnd::List(_)))
        } else {
            first == original
        };
        if !matches_original {
            return None;
        }
        Some(Self {
            source: normalized,
            paragraph,
            task,
            keys,
        })
    }

    pub(crate) fn events(
        &self,
        references: &References,
        budget: usize,
    ) -> (Vec<Event<'static>>, usize, std::ops::Range<usize>) {
        let (mut events, calls) =
            crate::events::scoped(&self.source, references, options(self.paragraph), budget);
        let (cost, interval) = expansion(calls.iter());
        if self.task {
            events.drain(..2);
            events.truncate(events.len().saturating_sub(2));
        }
        (
            events.into_iter().map(|(event, _)| event).collect(),
            cost,
            interval,
        )
    }
}

// The same parse is valid until a budget crosses a reference-attempt boundary.
// MAX cost marks an exhausted suffix whose full cost is deliberately unknown.
pub(crate) fn expansion<'a>(
    calls: impl Iterator<Item = &'a (usize, usize)>,
) -> (usize, std::ops::Range<usize>) {
    let (mut cost, mut floor) = (0usize, 0);
    for (_, charge) in calls {
        if *charge == usize::MAX {
            return (usize::MAX, floor..cost.saturating_add(1));
        }
        floor = cost.saturating_add(1);
        cost = cost.saturating_add(*charge);
    }
    (cost, floor..usize::MAX)
}

pub(crate) fn extend_expansion<'a>(
    cost: usize,
    interval: &std::ops::Range<usize>,
    calls: impl Iterator<Item = &'a (usize, usize)>,
) -> (usize, std::ops::Range<usize>) {
    let (additional, valid) = expansion(calls);
    let floor = if valid.start == 0 {
        0
    } else {
        cost.saturating_add(valid.start)
    };
    (
        cost.saturating_add(additional),
        interval.start.max(floor)..interval.end.min(cost.saturating_add(valid.end)),
    )
}

fn options(paragraph: bool) -> pulldown_cmark::Options {
    let mut options = crate::options();
    if paragraph {
        options.remove(pulldown_cmark::Options::ENABLE_TABLES);
    }
    options
}
