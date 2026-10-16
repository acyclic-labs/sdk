use pulldown_cmark::{BrokenLink, Event, Parser, Tag};
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
    crate::work::copy(label.len());
    unicase::UniCase::new(label.to_owned())
}

#[derive(Default)]
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
        #[cfg(test)]
        PARSED_BYTES.with(|work| work.set(work.get() + source.len()));
        let parser = Parser::new_ext(source, crate::options());
        let pending: HashMap<_, _> = parser
            .reference_definitions()
            .iter()
            .map(|(key, value)| {
                #[cfg(test)]
                crate::work::copy(
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
    ) -> Self {
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
        crate::work::copy(normalized.len());
        let mut keys = HashSet::new();
        let mut callback = |link: BrokenLink<'_>| {
            keys.insert(normalize(&link.reference));
            None
        };
        #[cfg(test)]
        PARSED_BYTES.with(|work| work.set(work.get() + normalized.len()));
        for _ in Parser::new_with_broken_link_callback(
            &normalized,
            options(paragraph),
            Some(&mut callback),
        ) {}
        Self {
            source: normalized,
            paragraph,
            task,
            keys,
        }
    }

    pub(crate) fn events(&self, references: &References) -> Vec<Event<'static>> {
        let mut events =
            crate::events::parse_with(&self.source, references, options(self.paragraph));
        if self.task {
            events.drain(..2);
            events.truncate(events.len().saturating_sub(2));
        }
        events.into_iter().map(|(event, _)| event).collect()
    }
}

fn options(paragraph: bool) -> pulldown_cmark::Options {
    let mut options = crate::options();
    if paragraph {
        options.remove(pulldown_cmark::Options::ENABLE_TABLES);
    }
    options
}

#[cfg(test)]
std::thread_local! {
    pub(crate) static PARSED_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
