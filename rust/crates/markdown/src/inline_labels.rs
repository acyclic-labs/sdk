#[path = "inline_destinations.rs"]
pub(super) mod destinations;
use super::projection::Projection;
use super::{Cache, Event, Range, References, Style, Tag};
use crate::inline_scan::{
    Scope, Slice,
    state::State,
    tokens::{Kind, Tokens},
};

struct Atom {
    range: Range<usize>,
    output: usize,
}

fn key(
    source: &Slice<'_>,
    scope: &Scope,
    events: &[(Event<'_>, Range<usize>)],
) -> Option<(usize, usize)> {
    let range = &events.get(scope.events.start)?.1;
    Some((
        source.base + range.start.saturating_sub(source.sentinel),
        source.base + range.end.saturating_sub(source.sentinel),
    ))
}

fn push<T>(values: &mut Vec<T>, value: T) {
    #[cfg(test)]
    crate::work::copy(
        std::mem::size_of::<T>()
            * (1 + if values.len() == values.capacity() {
                values.len()
            } else {
                0
            }),
    );
    values.push(value);
}

fn label_state(
    tokens: &Tokens,
    begin: usize,
    atoms: &[Atom],
    breaks: &std::collections::HashSet<usize>,
) -> Option<(State, usize)> {
    let mut token_index = tokens.after(begin)?;
    let mut position = begin;
    let mut atom_index = 0;
    let mut state = State::default();
    let end = loop {
        #[cfg(test)]
        crate::work::scan(1);
        let token = tokens.ready.get(token_index)?;
        let start = position.max(token.range.start);
        if token.range.end <= start {
            token_index += 1;
            continue;
        }
        if let Some(atom) = atoms
            .get(atom_index)
            .filter(|atom| atom.range.start < token.range.end)
        {
            if start < atom.range.start {
                state.feed(start..atom.range.start, token.kind, false);
                position = atom.range.start;
                continue;
            }
            if start != atom.range.start {
                return None;
            }
            #[cfg(test)]
            let output = if FORGET_CHILD.get() { 0 } else { atom.output };
            #[cfg(not(test))]
            let output = atom.output;
            state.forest.atom(atom.range.clone(), output);
            position = atom.range.end;
            if let Some(after) = tokens.after(position) {
                token_index = after;
            } else {
                while tokens
                    .ready
                    .get(token_index)
                    .is_some_and(|token| token.range.end <= position)
                {
                    #[cfg(test)]
                    crate::work::scan(1);
                    token_index += 1;
                }
            }
            atom_index += 1;
            continue;
        }
        if token.kind == Kind::Close && state.outside_brackets() {
            break start;
        }
        match token.kind {
            Kind::Escape('\n') if !breaks.contains(&token.range.end) => {
                state.feed(start..start + 1, Kind::Text, false);
            }
            Kind::Break if !breaks.contains(&token.range.end) => {}
            _ => state.feed(start..token.range.end, token.kind, false),
        }
        position = token.range.end;
        token_index += 1;
    };
    Some((state, end))
}

impl Cache {
    pub(in crate::inline_scan) fn compile(
        &mut self,
        source: &Slice<'_>,
        scope: usize,
        scopes: &[Scope],
        events: &[(Event<'_>, Range<usize>)],
        tokens: &Tokens,
        references: &References,
    ) -> Option<usize> {
        #[cfg(test)]
        let rendering = crate::work::rendering(true);
        let result = (|| {
            let mut pending = vec![(scope, false)];
            while let Some((index, ready)) = pending.pop() {
                #[cfg(test)]
                crate::work::scan(1);
                let scope = scopes.get(index)?;
                let key = key(source, scope, events)?;
                if self.atoms.contains_key(&key) {
                    continue;
                }
                if !ready && index + 1 < scope.next {
                    push(&mut pending, (index, true));
                    let mut child = index + 1;
                    while child < scope.next {
                        push(&mut pending, (child, false));
                        child = scopes.get(child)?.next;
                    }
                    continue;
                }
                self.input.feed(source.original, source.start);
                if self.nodes.is_empty() {
                    self.store(super::Output::default());
                }
                let window = events.get(scope.events.clone())?;
                let (body, independent) = match &window.first()?.0 {
                    Event::Start(Tag::Link { link_type, .. } | Tag::Image { link_type, .. })
                        if !matches!(
                            link_type,
                            pulldown_cmark::LinkType::Autolink | pulldown_cmark::LinkType::Email
                        ) =>
                    {
                        self.label(source, index, scopes, events, tokens, references)?
                    }
                    _ => (
                        self.reduce(
                            &Slice {
                                boundary: source.sentinel,
                                classified: true,
                                ..*source
                            },
                            window,
                            references,
                            0,
                        )?
                        .0,
                        true,
                    ),
                };
                #[cfg(test)]
                crate::work::copy(
                    std::mem::size_of::<((usize, usize), (usize, bool))>()
                        * (1 + if self.atoms.len() == self.atoms.capacity() {
                            self.atoms.len()
                        } else {
                            0
                        }),
                );
                self.atoms.insert(key, (body, independent));
            }
            self.atoms
                .get(&key(source, scopes.get(scope)?, events)?)
                .map(|&(body, _)| body)
        })();
        #[cfg(test)]
        crate::work::rendering(rendering);
        result
    }

    pub(in crate::inline_scan) fn independent_label(
        &self,
        source: &Slice<'_>,
        scope: &Scope,
        events: &[(Event<'_>, Range<usize>)],
    ) -> bool {
        key(source, scope, events)
            .and_then(|key| self.atoms.get(&key))
            .is_some_and(|(_, independent)| *independent)
    }

    fn label_atoms(
        &mut self,
        source: &Slice<'_>,
        index: usize,
        scopes: &[Scope],
        events: &[(Event<'_>, Range<usize>)],
    ) -> Option<(Vec<Atom>, std::collections::HashSet<usize>, bool)> {
        let scope = scopes.get(index)?;
        let mut foreign_child = false;
        let mut atoms = Vec::new();
        let mut breaks = std::collections::HashSet::new();
        let mut child = index + 1;
        let mut event_index = scope.events.start + 1;
        while event_index + 1 < scope.events.end {
            #[cfg(test)]
            crate::work::scan(1);
            if child < scope.next && scopes.get(child)?.events.start == event_index {
                let child_scope = scopes.get(child)?;
                let (output, independent) = *self.atoms.get(&key(source, child_scope, events)?)?;
                foreign_child |= !independent;
                push(
                    &mut atoms,
                    Atom {
                        range: child_scope.range.clone(),
                        output,
                    },
                );
                event_index = child_scope.events.end;
                child = child_scope.next;
            } else {
                let (event, relative) = events.get(event_index)?;
                if matches!(event, Event::SoftBreak | Event::HardBreak) {
                    #[cfg(test)]
                    crate::work::copy(
                        std::mem::size_of::<usize>()
                            * (1 + if breaks.len() == breaks.capacity() {
                                breaks.len()
                            } else {
                                0
                            }),
                    );
                    breaks.insert(source.base + relative.end.saturating_sub(source.sentinel));
                }
                if let Event::Code(text) = event {
                    let range = source.base + relative.start.saturating_sub(source.sentinel)
                        ..source.base + relative.end.saturating_sub(source.sentinel);
                    let output = self.value(
                        1,
                        range.clone(),
                        text,
                        source,
                        Style {
                            foreground: Some(crate::style::YELLOW),
                            ..Style::default()
                        },
                    )?;
                    push(&mut atoms, Atom { range, output });
                }
                event_index += 1;
            }
        }
        Some((atoms, breaks, foreign_child))
    }

    fn label(
        &mut self,
        source: &Slice<'_>,
        index: usize,
        scopes: &[Scope],
        events: &[(Event<'_>, Range<usize>)],
        tokens: &Tokens,
        references: &References,
    ) -> Option<(usize, bool)> {
        let scope = scopes.get(index)?;
        let first = &events.get(scope.events.start)?.0;
        let (dest_url, id, image, link_type) = match first {
            Event::Start(Tag::Link {
                dest_url,
                id,
                link_type,
                ..
            }) => (dest_url, id, false, link_type),
            Event::Start(Tag::Image {
                dest_url,
                id,
                link_type,
                ..
            }) => (dest_url, id, true, link_type),
            _ => return None,
        };
        let (atoms, breaks, foreign_child) = self.label_atoms(source, index, scopes, events)?;
        let begin = scope.range.start + 1 + usize::from(image);
        let (mut state, end) = label_state(tokens, begin, &atoms, &breaks)?;
        if state.invalid {
            return None;
        }
        let root = state.forest.snapshot();
        let output = self
            .resolve(
                &mut state.forest,
                root,
                &Projection {
                    source: source.original,
                    visible: source.start..end,
                    hard_breaks: &Default::default(),
                },
            )?
            .full;
        let target = if *link_type == pulldown_cmark::LinkType::Inline {
            #[cfg(test)]
            crate::work::copy(dest_url.len());
            std::sync::Arc::from(dest_url.as_ref())
        } else {
            self.target(&scope.range, dest_url, id, references)
        };
        let independent = !foreign_child && state.foreign_neutral();
        Some((
            self.mask(
                output,
                Style {
                    foreground: Some(crate::style::BLUE),
                    underline: true,
                    ..Style::default()
                },
                Some(target),
            )?,
            independent,
        ))
    }
}

#[cfg(test)]
thread_local! { static FORGET_CHILD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
#[path = "../tests/inline_labels.rs"]
mod tests;
