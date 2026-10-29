use super::InvalidPolicy;
include!(concat!(env!("OUT_DIR"), "/case_fold.rs"));
use super::compilation::spend;

pub(super) fn valid_path(path: &str) -> bool {
    !path.contains(['\\', '\0'])
        && (path.is_empty()
            || path
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != ".."))
}

pub(super) fn validate(pattern: &str) -> Result<(), InvalidPolicy> {
    if !valid_path(pattern)
        || pattern.contains(['[', ']', '{', '}'])
        || pattern
            .split('/')
            .any(|part| part.contains("**") && part != "**")
    {
        return Err(InvalidPolicy(
            "expected canonical glob with *, ? or ** components",
        ));
    }
    Ok(())
}

// Independent reference matcher: dynamic programming over path components.
pub(super) fn matches(pattern: &str, path: &str, folded: bool) -> bool {
    let parts: Vec<_> = if path.is_empty() {
        Vec::new()
    } else {
        path.split('/').collect()
    };
    let mut matched: Vec<_> = (0..=parts.len()).map(|index| index == 0).collect();
    for component in pattern.split('/').filter(|part| !part.is_empty()) {
        let mut next = vec![false; parts.len() + 1];
        if component == "**" {
            let mut seen = false;
            for (out, before) in next.iter_mut().zip(&matched) {
                seen |= *before;
                *out = seen;
            }
        } else {
            for ((out, before), part) in next.iter_mut().skip(1).zip(&matched).zip(&parts) {
                *out = *before && component_matches(component, part, folded);
            }
        }
        matched = next;
    }
    matched.last().copied().unwrap_or(false)
}

fn component_matches(pattern: &str, text: &str, folded: bool) -> bool {
    let chars: Vec<_> = text.chars().collect();
    let mut matched: Vec<_> = (0..=chars.len()).map(|index| index == 0).collect();
    for token in pattern.chars() {
        let mut next = vec![false; chars.len() + 1];
        if token == '*' {
            let mut seen = false;
            for (out, before) in next.iter_mut().zip(&matched) {
                seen |= *before;
                *out = seen;
            }
        } else {
            for ((out, before), ch) in next.iter_mut().skip(1).zip(&matched).zip(&chars) {
                *out = *before && (token == '?' || token == *ch);
            }
        }
        if token == '?' && folded {
            for (index, _) in matched.iter().enumerate().filter(|(_, before)| **before) {
                for expansion in CASE_FOLD_EXPANSIONS {
                    let end = index + expansion.chars().count();
                    if chars
                        .get(index..end)
                        .is_some_and(|part| part.iter().copied().eq(expansion.chars()))
                    {
                        if let Some(out) = next.get_mut(end) {
                            *out = true;
                        }
                    }
                }
            }
        }
        matched = next;
    }
    matched.last().copied().unwrap_or(false)
}

#[derive(Clone, Copy)]
pub(super) enum Symbol {
    Literal(char),
    Component,
    Any,
}

pub(super) fn folded_question_states() -> usize {
    CASE_FOLD_EXPANSIONS
        .iter()
        .map(|expansion| expansion.chars().count().saturating_sub(1))
        .sum()
}

impl Symbol {
    pub(super) fn matches(self, ch: char) -> bool {
        match self {
            Self::Literal(c) => c == ch,
            Self::Component => ch != '/',
            Self::Any => true,
        }
    }
}

#[derive(Default)]
pub(super) struct Node {
    pub edges: Vec<(Symbol, usize)>,
    pub epsilon: Vec<usize>,
}

#[derive(Default)]
pub(super) struct Nfa {
    pub nodes: Vec<Node>,
}

/// Reuse a dense visitation epoch across transitions, without clearing the NFA
/// or searching the growing closure for every edge.
pub(super) struct ClosureScratch {
    seen: Vec<usize>,
    epoch: usize,
}

impl ClosureScratch {
    pub fn new(nodes: usize) -> Self {
        Self {
            seen: vec![0; nodes],
            epoch: 0,
        }
    }

    fn begin(&mut self, work: &mut usize) -> Result<(), InvalidPolicy> {
        if self.epoch == usize::MAX {
            spend(work, self.seen.len())?;
            self.seen.fill(0);
            self.epoch = 0;
        }
        self.epoch += 1;
        Ok(())
    }

    fn insert(&mut self, state: usize) -> Result<bool, InvalidPolicy> {
        let seen = self
            .seen
            .get_mut(state)
            .ok_or(InvalidPolicy("invalid glob state"))?;
        let fresh = *seen != self.epoch;
        *seen = self.epoch;
        Ok(fresh)
    }
}

impl Nfa {
    fn node_mut(&mut self, id: usize) -> Result<&mut Node, InvalidPolicy> {
        self.nodes
            .get_mut(id)
            .ok_or(InvalidPolicy("invalid glob state"))
    }

    pub fn node(&mut self) -> Result<usize, InvalidPolicy> {
        let id = self.nodes.len();
        self.nodes.push(Node::default());
        Ok(id)
    }

    pub fn pattern(
        &mut self,
        pattern: &str,
        folded: bool,
    ) -> Result<(usize, usize), InvalidPolicy> {
        let start = self.node()?;
        let mut current = start;
        let mut components: Vec<_> = pattern.split('/').filter(|part| !part.is_empty()).collect();
        components.dedup_by(|a, b| *a == "**" && *b == "**");
        let mut parts = components.into_iter().peekable();
        while let Some(part) = parts.next() {
            let last = parts.peek().is_none();
            if part == "**" {
                let next = self.node()?;
                self.node_mut(current)?.epsilon.push(next);
                if last {
                    self.node_mut(current)?.edges.push((Symbol::Any, current));
                } else {
                    let looping = self.node()?;
                    self.node_mut(current)?.edges.push((Symbol::Any, looping));
                    self.node_mut(looping)?.edges.push((Symbol::Any, looping));
                    self.node_mut(current)?
                        .edges
                        .push((Symbol::Literal('/'), next));
                    self.node_mut(looping)?
                        .edges
                        .push((Symbol::Literal('/'), next));
                }
                current = next;
            } else {
                if part == "*" {
                    // A component wildcard cannot consume the volume root.
                    let next = self.node()?;
                    self.node_mut(current)?
                        .edges
                        .push((Symbol::Component, next));
                    self.node_mut(next)?.edges.push((Symbol::Component, next));
                    current = next;
                }
                for ch in part.chars().filter(|_| part != "*") {
                    let next = self.node()?;
                    match ch {
                        '*' => {
                            self.node_mut(current)?.epsilon.push(next);
                            self.node_mut(current)?
                                .edges
                                .push((Symbol::Component, current));
                        }
                        '?' => {
                            self.node_mut(current)?
                                .edges
                                .push((Symbol::Component, next));
                            if folded {
                                for expansion in CASE_FOLD_EXPANSIONS {
                                    let mut from = current;
                                    let mut chars = expansion.chars().peekable();
                                    while let Some(ch) = chars.next() {
                                        let to = if chars.peek().is_none() {
                                            next
                                        } else {
                                            self.node()?
                                        };
                                        self.node_mut(from)?.edges.push((Symbol::Literal(ch), to));
                                        from = to;
                                    }
                                }
                            }
                        }
                        _ => self
                            .node_mut(current)?
                            .edges
                            .push((Symbol::Literal(ch), next)),
                    }
                    current = next;
                }
                if !last {
                    if parts.peek() == Some(&"**") && parts.len() == 1 {
                        let end = self.node()?;
                        let looping = self.node()?;
                        self.node_mut(current)?.epsilon.push(end);
                        self.node_mut(current)?
                            .edges
                            .push((Symbol::Literal('/'), looping));
                        self.node_mut(looping)?.epsilon.push(end);
                        self.node_mut(looping)?.edges.push((Symbol::Any, looping));
                        return Ok((start, end));
                    }
                    let next = self.node()?;
                    self.node_mut(current)?
                        .edges
                        .push((Symbol::Literal('/'), next));
                    current = next;
                }
            }
        }
        Ok((start, current))
    }

    pub fn close(
        &self,
        states: &mut Vec<usize>,
        work: &mut usize,
        scratch: &mut ClosureScratch,
    ) -> Result<(), InvalidPolicy> {
        let mut cursor = 0;
        while let Some(id) = states.get(cursor).copied() {
            spend(work, 1)?;
            cursor += 1;
            for next in &self
                .nodes
                .get(id)
                .ok_or(InvalidPolicy("invalid glob state"))?
                .epsilon
            {
                spend(work, 1)?;
                if scratch.insert(*next)? {
                    states.push(*next);
                }
            }
        }
        spend(
            work,
            states
                .len()
                .checked_mul(states.len().checked_ilog2().unwrap_or(0) as usize + 1)
                .ok_or(InvalidPolicy("glob compilation work overflow"))?,
        )?;
        states.sort_unstable();
        Ok(())
    }

    pub fn start(
        &self,
        states: &mut Vec<usize>,
        work: &mut usize,
        scratch: &mut ClosureScratch,
    ) -> Result<(), InvalidPolicy> {
        scratch.begin(work)?;
        let mut unique = 0;
        for index in 0..states.len() {
            spend(work, 1)?;
            if scratch.insert(states[index])? {
                states[unique] = states[index];
                unique += 1;
            }
        }
        states.truncate(unique);
        self.close(states, work, scratch)
    }

    pub fn step(
        &self,
        states: &[usize],
        ch: char,
        work: &mut usize,
        scratch: &mut ClosureScratch,
    ) -> Result<Vec<usize>, InvalidPolicy> {
        scratch.begin(work)?;
        let mut next = Vec::new();
        for id in states {
            spend(work, 1)?;
            for (symbol, target) in &self
                .nodes
                .get(*id)
                .ok_or(InvalidPolicy("invalid glob state"))?
                .edges
            {
                spend(work, 1)?;
                if symbol.matches(ch) {
                    spend(work, 1)?;
                    if scratch.insert(*target)? {
                        next.push(*target);
                    }
                }
            }
        }
        self.close(&mut next, work, scratch)?;
        Ok(next)
    }
}
