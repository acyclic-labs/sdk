//! Canonical volume-relative glob matching and automata.

use super::{GlobError, spend};

include!(concat!(env!("OUT_DIR"), "/case_fold.rs"));

/// Checks that a volume-relative path is canonical.
pub fn valid_path(path: &str) -> bool {
    !path.contains(['\\', '\0'])
        && (path.is_empty()
            || path
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != ".."))
}

/// Validates the supported canonical glob grammar.
pub fn validate(pattern: &str) -> Result<(), GlobError> {
    if pattern.contains('\0')
        || (!pattern.is_empty()
            && pattern
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == ".."))
    {
        return Err(GlobError(
            "expected canonical glob with *, ? or ** components and escaped literals",
        ));
    }
    for part in pattern.split('/') {
        let mut star = false;
        for token in tokens(part) {
            let next_star = matches!(token?, Token::Star);
            if star && next_star && part != "**" {
                return Err(GlobError("** must occupy a complete component"));
            }
            star = next_star;
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Token {
    Literal(char),
    Star,
    Question,
}

fn tokens(pattern: &str) -> impl Iterator<Item = Result<Token, GlobError>> + '_ {
    let mut chars = pattern.chars();
    std::iter::from_fn(move || {
        Some(match chars.next()? {
            '\\' => match chars.next() {
                Some(ch @ ('*' | '?' | '[' | ']' | '{' | '}')) => Ok(Token::Literal(ch)),
                _ => Err(GlobError("invalid glob literal escape")),
            },
            '*' => Ok(Token::Star),
            '?' => Ok(Token::Question),
            '[' | ']' | '{' | '}' => Err(GlobError("glob classes are not supported")),
            ch => Ok(Token::Literal(ch)),
        })
    })
}

/// Counts single-character wildcard tokens.
pub fn question_count(pattern: &str) -> usize {
    tokens(pattern)
        .filter(|token| matches!(token, Ok(Token::Question)))
        .count()
}

/// Escapes a canonical path as a recursive subtree pattern.
pub fn literal_subtree(path: &str) -> Result<String, GlobError> {
    if !valid_path(path) {
        return Err(GlobError("expected canonical literal subtree"));
    }
    let escapes = path
        .chars()
        .filter(|ch| matches!(*ch, '*' | '?' | '[' | ']' | '{' | '}'))
        .count();
    let capacity = path
        .len()
        .checked_add(escapes)
        .and_then(|bytes| bytes.checked_add(3))
        .ok_or(GlobError("literal subtree size overflow"))?;
    let mut escaped = String::with_capacity(capacity);
    for ch in path.chars() {
        if matches!(ch, '*' | '?' | '[' | ']' | '{' | '}') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped.push_str("/**");
    Ok(escaped)
}

// Independent reference matcher: dynamic programming over path components.
/// Matches a canonical path, with optional Unicode folding.
pub fn matches(pattern: &str, path: &str, folded: bool) -> bool {
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
    for token in tokens(pattern) {
        let Ok(token) = token else {
            return false;
        };
        let mut next = vec![false; chars.len() + 1];
        if matches!(token, Token::Star) {
            let mut seen = false;
            for (out, before) in next.iter_mut().zip(&matched) {
                seen |= *before;
                *out = seen;
            }
        } else {
            for ((out, before), ch) in next.iter_mut().skip(1).zip(&matched).zip(&chars) {
                *out = *before
                    && match token {
                        Token::Question => true,
                        Token::Literal(literal) => literal == *ch,
                        Token::Star => false,
                    };
            }
        }
        if matches!(token, Token::Question) && folded {
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
/// A transition predicate in the glob automaton.
pub enum Symbol {
    /// Matches a single literal character.
    Literal(char),
    /// Matches one character within a path component.
    Component,
    /// Matches any character, including a separator.
    Any,
}

/// Counts extra automaton states needed by Unicode expansions.
pub fn folded_question_states() -> usize {
    CASE_FOLD_EXPANSIONS
        .iter()
        .map(|expansion| expansion.chars().count().saturating_sub(1))
        .sum()
}

impl Symbol {
    /// Checks a transition against the next character.
    pub fn matches(self, ch: char) -> bool {
        match self {
            Self::Literal(c) => c == ch,
            Self::Component => ch != '/',
            Self::Any => true,
        }
    }
}

#[derive(Default)]
/// One state and its outgoing transitions.
pub struct Node {
    /// Character transitions and destination states.
    pub edges: Vec<(Symbol, usize)>,
    /// Transitions that consume no input.
    pub epsilon: Vec<usize>,
}

#[derive(Default)]
/// Nondeterministic automaton for a set of glob patterns.
pub struct Nfa {
    /// Dense state table.
    pub nodes: Vec<Node>,
}

/// Reuse a dense visitation epoch across transitions, without clearing the NFA
/// or searching the growing closure for every edge.
pub struct ClosureScratch {
    seen: Vec<usize>,
    epoch: usize,
}

impl ClosureScratch {
    /// Creates scratch for the given number of states.
    pub fn new(nodes: usize) -> Self {
        Self {
            seen: vec![0; nodes],
            epoch: 0,
        }
    }

    fn begin(&mut self, work: &mut usize) -> Result<(), GlobError> {
        if self.epoch == usize::MAX {
            spend(work, self.seen.len())?;
            self.seen.fill(0);
            self.epoch = 0;
        }
        self.epoch += 1;
        Ok(())
    }

    fn insert(&mut self, state: usize) -> Result<bool, GlobError> {
        let seen = self
            .seen
            .get_mut(state)
            .ok_or(GlobError("invalid glob state"))?;
        let fresh = *seen != self.epoch;
        *seen = self.epoch;
        Ok(fresh)
    }
}

impl Nfa {
    fn node_mut(&mut self, id: usize) -> Result<&mut Node, GlobError> {
        self.nodes
            .get_mut(id)
            .ok_or(GlobError("invalid glob state"))
    }

    /// Appends an empty state.
    pub fn node(&mut self) -> Result<usize, GlobError> {
        let id = self.nodes.len();
        self.nodes.push(Node::default());
        Ok(id)
    }

    /// Adds a pattern and returns its entry and accepting states.
    pub fn pattern(&mut self, pattern: &str, folded: bool) -> Result<(usize, usize), GlobError> {
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
                for token in tokens(part).filter(|_| part != "*") {
                    let next = self.node()?;
                    match token? {
                        Token::Star => {
                            self.node_mut(current)?.epsilon.push(next);
                            self.node_mut(current)?
                                .edges
                                .push((Symbol::Component, current));
                        }
                        Token::Question => {
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
                        Token::Literal(ch) => self
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

    /// Adds all states reachable through epsilon transitions.
    pub fn close(
        &self,
        states: &mut Vec<usize>,
        work: &mut usize,
        scratch: &mut ClosureScratch,
    ) -> Result<(), GlobError> {
        let mut cursor = 0;
        while let Some(id) = states.get(cursor).copied() {
            spend(work, 1)?;
            cursor += 1;
            for next in &self
                .nodes
                .get(id)
                .ok_or(GlobError("invalid glob state"))?
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
                .ok_or(GlobError("glob compilation work overflow"))?,
        )?;
        states.sort_unstable();
        Ok(())
    }

    /// Closes the initial state set.
    pub fn start(
        &self,
        states: &mut Vec<usize>,
        work: &mut usize,
        scratch: &mut ClosureScratch,
    ) -> Result<(), GlobError> {
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

    /// Advances through one character, then closes the resulting state set.
    pub fn step(
        &self,
        states: &[usize],
        ch: char,
        work: &mut usize,
        scratch: &mut ClosureScratch,
    ) -> Result<Vec<usize>, GlobError> {
        scratch.begin(work)?;
        let mut next = Vec::new();
        for id in states {
            spend(work, 1)?;
            for (symbol, target) in &self
                .nodes
                .get(*id)
                .ok_or(GlobError("invalid glob state"))?
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
