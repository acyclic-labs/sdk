use super::compilation::{
    MAXIMUM_STATE_MEMBERS, MAXIMUM_STATES, MAXIMUM_TEXT_BYTES, MAXIMUM_TRANSITIONS, MAXIMUM_WORK,
    spend,
};
use super::glob::{Nfa, Symbol};
use super::{
    Action, Capability, Context, Decision, Denial, InvalidPolicy, Policy, Principal, Resource,
};
use std::collections::{BTreeMap, BTreeSet};

mod reachability;

#[derive(Clone, Copy)]
struct Outcome {
    rule: Option<usize>,
    denial: Option<Denial>,
}

struct State {
    transitions: Vec<usize>,
    decisions: [Outcome; 12],
    actions: u16,
}

/// Determinized glob trie for one principal, volume, generation, and context.
/// Prepare paths once during namespace lookup, then check the resulting handle
/// without scanning names, allocating, or depending on policy size.
pub struct PathTrie {
    alphabet: Alphabet,
    states: Vec<State>,
    rules: Vec<String>,
    version: u64,
    valid_from: u64,
    valid_until: Option<u64>,
    config: crate::VolumeConfig,
}

/// A path bound to its immutable compiled view. Its private state cannot be
/// forged or transferred to a different policy, principal or capability.
#[derive(Clone, Copy)]
pub struct PreparedPath<'a> {
    trie: &'a PathTrie,
    state: usize,
    bytes: usize,
    depth: usize,
}

impl<'a> PreparedPath<'a> {
    /// Prepare a child during namespace lookup. Only this new name is scanned;
    /// previously prepared ancestors are never scanned again.
    pub fn child(self, name: &str) -> Result<Self, InvalidPolicy> {
        if name.is_empty() || name.contains('/') {
            return Err(InvalidPolicy("expected one path component"));
        }
        let key = super::names::key(name, self.trie.config)?;
        self.advance(
            super::names::encoded_len(name, self.trie.config),
            key.chars().map(Ok),
        )
    }

    fn advance(
        self,
        length: usize,
        chars: impl Iterator<Item = Result<char, InvalidPolicy>>,
    ) -> Result<Self, InvalidPolicy> {
        let bytes = length
            .checked_add(1)
            .and_then(|length| self.bytes.checked_add(length))
            .ok_or(InvalidPolicy("path length overflow"))?;
        let depth = self.depth + 1;
        if bytes > self.trie.config.limits.maximum_path_bytes as usize
            || depth > self.trie.config.limits.maximum_path_depth as usize
        {
            return Err(InvalidPolicy("path exceeds volume limits"));
        }
        let mut state = self.state;
        if self.depth != 0 {
            state = self.trie.step(state, '/')?;
        }
        for ch in chars {
            state = self.trie.step(state, ch?)?;
        }
        Ok(Self {
            state,
            bytes,
            depth,
            ..self
        })
    }

    /// Prepare an authenticated native name without losing its encoding identity.
    /// Unsupported profile/encoding pairs and non-Unicode names fail closed.
    pub fn child_name(self, name: &crate::kernel::LogicalName) -> Result<Self, InvalidPolicy> {
        use crate::kernel::NameEncoding;
        if name.encoding() != super::names::encoding(self.trie.config)
            || name.as_bytes().len() > self.trie.config.limits.maximum_component_bytes as usize
        {
            return Err(InvalidPolicy(
                "name encoding does not match the access view profile",
            ));
        }
        match name.encoding() {
            NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                let text = std::str::from_utf8(name.as_bytes())
                    .map_err(|_| InvalidPolicy("non-Unicode access path is unsupported"))?;
                self.child(text)
            }
            NameEncoding::WindowsUtf16Le => {
                let units = crate::kernel::types::utf16le_units(name.as_bytes())
                    .ok_or(InvalidPolicy("invalid UTF-16 access name"))?;
                self.advance(
                    name.as_bytes().len(),
                    char::decode_utf16(units).map(|ch| {
                        ch.map_err(|_| InvalidPolicy("non-Unicode access path is unsupported"))
                    }),
                )
            }
        }
    }

    /// Constant-time, allocation-free decision, including the pinned deadline.
    #[must_use]
    pub fn decide(self, action: Action, now: u64) -> Decision<'a> {
        self.trie.decision(self.state, action, now)
    }

    pub(super) fn action_mask(self, now: u64) -> u16 {
        self.trie
            .state_at(self.state, now)
            .map_or(0, |state| state.actions)
    }
}

// Two-level direct Unicode table: two indexed loads per scalar, no searches.
// The scalar space and the NFA literal limit bound its storage.
struct Alphabet {
    pages: Vec<[u16; 256]>,
    page_ids: Box<[u16; 4352]>,
}

impl Alphabet {
    fn new(symbols: &[char]) -> Result<Self, InvalidPolicy> {
        let other =
            u16::try_from(symbols.len()).map_err(|_| InvalidPolicy("alphabet exceeds limit"))?;
        let mut result = Self {
            pages: vec![[other; 256]],
            page_ids: Box::new([0; 4352]),
        };
        for (symbol, ch) in symbols.iter().enumerate() {
            let code = *ch as usize;
            if let Some(page) = result.page_ids.get_mut(code >> 8) {
                if *page == 0 {
                    *page = u16::try_from(result.pages.len())
                        .map_err(|_| InvalidPolicy("alphabet pages exceed limit"))?;
                    result.pages.push([other; 256]);
                }
                if let Some(slot) = result
                    .pages
                    .get_mut(usize::from(*page))
                    .and_then(|p| p.get_mut(code & 255))
                {
                    *slot = u16::try_from(symbol)
                        .map_err(|_| InvalidPolicy("alphabet exceeds limit"))?;
                }
            }
        }
        Ok(result)
    }

    fn symbol(&self, ch: char) -> Option<usize> {
        let code = ch as usize;
        self.pages
            .get(usize::from(*self.page_ids.get(code >> 8)?))?
            .get(code & 255)
            .copied()
            .map(usize::from)
    }
}

impl PathTrie {
    /// Compile the same decision reducer used by `decide` into terminal states.
    /// `resource.path` is ignored; every other input is pinned for this view.
    /// Rejects policies whose glob automaton exceeds bounded state, storage or
    /// compilation work, including short patterns with exponential state growth.
    pub fn compile(
        policy: &Policy,
        principal: &Principal,
        resource: Resource<'_>,
        context: Context,
        capability: &Capability,
    ) -> Result<Self, InvalidPolicy> {
        policy.validate()?;
        capability.validate()?;
        if resource.config.validate().is_err() || resource.generation > resource.head {
            return Err(InvalidPolicy("generation exceeds head"));
        }
        let mut nfa = Nfa::default();
        let mut starts = Vec::new();
        let mut rules = BTreeMap::new();
        let mut grants = BTreeMap::new();
        let mut text_left = MAXIMUM_TEXT_BYTES;
        let mut valid_until = capability.expires_at.filter(|expiry| *expiry > context.now);
        for rule in policy.layers.iter().flatten() {
            if let Some(expiry) = rule
                .conditions
                .expires_at
                .filter(|expiry| *expiry > context.now)
            {
                valid_until = Some(valid_until.map_or(expiry, |old| old.min(expiry)));
            }
            if super::decision::rule_matches(rule, principal, resource, context) {
                spend(&mut text_left, rule.resource.path.len())?;
                let key = super::names::fold(&rule.resource.path, resource.config);
                let (start, end) =
                    nfa.pattern(&key, super::names::folds_unicode(resource.config))?;
                starts.push(start);
                rules.insert(rule.id.as_str(), end);
            }
        }
        for grant in capability.layers.iter().flatten() {
            if grant.resource.matches_view(resource) {
                spend(&mut text_left, grant.resource.path.len())?;
                let key = super::names::fold(&grant.resource.path, resource.config);
                let (start, end) =
                    nfa.pattern(&key, super::names::folds_unicode(resource.config))?;
                starts.push(start);
                grants.insert(std::ptr::from_ref(grant), end);
            }
        }
        let (symbols, other) = alphabet(&nfa)?;
        let rule_ids: Vec<_> = rules
            .keys()
            .map(|id| {
                text_left = text_left
                    .checked_sub(id.len())
                    .ok_or(InvalidPolicy("compiled rule ids exceed text limit"))?;
                Ok((*id).to_owned())
            })
            .collect::<Result<_, InvalidPolicy>>()?;
        let rules = rules
            .into_iter()
            .enumerate()
            .map(|(index, (id, end))| (id, (end, index)))
            .collect();
        let states = determinize(
            &nfa,
            starts,
            &symbols,
            other,
            |subset| {
                outcomes(
                    policy,
                    capability,
                    context,
                    resource.generation != resource.head,
                    &rules,
                    &grants,
                    subset,
                )
            },
            policy.layers.len()
                + capability.layers.len()
                + policy.layers.iter().map(Vec::len).sum::<usize>()
                + capability.layers.iter().map(Vec::len).sum::<usize>(),
        )?;
        Ok(Self {
            alphabet: Alphabet::new(&symbols)?,
            states,
            rules: rule_ids,
            version: policy.version,
            valid_from: context.now,
            valid_until,
            config: resource.config,
        })
    }

    /// Earliest time at which the caller must rebuild this immutable view.
    ///
    /// ```compile_fail
    /// use acyclic_fs::access::PathTrie;
    /// fn disable_expiry(trie: &mut PathTrie) {
    ///     trie.valid_until = None;
    /// }
    /// ```
    #[must_use]
    pub const fn valid_until(&self) -> Option<u64> {
        self.valid_until
    }

    /// Prepare an entire path. This scans its bytes and may allocate a folded
    /// name. Native lookups should retain handles and use `PreparedPath::child`
    /// to prepare only new components; hot checks use `PreparedPath::decide`.
    pub fn prepare(&self, path: &str) -> Result<PreparedPath<'_>, InvalidPolicy> {
        let mut prepared = PreparedPath {
            trie: self,
            state: 0,
            bytes: 0,
            depth: 0,
        };
        if !path.is_empty() {
            for name in path.split('/') {
                prepared = prepared.child(name)?;
            }
        }
        Ok(prepared)
    }

    /// Convenience decision for an unprepared UTF-8 path. Preparation is linear
    /// in path bytes; the bound handle's decision itself is constant time.
    #[must_use]
    pub fn decide(&self, action: Action, path: &str, now: u64) -> Decision<'_> {
        self.prepare(path).map_or_else(
            |_| Decision::denied(self.version, Denial::Invalid),
            |path| path.decide(action, now),
        )
    }

    fn step(&self, state: usize, ch: char) -> Result<usize, InvalidPolicy> {
        let symbol = self
            .alphabet
            .symbol(ch)
            .ok_or(InvalidPolicy("invalid alphabet"))?;
        self.states
            .get(state)
            .and_then(|node| node.transitions.get(symbol))
            .copied()
            .ok_or(InvalidPolicy("invalid compiled path"))
    }

    fn decision(&self, state: usize, action: Action, now: u64) -> Decision<'_> {
        match self
            .state_at(state, now)
            .and_then(|node| node.decisions.get(action as usize))
        {
            Some(outcome) => Decision {
                allowed: outcome.denial.is_none(),
                rule_id: outcome
                    .rule
                    .and_then(|id| self.rules.get(id).map(String::as_str)),
                denial: outcome.denial,
                policy_version: self.version,
            },
            None => Decision::denied(self.version, Denial::Invalid),
        }
    }

    fn state_at(&self, state: usize, now: u64) -> Option<&State> {
        if now < self.valid_from || self.valid_until.is_some_and(|expiry| now >= expiry) {
            None
        } else {
            self.states.get(state)
        }
    }
}

fn outcomes(
    policy: &Policy,
    capability: &Capability,
    context: Context,
    historical: bool,
    rules: &BTreeMap<&str, (usize, usize)>,
    grants: &BTreeMap<*const super::Grant, usize>,
    subset: &[usize],
) -> [Outcome; 12] {
    Action::ALL.map(|action| {
        let decision = super::decision::evaluate(
            policy,
            capability,
            context,
            action,
            historical,
            |rule| {
                rules
                    .get(rule.id.as_str())
                    .is_some_and(|(end, _)| subset.binary_search(end).is_ok())
            },
            |grant| {
                grants
                    .get(&std::ptr::from_ref(grant))
                    .is_some_and(|end| subset.binary_search(end).is_ok())
            },
        );
        Outcome {
            rule: decision
                .rule_id
                .and_then(|id| rules.get(id).map(|(_, index)| *index)),
            denial: decision.denial,
        }
    })
}

fn alphabet(nfa: &Nfa) -> Result<(Vec<char>, char), InvalidPolicy> {
    let mut alphabet: BTreeSet<char> = nfa
        .nodes
        .iter()
        .flat_map(|node| node.edges.iter())
        .filter_map(|(symbol, _)| match symbol {
            Symbol::Literal(c) => Some(*c),
            _ => None,
        })
        .collect();
    alphabet.insert('/');
    // One representative for every character not named literally.
    let other = (1..=0x10ffff)
        .filter_map(char::from_u32)
        .find(|ch| !alphabet.contains(ch))
        .ok_or(InvalidPolicy("exhausted glob alphabet"))?;
    let alphabet: Vec<_> = alphabet.into_iter().collect();
    Ok((alphabet, other))
}

fn determinize(
    nfa: &Nfa,
    mut starts: Vec<usize>,
    alphabet: &[char],
    other: char,
    decide: impl Fn(&[usize]) -> [Outcome; 12],
    rules: usize,
) -> Result<Vec<State>, InvalidPolicy> {
    let mut work = MAXIMUM_WORK;
    nfa.close(&mut starts, &mut work)?;
    let mut members = MAXIMUM_STATE_MEMBERS;
    spend(&mut members, starts.len())?;
    let mut transitions_left = MAXIMUM_TRANSITIONS;
    let mut ids = BTreeMap::from([(starts.clone(), 0)]);
    let mut subsets = vec![starts];
    let mut states = Vec::new();
    while states.len() < subsets.len() {
        spend(
            &mut work,
            rules
                .checked_mul(24)
                .ok_or(InvalidPolicy("glob compilation exceeds work limit"))?,
        )?;
        let subset = subsets
            .get(states.len())
            .ok_or(InvalidPolicy("invalid glob state"))?
            .clone();
        let decisions = decide(&subset);
        spend(&mut transitions_left, alphabet.len() + 1)?;
        let mut transitions = Vec::with_capacity(alphabet.len() + 1);
        for ch in alphabet.iter().copied().chain(std::iter::once(other)) {
            let next = nfa.step(&subset, ch, &mut work)?;
            let id = if let Some(id) = ids.get(&next) {
                *id
            } else {
                if subsets.len() == MAXIMUM_STATES {
                    return Err(InvalidPolicy("glob compilation exceeds DFA state limit"));
                }
                spend(&mut members, next.len())?;
                let id = subsets.len();
                ids.insert(next.clone(), id);
                subsets.push(next);
                id
            };
            transitions.push(id);
        }
        states.push(State {
            transitions,
            actions: decisions
                .iter()
                .enumerate()
                .fold(0, |mask, (bit, outcome)| {
                    mask | if outcome.denial.is_none() {
                        1 << bit
                    } else {
                        0
                    }
                }),
            decisions,
        });
    }

    Ok(states)
}
