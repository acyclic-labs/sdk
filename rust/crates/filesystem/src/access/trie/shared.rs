use super::*;
use crate::access::{Conditions, Effect, Generations, PrincipalKind, ResourcePattern, Rule};
use std::cell::RefCell;
use std::sync::Arc;

#[cfg(test)]
mod tests;

// No principal or credential is retained here. The flattened records use vectors,
// rather than allocator-private BTree nodes, so retained capacity is measurable.
struct ProgramRule {
    id: String,
    effect: Effect,
    actions: u16,
    kind: Option<PrincipalKind>,
    subject: Option<String>,
    group: Option<String>,
    attributes: Vec<(String, String)>,
    resource: ResourcePattern,
    conditions: Conditions,
}

impl ProgramRule {
    fn new(rule: Rule) -> Self {
        Self {
            id: rule.id,
            effect: rule.effect,
            actions: rule
                .actions
                .into_iter()
                .fold(0, |mask, action| mask | 1 << action as u16),
            kind: rule.principal.kind,
            subject: rule.principal.id,
            group: rule.principal.group,
            attributes: rule.principal.attributes.into_iter().collect(),
            // The path is represented by the shared DFA, not retained a second time.
            resource: ResourcePattern {
                path: String::new(),
                ..rule.resource
            },
            conditions: rule.conditions,
        }
    }

    fn matches(&self, principal: &Caller, resource: Resource<'_>, context: Context) -> bool {
        super::super::decision::predicates_match(
            super::super::decision::Selector {
                kind: self.kind,
                id: self.subject.as_deref(),
                group: self.group.as_deref(),
                attributes: self.attributes.iter().map(|(key, value)| (key, value)),
            },
            principal,
            &self.resource,
            &self.conditions,
            resource,
            context,
        )
    }
    fn owned_bytes(&self) -> usize {
        self.id.capacity()
            + self.subject.as_ref().map_or(0, String::capacity)
            + self.group.as_ref().map_or(0, String::capacity)
            + self.attributes.capacity() * size_of::<(String, String)>()
            + self
                .attributes
                .iter()
                .map(|(key, value)| key.capacity() + value.capacity())
                .sum::<usize>()
            + self.resource.volume.as_ref().map_or(0, String::capacity)
            + self
                .resource
                .lineage_root
                .as_ref()
                .map_or(0, String::capacity)
            + self.conditions.role.as_ref().map_or(0, String::capacity)
            + self.conditions.tag.as_ref().map_or(0, String::capacity)
    }
}

/// Principal-independent policy paths and rule programs. Filesystem handles share
/// this object by authenticated volume and metadata revision. Binding a caller
/// never copies its paths, transition tables, rule records, or rule identifiers.
pub struct CompiledPolicy {
    pub(super) alphabet: Alphabet,
    pub(super) states: Vec<State>,
    matches: Vec<Vec<usize>>,
    rules: Vec<ProgramRule>,
    layers: Vec<std::ops::Range<usize>>,
    version: u64,
    config: crate::VolumeConfig,
    needs_ordinal: bool,
    generation_sensitive: bool,
    authenticated_facts: bool,
    metadata_stream: bool,
    coarse: bool,
    constant: bool,
    expiries: Vec<u64>,
    pub(crate) cache_charge: Option<crate::facade::policy_cache::CacheCharge>,
}

impl CompiledPolicy {
    /// Compile and admit all policy paths once, independently of the principal.
    pub fn compile_with_work(
        policy: &Policy,
        config: crate::VolumeConfig,
        remaining: &mut usize,
        mut reserve: impl FnMut(usize) -> Result<(), InvalidPolicy>,
    ) -> Result<Self, InvalidPolicy> {
        let mut available = (*remaining).min(MAXIMUM_WORK);
        let before = available;
        let result = (|| {
            let empty = Capability {
                layers: vec![],
                expires_at: None,
            };
            let (bytes, validation) = super::super::compilation::preparation(
                policy,
                &empty,
                super::super::names::folds_unicode(config),
            )?;
            spend(&mut available, validation)?;
            reserve(bytes)?;
            policy.validate()?;
            config
                .validate()
                .map_err(|_| InvalidPolicy("invalid volume configuration"))?;
            let paths = compile_paths(policy, config, &mut available, &mut reserve)?;
            Self::assemble(policy, config, paths)
        })();
        *remaining -= before - available;
        result
    }

    fn assemble(
        policy: &Policy,
        config: crate::VolumeConfig,
        paths: Paths,
    ) -> Result<Self, InvalidPolicy> {
        let Paths {
            symbols,
            mut states,
            matches,
            rules: count,
        } = paths;
        let needs_ordinal = policy.layers.iter().flatten().any(|rule| {
            matches!(rule.resource.generations, Generations::Range(_))
                || rule.conditions.generations.is_some()
        });
        let generation_sensitive = policy.layers.iter().flatten().any(|rule| {
            rule.resource.generations != Generations::All || rule.conditions.generations.is_some()
        });
        let authenticated_facts = policy.layers.iter().flatten().all(|rule| {
            rule.principal.group.is_none()
                && rule.principal.attributes.is_empty()
                && rule.conditions.role.is_none()
                && rule.conditions.tag.is_none()
                && rule.conditions.relation.is_none()
                && rule.conditions.approvals.is_none()
                && rule.resource.lineage_root.is_none()
        });
        let coarse = policy.layers.iter().flatten().all(|rule| {
            rule.principal == super::super::PrincipalPattern::default()
                && rule.resource.lineage_root.is_none()
                && rule.resource.generations == Generations::All
                && rule.conditions.generations.is_none()
                && rule.conditions.relation.is_none()
                && rule.conditions.role.is_none()
                && rule.conditions.tag.is_none()
                && rule.conditions.approvals.is_none()
        });
        let metadata_stream = policy.layers.iter().flatten().all(|rule| {
            rule.resource.lineage_root.is_none()
                && rule.resource.generations == Generations::All
                && rule.conditions.generations.is_none()
                && rule.conditions.relation.is_none()
                && rule.conditions.tag.is_none()
                && rule.conditions.approvals.is_none()
        });
        let mut rules = Vec::with_capacity(count);
        let mut layers = Vec::with_capacity(policy.layers.len());
        for layer in &policy.layers {
            let start = rules.len();
            rules.extend(layer.iter().cloned().map(ProgramRule::new));
            layers.push(start..rules.len());
        }
        let constant = rules.iter().all(|rule| {
            rule.kind.is_none()
                && rule.subject.is_none()
                && rule.group.is_none()
                && rule.attributes.is_empty()
                && rule.resource.volume.is_none()
                && rule.resource.lineage_root.is_none()
                && rule.resource.generations == Generations::All
                && rule.conditions == Conditions::default()
        });
        if constant {
            for (state, matching) in states.iter_mut().zip(&matches) {
                state.decisions =
                    Action::ALL.map(|action| program(&rules, &layers, matching, action, |_| true));
            }
        }
        let mut expiries: Vec<_> = rules
            .iter()
            .filter_map(|rule| rule.conditions.expires_at)
            .collect();
        expiries.sort_unstable();
        expiries.dedup();
        Ok(Self {
            alphabet: Alphabet::new(&symbols)?,
            states,
            matches,
            rules,
            layers,
            version: policy.version,
            config,
            needs_ordinal,
            generation_sensitive,
            authenticated_facts,
            metadata_stream,
            coarse,
            constant,
            expiries,
            cache_charge: None,
        })
    }

    /// Whether this policy needs authenticated publication ordinals.
    pub const fn needs_ordinal(&self) -> bool {
        self.needs_ordinal
    }
    /// Whether all facts used by this policy are supplied by signed identity and
    /// generation alone; other facts require an embedding's authenticated source.
    pub const fn uses_only_authenticated_identity(&self) -> bool {
        self.authenticated_facts
    }
    pub(crate) const fn permits_coarse_binding(&self) -> bool {
        self.coarse
    }
    pub(crate) const fn permits_metadata_stream(&self) -> bool {
        self.metadata_stream
    }

    /// Exact retained vector/string capacities, including the shared Arc header.
    pub fn logical_owned_bytes(&self) -> u64 {
        (size_of::<Self>()
            + 2 * size_of::<usize>()
            + self.alphabet.pages.capacity() * size_of::<[u16; 256]>()
            + self
                .alphabet
                .page_ids
                .as_ref()
                .map_or(0, |_| size_of::<[u16; 4352]>())
            + self.alphabet.symbols.capacity() * size_of::<char>()
            + self.states.capacity() * size_of::<State>()
            + self
                .states
                .iter()
                .map(|state| state.transitions.capacity() * size_of::<usize>())
                .sum::<usize>()
            + self.matches.capacity() * size_of::<Vec<usize>>()
            + self
                .matches
                .iter()
                .map(|matches| matches.capacity() * size_of::<usize>())
                .sum::<usize>()
            + self.rules.capacity() * size_of::<ProgramRule>()
            + self
                .rules
                .iter()
                .map(ProgramRule::owned_bytes)
                .sum::<usize>()
            + self.layers.capacity() * size_of::<std::ops::Range<usize>>()
            + self.expiries.capacity() * size_of::<u64>()) as u64
    }

    pub(super) fn step(&self, state: usize, ch: char) -> Result<usize, InvalidPolicy> {
        self.alphabet
            .symbol(ch)
            .and_then(|symbol| self.states.get(state)?.transitions.get(symbol).copied())
            .ok_or(InvalidPolicy("invalid shared policy path"))
    }

    /// Bind only caller facts and the capability ceiling. Policy storage remains
    /// owned and charged by the filesystem cache, independently of this handle.
    pub fn bind_with_work(
        self: &Arc<Self>,
        principal: &Principal,
        resource: Resource<'_>,
        context: Context,
        capability: &Capability,
        remaining: &mut usize,
        mut reserve: impl FnMut(usize) -> Result<(), InvalidPolicy>,
    ) -> Result<PathTrie, InvalidPolicy> {
        if self.config != resource.config || resource.generation > resource.head {
            return Err(InvalidPolicy("policy resource differs"));
        }
        reserve(Binding::admission(principal, resource.lineage))?;
        let mut view = self.ceiling(
            principal,
            resource,
            context,
            capability,
            remaining,
            &mut reserve,
        )?;
        let reachable = self.reachable_pairs(&view, remaining, &mut reserve)?;
        let binding = Binding {
            policy: Arc::clone(self),
            owns_program: false,
            reachable,
            principal: Caller::new(principal),
            lineage: resource.lineage.to_vec(),
            generation: resource.generation,
            head: resource.head,
            context,
        };
        view.version = self.version;
        view.valid_until = self
            .expiries
            .get(
                self.expiries
                    .partition_point(|expiry| *expiry <= context.now),
            )
            .copied()
            .into_iter()
            .chain(view.valid_until)
            .min();
        spend(
            remaining,
            self.expiries.len().checked_ilog2().unwrap_or(0) as usize + 1,
        )?;
        view.shared = Some(Box::new(binding));
        let evaluation_work = if self.constant {
            1
        } else {
            self.rules.len().saturating_add(self.layers.len())
        };
        spend(
            remaining,
            view.state_count()
                .saturating_mul(evaluation_work)
                .saturating_mul(36),
        )?;
        view.generation_sensitive = self.generation_sensitive
            || capability
                .layers
                .iter()
                .flatten()
                .any(|grant| grant.resource.generations != Generations::All)
            || view.state_ids().any(|state| {
                (view.decision(state, Action::Read, context.now).allowed
                    || view.decision(state, Action::List, context.now).allowed)
                    && !view
                        .decision(state, Action::ReadHistory, context.now)
                        .allowed
            });
        Ok(view)
    }

    // Keep product-state identifiers so path steps need no lookup or allocation.
    // Only reachable pairs count towards the DFA bound. The common universal
    // ceiling has one state and can use the shared program's dense identifiers.
    fn reachable_pairs(
        &self,
        view: &PathTrie,
        remaining: &mut usize,
        reserve: &mut impl FnMut(usize) -> Result<(), InvalidPolicy>,
    ) -> Result<Option<Vec<usize>>, InvalidPolicy> {
        if view.states.len() == 1 {
            return Ok(None);
        }
        let maximum = self
            .states
            .len()
            .saturating_mul(view.states.len())
            .min(MAXIMUM_STATES);
        reserve(2 * maximum * size_of::<usize>())?;
        let mut reachable = Vec::with_capacity(maximum);
        let mut pending = Vec::with_capacity(maximum);
        reachable.push(0);
        pending.push(0);
        let symbols = || {
            self.alphabet
                .symbols
                .iter()
                .chain(&view.alphabet.symbols)
                .copied()
        };
        let mut other = None;
        for ch in (1..=0x10ffff).filter_map(char::from_u32) {
            spend(
                remaining,
                self.alphabet.symbols.len() + view.alphabet.symbols.len() + 1,
            )?;
            if !symbols().any(|symbol| symbol == ch) {
                other = Some(ch);
                break;
            }
        }
        let other = other.ok_or(InvalidPolicy("combined policy has no other symbol"))?;
        let mut cursor = 0;
        while let Some(&state) = pending.get(cursor) {
            cursor += 1;
            for ch in symbols().chain(std::iter::once(other)) {
                spend(
                    remaining,
                    reachable.len().checked_ilog2().unwrap_or(0) as usize + 3,
                )?;
                let next = self.step(state / view.states.len(), ch)? * view.states.len()
                    + view.ceiling_step(state % view.states.len(), ch)?;
                if let Err(index) = reachable.binary_search(&next) {
                    if reachable.len() == MAXIMUM_STATES {
                        return Err(InvalidPolicy("combined policy exceeds DFA state limit"));
                    }
                    spend(remaining, reachable.len() - index + 1)?;
                    reachable.insert(index, next);
                    pending.push(next);
                }
            }
        }
        Ok(Some(reachable))
    }
}

pub(super) struct Binding {
    pub(super) policy: Arc<CompiledPolicy>,
    pub(super) owns_program: bool,
    pub(super) reachable: Option<Vec<usize>>,
    principal: Caller,
    lineage: Vec<String>,
    generation: u64,
    head: u64,
    context: Context,
}

impl Binding {
    fn admission(principal: &Principal, lineage: &[String]) -> usize {
        size_of::<Self>()
            + Caller::admission(principal)
            + size_of_val(lineage)
            + lineage.iter().map(String::len).sum::<usize>()
    }

    pub(super) fn logical_owned_bytes(&self) -> u64 {
        // Cached programs carry their own charge. A standalone public compile
        // instead transfers its program storage to this view's caller.
        (size_of::<Self>()
            + self
                .reachable
                .as_ref()
                .map_or(0, |states| states.capacity() * size_of::<usize>())
            + self.principal.id.capacity()
            + self.principal.lineage.capacity() * size_of::<String>()
            + self
                .principal
                .lineage
                .iter()
                .map(String::capacity)
                .sum::<usize>()
            + self.lineage.capacity() * size_of::<String>()
            + self.lineage.iter().map(String::capacity).sum::<usize>()
            + self.principal.groups.capacity() * size_of::<String>()
            + self.principal.roles.capacity() * size_of::<String>()
            + self.principal.tags.capacity() * size_of::<String>()
            + self
                .principal
                .groups
                .iter()
                .chain(&self.principal.roles)
                .chain(&self.principal.tags)
                .map(String::capacity)
                .sum::<usize>()
            + self.principal.attributes.capacity() * size_of::<(String, String)>()
            + self
                .principal
                .attributes
                .iter()
                .map(|(key, value)| key.capacity() + value.capacity())
                .sum::<usize>()) as u64
            + if self.owns_program {
                self.policy.logical_owned_bytes()
            } else {
                0
            }
    }

    pub(super) fn decision<'a>(
        &'a self,
        view: &'a PathTrie,
        state: usize,
        action: Action,
        now: u64,
    ) -> Decision<'a> {
        let Some(matches) = self.policy.matches.get(state / view.states.len()) else {
            return Decision::denied(view.version, Denial::Invalid);
        };
        let resource = Resource {
            config: view.config,
            volume: &view.volume,
            lineage: &self.lineage,
            generation: self.generation,
            head: self.head,
            path: "",
        };
        let outcome = if self.policy.constant {
            let Some(outcome) = self
                .policy
                .states
                .get(state / view.states.len())
                .and_then(|state| state.decisions.get(action as usize))
            else {
                return Decision::denied(view.version, Denial::Invalid);
            };
            *outcome
        } else {
            program(
                &self.policy.rules,
                &self.policy.layers,
                matches,
                action,
                |rule| rule.matches(&self.principal, resource, self.context),
            )
        };
        let mut result = Decision {
            allowed: outcome.denial.is_none(),
            denial: outcome.denial,
            rule_id: outcome
                .rule
                .and_then(|index| self.policy.rules.get(index).map(|rule| rule.id.as_str())),
            policy_version: view.version,
        };
        if !result.allowed {
            return result;
        }
        let ceiling = view.ceiling_decision(state % view.states.len(), action, now);
        result.allowed = ceiling.allowed;
        result.denial = ceiling.denial;
        if result.allowed
            && self.generation != self.head
            && matches!(action, Action::Read | Action::List)
        {
            let historical = self.decision(view, state, Action::ReadHistory, now);
            if !historical.allowed {
                return historical;
            }
        }
        result
    }
}

// Caller facts are vectors as well: no retained allocator-private tree nodes.
struct Caller {
    kind: PrincipalKind,
    id: String,
    lineage: Vec<String>,
    groups: Vec<String>,
    roles: Vec<String>,
    tags: Vec<String>,
    attributes: Vec<(String, String)>,
}
impl super::super::decision::Claims for Caller {
    fn kind(&self) -> PrincipalKind {
        self.kind
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn lineage(&self) -> &[String] {
        &self.lineage
    }
    fn contains(&self, claim: super::super::decision::Claim, value: &str) -> bool {
        use super::super::decision::Claim;
        let values = match claim {
            Claim::Group => &self.groups,
            Claim::Role => &self.roles,
            Claim::Tag => &self.tags,
        };
        values.iter().any(|claim| claim == value)
    }
    fn attribute(&self, key: &str, value: &str) -> bool {
        self.attributes
            .iter()
            .any(|(candidate, claim)| candidate == key && claim == value)
    }
}
impl Caller {
    fn new(principal: &Principal) -> Self {
        Self {
            kind: principal.kind,
            id: principal.id.clone(),
            lineage: principal.lineage.clone(),
            groups: principal.groups.iter().cloned().collect(),
            roles: principal.roles.iter().cloned().collect(),
            tags: principal.tags.iter().cloned().collect(),
            attributes: principal
                .attributes
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        }
    }
    fn admission(principal: &Principal) -> usize {
        principal.id.len()
            + (principal.lineage.len()
                + principal.groups.len()
                + principal.roles.len()
                + principal.tags.len())
                * size_of::<String>()
            + principal
                .lineage
                .iter()
                .chain(&principal.groups)
                .chain(&principal.roles)
                .chain(&principal.tags)
                .map(String::len)
                .sum::<usize>()
            + principal.attributes.len() * size_of::<(String, String)>()
            + principal
                .attributes
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>()
    }
}

fn program(
    rules: &[ProgramRule],
    layers: &[std::ops::Range<usize>],
    matching: &[usize],
    action: Action,
    mut matches: impl FnMut(&ProgramRule) -> bool,
) -> Outcome {
    let (rule, denial) = super::super::decision::policy_layers(
        layers
            .iter()
            .map(|layer| matching.iter().filter(|index| layer.contains(index))),
        |&index| {
            rules.get(index).and_then(|rule| {
                (rule.actions & (1 << action as u16) != 0 && matches(rule))
                    .then_some((rule.effect, index))
            })
        },
    );
    Outcome { rule, denial }
}

struct Paths {
    symbols: Vec<char>,
    states: Vec<State>,
    matches: Vec<Vec<usize>>,
    rules: usize,
}
fn compile_paths(
    policy: &Policy,
    config: crate::VolumeConfig,
    available: &mut usize,
    reserve: &mut impl FnMut(usize) -> Result<(), InvalidPolicy>,
) -> Result<Paths, InvalidPolicy> {
    let mut nfa = Nfa::default();
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    for rule in policy.layers.iter().flatten() {
        let path = super::super::names::fold(&rule.resource.path, config);
        let (start, end) = nfa.pattern(&path, super::super::names::folds_unicode(config))?;
        starts.push(start);
        ends.push(end);
    }
    let (symbols, other) = alphabet(&nfa)?;
    let uniform = policy
        .layers
        .iter()
        .flatten()
        .all(|rule| rule.resource.path == "**");
    reserve(super::super::compilation::determinization(
        nfa.nodes.len(),
        symbols.len(),
        size_of::<State>() + size_of::<Vec<usize>>() + ends.len() * size_of::<usize>(),
        if uniform { 1 } else { MAXIMUM_STATES },
    )?)?;
    let matches = RefCell::new(Vec::<Vec<usize>>::new());
    let outcomes = |subset: &[usize]| {
        matches.borrow_mut().push(
            ends.iter()
                .enumerate()
                .filter_map(|(index, end)| subset.binary_search(end).is_ok().then_some(index))
                .collect(),
        );
        [Outcome {
            rule: None,
            denial: Some(Denial::NoAllow),
        }; 12]
    };
    let states = if uniform {
        spend(available, nfa.nodes.len() + ends.len() * 24)?;
        vec![terminal(
            vec![0; symbols.len() + 1],
            outcomes(&(0..nfa.nodes.len()).collect::<Vec<_>>()),
        )]
    } else {
        determinize(
            &nfa,
            starts,
            &symbols,
            other,
            outcomes,
            ends.len(),
            available,
        )?
    };
    Ok(Paths {
        symbols,
        states,
        matches: matches.into_inner(),
        rules: ends.len(),
    })
}

impl CompiledPolicy {
    fn ceiling(
        &self,
        principal: &Principal,
        resource: Resource<'_>,
        context: Context,
        capability: &Capability,
        remaining: &mut usize,
        reserve: &mut impl FnMut(usize) -> Result<(), InvalidPolicy>,
    ) -> Result<PathTrie, InvalidPolicy> {
        if capability
            .layers
            .iter()
            .flatten()
            .all(|grant| grant.resource.path == "**")
        {
            capability.validate()?;
            let work =
                capability.layers.len() + capability.layers.iter().map(Vec::len).sum::<usize>();
            spend(remaining, work * 24 + 1)?;
            reserve(size_of::<State>() + resource.volume.len() + principal.id.len())?;
            let decisions = Action::ALL.map(|action| {
                let denial = if capability
                    .expires_at
                    .is_some_and(|expiry| context.now >= expiry)
                {
                    Some(Denial::Expired)
                } else if capability.layers.is_empty()
                    || !capability.layers.iter().all(|layer| {
                        layer.iter().any(|grant| {
                            grant.actions.contains(&action) && grant.resource.matches_view(resource)
                        })
                    })
                {
                    Some(Denial::Capability)
                } else {
                    None
                };
                Outcome { rule: None, denial }
            });
            Ok(PathTrie {
                shared: None,
                alphabet: Alphabet {
                    pages: vec![],
                    page_ids: None,
                    symbols: vec![],
                },
                states: vec![terminal(vec![], decisions)],
                rules: vec![],
                version: self.version,
                head: resource.head,
                valid_from: context.now,
                valid_until: capability.expires_at.filter(|expiry| *expiry > context.now),
                config: resource.config,
                volume: resource.volume.to_owned(),
                subject: (principal.kind, principal.id.clone()),
                capability_binding: super::super::capability::binding(capability)?,
                policy_revision: None,
                data_generation: None,
                generation_sensitive: false,
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: None,
            })
        } else {
            PathTrie::compile_ceiling(principal, resource, context, capability, remaining, reserve)
        }
    }
}
