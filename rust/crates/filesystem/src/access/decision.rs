use super::{
    Action, Capability, Context, Effect, Policy, Principal, PrincipalKind, Resource, Rule,
};

/// Reason access was refused, including implicit and capability denials.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Denial {
    /// A matching deny rule.
    Rule,
    /// At least one policy layer has no matching allow.
    NoAllow,
    /// The presented signed capability does not grant this operation.
    Capability,
    /// The presented capability expired.
    Expired,
    /// Malformed policy, grant, path, or generation.
    Invalid,
}

/// Borrowed decision suitable for recording alongside the original request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Decision<'a> {
    /// True exactly when both policy and capability permit the request.
    pub allowed: bool,
    /// Decisive deny rule, or the first allowing rule on success.
    pub rule_id: Option<&'a str>,
    /// Refusal reason, absent on success.
    pub denial: Option<Denial>,
    /// Pinned policy revision for audit attribution.
    pub policy_version: u64,
}

impl<'a> Decision<'a> {
    pub(super) const fn denied(version: u64, denial: Denial) -> Self {
        Self {
            allowed: false,
            rule_id: None,
            denial: Some(denial),
            policy_version: version,
        }
    }
}

pub(super) fn rule_matches(
    rule: &Rule,
    principal: &Principal,
    resource: Resource<'_>,
    context: Context,
) -> bool {
    let selector = &rule.principal;
    predicates_match(
        Selector {
            kind: selector.kind,
            id: selector.id.as_deref(),
            group: selector.group.as_deref(),
            attributes: selector.attributes.iter(),
        },
        principal,
        &rule.resource,
        &rule.conditions,
        resource,
        context,
    )
}

pub(super) enum Claim {
    Group,
    Role,
    Tag,
}

/// Borrowed claims over either decoded maps or the cache's measured vectors.
pub(super) trait Claims {
    fn kind(&self) -> PrincipalKind;
    fn id(&self) -> &str;
    fn lineage(&self) -> &[String];
    fn contains(&self, claim: Claim, value: &str) -> bool;
    fn attribute(&self, key: &str, value: &str) -> bool;
}

impl Claims for Principal {
    fn kind(&self) -> PrincipalKind {
        self.kind
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn lineage(&self) -> &[String] {
        &self.lineage
    }
    fn contains(&self, claim: Claim, value: &str) -> bool {
        match claim {
            Claim::Group => self.groups.contains(value),
            Claim::Role => self.roles.contains(value),
            Claim::Tag => self.tags.contains(value),
        }
    }
    fn attribute(&self, key: &str, value: &str) -> bool {
        self.attributes.get(key).is_some_and(|claim| claim == value)
    }
}

pub(super) struct Selector<'a, I> {
    pub kind: Option<PrincipalKind>,
    pub id: Option<&'a str>,
    pub group: Option<&'a str>,
    pub attributes: I,
}

/// One predicate evaluator for parsed records and shared compiled records.
pub(super) fn predicates_match<'a>(
    mut selector: Selector<'_, impl Iterator<Item = (&'a String, &'a String)>>,
    principal: &impl Claims,
    pattern: &super::ResourcePattern,
    conditions: &super::Conditions,
    resource: Resource<'_>,
    context: Context,
) -> bool {
    let trusted = principal.kind() != PrincipalKind::Anonymous;
    selector.kind.is_none_or(|kind| kind == principal.kind())
        && selector.id.is_none_or(|id| trusted && id == principal.id())
        && selector
            .group
            .is_none_or(|group| trusted && principal.contains(Claim::Group, group))
        && selector
            .attributes
            .all(|(key, value)| trusted && principal.attribute(key, value))
        && pattern.matches_view(resource)
        && conditions
            .expires_at
            .is_none_or(|expiry| context.now < expiry)
        && conditions
            .generations
            .is_none_or(|range| range.contains(resource.generation))
        && conditions.relation.is_none_or(|relation| {
            trusted && relation.matches(principal.lineage(), resource.lineage)
        })
        && conditions
            .role
            .as_ref()
            .is_none_or(|role| trusted && principal.contains(Claim::Role, role))
        && conditions
            .tag
            .as_ref()
            .is_none_or(|tag| trusted && principal.contains(Claim::Tag, tag))
        && conditions
            .approvals
            .is_none_or(|count| context.approvals >= count)
}

/// The policy intersection reducer shared by parsed and compiled rule records.
pub(super) fn policy_layers<R, K: Copy>(
    layers: impl IntoIterator<Item = impl IntoIterator<Item = R>>,
    mut matching: impl FnMut(R) -> Option<(Effect, K)>,
) -> (Option<K>, Option<Denial>) {
    let mut first = None;
    let mut all_layers = true;
    let mut count = 0;
    for layer in layers {
        count += 1;
        let mut allowed = false;
        for rule in layer {
            let Some((effect, id)) = matching(rule) else {
                continue;
            };
            if effect == Effect::Deny {
                return (Some(id), Some(Denial::Rule));
            }
            allowed = true;
            first.get_or_insert(id);
        }
        all_layers &= allowed;
    }
    if count != 0 && all_layers {
        (first, None)
    } else {
        (None, Some(Denial::NoAllow))
    }
}

// Both the reference evaluator and compiled terminal states feed the same
// policy intersection reducer, then intersect capability and history authority.
pub(super) fn evaluate<'a>(
    policy: &'a Policy,
    capability: &Capability,
    context: Context,
    action: Action,
    historical: bool,
    mut rule_matches: impl FnMut(&Rule) -> bool,
    mut grant_matches: impl FnMut(&super::Grant) -> bool,
) -> Decision<'a> {
    let (rule_id, denial) = policy_layers(&policy.layers, |rule| {
        (rule.actions.contains(&action) && rule_matches(rule))
            .then_some((rule.effect, rule.id.as_str()))
    });
    let mut result = Decision {
        allowed: false,
        rule_id,
        denial,
        policy_version: policy.version,
    };
    if result.denial.is_some() {
        return result;
    }
    result.denial = if capability
        .expires_at
        .is_some_and(|expiry| context.now >= expiry)
    {
        Some(Denial::Expired)
    } else if capability.layers.is_empty()
        || !capability.layers.iter().all(|layer| {
            layer
                .iter()
                .any(|grant| grant.actions.contains(&action) && grant_matches(grant))
        })
    {
        Some(Denial::Capability)
    } else {
        None
    };
    result.allowed = result.denial.is_none();
    if result.allowed && historical && matches!(action, Action::Read | Action::List) {
        let history = evaluate(
            policy,
            capability,
            context,
            Action::ReadHistory,
            false,
            rule_matches,
            grant_matches,
        );
        if !history.allowed {
            return history;
        }
    }
    result
}

/// The shared reference decision. Callers must authenticate the principal,
/// verify the signed capability, and supply trusted time, head and approvals.
/// Historical reads and listings additionally require `ReadHistory` permission.
#[must_use]
pub fn decide<'a>(
    policy: &'a Policy,
    principal: &Principal,
    action: Action,
    resource: Resource<'_>,
    context: Context,
    capability: &Capability,
) -> Decision<'a> {
    if policy.validate().is_err()
        || capability.validate().is_err()
        || resource.config.validate().is_err()
        || resource.generation > resource.head
    {
        return Decision::denied(policy.version, Denial::Invalid);
    }
    let Ok(path) = super::names::key(resource.path, resource.config) else {
        return Decision::denied(policy.version, Denial::Invalid);
    };
    evaluate(
        policy,
        capability,
        context,
        action,
        resource.generation != resource.head,
        |rule| {
            rule_matches(rule, principal, resource, context)
                && super::glob::matches(
                    &super::names::fold(&rule.resource.path, resource.config),
                    &path,
                    super::names::folds_unicode(resource.config),
                )
        },
        |grant| {
            grant.resource.matches_view(resource)
                && super::glob::matches(
                    &super::names::fold(&grant.resource.path, resource.config),
                    &path,
                    super::names::folds_unicode(resource.config),
                )
        },
    )
}
