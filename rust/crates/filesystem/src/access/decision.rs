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
    let conditions = &rule.conditions;
    let trusted = principal.kind != PrincipalKind::Anonymous;
    selector.kind.is_none_or(|kind| kind == principal.kind)
        && selector
            .id
            .as_deref()
            .is_none_or(|id| trusted && id == principal.id)
        && selector
            .group
            .as_ref()
            .is_none_or(|group| trusted && principal.groups.contains(group))
        && selector
            .attributes
            .iter()
            .all(|(key, value)| trusted && principal.attributes.get(key) == Some(value))
        && rule.resource.matches_view(resource)
        && conditions
            .expires_at
            .is_none_or(|expiry| context.now < expiry)
        && conditions
            .generations
            .is_none_or(|range| range.contains(resource.generation))
        && conditions.relation.is_none_or(|relation| {
            trusted && relation.matches(&principal.lineage, resource.lineage)
        })
        && conditions
            .role
            .as_ref()
            .is_none_or(|role| trusted && principal.roles.contains(role))
        && conditions
            .tag
            .as_ref()
            .is_none_or(|tag| trusted && principal.tags.contains(tag))
        && conditions
            .approvals
            .is_none_or(|count| context.approvals >= count)
}

// This reducer is the single policy/capability decision algorithm. Both the
// reference evaluator and compiled terminal states feed it match predicates.
pub(super) fn evaluate<'a>(
    policy: &'a Policy,
    capability: &Capability,
    context: Context,
    action: Action,
    historical: bool,
    mut rule_matches: impl FnMut(&Rule) -> bool,
    mut grant_matches: impl FnMut(&super::Grant) -> bool,
) -> Decision<'a> {
    let mut result = Decision::denied(policy.version, Denial::NoAllow);
    let mut all_layers = !policy.layers.is_empty();
    for layer in &policy.layers {
        let mut allowed = false;
        for rule in layer {
            if !rule.actions.contains(&action) || !rule_matches(rule) {
                continue;
            }
            if rule.effect == Effect::Deny {
                return Decision {
                    rule_id: Some(&rule.id),
                    denial: Some(Denial::Rule),
                    ..result
                };
            }
            allowed = true;
            if result.rule_id.is_none() {
                result.rule_id = Some(&rule.id);
            }
        }
        all_layers &= allowed;
    }
    if !all_layers {
        result.rule_id = None;
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
