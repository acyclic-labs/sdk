//! `policy.ts` is plain data. Each admitted policy lowers to one layer of the
//! filesystem's access policy, and the layers are met: every layer must
//! allow and any denial wins, so no lower policy can widen a higher one.
use super::{Action, Policy, PolicyRule};
use crate::{Error, Result};
use acyclic_fs::access::{
    self, Conditions, Effect, Generations, PrincipalPattern, ResourcePattern,
};
use std::collections::BTreeSet;

/// Decodes an evaluated `policy.ts` default export. Unknown fields are
/// refused; the evaluator has already refused a function export
/// ([`super::Surface::Data`]).
///
/// # Errors
/// [`Error::Invalid`] naming the field or rule that does not decode.
pub fn decode(value: serde_json::Value) -> Result<Policy> {
    serde_json::from_value(value)
        .map_err(|error| Error::Invalid(format!("{}: {error}", super::POLICY)))
}

/// The filesystem actions a policy action covers. `spawn` and `call` are
/// not file actions: they are judged where children and calls are created.
fn file_actions(action: Action) -> &'static [access::Action] {
    match action {
        Action::Read => &[
            access::Action::List,
            access::Action::Read,
            access::Action::ReadHistory,
        ],
        Action::Write => &[
            access::Action::Create,
            access::Action::Write,
            access::Action::Delete,
            access::Action::Rename,
        ],
        Action::Append => &[access::Action::Append],
        Action::Spawn | Action::Call => &[],
    }
}

fn rule(id: String, effect: Effect, actions: BTreeSet<access::Action>, path: &str) -> access::Rule {
    access::Rule {
        id,
        effect,
        principal: PrincipalPattern::default(),
        actions,
        resource: ResourcePattern {
            lineage_root: None,
            volume: None,
            generations: Generations::All,
            path: path.to_owned(),
        },
        conditions: Conditions::default(),
    }
}

/// One policy as one access layer, its rule ids prefixed by `layer`. With
/// no `only`, everything is allowed; with `only`, only its matching file
/// actions are allowed. An empty list allows nothing. `deny` rules deny on top.
fn lower(layer: &str, policy: &Policy) -> Vec<access::Rule> {
    let actions = |rule: &PolicyRule| -> BTreeSet<access::Action> {
        rule.actions
            .iter()
            .flat_map(|action| file_actions(*action))
            .copied()
            .collect()
    };
    let mut rules = Vec::new();
    match &policy.only {
        None => rules.push(rule(
            format!("{layer}/open"),
            Effect::Allow,
            access::Action::ALL.into_iter().collect(),
            "**",
        )),
        Some(only) => {
            for (index, only) in only.iter().enumerate() {
                let actions = actions(only);
                if !actions.is_empty() {
                    rules.push(rule(
                        format!("{layer}/only/{index}"),
                        Effect::Allow,
                        actions,
                        &only.path,
                    ));
                }
            }
        }
    }
    for (index, deny) in policy.deny.iter().flatten().enumerate() {
        let actions = actions(deny);
        if !actions.is_empty() {
            rules.push(rule(
                format!("{layer}/deny/{index}"),
                Effect::Deny,
                actions,
                &deny.path,
            ));
        }
    }
    rules
}

/// The meet of `policies`, highest issuer first, as one access policy at
/// `version`: one layer per policy, in order.
///
/// # Errors
/// [`Error::Invalid`] when a rule's path is not a canonical glob (`*`, `?`
/// and whole `**` components).
pub fn meet<'a>(
    policies: impl IntoIterator<Item = (&'a str, &'a Policy)>,
    version: u64,
) -> Result<access::Policy> {
    let policy = access::Policy {
        schema_version: access::POLICY_SCHEMA_VERSION,
        version,
        layers: policies
            .into_iter()
            .map(|(layer, policy)| lower(layer, policy))
            .collect(),
    };
    policy
        .validate()
        .map_err(|error| Error::Invalid(format!("{}: {error}", super::POLICY)))?;
    Ok(policy)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn decoded(value: serde_json::Value) -> Policy {
        decode(value).unwrap()
    }

    #[test]
    fn file_policy_meet_cannot_widen_an_issuer_or_an_only_list() -> Result<()> {
        let principal = access::Principal {
            kind: access::PrincipalKind::Agent,
            id: "child".into(),
            lineage: vec![],
            groups: Default::default(),
            roles: Default::default(),
            tags: Default::default(),
            attributes: Default::default(),
        };
        let capability = access::Capability::whole_volume(None, &access::Action::ALL);
        let resource = access::Resource {
            config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
            lineage: &[],
            volume: "home",
            generation: 1,
            head: 1,
            path: "",
        };
        let context = access::Context {
            now: 0,
            approvals: 0,
        };
        for (issuer, subject, action, path, allowed) in [
            (json!({}), json!({}), access::Action::Read, "notes", true),
            (
                json!({"deny": [{"path": "plugins.ts", "actions": ["write"]}]}),
                json!({}),
                access::Action::Write,
                "plugins.ts",
                false,
            ),
            (
                json!({"only": [{"path": "src/**", "actions": ["write"]}]}),
                json!({}),
                access::Action::Write,
                "src/lib.rs",
                true,
            ),
            (
                json!({"only": [{"path": "src/**", "actions": ["write"]}]}),
                json!({}),
                access::Action::Write,
                "plugins.ts",
                false,
            ),
            (
                json!({"only": [{"path": "src/**", "actions": ["write"]}]}),
                json!({}),
                access::Action::Read,
                "src/lib.rs",
                false,
            ),
            (
                json!({"only": []}),
                json!({}),
                access::Action::Read,
                "src/lib.rs",
                false,
            ),
            (
                json!({"only": [{"path": "child", "actions": ["spawn"]}]}),
                json!({}),
                access::Action::Read,
                "child",
                false,
            ),
        ] {
            let parent = decoded(issuer);
            let child = decoded(subject);
            let met = meet([("parent", &parent), ("child", &child)], 7)?;
            let view = access::PathTrie::compile(&met, &principal, resource, context, &capability)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(
                view.decide(action, path, 0).allowed,
                allowed,
                "{met:?} {action:?} {path}"
            );
        }
        Ok(())
    }

    #[test]
    fn unknown_fields_and_bad_globs_are_invalid() {
        for value in [
            json!({"preset": "locked"}),
            json!({"deny": [{"path": "a", "actions": ["write"], "why": 1}]}),
            json!({"deny": [{"path": "a", "actions": ["merge"]}]}),
        ] {
            assert!(
                matches!(decode(value.clone()), Err(Error::Invalid(_))),
                "{value}"
            );
        }
        let bad = decoded(json!({"deny": [{"path": "a/**b", "actions": ["read"]}]}));
        assert!(matches!(meet([("home", &bad)], 1), Err(Error::Invalid(_))));
    }
}
