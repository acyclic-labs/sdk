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
        // A file append is create-only. Mutable append-only data belongs in
        // Streams; granting Append on a file would permit changing its bytes.
        Action::Append => &[access::Action::Create],
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
    let mut layers: Vec<_> = policies
        .into_iter()
        .map(|(layer, policy)| lower(layer, policy))
        .collect();
    if layers.is_empty() {
        layers.push(lower("open", &Policy::default()));
    }
    let policy = access::Policy {
        schema_version: access::POLICY_SCHEMA_VERSION,
        version,
        layers,
    };
    policy
        .validate()
        .map_err(|error| Error::Invalid(format!("{}: {error}", super::POLICY)))?;
    Ok(policy)
}

/// Original access programs for issuer policy and the subject's own advisory
/// policy. Only the host's authenticated committer may classify a policy as
/// advisory; a path name or a decoded configuration cannot establish that fact.
/// Both programs intersect the same original principal and static ceiling.
pub struct Permissions {
    issuer: access::PathTrie,
    advisory: Option<access::PathTrie>,
}

impl Permissions {
    /// Compiles admitted issuer layers and, separately, a policy committed by
    /// the subject. An issuer-committed home policy belongs in `issuer`.
    ///
    /// # Errors
    /// [`Error::Invalid`] for invalid rules or access bindings.
    pub fn compile(
        issuer: &access::Policy,
        advisory: Option<&Policy>,
        principal: &access::Principal,
        resource: access::Resource<'_>,
        context: access::Context,
        capability: &access::Capability,
    ) -> Result<Self> {
        let compile = |policy: &access::Policy| {
            access::PathTrie::compile(policy, principal, resource, context, capability)
                .map_err(|error| Error::Invalid(format!("{}: {error}", super::POLICY)))
        };
        Ok(Self {
            issuer: compile(issuer)?,
            advisory: advisory
                .map(|policy| meet([("advisory", policy)], issuer.version))
                .transpose()?
                .as_ref()
                .map(compile)
                .transpose()?,
        })
    }

    /// Checks current issuer policy and the static ceiling first. Own denies
    /// never prevent removing a self-imposed policy or plugin lock. Recovery
    /// grants no read, admin or issuer-policy bypass.
    #[must_use]
    pub fn allows(&self, action: access::Action, path: &str, now: u64) -> bool {
        Self::decide(
            action,
            now,
            matches!(path, "policy.ts" | "plugins.ts"),
            self.issuer.prepare(path),
            self.advisory.as_ref().map(|policy| policy.prepare(path)),
        )
    }

    /// Checks a row produced by the original filesystem generation frontier.
    /// The caller cannot describe an overwrite as a create: the sealed row
    /// supplies its original before/after bindings. Names remain borrowed in
    /// their native encoding, using the shared access compiler's path decoder.
    #[must_use]
    pub fn allows_change(&self, row: &acyclic_fs::ChangedPathEntry, now: u64) -> bool {
        let change = row.value();
        let action = match (&change.before, &change.after) {
            (None, Some(_)) => access::Action::Create,
            (Some(_), None) => access::Action::Delete,
            (Some(_), Some(_)) => access::Action::Write,
            (None, None) => return false,
        };
        let recovery = match change.path.components() {
            [name] => [b"policy.ts".as_slice(), b"plugins.ts".as_slice()]
                .into_iter()
                .any(|path| match name.encoding() {
                    acyclic_fs::kernel::NameEncoding::Utf8
                    | acyclic_fs::kernel::NameEncoding::PosixBytes => name.as_bytes() == path,
                    acyclic_fs::kernel::NameEncoding::WindowsUtf16Le => name
                        .as_bytes()
                        .chunks_exact(2)
                        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                        .eq(path.iter().copied().map(u16::from)),
                }),
            _ => false,
        };
        Self::decide(
            action,
            now,
            recovery,
            self.issuer.prepare_namespace(&change.path),
            self.advisory
                .as_ref()
                .map(|policy| policy.prepare_namespace(&change.path)),
        )
    }

    fn decide(
        action: access::Action,
        now: u64,
        recovery_path: bool,
        issuer: std::result::Result<access::PreparedPath<'_>, access::InvalidPolicy>,
        advisory: Option<std::result::Result<access::PreparedPath<'_>, access::InvalidPolicy>>,
    ) -> bool {
        let recovery = recovery_path
            && matches!(
                action,
                access::Action::Create
                    | access::Action::Write
                    | access::Action::Delete
                    | access::Action::Rename
            );
        issuer.is_ok_and(|path| path.decide(action, now).allowed)
            && (recovery
                || advisory
                    .is_none_or(|path| path.is_ok_and(|path| path.decide(action, now).allowed)))
    }
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
        let empty = decoded(json!({}));
        let locked = decoded(json!({"only": []}));
        let issuer_lock = decoded(json!({
            "deny": [{"path": "policy.ts", "actions": ["write"]}]
        }));
        for (issuer, advisory, action, path, allowed) in [
            (&empty, &locked, access::Action::Write, "policy.ts", true),
            (&empty, &locked, access::Action::Delete, "plugins.ts", true),
            (
                &empty,
                &locked,
                access::Action::Write,
                "plugins/copy.ts",
                false,
            ),
            (&empty, &locked, access::Action::Read, "policy.ts", false),
            (
                &issuer_lock,
                &empty,
                access::Action::Write,
                "policy.ts",
                false,
            ),
            (
                &issuer_lock,
                &locked,
                access::Action::Write,
                "policy.ts",
                false,
            ),
        ] {
            let issuer = meet([("parent", issuer)], 7)?;
            let compiled = Permissions::compile(
                &issuer,
                Some(advisory),
                &principal,
                resource,
                context,
                &capability,
            )?;
            assert_eq!(
                compiled.allows(action, path, 0),
                allowed,
                "{action:?} {path}"
            );
        }
        // Recovery cannot exceed the original static grant ceiling.
        let read_only = access::Capability::whole_volume(None, &[access::Action::Read]);
        let compiled = Permissions::compile(
            &meet([], 7)?,
            Some(&locked),
            &principal,
            resource,
            context,
            &read_only,
        )?;
        assert!(!compiled.allows(access::Action::Write, "policy.ts", 0));

        // File append is an absent-path create, never permission to modify an
        // existing file or append mutable bytes in place.
        let create_only = decoded(json!({
            "only": [{"path": "out/**", "actions": ["append"]}]
        }));
        let compiled = Permissions::compile(
            &meet([("parent", &create_only)], 7)?,
            None,
            &principal,
            resource,
            context,
            &capability,
        )?;
        assert!(compiled.allows(access::Action::Create, "out/new", 0));
        assert!(!compiled.allows(access::Action::Write, "out/new", 0));
        assert!(!compiled.allows(access::Action::Append, "out/new", 0));
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
