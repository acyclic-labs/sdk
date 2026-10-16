//! Default ownership policy, compiled through the common composed-view path.
use super::*;
use acyclic_fs::access::*;

pub(super) fn owner(root: &Path, volume: &VolumeRef) -> Result<crate::workspace::MountTable<()>> {
    let principal = Principal {
        kind: PrincipalKind::Agent,
        id: "owner".into(),
        lineage: vec!["owner".into()],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    };
    let resource = ResourcePattern {
        lineage_root: None,
        volume: Some(volume.id().into()),
        generations: Generations::All,
        path: "**".into(),
    };
    let actions = Action::ALL.into_iter().collect();
    let policy = Arc::new(Policy {
        schema_version: POLICY_SCHEMA_VERSION,
        version: 1,
        layers: vec![vec![Rule {
            id: "owner".into(),
            effect: Effect::Allow,
            principal: PrincipalPattern::default(),
            actions,
            resource: resource.clone(),
            conditions: Conditions {
                relation: Some(Relation::SelfTask),
                ..Conditions::default()
            },
        }]],
    });
    let capability = Capability {
        layers: vec![vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource,
        }]],
        expires_at: None,
    };
    crate::workspace::compose(
        root.into(),
        &principal,
        &capability,
        Context {
            now: 0,
            approvals: 0,
        },
        crate::workspace::VolumeView {
            name: "project".into(),
            agent: principal.id.clone(),
            volume: volume.clone(),
            lineage: principal.lineage.clone(),
            generation: 0,
            head: 0,
            config: acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable),
            policy,
            source: (),
        },
        [],
    )
}
