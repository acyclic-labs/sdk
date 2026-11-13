//! Default ownership policy, compiled through the common composed-view path.
use super::*;
use crate::agent::WorkspaceMode;
use acyclic_fs::access::*;

/// Retain the admitted binding mode when moving a composed native root.
pub(super) fn mode<T>(mount: &crate::workspace::Mount<T>) -> WorkspaceMode {
    if mount.mode.read_only() {
        WorkspaceMode::ReadOnly
    } else {
        WorkspaceMode::ReadWrite
    }
}

/// The selected cwd must retain the primary slot's name and source identity.
pub(super) fn primary<'a>(
    bindings: &'a [crate::agent::WorkspaceEntry],
    volume: &VolumeRef,
) -> Result<&'a crate::agent::WorkspaceEntry> {
    crate::agent::WorkspaceTable::primary_in(bindings)
        .filter(|entry| entry.as_volume() == Some(volume))
        .ok_or_else(|| {
            Error::Unauthorized("primary workspace does not match the selected table".into())
        })
}

/// The selected task's visible volumes, excluding its primary and internal volume.
pub(super) fn auxiliary(
    bindings: &[crate::agent::WorkspaceEntry],
) -> impl Iterator<Item = (&crate::agent::WorkspaceEntry, &VolumeRef)> {
    let primary = crate::agent::WorkspaceTable::primary_in(bindings).map(|entry| entry.id);
    bindings
        .iter()
        .filter(move |binding| {
            Some(binding.id) != primary && binding.role != crate::agent::WorkspaceRole::Internal
        })
        .filter_map(|binding| binding.as_volume().map(|volume| (binding, volume)))
}

pub(super) fn owner<T>(
    root: &Path,
    primary: (&str, &VolumeRef, WorkspaceMode, T),
    volumes: impl IntoIterator<Item = (String, VolumeRef, WorkspaceMode, T)>,
) -> Result<crate::workspace::MountTable<T>> {
    let (name, volume, mode, source) = primary;
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
        volume: None,
        generations: Generations::All,
        path: "**".into(),
    };
    let policy = |actions: &[Action]| {
        Arc::new(Policy {
            schema_version: POLICY_SCHEMA_VERSION,
            version: 1,
            layers: vec![vec![Rule {
                id: "owner".into(),
                effect: Effect::Allow,
                principal: PrincipalPattern::default(),
                actions: actions.iter().copied().collect(),
                resource: resource.clone(),
                conditions: Conditions {
                    relation: Some(Relation::SelfTask),
                    ..Conditions::default()
                },
            }]],
        })
    };
    let writable = policy(ActionSet::Owner.actions());
    let read_only = policy(ActionSet::MountRead.actions());
    let volumes: Vec<_> = volumes.into_iter().collect();
    let capability = Capability {
        layers: vec![
            std::iter::once(volume)
                .chain(volumes.iter().map(|(_, volume, _, _)| volume))
                .flat_map(|volume| {
                    Capability::whole_volume(Some(volume.id().into()), ActionSet::Owner.actions())
                        .layers
                        .into_iter()
                        .flatten()
                })
                .collect(),
        ],
        expires_at: None,
    };
    let view = |name, volume, mode, source| crate::workspace::VolumeView {
        name,
        agent: principal.id.clone(),
        volume,
        lineage: principal.lineage.clone(),
        generation: 0,
        head: 0,
        config: acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable),
        // Compile each alias independently. A writable sibling of the same
        // volume must not promote this binding's read-only mode.
        policy: Arc::clone(match mode {
            WorkspaceMode::ReadOnly => &read_only,
            WorkspaceMode::ReadWrite => &writable,
        }),
        source,
    };
    crate::workspace::compose(
        root.into(),
        &principal,
        &capability,
        Context {
            now: 0,
            approvals: 0,
        },
        view(name.into(), volume.clone(), mode, source),
        volumes
            .into_iter()
            .map(|(name, volume, mode, source)| view(name, volume, mode, source)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        // Replacing a provider binding cannot change the namespace, access
        // decisions or volume identity compiled at composition time.
        #[test]
        fn binding_keeps_the_composed_authority(sources in any::<[u8; 2]>(), entry in "[a-z]{1,8}(/[a-z]{1,8}){0,3}") {
            let provider = crate::resources::ProviderRef::new(
                "binding".into(), "filesystem".into(), "1".into(), &mut None,
            ).unwrap();
            let volume = |name: &str| VolumeRef::new(provider.clone(), name.into(),
                VolumeClass::Project, VolumeOwner::Project("owner".into()), &mut None).unwrap();
            let root = std::env::current_dir().unwrap();
            let mut mounts = owner(&root, ("project", &volume("project"), WorkspaceMode::ReadWrite, sources[0]),
                [("data".into(), volume("data"), WorkspaceMode::ReadWrite, sources[1])]).unwrap();
            let before: Vec<_> = mounts.mounts().map(|(path, mount)|
                (path.to_owned(), mount.view.volume.clone(), mount.mode, mount.view.source)).collect();
            for (_, mount) in mounts.mounts_mut() {
                mount.view.source = mount.view.source.wrapping_add(1);
            }
            for (path, volume, mode, source) in before {
                let (mounted, relative) = mounts.resolve(Path::new(&path)).unwrap();
                let qualifier = if path.is_empty() { "project" } else { path.strip_prefix("volume:///").unwrap() };
                let (selected_path, selected) = mounts.named(Some(qualifier)).unwrap();
                prop_assert_eq!(selected_path, path.as_str());
                prop_assert_eq!(&selected.view.volume, &volume);
                prop_assert!(relative.is_empty());
                prop_assert_eq!(&mounted.view.volume, &volume);
                prop_assert_eq!(mounted.mode, mode);
                prop_assert_eq!(mounted.view.source, source.wrapping_add(1));
                prop_assert!(mounted.check(Action::Read, "file", 0).is_ok());
                prop_assert!(mounted.check(Action::Write, "file", 0).is_ok());
            }
            for (path, mount) in mounts.mounts() {
                let task_path = if path.is_empty() { entry.clone() } else { format!("{path}/{entry}") };
                let (routed, relative) = mounts.file(None, &task_path).unwrap();
                prop_assert_eq!(&routed.view.volume, &mount.view.volume);
                prop_assert_eq!(&relative, &entry);
                let (routed_again, same_path) = mounts.file(Some(&routed.view.volume), &relative).unwrap();
                prop_assert_eq!(&routed_again.view.volume, &mount.view.volume);
                prop_assert_eq!(same_path.as_ref(), relative.as_ref());
            }
            for path in [".volumes/data/entry", ".system/state", ".git/info/exclude", ".home-seed-v1", ".acyclic-sdk/state"] {
                for explicit in [None, Some(volume("project"))] {
                    let (selected, relative) = mounts.file(explicit.as_ref(), path).unwrap();
                    prop_assert_eq!(&selected.view.volume, &volume("project"));
                    prop_assert_eq!(relative.as_ref(), path);
                }
            }
            prop_assert!(mounts.file(None, "volume:///missing/entry").is_err());
            prop_assert!(mounts.file(None, "../entry").is_err());
            prop_assert!(mounts.named(Some("journal")).is_err());
            prop_assert!(mounts.named(Some("../data")).is_err());
            prop_assert_eq!(&mounts.named(None).unwrap().1.view.volume, &volume("project"));
        }
    }
}
