//! Task namespace and shared access-decision scenarios.
use acyclic_fs::access::*;
use acyclic_harness::{
    Error, Result,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
    workspace::{VolumeView, compose},
};
use proptest::prelude::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

fn principal(agent: &str) -> Principal {
    Principal {
        kind: PrincipalKind::Agent,
        id: agent.into(),
        lineage: vec!["root".into(), agent.into()],
        groups: BTreeSet::new(),
        roles: BTreeSet::new(),
        tags: BTreeSet::new(),
        attributes: BTreeMap::new(),
    }
}
fn resource_pattern(path: &str) -> ResourcePattern {
    ResourcePattern {
        lineage_root: None,
        volume: None,
        generations: Generations::All,
        path: path.into(),
    }
}
fn capability() -> Capability {
    Capability {
        layers: vec![vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource: resource_pattern("**"),
        }]],
        expires_at: None,
    }
}
fn rule(id: &str, actions: &[Action], own: bool, path: &str) -> Rule {
    Rule {
        id: id.into(),
        effect: Effect::Allow,
        principal: PrincipalPattern {
            kind: Some(PrincipalKind::Agent),
            ..Default::default()
        },
        actions: actions.iter().copied().collect(),
        resource: resource_pattern(path),
        conditions: Conditions {
            relation: own.then_some(Relation::SelfTask),
            ..Default::default()
        },
    }
}
fn policy(read_all: bool) -> Arc<Policy> {
    Arc::new(Policy {
        schema_version: POLICY_SCHEMA_VERSION,
        version: 1,
        layers: vec![vec![
            rule(
                "read",
                &[Action::List, Action::Read, Action::ReadHistory],
                !read_all,
                "**",
            ),
            rule(
                "write",
                &[
                    Action::Create,
                    Action::Write,
                    Action::Append,
                    Action::Delete,
                    Action::Rename,
                    Action::Fork,
                    Action::MergeInto,
                ],
                true,
                "**",
            ),
        ]],
    })
}
fn volume(agent: &str, name: &str, policy: Arc<Policy>) -> Result<VolumeView<()>> {
    Ok(VolumeView {
        name: name.into(),
        agent: agent.into(),
        volume: VolumeRef::new(
            ProviderRef::new(
                "task-view".into(),
                "filesystem".into(),
                "1".into(),
                &mut None,
            )?,
            format!("{agent}-{name}"),
            VolumeClass::Project,
            VolumeOwner::Project("project".into()),
            &mut None,
        )?,
        lineage: vec!["root".into(), agent.into()],
        generation: 10,
        head: 10,
        policy,
        config: acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable),
        source: (),
    })
}
fn root() -> PathBuf {
    std::env::current_dir().unwrap().join("task-view")
}
const CONTEXT: Context = Context {
    now: 100,
    approvals: 0,
};

#[test]
fn own_views_compose_and_ungranted_views_are_absent() -> Result<()> {
    let principal = principal("B");
    let policy = policy(false);
    let table = compose(
        root(),
        &principal,
        &capability(),
        CONTEXT,
        volume("B", "project", policy.clone())?,
        [
            volume("B", "data", policy.clone())?,
            volume("A", "project", policy)?,
        ],
    )?;
    assert_eq!(table.mounts().count(), 2);
    assert_eq!(table.context_line(), "Volumes: . (rw), .volumes/data (rw)");
    let hints = table.environment()?;
    assert_eq!(
        hints.get("ACYCLIC_VOLUME_DATA_MODE").map(String::as_str),
        Some("rw")
    );
    assert_eq!(
        hints.get("ACYCLIC_VOLUME_DATA").map(PathBuf::from),
        Some(root().join(".volumes/data"))
    );
    let (data, path) = table.resolve(Path::new(".volumes/data/x"))?;
    assert_eq!(data.view.volume.id(), "B-data");
    assert_eq!(path, "x");
    assert!(data.mode.allows(Action::Write));
    data.check(Action::Read, &path, 100)?;
    table
        .resolve(Path::new("src/x"))?
        .0
        .check(Action::Write, "src/x", 100)?;
    assert!(matches!(
        table.resolve(Path::new(".volumes/agents/A/x")),
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        table.resolve(Path::new("../x")),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(table.resolve(&root()), Err(Error::Invalid(_))));
    Ok(())
}

#[test]
fn read_all_write_own_exposes_foreign_mounts_read_only() -> Result<()> {
    let principal = principal("B");
    let policy = policy(true);
    let build = |include_c| -> Result<_> {
        let mut others = vec![volume("A", "project", policy.clone())?];
        if include_c {
            others.push(volume("C", "project", policy.clone())?);
        }
        compose(
            root(),
            &principal,
            &capability(),
            CONTEXT,
            volume("B", "project", policy.clone())?,
            others,
        )
    };
    let table = build(false)?;
    let (foreign, path) = table.resolve(Path::new(".volumes/agents/A/x"))?;
    assert!(foreign.mode.read_only());
    assert_eq!(
        table
            .environment()?
            .get("ACYCLIC_VOLUME_AGENTS_A_MODE")
            .map(String::as_str),
        Some("ro")
    );
    foreign.check(Action::Read, &path, 100)?;
    assert!(matches!(
        foreign.check(Action::Write, &path, 100),
        Err(Error::Unauthorized(_))
    ));
    assert!(!table.mounts().any(|(path, _)| path == ".volumes/agents/C"));
    assert!(
        build(true)?
            .mounts()
            .any(|(path, _)| path == ".volumes/agents/C")
    );
    let mut stale = volume("A", "project", policy.clone())?;
    stale.generation -= 1;
    assert!(matches!(
        compose(
            root(),
            &principal,
            &capability(),
            CONTEXT,
            volume("B", "project", policy)?,
            [stale]
        ),
        Err(Error::Conflict(_))
    ));
    Ok(())
}

#[test]
fn descendant_rules_choose_a_writable_mount_and_keep_path_denials() -> Result<()> {
    let principal = principal("B");
    let policy = Arc::new(Policy {
        schema_version: POLICY_SCHEMA_VERSION,
        version: 1,
        layers: vec![vec![
            rule("read", &[Action::Read, Action::List], true, "**"),
            rule("write-output", &[Action::Write], true, "output/**"),
        ]],
    });
    let table = compose(
        root(),
        &principal,
        &capability(),
        CONTEXT,
        volume("B", "project", policy)?,
        [],
    )?;
    let mount = table.resolve(Path::new(""))?.0;
    assert!(mount.mode.allows(Action::Write));
    mount.check(Action::Write, "output/result", 100)?;
    assert!(mount.check(Action::Write, "src/main.rs", 100).is_err());
    Ok(())
}

#[test]
fn hidden_stale_views_do_not_block_or_hide_primary_user_directories() -> Result<()> {
    let caller = principal("B");
    let policy = policy(false);
    let mut hidden = volume("A", "project", policy.clone())?;
    hidden.generation -= 1;
    let table = compose(
        root(),
        &caller,
        &capability(),
        CONTEXT,
        volume("B", "project", policy)?,
        [hidden],
    )?;
    assert_eq!(table.mounts().count(), 1);
    let (primary, path) = table.resolve(Path::new(".volumes/user.txt"))?;
    assert_eq!(primary.view.volume.id(), "B-project");
    assert_eq!(path, ".volumes/user.txt");
    primary.check(Action::Write, &path, 100)?;
    assert!(matches!(
        table.resolve(Path::new(".volumes/agents/A/file")),
        Err(Error::NotFound(_))
    ));
    assert_eq!(table.context_line(), "Volumes: . (rw)");
    assert!(!table.environment()?.contains_key("ACYCLIC_VOLUME_AGENTS_A"));
    Ok(())
}

#[test]
fn duplicate_hidden_mount_paths_are_rejected() -> Result<()> {
    let caller = principal("B");
    let policy = policy(false);
    assert!(matches!(
        compose(
            root(),
            &caller,
            &capability(),
            CONTEXT,
            volume("B", "project", policy.clone())?,
            [
                volume("A", "project", policy.clone())?,
                volume("A", "project", policy)?
            ],
        ),
        Err(Error::Conflict(_))
    ));
    Ok(())
}

proptest! {
    #[test]
    fn mounts_and_direct_decisions_agree(read in any::<bool>(), write in any::<bool>(), create in any::<bool>(), own in any::<bool>(), path in prop::sample::select(vec!["", "x", "output", "output/x", "src/x"])) {
        let caller = principal("B");
        let mut actions = vec![Action::List];
        if read { actions.push(Action::Read); }
        if write { actions.push(Action::Write); }
        if create { actions.push(Action::Create); }
        let policy = Arc::new(Policy { schema_version: POLICY_SCHEMA_VERSION, version: 1, layers: vec![vec![rule("selected", &actions, own, "**")]] });
        let capability = capability();
        let table = compose(root(), &caller, &capability, CONTEXT, volume("B", "project", policy.clone()).unwrap(), [volume("A", "project", policy.clone()).unwrap()]).unwrap();
        for agent in ["A", "B"] {
            let lineage = vec!["root".into(), agent.into()];
            let id = format!("{agent}-project");
            let selected = if agent == "B" { path.to_owned() } else { format!(".volumes/agents/A/{path}") };
            for action in [Action::Read, Action::Write, Action::Create] {
                let direct = decide(&policy, &caller, action, Resource { lineage: &lineage, volume: &id, generation: 10, head: 10, path, config: acyclic_fs::VolumeConfig::native(acyclic_fs::Lifecycle::Durable) }, CONTEXT, &capability).allowed;
                let mounted = table.resolve(Path::new(&selected)).is_ok_and(|(mount, relative)| mount.check(action, &relative, 100).is_ok());
                prop_assert_eq!(mounted, direct);
            }
        }
    }
}
