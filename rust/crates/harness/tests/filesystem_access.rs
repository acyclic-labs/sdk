//! Signed filesystem delegation boundaries.
#[path = "volume_composition.rs"]
mod volume_composition;
#[path = "filesystem_access_compilation.rs"]
mod compilation;
#[cfg(feature = "filesystem")]
#[path = "filesystem_access_reads.rs"]
mod reads;
use acyclic_fs::access::{Action, Capability, Generations, Grant, ResourcePattern};
use acyclic_harness::{
    Capabilities,
    core::{AggregateKind, Authority, AuthorityIssuer},
};

fn grant(path: &str) -> Grant {
    Grant {
        actions: [Action::Read].into_iter().collect(),
        resource: ResourcePattern {
            lineage_root: None,
            volume: Some("project".into()),
            generations: Generations::Head,
            path: path.into(),
        },
    }
}

#[test]
fn signed_delegation_only_narrows() -> Result<(), Box<dyn std::error::Error>> {
    let issuer = AuthorityIssuer::new(
        "host",
        [7; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "task".into(),
        },
    );
    let verifier = issuer.verifier(&mut None)?;
    let parent = Capability {
        layers: vec![vec![grant("src/**")]],
        expires_at: Some(100),
    };
    let parent_scope = issuer.issue(
        "root",
        &Capabilities::new([parent.to_scope_label()?]),
        None,
        None,
        &mut None,
    )?;
    let child = parent.clone().attenuate(vec![grant("**/*.rs")], Some(200));
    let child_scope = issuer.attenuate(
        &parent_scope,
        "child",
        &Capabilities::new([child.to_scope_label()?]),
    )?;
    assert_eq!(verifier.filesystem_capability(&child_scope)?, child);
    assert_eq!(child.expires_at, Some(100));
    let widened = Capability {
        layers: vec![vec![grant("**")]],
        expires_at: Some(100),
    };
    assert!(
        issuer
            .attenuate(
                &child_scope,
                "widened",
                &Capabilities::new([widened.to_scope_label()?])
            )
            .is_err()
    );
    let extended = Capability {
        expires_at: Some(101),
        ..child.clone()
    };
    assert!(
        issuer
            .attenuate(
                &child_scope,
                "extended",
                &Capabilities::new([extended.to_scope_label()?])
            )
            .is_err()
    );
    let grandchild = child.attenuate(vec![grant("src/lib.rs")], Some(90));
    let grandchild_scope = issuer.attenuate(
        &child_scope,
        "grandchild",
        &Capabilities::new([grandchild.to_scope_label()?]),
    )?;
    assert_eq!(
        verifier.filesystem_capability(&grandchild_scope)?,
        grandchild
    );
    let mut tampered = serde_json::to_value(&grandchild_scope)?;
    tampered["capabilities"] = serde_json::json!([widened.to_scope_label()?]);
    assert!(
        verifier
            .filesystem_capability(&serde_json::from_value(tampered)?)
            .is_err()
    );
    Ok(())
}

#[test]
fn production_labels_use_the_filesystem_vocabulary() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_fs::access::{
        DIRECTORY_READ_PREFIX, FILE_READ_PREFIX, SCOPE_PREFIX, VolumeOperation,
    };
    let prefixes = [
        VolumeOperation::Read.capability_prefix(),
        VolumeOperation::Write.capability_prefix(),
        FILE_READ_PREFIX,
        DIRECTORY_READ_PREFIX,
        SCOPE_PREFIX,
    ];
    let mut directories = vec![std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src")];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            if path.is_dir() {
                directories.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let source = std::fs::read_to_string(&path)?;
                for prefix in prefixes {
                    assert!(
                        !source.contains(&format!("\"{prefix}")),
                        "duplicated vocabulary in {}",
                        path.display()
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn policy_intersection_preserves_signed_narrowed_grants() -> Result<(), Box<dyn std::error::Error>>
{
    use acyclic_harness::{AuthorityLevel, AuthorityPolicy, PolicyLayer, resolve_policy_layers};
    let parent = Capability {
        layers: vec![vec![grant("src/**")]],
        expires_at: Some(100),
    };
    let child = parent.clone().attenuate(vec![grant("**/*.rs")], Some(90));
    let broad = Capabilities::new([
        parent.to_scope_label()?,
        "shared".into(),
        "runtime-only".into(),
    ]);
    let narrow = Capabilities::new([
        child.to_scope_label()?,
        "shared".into(),
        "agent-only".into(),
    ]);
    let expected = Capabilities::new([child.to_scope_label()?, "shared".into()]);
    assert_eq!(broad.intersect(&narrow), expected);
    assert_eq!(narrow.intersect(&broad), expected);
    assert_eq!(expected.intersect(&expected), expected);
    assert!(expected.is_subset_of(&broad) && expected.is_subset_of(&narrow));
    let parent_denial = Capabilities::new([parent.to_scope_label()?]);
    assert_eq!(
        expected.without(&parent_denial),
        Capabilities::new(["shared"]),
    );
    assert_eq!(
        acyclic_harness::resolve_policies(&[
            AuthorityPolicy {
                grants: broad.clone(),
                denies: parent_denial
            },
            AuthorityPolicy {
                grants: narrow.clone(),
                denies: Capabilities::default()
            },
        ]),
        Capabilities::new(["shared"]),
    );
    let resolved = resolve_policy_layers(&[
        PolicyLayer {
            level: AuthorityLevel::Runtime,
            policy: AuthorityPolicy {
                grants: broad,
                denies: Capabilities::default(),
            },
        },
        PolicyLayer {
            level: AuthorityLevel::Agent,
            policy: AuthorityPolicy {
                grants: narrow,
                denies: Capabilities::default(),
            },
        },
    ])?;
    assert_eq!(resolved, expected);
    let issuer = AuthorityIssuer::new(
        "host",
        [7; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "task".into(),
        },
    );
    let scope = issuer.issue("resolved", &resolved, None, None, &mut None)?;
    assert_eq!(
        issuer.verifier(&mut None)?.filesystem_capability(&scope)?,
        child
    );
    let unrelated = Capability {
        layers: vec![vec![grant("elsewhere/**")]],
        expires_at: Some(100),
    };
    assert!(
        resolved
            .intersect(&Capabilities::new([unrelated.to_scope_label()?]))
            .iter()
            .next()
            .is_none()
    );
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn set_operations_preserve_filesystem_ceilings(
        legacy_kind in 0usize..4,
        parent_expiry in proptest::option::of(proptest::prelude::any::<u64>()),
        child_expiry in proptest::option::of(proptest::prelude::any::<u64>()),
    ) {
        use acyclic_fs::access::{DIRECTORY_READ_PREFIX, FILE_READ_PREFIX, SCOPE_PREFIX, VolumeOperation, is_filesystem_capability};
        let prefix = [FILE_READ_PREFIX, DIRECTORY_READ_PREFIX,
            VolumeOperation::Read.capability_prefix(), VolumeOperation::Write.capability_prefix()][legacy_kind];
        let legacy = format!("{prefix}volume");
        let parent = Capability { layers: vec![vec![grant("src/**")]], expires_at: parent_expiry };
        let parent = Capabilities::new([legacy.clone(), parent.to_scope_label()?, "task:spawn:child".into()]);
        let only_legacy = Capabilities::new([legacy.clone(), "task:spawn:child".into()]);
        proptest::prop_assert!(!only_legacy.is_subset_of(&parent));
        let denied = Capabilities::new(parent.iter().filter(|label| label.starts_with(SCOPE_PREFIX)));
        for restricted in [parent.intersect(&only_legacy), parent.without(&denied)] {
            proptest::prop_assert!(restricted.is_subset_of(&parent));
            proptest::prop_assert!(restricted.iter().all(|label| !is_filesystem_capability(label)));
            proptest::prop_assert!(restricted.contains("task:spawn:child"));
        }
        let root = parent.iter().find_map(|label| Capability::from_scope_label(label).ok().flatten()).unwrap();
        let child = root.attenuate(vec![grant("src/private/**")], child_expiry);
        let child = Capabilities::new([legacy, child.to_scope_label()?, "task:spawn:child".into()]);
        proptest::prop_assert!(child.is_subset_of(&parent));
        for intersection in [parent.intersect(&child), child.intersect(&parent)] {
            proptest::prop_assert!(intersection.is_subset_of(&parent));
            proptest::prop_assert!(intersection.is_subset_of(&child));
            proptest::prop_assert!(intersection.iter().any(|label| label.starts_with(SCOPE_PREFIX)));
        }
    }
}
