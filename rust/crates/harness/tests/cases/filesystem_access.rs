//! Signed filesystem delegation boundaries.
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
    let mut directories =
        vec![std::path::Path::new(acyclic_harness::build_metadata::MANIFEST_DIR).join("src")];
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
