use super::*;
use acyclic_stream::{AppendRecord, CommitMutation, CommitRequest, MemoryStream, StreamPath};
use std::sync::Arc;

#[tokio::test]
async fn every_original_layer_fences_the_same_consuming_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let stream = Arc::new(MemoryStream::default());
    let fs = Fs::new(
        StreamAuthorityStore::new(stream),
        acyclic_fs::LogicalObjectStore::new(Arc::new(acyclic_objects::MemoryObjects::memory())),
        acyclic_fs::EmbeddedCapabilities::MEMORY,
    );
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = WorkBudget::UNBOUNDED;
    let mut workspaces = Vec::new();
    for name in super::super::ORDER {
        workspaces.push(
            fs.create_workspace(name, budget, &cancellation)
                .await?
                .value,
        );
    }
    let mut generations = Vec::new();
    for workspace in &workspaces {
        generations.push(workspace.head(budget, &cancellation).await?.value);
    }
    let request = |key: u8| -> Result<_, Box<dyn std::error::Error>> {
        Ok(FilesystemCommit::new(CommitRequest {
            deadline_unix_millis: None,
            conditions: Vec::new(),
            mutations: vec![CommitMutation::Append {
                path: StreamPath::new(format!("config/activated/{key}"))?,
                records: vec![AppendRecord::from(acyclic_stream::OwnedBytes::from_static(
                    b"cut",
                ))],
            }],
            idempotency_key: acyclic_stream::IdempotencyKey::new(vec![key])?,
        }))
    };
    let original = Manifest::new(std::array::from_fn(|index| &generations[index]))
        .condition(&fs, request(0)?, budget, &cancellation)
        .await
        .map_err(|failure| failure.failure)?
        .value;
    assert_eq!(
        original.request().conditions.len(),
        super::super::ORDER.len()
    );
    assert!(matches!(
        fs.submit_commit(original, budget, &cancellation)
            .await?
            .value,
        acyclic_fs::FilesystemCommitOutcome::Committed(_)
    ));
    for (index, workspace) in workspaces.iter().enumerate() {
        let references = std::array::from_fn(|index| &generations[index]);
        let manifest = Manifest::new(references);
        // Capture conditions BEFORE the writer changes the source, so this
        // proves commit-time fencing rather than a defensive head re-read.
        let commit = manifest
            .condition(&fs, request(index as u8 + 1)?, budget, &cancellation)
            .await
            .map_err(|failure| failure.failure)?
            .value;
        workspace
            .write_text("/agent.ts", "export default {};")
            .await?;
        let advanced = workspace.head(budget, &cancellation).await?.value;
        if index == 0 {
            use acyclic_fs::access;
            let only_create = crate::config::Policy {
                only: Some(vec![crate::config::PolicyRule {
                    path: "**".into(),
                    actions: vec![crate::config::Action::Append],
                }]),
                deny: None,
            };
            let issuer = crate::config::policy::meet([("parent", &only_create)], 1)?;
            let principal = access::Principal {
                kind: access::PrincipalKind::Agent,
                id: "subject".into(),
                lineage: vec![],
                groups: Default::default(),
                roles: Default::default(),
                tags: Default::default(),
                attributes: Default::default(),
            };
            let resource = access::Resource {
                config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
                lineage: &[],
                volume: "seed",
                generation: 1,
                head: 1,
                path: "",
            };
            let permissions = crate::config::policy::Permissions::compile(
                &issuer,
                None,
                &principal,
                resource,
                access::Context {
                    now: 0,
                    approvals: 0,
                },
                &access::Capability::whole_volume(None, &access::Action::ALL),
            )?;
            let mut created = generations[index]
                .changed_paths_to(&advanced, budget, &cancellation)
                .await?
                .value;
            let row = created.next().await?.value.ok_or("missing created path")?;
            assert!(permissions.allows_change(&row, 0));
            assert!(created.next().await?.value.is_none());
            workspace
                .write_text("/agent.ts", "export default {model: 'other'};")
                .await?;
            let replaced = workspace.head(budget, &cancellation).await?.value;
            let mut overwritten = advanced
                .changed_paths_to(&replaced, budget, &cancellation)
                .await?
                .value;
            let row = overwritten
                .next()
                .await?
                .value
                .ok_or("missing overwritten path")?;
            assert!(!permissions.allows_change(&row, 0));
            // The same pathname passes a caller-described Create. Only the
            // original producer's before binding prevents this forgery.
            assert!(permissions.allows(access::Action::Create, "agent.ts", 0));
            assert!(overwritten.next().await?.value.is_none());
        }
        let mut changed = references;
        changed[index] = &advanced;
        assert_ne!(manifest.digest(), Manifest::new(changed).digest());
        let result = fs.submit_commit(commit, budget, &cancellation).await?.value;
        assert!(
            matches!(result, acyclic_fs::FilesystemCommitOutcome::Rejected(_)),
            "layer {} escaped the consuming commit",
            super::super::ORDER[index]
        );
        // Negative control: omission of original source conditions permits
        // the same activation despite the changed source.
        assert!(matches!(
            fs.submit_commit(request(index as u8 + 16)?, budget, &cancellation)
                .await?
                .value,
            acyclic_fs::FilesystemCommitOutcome::Committed(_)
        ));
        generations[index] = workspace.head(budget, &cancellation).await?.value;
    }
    cancellation.cancel();
    let manifest = Manifest::new(std::array::from_fn(|index| &generations[index]));
    let refused = manifest
        .condition(&fs, request(32)?, budget, &cancellation)
        .await;
    let retained = match refused {
        Err(failure) => {
            assert!(matches!(
                failure.failure.error,
                WorkspaceError::Cancelled(_)
            ));
            failure.commit
        }
        Ok(_) => panic!("cancelled manifest admission succeeded"),
    };
    assert_eq!(retained.request().idempotency_key.as_bytes(), &[32]);
    Ok(())
}
