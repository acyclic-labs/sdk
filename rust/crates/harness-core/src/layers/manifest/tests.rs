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
    for (index, workspace) in workspaces.iter().enumerate() {
        workspace
            .write_text(
                &format!("/layer-{index}.ts"),
                &format!("export default {index};"),
            )
            .await?;
    }
    workspaces[0]
        .write_text("/hidden.ts", "export default 1;")
        .await?;
    workspaces[5].write_text("/hidden.ts", "off: true").await?;
    for (index, value) in [(0, "seed"), (5, "subject")] {
        let mut transaction = workspaces[index]
            .begin_transaction(acyclic_fs::IdempotencyKey::new(), budget, &cancellation)
            .await?
            .value;
        transaction
            .create_dir_all("/sys", budget, &cancellation)
            .await?;
        transaction
            .write_text("/sys/source.ts", &format!("export default '{value}';"))
            .await?;
        transaction
            .commit::<WorkspaceError, _, _>(
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
                budget,
                &cancellation,
                |_| async {
                    Ok(OperationReceipt {
                        value: (),
                        work: WorkCounters::UNCHARGED,
                    })
                },
            )
            .await?;
    }
    let mut generations = Vec::new();
    for workspace in &workspaces {
        generations.push(workspace.head(budget, &cancellation).await?.value);
    }
    {
        use crate::config::Tree as _;
        let io = crate::filesystem::PreparationIo::for_unmeasured(&cancellation)?;
        let manifest = Manifest::new(std::array::from_fn(|index| &generations[index]));
        let tree = manifest.tree(&io, u64::MAX);
        assert!(tree.read("hidden.ts").await?.is_none());
        assert!(tree.read("missing.ts").await?.is_none());
        assert_eq!(
            tree.read("sys/source.ts")
                .await?
                .ok_or("missing seed module")?
                .as_ref(),
            b"export default 'seed';"
        );
        // Each import exists in only its original layer. The production VM
        // resolves the closure through the same six-generation view.
        let source = b"import a from './layer-0.ts'; import b from './layer-1.ts';
            import c from './layer-2.ts'; import d from './layer-3.ts';
            import e from './layer-4.ts'; import f from './layer-5.ts';
            export default a+b+c+d+e+f;";
        let base = serde_json::Value::Null;
        let evaluate = || {
            crate::config::evaluate(
                "settings.ts",
                source,
                &tree,
                &base,
                crate::config::Surface::Data,
            )
        };
        assert_eq!(evaluate().await?.value, serde_json::json!(15));
        workspaces[5]
            .write_text("/layer-5.ts", "export default 100;")
            .await?;
        assert_eq!(evaluate().await?.value, serde_json::json!(15));
        let mut current = std::array::from_fn(|index| &generations[index]);
        let advanced = workspaces[5].head(budget, &cancellation).await?.value;
        current[5] = &advanced;
        let changed = Manifest::new(current);
        assert_eq!(
            crate::config::evaluate(
                "settings.ts",
                source,
                &changed.tree(&io, u64::MAX),
                &serde_json::Value::Null,
                crate::config::Surface::Data
            )
            .await?
            .value,
            serde_json::json!(110)
        );
        // Cancellation is an original operation failure, never a missing
        // module that falls through to another layer.
        let stopped = acyclic_fs::CancellationToken::new();
        let stopped_io = crate::filesystem::PreparationIo::for_unmeasured(&stopped)?;
        let stopped_tree = manifest.tree(&stopped_io, u64::MAX);
        stopped.cancel();
        assert!(stopped_tree.read("layer-5.ts").await.is_err());
    }
    generations[5] = workspaces[5].head(budget, &cancellation).await?.value;
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
