use super::*;
use acyclic_fs::access::{Action as FsAction, Effect, Policy};
use acyclic_fs::{
    CancellationToken, WorkBudget,
    model::{CheckoutMode, GenerationSelector},
};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn cached_bytes_never_bypass_policy_revisions(revisions in prop::collection::vec(any::<bool>(), 1..5)) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
            let fs = Fs::memory();
            let storage = MemoryHarnessStorage::from_providers(
                AgentId::new(), 4096, fs.clone(), Arc::new(MemoryStream::default()),
            ).await?;
            let file = storage.stage(OperationId::new(), "secrets/key", b"cached", "text/plain", "key").await?;
            assert!(storage.memory_store.lock().unwrap().read(&file).is_ok());
            for (index, denied) in revisions.into_iter().enumerate() {
                policy(&fs, &storage, index as u64 + 2, denied.then_some(FsAction::Read), "secrets/**").await?;
                // Keep proving that the old shortcut still has the bytes: a
                // denial must come from the current volume policy, not eviction.
                assert!(storage.memory_store.lock().unwrap().read(&file).is_ok());
                let read = storage.read(&file).await;
                assert_eq!(read.is_ok(), !denied, "{read:?}");
                if let Ok(bytes) = read { assert_eq!(bytes.as_ref(), b"cached"); }
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

async fn policy(
    fs: &acyclic_fs::MemoryFs,
    storage: &MemoryHarnessStorage,
    version: u64,
    deny: Option<FsAction>,
    path: &str,
) -> Result<()> {
    let cancel = CancellationToken::new();
    let workspace = fs
        .open_workspace(
            storage.volume.storage_name(&mut None)?,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| super::super::super::map_error(failure.error))?
        .value;
    let mut policy: Policy = serde_json::from_slice(super::super::super::access::INITIAL_POLICY)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    policy.version = version;
    if let Some(action) = deny {
        let mut deny = policy.layers[0][0].clone();
        deny.id = "hide-cached-access".into();
        deny.effect = Effect::Deny;
        deny.actions = [action].into_iter().collect();
        deny.resource.path = path.into();
        policy.layers[0].push(deny);
    }
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| super::super::super::map_error(failure.error))?
        .value;
    checkout
        .write_access_policy(
            &serde_json::to_vec(&policy).map_err(|error| Error::Invalid(error.to_string()))?,
            acyclic_fs::NamedAttributeWriteMode::Replace,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(|failure| super::super::super::map_error(failure.error.into()))?;
    checkout
        .commit::<acyclic_fs::FsError, _, _>(
            acyclic_fs::OperationId::new(),
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancel,
            |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .map_err(|failure| super::super::super::map_error(failure.error.into()))?;
    Ok(())
}

mod context {
    extern crate self as acyclic_harness;
    include!("../../../../../harness/tests/support/content_context.rs");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn cached_directory_replies_pin_data_and_recheck_current_disclosure(
        revisions in prop::collection::vec(any::<bool>(), 1..5), narrowed in any::<bool>(),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
            let fs = Fs::memory();
            let storage = MemoryHarnessStorage::from_providers(
                AgentId::new(), 4096, fs.clone(), Arc::new(MemoryStream::default()),
            ).await?;
            for name in ["one", "two"] {
                storage.stage(OperationId::new(), &format!("notes/{name}"), b"data", "text/plain", name).await?;
            }
            let tool = storage.list_files_tool(Limits { file_bytes: 4096, ..Limits::default() })?;
            let invocation = ToolInvocation {
                operation_id: OperationId::new(), call_id: "cached-list".into(), name: "acyclic.list_files".into(),
                arguments: json!({"path":"notes", "maximum_entries":8}),
            };
            let original = tool.executor.execute(invocation.clone(), None).await?;
            assert_eq!(original.value["entries"].as_array().unwrap().len(), 2);
            storage.stage(OperationId::new(), "notes/three", b"later", "text/plain", "three").await?;
            let grants = if narrowed {
                use acyclic_fs::access::{Capability, Grant, Generations, ResourcePattern};
                Capabilities::new([Capability {
                    layers: vec![vec!["notes", "notes/one"].into_iter().map(|path| Grant {
                        actions: [FsAction::List, FsAction::ReadHistory].into_iter().collect(),
                        resource: ResourcePattern { lineage_root: None, volume: None,
                            generations: Generations::All, path: path.into() },
                    }).collect()], expires_at: None,
                }.to_scope_label().map_err(|error| Error::Invalid(error.to_string()))?])
            } else { Capabilities::new([storage.read_capability.clone()]) };
            let task = context::content_context(storage.content_verifier.clone(), None, grants).await?;
            let context = crate::runtime::ToolContext::new(task, invocation.operation_id, &invocation.call_id, None)?;
            for (index, denied) in revisions.into_iter().enumerate() {
                policy(&fs, &storage, index as u64 + 2, denied.then_some(FsAction::List), "notes/two").await?;
                let replay = tool.executor.execute(invocation.clone(), Some(context.clone())).await;
                let reconciled = tool.executor.reconcile(invocation.clone(), Some(context.clone())).await;
                let allowed = !denied && !narrowed;
                assert_eq!(replay.is_ok(), allowed, "{replay:?}");
                assert_eq!(reconciled.is_ok(), allowed, "{reconciled:?}");
                if allowed {
                    assert_eq!(replay?, original);
                    assert_eq!(reconciled?, Some(original.clone()));
                }
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
