use super::*;
use crate::{
    AgentId,
    conversation::{VolumeClass, VolumeOwner},
    durable_host::tests::created_fixture::CreationFixture,
    filesystem::workspace_ref,
};
use proptest::prelude::*;

fn budget(bytes: u64) -> acyclic_fs::WorkBudget {
    acyclic_fs::WorkBudget {
        items_examined: 1_048_576,
        allocation_operations: 131_072,
        ..acyclic_fs::performance::work_budget(16_384, bytes)
    }
}

#[test]
fn resource_checks_preserve_original_metadata_and_exact_volume_context() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread test runtime");
    let fixture = runtime.block_on(CreationFixture::new())?;
    let volume = VolumeRef::new(
        fixture.filesystem_provider.clone(),
        "scheduler".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
        &mut None,
    )?;
    let workspace = workspace_ref(
        fixture.filesystem_provider.clone(),
        &volume.storage_name(&mut None)?,
    )?;
    let generation = runtime
        .block_on(fixture.filesystem.resolve(&workspace, None))?
        .generation;
    let fence = crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation);
    let (verifier, scope) = fixture.signed_authority();
    // Property: nested finite volume allowances can authorize their own volume,
    // while journal verification retains the original metadata context. Neither
    // sibling allowances nor foreign ledgers can use a captured callback.
    proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        source_file: Some(file!()),
        ..Default::default()
    })
    .run(
        &(any::<bool>(), 2_097_152_u64..=16_777_216, 1_usize..=3),
        |(writable, bytes, depth)| {
            runtime.block_on(async {
                let original = NativeMaterializationWork::new(
                    budget(16_777_216),
                    &acyclic_fs::CancellationToken::new(),
                )
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
                let context = fixture
                    .runtime
                    .task_context_with_native_work(fixture.root, &fence, &original)
                    .await?;
                let owner =
                    context.journal_owner::<acyclic_stream::MemoryStream>(&fence, &original)?;
                let mut resource = original.clone();
                for _ in 0..depth {
                    resource = resource.native_volume_work(budget(bytes))?;
                }
                prop_assert!(resource.shares_ledger(&original));
                prop_assert!(!resource.shares_allocation_context(&original));
                let binding = BorrowedNativeVolumeBinding {
                    host: &fixture.filesystem,
                    volume: &volume,
                    generation: &generation,
                    namespace: &NativeVolumeNamespace::MaterializedDirectory,
                    mounted: None,
                    path: "volume",
                    writable,
                    verifier,
                    scope,
                    publication: PublicationPermit::Unrestricted,
                };
                super::verify_binding(&owner, &binding, &context, &resource).await?;
                let callback = context.volume_callback(&owner, &volume, writable, &resource)?;
                let ungranted = VolumeRef::new(
                    fixture.filesystem_provider.clone(),
                    "ungranted".into(),
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
                    &mut None,
                )?;
                prop_assert!(
                    context
                        .volume_callback(&owner, &ungranted, writable, &resource)
                        .is_err()
                );
                callback.check(&resource)?.await?;
                prop_assert_eq!(callback.check_write(&resource)?.await.is_ok(), writable);
                let sibling = original.native_volume_work(budget(bytes))?;
                prop_assert!(callback.check(&sibling)?.await.is_err());
                let foreign = NativeMaterializationWork::new(
                    budget(16_777_216),
                    &acyclic_fs::CancellationToken::new(),
                )
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
                prop_assert!(callback.check(&foreign)?.await.is_err());
                prop_assert!(
                    super::verify_binding(&owner, &binding, &context, &foreign)
                        .await
                        .is_err()
                );
                resource.cancellation().cancel();
                let cancelled = match callback.check(&resource) {
                    Ok(future) => future.await.is_err(),
                    Err(_) => true,
                };
                prop_assert!(cancelled);
                Ok(())
            })
        },
    )
    .map_err(|error| Error::Storage(error.to_string()))
}
