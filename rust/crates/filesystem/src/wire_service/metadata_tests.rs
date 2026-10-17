use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn wire_root_metadata_preserves_the_persisted_policy(mode in any::<u32>(), time in any::<i64>()) {
        futures::executor::block_on(async {
            let fs = Fs::memory();
            let cancellation = CancellationToken::new();
            let budget = WorkBudget::UNBOUNDED;
            let workspace = fs.create_workspace_with_operation::<_, (), WorkspaceError>("wire-metadata",
                crate::VolumeInitialization { config: VolumeConfig::portable(Lifecycle::Ephemeral), access_policy: Some(crate::access::INITIAL_POLICY) },
                crate::OperationId::new(), budget, &cancellation, None).await?.value.workspace();
            let mut transaction = workspace.begin_transaction(IdempotencyKey::new(), budget, &cancellation).await?.value;
            let update = wire::Metadata { posix_mode: Some(wire::OptionalU32 { value: Some(wire::optional_u32::Value::Present(mode)) }),
                modified_ns: Some(wire::OptionalI64 { value: Some(wire::optional_i64::Value::Present(time)) }), ..Default::default() };
            apply_mutation(&mut transaction, wire::Mutation { mutation: Some(wire::mutation::Mutation::SetMetadata(wire::SetMetadata { path: "/".into(), metadata: Some(update) })) }).await?;
            let (policy, _) = transaction.checkout.read_access_policy(budget, &cancellation).await?.value;
            assert_eq!(policy.ok_or("policy was erased")?.bytes.as_ref(), crate::access::INITIAL_POLICY);
            let root = transaction.checkout.root().root_file_id;
            let changed = transaction.checkout.read_metadata(root, budget, &cancellation).await?.value;
            assert_eq!(changed.posix_mode, MetadataField::Value(mode));
            assert_eq!(changed.modified_ns, MetadataField::Value(time));
            // The former translation overwrites the policy root. Run it through
            // the same checkout and prove the preservation assertion detects it.
            transaction.set_metadata("/", metadata(None)?, budget, &cancellation).await?;
            assert!(transaction.checkout.read_access_policy(budget, &cancellation).await?.value.0.is_none());
            Ok::<_, Box<dyn std::error::Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
