use super::*;

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn attach_publishes_policy_in_its_original_generation(version in 1u64..1000) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?
            .block_on(async {
                let fs = Fs::memory();
                let source = Arc::new(CountingSource::new(Bytes::from_static(b"source")));
                let store = MemoryLazyWorkspaceStore::default();
                let operation = crate::OperationId::new();
                let policy = format!(r#"{{"schema_version":1,"version":{version},"layers":[]}}"#);
                let cancel = CancellationToken::new();
                let (lazy, original) = LazyWorkspace::attach(
                    &fs, "policy", source.clone(), store.clone(),
                    crate::VolumeInitialization {
                        config: VolumeConfig::portable(crate::Lifecycle::Ephemeral),
                        access_policy: Some(policy.as_bytes()),
                    },
                    operation, accounting!(),
                ).await?;
                let (_, recovered) = LazyWorkspace::attach(
                    &fs, "policy", source.clone(), store,
                    crate::VolumeInitialization {
                        config: VolumeConfig::portable(crate::Lifecycle::Ephemeral),
                        access_policy: None,
                    },
                    operation, accounting!(),
                ).await?;
                assert_eq!(original.id(), recovered.id());
                assert_eq!(lazy.head(accounting!()).await?.id(), original.id());
                let mut checkout = lazy.workspace().checkout(
                    crate::model::GenerationSelector::Exact(original.id()),
                    crate::model::CheckoutMode::read_only_pinned(),
                    WorkBudget::UNBOUNDED, &cancel,
                ).await?.value;
                let recorded = checkout.read_access_policy(WorkBudget::UNBOUNDED, &cancel)
                    .await?.value.0.ok_or("initial policy is absent")?;
                assert_eq!(recorded.bytes.as_ref(), policy.as_bytes());
                assert_eq!(source.counts.lookups.load(Ordering::Relaxed), 0);
                assert_eq!(source.counts.pages.load(Ordering::Relaxed), 0);
                assert_eq!(source.counts.ranges.load(Ordering::Relaxed), 0);
                Ok::<_, Box<dyn std::error::Error>>(())
            }).map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
    }
}
