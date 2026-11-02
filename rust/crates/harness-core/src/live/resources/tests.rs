use super::*;
use crate::executor::{
    ModelCacheAccess, ModelCacheBreakpoint, ModelCacheObservation, ModelCacheTtl, ModelPurpose,
    ModelTokenUsage, TokenPrice,
};
use acyclic_stream::{
    AppendRequest, ForkRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamProvider as _,
    StreamProviderExt as _,
};
use futures::TryStreamExt as _;

async fn read(provider: &MemoryStream, path: &StreamPath, from: u64) -> Record {
    provider
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from,
            limit: 1,
            at_least: Default::default(),
        })
        .await
        .unwrap()
        .try_next()
        .await
        .unwrap()
        .unwrap()
}

async fn append(provider: &MemoryStream, path: &StreamPath, value: Vec<u8>) {
    provider
        .append(AppendRequest {
            path: path.clone(),
            records: vec![value.into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::unique(),
        })
        .await
        .unwrap();
}

// Generated internal accounting operands, never fabricated provider responses.
fn usage() -> ModelUsageRecord {
    ModelUsageRecord {
        version: ModelUsageRecord::VERSION,
        completed: true,
        dispatch: crate::model::ModelDispatch {
            operation_id: crate::OperationId::from_bytes([3; 16]),
            step: 2,
            request_digest: [4; 32],
        },
        purpose: ModelPurpose::Response,
        provider: "generated-provider".into(),
        model: "generated-model".into(),
        agent_path: Some("agents/original".into()),
        sampling: None,
        max_output_tokens: None,
        tokens: ModelTokenUsage {
            input: Some(10),
            cached: Some(3),
            cache_write: Some(1),
            output: Some(2),
            reasoning: None,
        },
        latency_ns: None,
        provider_request_id: None,
        system_fingerprint: None,
        ttft_ns: None,
        request_body: None,
        cache: Some(ModelCacheObservation {
            version: ModelCacheObservation::VERSION,
            credential_scope: Some([5; 32]),
            profile_prefix_digest: Some([6; 32]),
            ttl: ModelCacheTtl::ProviderManaged,
            breakpoint: Some(ModelCacheBreakpoint {
                digest: [7; 32],
                parent_digest: [8; 32],
                messages: 1,
            }),
        }),
    }
}

#[cfg(feature = "filesystem")]
#[tokio::test]
async fn storage_file_borrows_the_proven_authority_fact_without_physical_inference() {
    use acyclic_fs::accounting::{AccountingCursor, AccountingTransition, accounting_page};
    use acyclic_fs::kernel::volume_authority_id;

    let stream = std::sync::Arc::new(MemoryStream::default());
    let objects = std::sync::Arc::new(acyclic_objects::MemoryObjects::memory());
    let fs = acyclic_fs::Fs::from_memory_providers(stream.clone(), objects.clone());
    let authority = acyclic_fs::distributed::StreamAuthorityStore::new(stream);
    let objects = acyclic_fs::LogicalObjectStore::new(objects);
    let cancel = acyclic_fs::CancellationToken::new();
    let workspace = fs
        .create_workspace(
            "resource-storage",
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await
        .unwrap()
        .value;
    let authority_id = volume_authority_id(workspace.id().volume_id());
    let page = accounting_page(
        &authority,
        &objects,
        authority_id,
        AccountingCursor::genesis(),
        1,
        acyclic_fs::performance::work_budget(4096, 16 * 1024 * 1024),
        &cancel,
    )
    .await
    .unwrap();
    assert!(page.complete);
    let fact = &page.facts[0];
    let account = AccountId::new(&[0xff, 3]).unwrap();
    let measured = acyclic_test_support::measure(|| {
        std::hint::black_box(Storage::project(&account, fact));
    });
    assert_eq!((measured.count_total, measured.bytes_total), (0, 0));
    let file = Storage::project(&account, fact);
    assert!(std::ptr::eq(file.fact, fact));
    assert_eq!(file.fact.authority_id, authority_id);
    let foreign = AccountId::new(&[0xff, 4]).unwrap();
    assert_ne!(file.account, Storage::project(&foreign, fact).account);
    assert!(matches!(
        file.fact.transition,
        AccountingTransition::Published {
            logical_regular_file_bytes: 0,
            ..
        }
    ));
    let wire = serde_json::to_value(&file).unwrap();
    assert_eq!(wire["account"], serde_json::json!([255, 3]));
    assert_eq!(wire["fact"]["transition"]["transition"], "published");
    assert_eq!(wire["fact"]["transition"]["logicalRegularFileBytes"], 0);
    if let AccountingTransition::Published {
        generation_root, ..
    } = fact.transition
    {
        assert_eq!(
            wire["fact"]["transition"]["generationRoot"],
            serde_json::to_value(generation_root.to_bytes().as_slice()).unwrap()
        );
    }
    // A known logical zero cannot substitute for absent physical coverage.
    assert!(wire["physicalSharedBytes"].is_null());
    assert!(wire["physicalCopiedBytes"].is_null());
    assert_eq!(
        wire["fact"]["commitDigest"],
        serde_json::to_value(fact.commit_digest).unwrap()
    );
}

#[test]
fn resource_files_retain_original_fork_evidence_and_unknown_physical_work() {
    futures::executor::block_on(async {
        let provider = std::sync::Arc::new(MemoryStream::default());
        let account = AccountId::new(&[0xff, 1]).unwrap();
        let root = StreamPath::new("agents/original").unwrap();
        let first = usage();
        let bytes = serde_json::to_vec(&first).unwrap();
        append(&provider, &root, bytes.clone()).await;
        let original = read(&provider, &root, 0).await;
        for name in ["agents/left", "agents/right"] {
            let selected = StreamPath::new(name).unwrap();
            provider
                .fork(ForkRequest {
                    exclude: Vec::new(),
                    source: root.clone(),
                    destination: selected.clone(),
                    at_tail: Some(1),
                    idempotency_key: IdempotencyKey::unique(),
                })
                .await
                .unwrap();
            let record = read(&provider, &selected, 0).await;
            let shared = Sharing::project(&account, &selected, &record).unwrap();
            assert!(shared.inherited);
            assert_eq!(shared.source.origin, root.as_str());
            assert_eq!(shared.source.stream, selected.as_str());
            assert_eq!(shared.source.commit_id, original.commit_id.as_bytes());
            assert_eq!(shared.source.sequence, original.sequence);
            assert_eq!(
                shared.source.committed_at_micros,
                original.committed_at_micros
            );
            assert_eq!(
                shared.logical_value_bytes,
                u64::try_from(record.value.len()).unwrap()
            );
            assert_eq!(
                (shared.physical_shared_bytes, shared.physical_copied_bytes),
                (None, None)
            );
        }
        // Identical bytes from an independent publication are not shared work.
        let unrelated = StreamPath::new("agents/unrelated").unwrap();
        append(&provider, &unrelated, bytes).await;
        let record = read(&provider, &unrelated, 0).await;
        let independent = Sharing::project(&account, &unrelated, &record).unwrap();
        assert!(!independent.inherited);
        assert_ne!(independent.source.commit_id, original.commit_id.as_bytes());
        let foreign = AccountId::new(&[0xff, 2]).unwrap();
        assert_ne!(
            Source::project(&account, &root, &original).account,
            Source::project(&foreign, &root, &original).account
        );

        let decoded: ModelUsageRecord = serde_json::from_slice(&original.value).unwrap();
        let price = ModelPriceRecord {
            version: 1,
            revision: "generated-price".into(),
            provider: decoded.provider.clone(),
            model: decoded.model.clone(),
            effective_at: 1,
            currency: "USD".into(),
            input_nano_units_per_million: Some(10_000_000),
            output_nano_units_per_million: Some(20_000_000),
            cached: TokenPrice::Subset {
                nano_units_per_million: Some(1_000_000),
            },
            cache_write: TokenPrice::Subset {
                nano_units_per_million: Some(2_000_000),
            },
            reasoning: TokenPrice::Included,
        };
        let source = Source::project(&account, &root, &original);
        let file = CacheCall::project(source, &decoded, Some(&price)).unwrap();
        assert_eq!(file.call.estimated_cost.token_nano_units, Some(105));
        assert_eq!(file.cache_savings_nano_units, Some(35));
        assert!(std::ptr::eq(file.call.usage.as_ref(), &decoded));
        assert_eq!(file.call.usage.cache, decoded.cache);
        let mut pending = decoded.clone();
        pending.completed = false;
        let pending_file = CacheCall::project(source, &pending, Some(&price)).unwrap();
        assert_eq!(pending_file.call.estimated_cost.token_nano_units, None);
        assert_eq!(pending_file.cache_savings_nano_units, None);
        let mut unsupported = decoded.clone();
        unsupported.cache.as_mut().unwrap().version ^= 1;
        assert!(matches!(
            CacheCall::project(source, &unsupported, Some(&price)),
            Err(Error::Version)
        ));
        let mut foreign_price = price.clone();
        foreign_price.model.push('x');
        assert!(matches!(
            CacheCall::project(source, &decoded, Some(&foreign_price)),
            Err(Error::Price(ModelPriceError::BindingMismatch))
        ));

        let mut totals = ModelUsageTotals::new("USD".into()).unwrap();
        totals.push(&file.call).unwrap();
        let counters = Counters::project(source, Some(&totals)).unwrap();
        assert_eq!(counters.model.unwrap().cached.value(), Some(3));
        #[cfg(feature = "filesystem")]
        assert!(counters.work.is_none());
        #[cfg(feature = "machines")]
        assert!(counters.machine.is_none());
        let wire = serde_json::to_value(&counters).unwrap();
        assert_eq!(wire["source"]["account"], serde_json::json!([255, 1]));
        assert!(Counters::project(source, None).unwrap().model.is_none());

        let key = decoded.cache.unwrap().key().unwrap();
        let estimate = crate::executor::ModelCacheResidency::observe(
            key,
            None,
            [9; 16],
            10,
            ModelCacheAccess::Write(ModelCacheTtl::Explicit { seconds: 1 }),
        )
        .unwrap();
        let residence = StreamPath::new("cache/original").unwrap();
        append(
            &provider,
            &residence,
            serde_json::to_vec(&estimate).unwrap(),
        )
        .await;
        let record = read(&provider, &residence, 0).await;
        let estimate = serde_json::from_slice(&record.value).unwrap();
        let estimate_source = Source::project(&account, &residence, &record);
        assert_eq!(
            CacheResidency::project(estimate_source, &estimate, Some(([9; 16], 10)))
                .unwrap()
                .estimated_resident,
            Some(true)
        );
        assert_eq!(
            CacheResidency::project(estimate_source, &estimate, Some(([9; 16], 1_000_000_010)))
                .unwrap()
                .estimated_resident,
            Some(false)
        );
        assert_eq!(
            CacheResidency::project(estimate_source, &estimate, Some(([10; 16], 10)))
                .unwrap()
                .estimated_resident,
            None
        );
        assert_eq!(
            CacheResidency::project(estimate_source, &estimate, None)
                .unwrap()
                .estimated_resident,
            None
        );
        #[cfg(feature = "machines")]
        {
            // Capture the actual lifecycle producer's settled output. The
            // simulator supplies a logical binding, not physical placement.
            let provider_ref = crate::resources::ProviderRef::new(
                "generated".into(),
                "machines".into(),
                "1".into(),
                &mut None,
            )
            .unwrap();
            let machines = crate::machines::AgentMachines::new(
                std::sync::Arc::clone(&provider),
                vec![crate::machines::ProvisionedTarget {
                    name: "generated-target".into(),
                    host: crate::machines::MachinesHost::new(
                        std::sync::Arc::new(acyclic_machines::SimulatedMachines::default()),
                        provider_ref.clone(),
                    )
                    .unwrap(),
                    artifact: Some(
                        crate::resources::ArtifactRef::new(provider_ref, [1; 32].into(), None)
                            .unwrap(),
                    ),
                    network_policy_digest: [2; 32],
                }],
            )
            .unwrap();
            let agent = crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes([14; 16]));
            let sandbox = machines.machine(&agent, "generated-target").await.unwrap();
            let path = agent.machine("generated-target").unwrap();
            let record = read(&provider, &path, 1).await;
            assert_eq!(record.sequence, 1);
            let mut binding = serde_json::from_slice(&record.value).unwrap();
            let binding_source = Source::project(&account, &path, &record);
            let placement = Placement::project(binding_source, &binding).unwrap();
            assert_eq!(placement.binding.machine, sandbox);
            assert!(std::ptr::eq(placement.binding, &binding));
            assert!(
                placement.host.is_none()
                    && placement.region.is_none()
                    && placement.numa.is_none()
                    && placement.core.is_none()
            );
            assert!(placement.executor_shard.is_none());
            binding.version ^= 1;
            assert!(matches!(
                Placement::project(binding_source, &binding),
                Err(Error::Version)
            ));
        }
        // Projection cost is independent of source payload size. Serialization
        // remains proportional to the visible output and is not measured here.
        for size in [1, 100, 100_000] {
            let mut large = original.clone();
            large.value = vec![0; size].into();
            let measured = acyclic_test_support::measure(|| {
                std::hint::black_box(Sharing::project(&account, &root, &large).unwrap());
                std::hint::black_box(CacheCall::project(source, &decoded, Some(&price)).unwrap());
                std::hint::black_box(
                    CacheResidency::project(estimate_source, &estimate, None).unwrap(),
                );
                std::hint::black_box(Counters::project(source, Some(&totals)).unwrap());
            });
            assert_eq!((measured.count_total, measured.bytes_total), (0, 0));
        }
        let negative = acyclic_test_support::measure(|| {
            std::hint::black_box(decoded.clone());
        });
        assert!(negative.count_total > 0);
    });
}
