use super::*;
use proptest::prelude::*;
use std::sync::atomic::Ordering;

proptest! {
    // Property: binding preserves exact source identities and declaration/stage
    // order. Borrowing through a cloned pipeline shares the same references and
    // performs no IO even after all backing files become unavailable.
    #[test]
    fn binding_retains_configuration_sources_without_copying_or_reloading(
        groups in prop::collection::vec(prop::collection::vec(".{0,24}", 0..6), 0..5),
    ) {
        futures::executor::block_on(async {
            let payloads = TestPayloadStore::new()?;
            let mut expected = Vec::new();
            let mut pipeline = ContextPipeline::default();
            for (group, bodies) in groups.into_iter().enumerate() {
                let mut sources = Vec::new();
                for (index, body) in bodies.into_iter().enumerate() {
                    let file = payloads.stage(
                        crate::OperationId::new(), &format!("config/{group}/{index}.json"),
                        body.as_bytes(), "application/json", "configuration.json", None, None,
                    )?.await?.0;
                    sources.push(file);
                }
                expected.extend(sources.iter().cloned());
                let manifest = ContextManifest { sources, ..Default::default() };
                let reference = manifest.publish(&payloads, crate::OperationId::new(),
                    &format!("manifest/{group}.json"), payload_limits()).await?;
                let (_, stage) = ContextManifest::bind(reference, &payloads,
                    crate::tool::ToolRegistry::default(), payload_limits()).await?;
                pipeline = pipeline.with(stage);
            }
            let reads = payloads.reads.load(Ordering::SeqCst);
            payloads.files.lock().unwrap().clear();
            let shared = pipeline.clone();
            #[cfg(not(target_arch = "wasm32"))]
            {
                let allocation_free = |collect| {
                    let measured = acyclic_test_support::measure(|| {
                        let matches = if collect {
                            std::hint::black_box(pipeline.configuration_sources().collect::<Vec<_>>())
                                .into_iter().eq(expected.iter())
                        } else {
                            pipeline.configuration_sources().eq(expected.iter())
                        };
                        assert!(matches);
                    });
                    (measured.count_total, measured.bytes_total) == (0, 0)
                };
                prop_assert!(allocation_free(false), "creating and consuming the iterator allocated");
                if !expected.is_empty() {
                    prop_assert!(!allocation_free(true), "temporary Vec allocation escaped detection");
                }
            }
            let retained = pipeline.configuration_sources().collect::<Vec<_>>();
            prop_assert!(retained.iter().copied().eq(expected.iter()));
            for (original, cloned) in retained.iter().zip(shared.configuration_sources()) {
                prop_assert!(std::ptr::eq(*original, cloned), "pipeline clone copied a source");
            }
            prop_assert_eq!(payloads.reads.load(Ordering::SeqCst), reads);
            // The former stage discarded this entire list after verification.
            // The same identity/order detector rejects that representation.
            if !expected.is_empty() {
                let discarded = ContextPipeline::default();
                prop_assert!(!discarded.configuration_sources().eq(expected.iter()));
            }
            Ok(())
        })?;
    }
}
