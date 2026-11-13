pub(super) use crate::javascript_allocator::{BridgeText, EngineAllocator, EngineResult};

#[cfg(test)]
mod tests {
    use super::*;

    fn native_work(bytes: u64) -> crate::Result<acyclic_fs::NativeMaterializationWork> {
        // The ledger owns its own Arc before the allocation under test starts.
        let maximum = acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + bytes;
        acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(1_000_000, maximum),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })
    }

    #[tokio::test]
    async fn bridge_text_keeps_original_credit_and_refuses_an_unfunded_copy() -> crate::Result<()> {
        let runtime = rquickjs::AsyncRuntime::new().unwrap();
        let context = rquickjs::AsyncContext::full(&runtime).await.unwrap();
        for bytes in [1, 8192] {
            let work = native_work(bytes)?;
            let baseline = work.live_allocation_bytes();
            let allocator = EngineAllocator::new(Some(&work));
            context
                .with(|ctx| {
                    let text = rquickjs::String::from_str(ctx, &"x".repeat(4096)).unwrap();
                    let copy = allocator.copy_text(&text);
                    if bytes == 1 {
                        assert!(copy.is_err());
                        assert!(allocator.failure().lock().unwrap().is_some());
                    } else {
                        let copy = copy.unwrap();
                        assert_eq!(copy.value.len(), 4096);
                        assert_eq!(work.live_allocation_bytes(), baseline + 4096);
                        drop(copy);
                    }
                    assert_eq!(work.live_allocation_bytes(), baseline);
                })
                .await;
        }
        Ok(())
    }

    fn work_with_budget(
        budget: acyclic_fs::WorkBudget,
    ) -> crate::Result<acyclic_fs::NativeMaterializationWork> {
        acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new())
            .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })
    }

    #[tokio::test]
    async fn real_engine_releases_native_memory_and_preserves_ledger_refusal() -> crate::Result<()>
    {
        let original = native_work(64 * 1024 * 1024)?;
        let baseline = original.live_allocation_bytes();
        let ledger_operations = original.receipt().allocation_operations;
        let runtime = rquickjs::AsyncRuntime::new_with_alloc(EngineAllocator::new(Some(&original)))
            .expect("funded runtime construction");
        let startup_operations = original.receipt().allocation_operations - ledger_operations;
        drop(runtime);
        assert_eq!(original.live_allocation_bytes(), baseline);
        assert!(startup_operations > 0);
        for admitted in 0..startup_operations {
            let original = work_with_budget(acyclic_fs::WorkBudget {
                allocation_operations: ledger_operations + admitted,
                ..acyclic_fs::WorkBudget::UNBOUNDED
            })?;
            let baseline = original.live_allocation_bytes();
            let allocator = EngineAllocator::new(Some(&original));
            let failure = allocator.failure();
            let runtime = rquickjs::AsyncRuntime::new_with_alloc(allocator);
            // QuickJS can recover an optional allocation failure. Both a
            // returned runtime and a refused startup must retire their bytes.
            let refused = runtime.is_err();
            drop(runtime);
            assert_eq!(
                original.live_allocation_bytes(),
                baseline,
                "runtime allocation prefix {admitted}/{startup_operations}"
            );
            if refused {
                assert!(matches!(
                    failure.lock().unwrap().as_ref(),
                    Some(crate::Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::Work(
                            acyclic_fs::WorkError::BudgetExceeded {
                                counter: "allocation_operations",
                                ..
                            }
                        ),
                        ..
                    })
                ));
            }
        }
        for bytes in [1, 64 * 1024 * 1024] {
            let work = native_work(bytes)?;
            let baseline = work.live_allocation_bytes();
            let allocator = EngineAllocator::new(Some(&work));
            let failure = allocator.failure();
            let (send, _receive) = tokio::sync::mpsc::channel(1);
            let result = super::super::interpreter::run(
                "export default Array.from({length: 1000}, (_, i) => ({i})).length".into(),
                acyclic_stream::OwnedBytes::from_static(b"[]"),
                send,
                tokio_util::sync::CancellationToken::new(),
                4096,
                allocator,
                super::super::interpreter::INTERRUPT_BUDGET,
                false,
            )
            .await;
            if bytes == 1 {
                assert!(result.is_err());
                assert!(matches!(
                    failure.lock().unwrap().as_ref(),
                    Some(crate::Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::Work(
                            acyclic_fs::WorkError::BudgetExceeded {
                                counter: "peak_allocation_bytes",
                                maximum,
                                ..
                            }
                        ), ..
                    }) if *maximum == baseline + bytes
                ));
            } else {
                assert_eq!(result.unwrap().value, serde_json::json!(1000));
                assert!(failure.lock().unwrap().is_none());
            }
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        Ok(())
    }

    #[tokio::test]
    async fn engine_results_retain_original_credit_and_refuse_unfunded_decoding()
    -> crate::Result<()> {
        let runtime = rquickjs::AsyncRuntime::new().unwrap();
        let context = rquickjs::AsyncContext::full(&runtime).await.unwrap();
        for (bytes, cancelled) in [(1, false), (1024 * 1024, false), (1024 * 1024, true)] {
            let work = native_work(bytes)?;
            let baseline = work.live_allocation_bytes();
            let allocator = EngineAllocator::new(Some(&work));
            if cancelled {
                work.cancellation().cancel();
            }
            context.with(|ctx| {
                // Capture the actual engine encoding, including escaped and
                // multi-byte text, rather than supplying a second JSON encoder.
                let json: rquickjs::String = ctx.eval(
                    r#"JSON.stringify({text:'β\\n'.repeat(64), values:Array.from({length:4}, (_, i) => ({i}))})"#
                ).unwrap();
                let json = json.to_cstring().unwrap();
                // The engine owns the source encoding. The original result
                // ledger must admit its preparation and decoded storage.
                let result = allocator.decode_result(json.as_str().as_bytes());
                if cancelled {
                    assert!(result.is_err());
                    let failure = allocator.failure();
                    let failure = failure.lock().unwrap();
                    assert!(matches!(failure.as_ref(),
                        Some(crate::Error::NativeWork {
                            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, ..
                        })
                    ), "{failure:?}");
                } else if bytes == 1 {
                    assert!(result.is_err());
                    let failure = allocator.failure();
                    let failure = failure.lock().unwrap();
                    assert!(matches!(failure.as_ref(),
                        Some(crate::Error::NativeWork {
                            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                                acyclic_fs::WorkError::BudgetExceeded {
                                    counter: "peak_allocation_bytes", observed, maximum,
                                }
                            ), ..
                        }) if *maximum == baseline + bytes && *observed > *maximum
                    ), "{failure:?}");
                } else {
                    let result = result.unwrap();
                    assert_eq!(result.value["text"].as_str().unwrap(), "β\\n".repeat(64));
                    assert_eq!(result.value["values"][3]["i"], 3);
                    let retained = result.custody.as_ref().unwrap().bytes();
                    assert!(retained > 0);
                    assert_eq!(work.live_allocation_bytes(), baseline + retained);
                    assert!(allocator.failure().lock().unwrap().is_none());
                    drop(result);
                }
                assert_eq!(work.live_allocation_bytes(), baseline);
            }).await;
        }
        // The production interpreter must transfer that same custody out of
        // the runtime; every engine allocation is already retired on return.
        let work = native_work(8 * 1024 * 1024)?;
        let baseline = work.live_allocation_bytes();
        let allocator = EngineAllocator::new(Some(&work));
        let failure = allocator.failure();
        let (send, _receive) = tokio::sync::mpsc::channel(1);
        let result = super::super::interpreter::run(
            "export default {text:'x'.repeat(4096)}".into(),
            acyclic_stream::OwnedBytes::from_static(b"[]"),
            send,
            tokio_util::sync::CancellationToken::new(),
            8192,
            allocator,
            super::super::interpreter::INTERRUPT_BUDGET,
            false,
        )
        .await
        .unwrap();
        assert_eq!(result.value["text"].as_str().unwrap().len(), 4096);
        assert_eq!(
            work.live_allocation_bytes(),
            baseline + result.custody.as_ref().unwrap().bytes()
        );
        assert!(failure.lock().unwrap().is_none());
        drop(result);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }
}
