use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};
use bytes::Bytes;

#[test]
fn relocation_requires_credit_for_both_original_and_replacement_buffers() -> Result<()> {
    const ORIGINAL: usize = 4096;
    // The former capacity-delta claim fit this budget, although reallocation
    // can retain the original buffer while allocating its full replacement.
    for overlap in [false, true] {
        let baseline = NativeMaterializationWork::accounting_state_bytes();
        let mut budget = acyclic_fs::performance::work_budget(100_000, 8_000_000);
        budget.peak_allocation_bytes = baseline + 64 + (2 + u64::from(overlap)) * ORIGINAL as u64;
        let work = NativeMaterializationWork::new(budget, &CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        {
            let _original =
                work.claim(64 + ORIGINAL as u64, 1, acyclic_fs::WorkCounters::UNCHARGED)?;
            let _former_delta =
                work.claim(ORIGINAL as u64, 1, acyclic_fs::WorkCounters::UNCHARGED)?;
        }
        let body = Box::pin(futures::stream::iter([
            Ok(Bytes::from(vec![b' '; ORIGINAL])),
            Ok(Bytes::from_static(b"x")),
        ]));
        let result = futures::executor::block_on(read(body, 4 * ORIGINAL, Some(&work)));
        if overlap {
            let body = result?;
            assert_eq!(body.bytes.len(), ORIGINAL + 1);
            assert_eq!(&body.bytes[..ORIGINAL], vec![b' '; ORIGINAL]);
            assert_eq!(body.bytes[ORIGINAL], b'x');
            assert!(body.custody.as_ref().is_some_and(|hold| {
                hold.work().shares_allocation_context(&work)
                    && hold.bytes() == 64 + body.bytes.capacity() as u64
            }));
            drop(body);
        } else {
            assert!(
                matches!(
                    result,
                    Err(Error::NativeWork {
                        cause: acyclic_fs::NativeTreeMaterializationError::Work(
                            acyclic_fs::WorkError::BudgetExceeded {
                                counter: "peak_allocation_bytes",
                                ..
                            }
                        ),
                        ..
                    })
                ),
                "relocation was admitted without overlap credit"
            );
        }
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn captured_json_fragmentation_obeys_bytes_and_caller_work(
        width in 1usize..32, empty in 1usize..5,
    ) {
        // Real llama.cpp b11889 context refusal, captured 2026-10-26;
        // only transport fragmentation changes, never the response bytes.
        let recorded = include_bytes!("../../../conformance/fixtures/inference/llamacpp/context.body");
        let fragments: Vec<_> = recorded.chunks(width).flat_map(|bytes| {
            std::iter::repeat_n(Bytes::new(), empty)
                .chain(std::iter::once(Bytes::from_static(bytes)))
        }).collect();
        let chunks = fragments.len() as u64;
        for admitted in [false, true] {
            let baseline = NativeMaterializationWork::accounting_state_bytes();
            let mut budget = acyclic_fs::performance::work_budget(100_000, 8_000_000);
            budget.items_examined = chunks - u64::from(!admitted);
            let work = NativeMaterializationWork::new(budget, &CancellationToken::new()).unwrap();
            let source = Box::pin(futures::stream::iter(fragments.iter().cloned().map(Ok).collect::<Vec<_>>()));
            let result = futures::executor::block_on(read(source, recorded.len(), Some(&work)));
            if admitted {
                let body = result.unwrap();
                proptest::prop_assert_eq!(body.bytes.as_slice(), recorded.as_slice());
                proptest::prop_assert_eq!(work.receipt().items_examined, chunks);
                proptest::prop_assert!(work.receipt().bytes_copied < 3 * recorded.len() as u64);
                proptest::prop_assert_eq!(body.custody.as_ref().unwrap().bytes(), 64 + body.bytes.capacity() as u64);
                drop(body);
                // Restore the removed counter: byte-valid bodies can exceed
                // that unrelated chunk cap at the same maximum byte count.
                if chunks > recorded.len() as u64 {
                    let former = fragments.iter().enumerate()
                        .try_for_each(|(index, _)| (index < recorded.len()).then_some(()));
                    proptest::prop_assert_eq!(former, None);
                }
            } else {
                proptest::prop_assert!(matches!(result, Err(Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Work(
                        acyclic_fs::WorkError::BudgetExceeded { counter: "items_examined", .. }
                    ), ..
                })), "the caller's item allowance must end collection");
            }
            proptest::prop_assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
}
