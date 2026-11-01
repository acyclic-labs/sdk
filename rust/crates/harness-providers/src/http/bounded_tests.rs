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
        let result = futures::executor::block_on(read(body, Some(4 * ORIGINAL), Some(&work)));
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
