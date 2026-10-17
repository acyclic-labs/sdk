use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork};

#[test]
fn sole_highlight_moves_its_pointer_and_custody_without_copying() -> Result<()> {
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(16_384, 16 * 1_024 * 1_024),
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    for length in [0, 1, 8192] {
        let bytes = serde_json::to_vec(&serde_json::json!({"results":[{
            "url":"https://example.com", "highlights":["東".repeat(length)]
        }]}))
        .map_err(|e| crate::Error::Invalid(e.to_string()))?;
        let raw = decode::<ExaResponse>(&bytes, Some(&work))?;
        let pointer = raw.0.results[0].highlights[0].as_ptr();
        let before = work.receipt();
        let result = normalize_exa(raw, 1, Some(&work))?;
        assert_eq!(result.0.results[0].snippet.as_ptr(), pointer);
        assert_eq!(result.0.results[0].snippet, "東".repeat(length));
        assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(work.receipt().items_examined - before.items_examined, 2);
        assert_eq!(
            work.receipt().allocation_operations - before.allocation_operations,
            1
        );
        assert_eq!(
            result.1.as_ref().unwrap().bytes(),
            crate::search::native::response_bytes(&result.0)?
        );
        assert!(result.1.as_ref().unwrap().work().shares_ledger(&work));
        drop(result);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}
