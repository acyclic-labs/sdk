use super::*;
use acyclic_fs::{CancellationToken, WorkBudget, WorkCounters};
use proptest::prelude::*;

#[path = "retention_profile.rs"]
mod production;

pub(super) fn fixture(count: u32) -> Result<MemoryStore> {
    let mut store = super::super::tests::store(1, u64::from(count) + 1)?;
    for index in 0..count {
        store.stage(&format!("files/{index:08}"), b"x", "text/plain", "x", true)?;
    }
    Ok(store)
}
pub(super) fn budget(items: u64, bytes: u64) -> WorkBudget {
    WorkBudget {
        items_examined: items,
        source_bytes_read: bytes,
        ..Default::default()
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Each admitted comparison charges exactly one item and the two path
    // lengths; a failed next preflight charges nothing, including at a conflict.
    #[test]
    fn metered_validation_preserves_exact_receipts_and_failure_boundaries(
        rows in 0..32_u32, position in 0..32_u32,
        item_allowance in 0..40_u64, byte_allowance in 0..1600_u64,
    ) {
        let store = fixture(rows)?;
        let path = format!("files/{position:08}/child");
        let quantum = 14 + u64::try_from(path.len()).map_err(|error| TestCaseError::fail(error.to_string()))?;
        let examined = u64::from(if position < rows { position + 1 } else { rows });
        let charged = examined.min(item_allowance).min(byte_allowance / quantum);
        let io = PreparationIo::from_budget(budget(item_allowance,byte_allowance),&CancellationToken::default())?;
        let result = store.validate_retained_path(&path,Some(&io));
        prop_assert_eq!(io.receipt(),WorkCounters { items_examined: charged, source_bytes_read: charged * quantum, ..Default::default() });
        prop_assert_eq!(result.is_ok(),charged == examined && position >= rows);
        let conflict = matches!(&result, Err(Error::Invalid(message)) if message == "file path conflicts with an existing directory");
        prop_assert_eq!(conflict, charged == examined && position < rows);
    }
}

#[test]
fn unmetered_validation_matches_metered_outcomes_and_rewrite_rules() -> Result<()> {
    let store = fixture(32)?;
    for path in [
        "files",
        "files/00000003",
        "files/00000003/child",
        "files/new",
        "missing",
    ] {
        let io =
            PreparationIo::from_budget(budget(1024, 1024 * 1024), &CancellationToken::default())?;
        let before = store.validate_retained_path(path, Some(&io));
        let after = store.validate_retained_path(path, None);
        assert_eq!(before.is_ok(), after.is_ok());
        assert_eq!(
            before.err().map(|error| error.to_string()),
            after.err().map(|error| error.to_string())
        );
    }
    Ok(())
}

#[test]
fn cancellation_prevents_the_next_charge_but_empty_namespaces_still_succeed() -> Result<()> {
    for rows in [0, 8] {
        let store = fixture(rows)?;
        let cancellation = CancellationToken::default();
        let io = PreparationIo::from_budget(budget(1024, 1024 * 1024), &cancellation)?;
        cancellation.cancel();
        assert_eq!(
            store.validate_retained_path("files/new", Some(&io)).is_ok(),
            rows == 0
        );
        assert_eq!(io.receipt(), WorkCounters::default());
    }
    Ok(())
}

// Independent original production loop for differential checks and paired profiles.
pub(super) fn scan(store: &MemoryStore, path: &str, io: &PreparationIo) -> Result<()> {
    for existing in store.paths.keys() {
        let examine = WorkCounters {
            items_examined: 1,
            source_bytes_read: u64::try_from(existing.len() + path.len())
                .map_err(|error| Error::Invalid(error.to_string()))?,
            ..Default::default()
        };
        io.preflight(examine)?;
        io.charge(examine)?;
        if paths_conflict(existing, path) {
            return Err(Error::Invalid(
                "file path conflicts with an existing directory".into(),
            ));
        }
    }
    Ok(())
}
