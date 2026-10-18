use super::*;
use acyclic_fs::{CancellationToken, WorkBudget};
use proptest::prelude::*;

thread_local! {
    /// Reintroduces the reviewed allocation-before-admission defect inside
    /// the production append path; ordinary builds contain no fault switch.
    pub(super) static ALLOCATE_BEFORE_ADMISSION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

const LINE: usize = 1 << 20;

fn budget(peak: u64, copied: u64) -> WorkBudget {
    WorkBudget {
        bytes_copied: copied,
        allocation_operations: 64,
        peak_allocation_bytes: peak,
        items_examined: 4096,
        ..WorkBudget::default()
    }
}

fn ledger(budget: WorkBudget) -> Result<PreparationIo> {
    PreparationIo::from_budget(budget, &CancellationToken::new())
}

fn denied_before_growth(length: usize, copy_short: bool) -> Result<()> {
    let line = vec![b'x'; length];
    let peak = length as u64 - u64::from(!copy_short);
    let copied = length as u64 - u64::from(copy_short);
    let allowance = budget(peak, copied);
    let io = ledger(allowance)?;
    let mut retained = Retained::default();
    let result = retained.append(&line, 0, &io);
    assert_eq!(retained.bytes.capacity(), 0, "allocation before admission");
    assert!(result.is_err());
    assert_eq!(io.owned_bytes()?, 0);
    let left = io.remaining()?;
    assert_eq!(left.allocation_operations, allowance.allocation_operations);
    assert_eq!(left.bytes_copied, allowance.bytes_copied);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Denying either storage or copying must precede allocation and copying,
    // for every selected-window length, rather than only the 1-MiB probe.
    #[test]
    fn denied_windows_never_allocate(length in 1usize..65_537, copy_short in any::<bool>()) {
        denied_before_growth(length, copy_short).unwrap();
    }
}

#[test]
fn negative_control_detects_allocation_before_admission() {
    ALLOCATE_BEFORE_ADMISSION.with(|flag| flag.set(true));
    let detected = std::panic::catch_unwind(|| denied_before_growth(17, false).unwrap());
    ALLOCATE_BEFORE_ADMISSION.with(|flag| flag.set(false));
    let panic = detected.expect_err("the real append mutant must violate the same denial oracle");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(message.contains("allocation before admission"), "{message}");
}

#[test]
fn growth_is_claimed_before_any_allocation_or_copy() -> Result<()> {
    let line = vec![b'a'; LINE];
    // A memory allowance one byte short denies before allocating or copying.
    let short = budget(LINE as u64 - 1, LINE as u64);
    let io = ledger(short)?;
    let mut retained = Retained::default();
    assert!(retained.append(&line, 0, &io).is_err());
    assert_eq!(retained.bytes.capacity(), 0);
    assert_eq!(io.owned_bytes()?, 0);
    let left = io.remaining()?;
    assert_eq!(left.allocation_operations, short.allocation_operations);
    assert_eq!(left.bytes_copied, short.bytes_copied);
    // A copy allowance one byte short also denies before allocating.
    let uncopyable = budget(LINE as u64, LINE as u64 - 1);
    let io = ledger(uncopyable)?;
    assert!(retained.append(&line, 0, &io).is_err());
    assert_eq!(retained.bytes.capacity(), 0);
    assert_eq!(io.owned_bytes()?, 0);
    assert_eq!(
        io.remaining()?.allocation_operations,
        uncopyable.allocation_operations
    );
    // Control: the exact allowance admits the line, claims every byte held
    // and charges one allocation and the copy.
    let exact = budget(LINE as u64, LINE as u64);
    let io = ledger(exact)?;
    retained.append(&line, 0, &io)?;
    assert_eq!(retained.bytes, line);
    assert_eq!(io.owned_bytes()?, retained.bytes.capacity() as u64);
    let left = io.remaining()?;
    assert_eq!(left.allocation_operations, exact.allocation_operations - 1);
    assert_eq!(left.bytes_copied, 0);
    Ok(())
}

#[test]
fn growth_is_amortized_and_never_claims_past_the_file() -> Result<()> {
    let generous = budget(1 << 24, 1 << 24);
    let io = ledger(generous)?;
    let mut retained = Retained::default();
    let chunk = [b'x'; 64];
    let total = 4096 * chunk.len();
    for index in 0..4096 {
        let remaining = (total - (index + 1) * chunk.len()) as u64;
        retained.append(&chunk, remaining, &io)?;
    }
    assert_eq!(retained.bytes.len(), total);
    // Doubling growth: logarithmically many allocations, not one per chunk.
    let allocations = generous.allocation_operations - io.remaining()?.allocation_operations;
    assert!(allocations <= 13, "{allocations} allocations");
    // The file bounds growth: nothing beyond its length is ever claimed.
    assert_eq!(retained.bytes.capacity(), total);
    assert_eq!(io.owned_bytes()?, total as u64);
    Ok(())
}
