use super::*;
use std::cell::Cell;

thread_local! {
    static COPIES: Cell<Option<usize>> = const { Cell::new(None) };
}
pub(super) fn observe() {
    COPIES.with(|copies| copies.set(copies.get().map(|count| count + 1)));
}

#[tokio::test]
async fn lookup_copies_only_returned_receipts() -> Result<(), Box<dyn std::error::Error>> {
    let mut checkout = crate::facade::test_fixtures::folded_checkout([]).await?;
    let cancellation = crate::CancellationToken::new();
    let path = crate::kernel::NamespacePath::new(vec![], checkout.volume_config().limits)?;
    let mut accounting = KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
    COPIES.set(Some(0));
    let result = checkout
        .lookup_no_follow_with_accounting(&path, &mut accounting, &cancellation, true)
        .await;
    let copies = COPIES.replace(None).unwrap();
    let (lookup, allocation) = result?;
    assert!(lookup.record.is_some());
    assert_eq!(lookup.work, accounting.spent());
    // Kernel, current checkout, then completed outer lookup: no discarded copies.
    assert_eq!(copies, 3, "lookup made a discarded accounting snapshot");
    drop(allocation);
    Ok(())
}
