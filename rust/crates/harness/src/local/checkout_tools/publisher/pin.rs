//! Pin only the requested lazy source file before producing its durable FileRef.
use super::*;
use acyclic_fs::facade::FsWork as _;

#[cfg(all(test, not(acyclic_harness_loom)))]
thread_local! {
    pub(super) static OMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(super) async fn path(
    view: &workspace::View,
    operation: OperationId,
    relative: &str,
    maximum: u64,
) -> Result<()> {
    #[cfg(all(test, not(acyclic_harness_loom)))]
    if OMIT.with(std::cell::Cell::get) {
        return Ok(());
    }
    let cancellation = acyclic_fs::CancellationToken::new();
    let pinned = view
        .promote(
            &format!("/{relative}"),
            maximum,
            acyclic_fs::IdempotencyKey::from_bytes(
                path_operation("acyclic.local.checkout-read/v1", operation, relative).into_bytes(),
            ),
            PublicationPermit::Unrestricted,
            &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
        )
        .await?;
    match pinned {
        acyclic_fs::TransactionCommit::Committed(_)
        | acyclic_fs::TransactionCommit::AlreadyCommitted(_) => Ok(()),
        _ => Err(Error::Conflict(
            "checkout changed while pinning the read".into(),
        )),
    }
}
