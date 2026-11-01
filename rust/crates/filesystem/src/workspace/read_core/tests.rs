use super::*;
use crate::Fs;

#[cfg(all(feature = "local", any(unix, windows)))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collecting_checkout_retirement_preserves_the_other_checkout_credit()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
    let workspace = fs
        .create_workspace("checkout-slot-credit", WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    workspace.write_text("/entry", "payload").await?;
    let original = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    workspace
        .write_text("/unrelated", "next generation")
        .await?;
    let selected = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;

    for include_retained_slot in [false, true] {
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        // The original shared-ledger control outlives individual checkouts.
        drop(
            crate::kernel::allocation::retained::RetainedAllocation::claim(0, 0, &mut accounting)?,
        );
        let baseline = accounting
            .standalone_parts()
            .ok_or(crate::WorkError::Overflow)?
            .0
            .live_bytes();
        let selected_checkout = workspace
            .open_checkout(
                GenerationSelector::Exact(selected.id()),
                CheckoutMode::read_only_pinned(),
                &mut accounting,
                &cancellation,
            )
            .await?;
        let selected_live = accounting
            .standalone_parts()
            .ok_or(crate::WorkError::Overflow)?
            .0
            .live_bytes();
        let mut original_checkout = workspace
            .open_checkout(
                GenerationSelector::Exact(original.id()),
                CheckoutMode::read_only_pinned(),
                &mut accounting,
                &cancellation,
            )
            .await?;
        assert!(original_checkout.bytes < original_checkout.value.logical_owned_bytes());
        if include_retained_slot {
            // Negative control: release the slot here and through its original lease.
            original_checkout.bytes = original_checkout.value.logical_owned_bytes();
        }
        let retired = original_checkout.retire(&mut accounting);
        let after = accounting
            .standalone_parts()
            .ok_or(crate::WorkError::Overflow)?
            .0
            .live_bytes();
        if include_retained_slot {
            assert!(retired.is_err() || after != selected_live);
            drop(selected_checkout);
        } else {
            retired?;
            assert_eq!(
                after, selected_live,
                "retirement consumed another checkout's credit"
            );
            selected_checkout.retire(&mut accounting)?;
            assert_eq!(
                accounting
                    .standalone_parts()
                    .ok_or(crate::WorkError::Overflow)?
                    .0
                    .live_bytes(),
                baseline,
            );
        }
    }
    let read = selected
        .read_range_if_unchanged(
            &original,
            "/entry",
            crate::ByteRange {
                offset: 0,
                length: 7,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    assert_eq!(read.value.bytes.as_ref(), b"payload");
    drop((read, selected, original, workspace));
    let released = fs
        .local_root_release_barrier()
        .ok_or("local root has no release barrier")?;
    drop(fs);
    released.released().await;
    Ok(())
}

async fn comparison_peak(
    workspace: &Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>,
    selected: GenerationId,
    original: GenerationId,
    duplicate: bool,
    ceiling: u64,
) -> Result<u64, WorkspaceError> {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(
        WorkBudget {
            peak_allocation_bytes: ceiling,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let (mut selected, path) = workspace
        .open_read_path(
            GenerationSelector::Exact(selected),
            "/entry",
            &mut accounting,
            &cancellation,
        )
        .await?;
    selected
        .value
        .lookup_no_follow_with_accounting(&path.value, &mut accounting, &cancellation, true)
        .await?;
    let mut original = workspace
        .open_checkout(
            GenerationSelector::Exact(original),
            CheckoutMode::read_only_pinned(),
            &mut accounting,
            &cancellation,
        )
        .await?;
    // Negative control: reintroduce the extra admission for the same checkout.
    let _duplicate = if duplicate {
        Some(accounting.claim_owned(original.value.logical_owned_bytes(), 0)?)
    } else {
        None
    };
    original
        .value
        .lookup_no_follow_with_accounting(&path.value, &mut accounting, &cancellation, false)
        .await?;
    original.retire(&mut accounting)?;
    Ok(accounting
        .standalone_parts()
        .ok_or(crate::WorkError::Overflow)?
        .1
        .peak_allocation_bytes)
}

async fn minimum_ceiling(
    workspace: &Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>,
    selected: GenerationId,
    original: GenerationId,
    duplicate: bool,
    upper: u64,
) -> Result<u64, WorkspaceError> {
    let (mut low, mut high) = (0, upper);
    while low < high {
        let candidate = low + (high - low) / 2;
        if comparison_peak(workspace, selected, original, duplicate, candidate)
            .await
            .is_ok()
        {
            high = candidate;
        } else {
            low = candidate + 1;
        }
    }
    comparison_peak(workspace, selected, original, duplicate, low).await?;
    Ok(low)
}

#[tokio::test]
async fn cold_original_checkout_transfers_its_existing_memory_charge()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let workspace = fs
        .create_workspace(
            "original-checkout-budget",
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    workspace.write_text("/entry", "payload").await?;
    let original = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    workspace.write_text("/unrelated", "new generation").await?;
    let selected = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let upper = comparison_peak(&workspace, selected.id(), original.id(), true, u64::MAX).await?;
    // Probe/cache capacities adapt to the allowance. Find actual admitted floors,
    // rather than assuming the unbounded peak is a lower bound at smaller budgets.
    let owned = minimum_ceiling(&workspace, selected.id(), original.id(), false, upper).await?;
    let duplicated = minimum_ceiling(&workspace, selected.id(), original.id(), true, upper).await?;
    eprintln!("checkout_live_ceiling={owned} duplicate_checkout_live_ceiling={duplicated}");
    assert!(duplicated > owned, "{duplicated} <= {owned}");
    assert!(
        comparison_peak(&workspace, selected.id(), original.id(), true, owned)
            .await
            .is_err()
    );
    let denied = selected
        .read_range_if_unchanged(
            &original,
            "/entry",
            crate::ByteRange {
                offset: 0,
                length: 0,
            },
            WorkBudget {
                peak_allocation_bytes: owned - 1,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .await;
    assert!(
        matches!(&denied, Err(failure) if matches!(&failure.error,
            WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", ..
            }) | WorkspaceError::Filesystem(FsError::Work(crate::WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", ..
            }))
        )),
        "original checkout escaped its required live allocation ceiling ({owned}): {denied:?}"
    );
    let admitted = selected
        .read_range_if_unchanged(
            &original,
            "/entry",
            crate::ByteRange {
                offset: 0,
                length: 0,
            },
            WorkBudget {
                peak_allocation_bytes: owned,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .await?;
    assert!(admitted.value.bytes.is_empty());
    assert!(admitted.work.peak_allocation_bytes <= owned);
    Ok(())
}
