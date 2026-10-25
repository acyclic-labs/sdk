use super::*;

// Full namespace scans arrive in file-identity order. Append first so a tiny
// directory rename never shifts the unrelated namespace once per path.
pub(super) fn append(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation),
    path: &NamespacePath,
    record: Option<crate::kernel::FileRecord>,
    before: bool,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
    let (path, mut allocation) = path.clone_with_accounting(accounting, cancellation)?;
    let copied = copy_work(std::mem::size_of::<ChangedPath>() as u64);
    accounting.admit(copied)?;
    paths.1.absorb(&mut allocation)?;
    paths.0.push(ChangedPath {
        path,
        before: if before { record } else { None },
        after: if before { None } else { record },
    });
    accounting.record_completed(copied)?;
    Ok(())
}

pub(super) fn sort_and_coalesce(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation),
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let count = paths.0.len();
    crate::kernel::sort_by_with_accounting(&mut paths.0, accounting, |left, right| {
        left.path.components().cmp(right.path.components())
    })?;
    let mut retained = 0_usize;
    let mut retired_bytes = 0_u64;
    for index in 0..count {
        accounting.charge_items(1)?;
        let entry = paths.0.get(index).ok_or(FsError::InvalidDiff)?;
        let previous = retained.checked_sub(1).and_then(|i| paths.0.get(i));
        if previous.is_some_and(|previous| previous.path == entry.path) {
            let (before, after) = (entry.before, entry.after);
            retired_bytes = retired_bytes
                .checked_add(entry.path.retained_bytes())
                .ok_or(crate::WorkError::Overflow)?;
            let copied =
                copy_work((2 * std::mem::size_of::<Option<crate::kernel::FileRecord>>()) as u64);
            accounting.admit(copied)?;
            let previous = paths.0.get_mut(retained - 1).ok_or(FsError::InvalidDiff)?;
            previous.before = previous.before.or(before);
            previous.after = previous.after.or(after);
            accounting.record_completed(copied)?;
        } else {
            if retained != index {
                swap(&mut paths.0, retained, index, accounting)?;
            }
            retained += 1;
        }
    }
    paths.0.truncate(retained);
    accounting.release_owned(&mut paths.1, retired_bytes)?;
    accounting.check()?;
    Ok(())
}

fn swap(
    values: &mut [ChangedPath],
    left: usize,
    right: usize,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let copied = copy_work((2 * std::mem::size_of::<ChangedPath>()) as u64);
    accounting.admit(copied)?;
    values.swap(left, right);
    accounting.record_completed(copied)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_path_merge_preserves_boundary_copy_budget() -> Result<(), Box<dyn std::error::Error>> {
        let budget = crate::performance::BOUNDARY_BUDGET;
        let cancel = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(budget, &cancel);
        let limits = crate::model::VolumeLimits::default();
        let mut paths = (Vec::new(), accounting.claim_owned(0, 0)?);
        // File identity order may oppose path order. Both retained endpoints
        // must merge without copying every previously collected path again.
        for before in [true, false] {
            for index in (0..2048).rev() {
                let path = NamespacePath::from_portable(
                    &crate::path::PortablePath::parse(&format!("/file-{index:04}"), limits)?,
                    limits,
                )?;
                append(&mut paths, &path, None, before, &mut accounting, &cancel)?;
            }
        }
        sort_and_coalesce(&mut paths, &mut accounting)?;
        assert_eq!(paths.0.len(), 2048);
        assert!(
            paths
                .0
                .iter()
                .zip(paths.0.iter().skip(1))
                .all(|(left, right)| {
                    left.path.components().cmp(right.path.components()).is_lt()
                })
        );
        let work = accounting.spent();
        assert!(work.bytes_copied < budget.bytes_copied);
        println!("PATH_MERGE_WORK {work:?}");
        Ok(())
    }
}
