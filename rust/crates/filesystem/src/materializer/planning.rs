use super::publication::publication_step;
use super::*;

pub(super) async fn generation_plan<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    from: &crate::Generation<A, O>,
    to: &crate::Generation<A, O>,
    request: &NativeWorkspacePublication<'_>,
    work: &NativeMaterializationWork,
) -> Result<(MaterializationPlan, NativeAllocation), NativeWorkspacePublicationError> {
    let (changes, changes_allocation) = publication_step(work, || {
        from.diff_to(to, u32::MAX, work, work.cancellation())
    })
    .await?;
    let (changed, changed_allocation) = publication_step(work, || {
        changes.changed_paths(u32::MAX, work, work.cancellation())
    })
    .await?;
    let mut changed_allocation =
        changed_allocation.ok_or(NativeTreeMaterializationError::MissingCustody)?;
    drop(changes);
    drop(changes_allocation);
    let root = crate::native_host::HostRoot::open(request.root, &mut &*work)?;
    if root.identity() != request.source_root_identity {
        return Err(NativeTreeMaterializationError::AllocationChanged {
            expected: request.source_root_identity,
            actual: root.identity(),
        }
        .into());
    }
    let mut storage = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut edits = Vec::new();
    for change in changed {
        if let Some(edit) = generation_edit(
            change,
            &mut changed_allocation,
            &root,
            (from, to),
            request,
            work,
        )
        .await?
        {
            push_buffer(&mut edits, edit, &mut storage)?;
        }
    }
    drop(changed_allocation);
    let mut index = 0;
    while index < edits.len() {
        let mut descendant = false;
        for (parent, structural, _) in &edits {
            if *structural {
                let path = edit_path(&edits[index].0);
                let parent = edit_path(parent);
                work.charge(
                    NativeResourceCredit::Items.delta(path.len().min(parent.len()) as u64),
                )?;
                if path != parent
                    && path
                        .strip_prefix(parent)
                        .is_some_and(|suffix| suffix.starts_with('/'))
                {
                    descendant = true;
                    break;
                }
            }
        }
        if descendant {
            edits.swap_remove(index);
        } else {
            index += 1;
        }
    }
    let mut failure = None;
    edits.sort_unstable_by(|a, b| {
        match native_compare(edit_path(&a.0).bytes(), edit_path(&b.0).bytes(), work) {
            Ok(order) => order,
            Err(error) => {
                failure.get_or_insert(error);
                std::cmp::Ordering::Equal
            }
        }
    });
    if let Some(error) = failure {
        return Err(error.into());
    }
    let mut allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut plan_edits = Vec::new();
    for (edit, _, mut owned) in edits {
        allocation.absorb(&mut owned)?;
        push_buffer(&mut plan_edits, edit, &mut allocation)?;
    }
    let plan = MaterializationPlan {
        operation_id: request.operation_id,
        intent: MaterializationIntent::GenerationTransition {
            from: request.from,
            to: request.to,
        },
        edits: plan_edits,
    };
    check_native_plan(&plan, work)?;
    Ok((plan, allocation))
}

async fn generation_edit<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    change: crate::ChangedPath,
    changed_allocation: &mut NativeAllocation,
    root: &crate::native_host::HostRoot,
    endpoints: (&crate::Generation<A, O>, &crate::Generation<A, O>),
    request: &NativeWorkspacePublication<'_>,
    work: &NativeMaterializationWork,
) -> Result<Option<(MaterializationEdit, bool, NativeAllocation)>, NativeWorkspacePublicationError>
{
    let (from, to) = endpoints;
    let before_directory = change
        .before
        .is_some_and(|value| value.kind == crate::kernel::FileKind::Directory);
    let after_directory = change
        .after
        .is_some_and(|value| value.kind == crate::kernel::FileKind::Directory);
    let fresh = change.before.is_none();
    let present = change.after.is_some();
    let regular = change
        .after
        .is_some_and(|value| value.kind == crate::kernel::FileKind::Regular);
    let retired = change.path.retained_bytes();
    let (mut path, allocation) = change
        .path
        .unicode_text_with_accounting(
            &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
            work.cancellation(),
        )
        .map_err(crate::WorkspaceError::from)?;
    let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(mut allocation) =
        allocation
    else {
        return Err(NativeTreeMaterializationError::InvalidLayout.into());
    };
    let shifted = NativeResourceCredit::Copy.delta(path.len().saturating_sub(1) as u64);
    work.admit(shifted)?;
    path.remove(0);
    work.spent(shifted)?;
    drop(change);
    changed_allocation.release(retired)?;
    let top = path.split('/').next().unwrap_or_default();
    let mut excluded = false;
    for name in request.excluded_names {
        if native_compare(top.bytes(), name.bytes(), work)? == std::cmp::Ordering::Equal {
            excluded = true;
            break;
        }
    }
    if excluded {
        return Ok(None);
    }
    let physical = host_stat(root, Path::new(&path), work)?;
    let promoted = fresh && after_directory && physical.as_ref().is_some_and(|stat| stat.is_dir());
    if before_directory
        && !present
        && physical.as_ref().is_some_and(|stat| stat.is_dir())
        && publication_step(work, || {
            host_directory_holds_unknown_entries(root, &path, from, work)
        })
        .await?
    {
        return Ok(None);
    }
    let (edit, structural) = if (before_directory || promoted) && after_directory {
        let (absolute, _absolute_allocation) = work.display(&format_args!("/{path}"))?;
        let stat = publication_step(work, || to.stat(&absolute, work, work.cancellation())).await?;
        let (image, mut image_allocation) = encode_native_metadata(&stat.metadata, work)?;
        allocation.absorb(&mut image_allocation)?;
        (MaterializationEdit::SetMetadata { path, image }, false)
    } else {
        if fresh
            && regular
            && physical.is_some()
            && publication_step(work, || host_holds_generation_file(root, &path, to, work)).await?
        {
            return Ok(None);
        }
        let structural = before_directory || after_directory;
        let edit = if present {
            MaterializationEdit::Install {
                path,
                image: copy_image(&[1], &mut allocation)?,
            }
        } else {
            MaterializationEdit::Remove { path }
        };
        (edit, structural)
    };
    Ok(Some((edit, structural, allocation)))
}
