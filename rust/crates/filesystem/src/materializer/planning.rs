use super::*;

pub(super) async fn generation_plan<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    from: &crate::Generation<A, O>,
    to: &crate::Generation<A, O>,
    request: &NativeWorkspacePublication<'_>,
    work: &NativeMaterializationWork,
) -> Result<(MaterializationPlan, NativeAllocation), NativeWorkspacePublicationError> {
    let (changes, changes_allocation) = from
        .diff_to(to, u32::MAX, work, work.cancellation())
        .await?;
    let (changed, changed_allocation) = changes
        .changed_paths(u32::MAX, work, work.cancellation())
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
        changed_allocation.release(retired as u64)?;
        let top = path.split('/').next().unwrap_or_default();
        let mut excluded = false;
        for name in request.excluded_names {
            if native_compare(top.bytes(), name.bytes(), work)? == std::cmp::Ordering::Equal {
                excluded = true;
                break;
            }
        }
        if excluded {
            continue;
        }
        let physical = host_stat(&root, Path::new(&path), work)?;
        let promoted =
            fresh && after_directory && physical.as_ref().is_some_and(|stat| stat.is_dir());
        if before_directory
            && !present
            && physical.as_ref().is_some_and(|stat| stat.is_dir())
            && !generation_children(&root, &path, from, None, work).await?
        {
            continue;
        }
        let (edit, structural) = if (before_directory || promoted) && after_directory {
            let (absolute, _absolute_allocation) = native_absolute_string(&path, work)?;
            let stat = to.stat(&absolute, work, work.cancellation()).await?;
            let (image, mut image_allocation) = encode_native_metadata(&stat.metadata, work)?;
            allocation.absorb(&mut image_allocation)?;
            (MaterializationEdit::SetMetadata { path, image }, false)
        } else {
            if fresh
                && regular
                && physical.is_some()
                && preflight(&root, &path, to, false, true, work).await?
            {
                continue;
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
        native_vec_push(&mut edits, (edit, structural, allocation), &mut storage)?;
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
        if !preflight(
            &root,
            edit_path(&edit),
            from,
            !matches!(edit, MaterializationEdit::SetMetadata { .. }),
            true,
            work,
        )
        .await?
        {
            return Err(NativeTreeMaterializationError::ExternalMutation.into());
        }
        allocation.absorb(&mut owned)?;
        native_vec_push(&mut plan_edits, edit, &mut allocation)?;
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
