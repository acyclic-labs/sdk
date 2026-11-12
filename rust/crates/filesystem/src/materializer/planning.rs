use super::publication::publication_step;
use super::*;

pub(super) async fn generation_plan<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    from: &crate::Generation<A, O>,
    to: &crate::Generation<A, O>,
    request: &NativeWorkspacePublication<'_>,
    work: &NativeMaterializationWork,
) -> Result<(MaterializationPlan, NativeAllocation), NativeWorkspacePublicationError> {
    let (changes, changes_allocation) =
        publication_step(work, || from.diff_to(to, None, work, work.cancellation())).await?;
    let (changed, changed_allocation) = publication_step(work, || {
        changes.changed_paths(None, work, work.cancellation())
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
    let mut allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut edits: Vec<MaterializationEdit> = Vec::new();
    // Changed paths arrive in component order, so a structural ancestor
    // immediately precedes its contiguous descendants.
    let mut structural_parent = None;
    for change in changed {
        if let Some((edit, structural, mut owned)) = generation_edit(
            change,
            &mut changed_allocation,
            &root,
            (from, to),
            request,
            work,
        )
        .await?
        {
            if let Some(index) = structural_parent {
                let parent = edit_path(&edits[index]);
                let path = edit_path(&edit);
                work.charge(
                    NativeResourceCredit::Items.delta(path.len().min(parent.len()) as u64),
                )?;
                if path
                    .strip_prefix(parent)
                    .is_some_and(|suffix| suffix.starts_with('/'))
                {
                    continue;
                }
            }
            if structural {
                structural_parent = Some(edits.len());
            }
            allocation.absorb(&mut owned)?;
            push_buffer(&mut edits, edit, &mut allocation)?;
        }
    }
    drop(changed_allocation);
    let plan = MaterializationPlan {
        operation_id: request.operation_id,
        intent: MaterializationIntent::GenerationTransition {
            from: request.from,
            to: request.to,
        },
        edits,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn merge_publication_touches_only_changed_files() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut changed_cost = None;
        for count in [32, 256] {
            let directory = tempfile::tempdir()?;
            let cancellation = crate::CancellationToken::new();
            let budget = crate::WorkBudget::UNBOUNDED;
            let workspace = crate::Fs::memory()
                .create_workspace("parent", budget, &cancellation)
                .await?
                .value;
            let mut seed = workspace
                .begin_transaction(crate::IdempotencyKey::new(), budget, &cancellation)
                .await?
                .value;
            seed.create_dir_all("/directory", budget, &cancellation)
                .await?;
            for index in 0..count {
                seed.write_text(&format!("/directory/file-{index:03}"), "original")
                    .await?;
            }
            seed.commit::<crate::WorkspaceError, _, _>(
                None,
                crate::PublicationPermit::Unrestricted,
                budget,
                &cancellation,
                |_| async {
                    Ok(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await?;
            let before = workspace.head(budget, &cancellation).await?.value;
            let child = workspace
                .fork::<_, _, crate::WorkspaceError>(
                    "child",
                    crate::ForkOptions::from_generation(
                        before.clone(),
                        crate::IdempotencyKey::new(),
                    ),
                    crate::PublicationPermit::Unrestricted,
                    budget,
                    &cancellation,
                    (),
                )
                .await?
                .value;
            let root = directory.path().join("checkout");
            std::fs::create_dir_all(root.join("directory"))?;
            for index in 0..count {
                std::fs::write(root.join(format!("directory/file-{index:03}")), "original")?;
            }
            let store = crate::LocalCoreStateStore::new(directory.path().join("state"));
            for changed in [false, true] {
                let before = workspace.head(budget, &cancellation).await?.value;
                if changed {
                    child.write_text("/directory/file-017", "updated").await?;
                }
                let plan = child
                    .join_into(&workspace)
                    .plan(budget, &cancellation)
                    .await?
                    .value;
                let outcome = plan
                    .apply(
                        crate::ApplyOptions {
                            if_target: before.id(),
                            idempotency_key: crate::IdempotencyKey::new(),
                        },
                        std::collections::BTreeMap::new(),
                        crate::PublicationPermit::Unrestricted,
                    )
                    .await?;
                let after = match outcome {
                    crate::JoinOutcome::Applied(result)
                    | crate::JoinOutcome::AlreadyApplied(result) => result.generation,
                    crate::JoinOutcome::NoChanges(generation) => generation,
                    _ => return Err("join refused".into()),
                };
                let work = NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(cause, _)| cause)?;
                let operation = directory.path().join("operation");
                let options = crate::MaterializeOptions::native(operation.join("target"));
                work.state.lock().write_probe = Some(crate::demand::WriteProbe {
                    writes: 0,
                    stop_before: None,
                });
                let identities = (0..count)
                    .map(|index| {
                        Ok(crate::file_identity(&std::fs::File::open(
                            root.join(format!("directory/file-{index:03}")),
                        )?)?)
                    })
                    .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
                let receipt = super::super::publication::publish_native_generation_transition(
                    &before,
                    &after,
                    &store,
                    NativeWorkspacePublication {
                        root: &root,
                        source_root_identity: work.root_identity(&root)?,
                        operation_directory: &operation,
                        operation_id: OperationId::new(),
                        from: before.id(),
                        to: after.id(),
                        excluded_names: &[],
                        options: &options,
                        budget,
                        cancellation: &cancellation,
                    },
                    work.clone(),
                    || std::future::ready(Ok(())),
                )
                .await
                .map_err(|(cause, _)| cause)?;
                let writes = work.state.lock().write_probe.take().unwrap().writes;
                let plan = &receipt.value.plan;
                let mut rewritten = 0;
                for (index, before) in identities.into_iter().enumerate() {
                    let file =
                        std::fs::File::open(root.join(format!("directory/file-{index:03}")))?;
                    rewritten += usize::from(crate::file_identity(&file)? != before);
                }
                assert_eq!(rewritten, usize::from(changed));
                if changed {
                    assert_eq!(plan.edits.len(), 1);
                    assert_eq!(edit_path(&plan.edits[0]), "directory/file-017");
                    assert_eq!(std::fs::read(root.join("directory/file-017"))?, b"updated");
                    let cost = (
                        receipt.work.materializations,
                        writes,
                        receipt.work.source_bytes_read,
                        receipt.work.object_bytes_written,
                    );
                    let baseline = *changed_cost.get_or_insert(cost);
                    assert_eq!(cost.0, baseline.0);
                    assert_eq!(cost.1, baseline.1);
                    assert!(cost.2 <= baseline.2 + 256);
                    assert!(cost.3 <= baseline.3 + 256);
                    eprintln!(
                        "merge_publication files={count} changed=1 materializations={} writes={} read={} written={}",
                        cost.0, cost.1, cost.2, cost.3
                    );
                } else {
                    assert!(plan.edits.is_empty());
                    assert_eq!(receipt.work.materializations, 0);
                    assert_eq!(receipt.work.source_bytes_read, 0);
                    assert_eq!(receipt.work.object_bytes_written, 0);
                    assert_eq!(writes, 0, "an unchanged merge must not append a journal");
                    eprintln!(
                        "merge_publication files={count} changed=0 materializations=0 writes=0 read=0 written=0"
                    );
                }
            }
        }
        Ok(())
    }
}
