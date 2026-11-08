use super::*;

fn preserved(source: &MemorySource, path: &MountPath) -> Result<bool, MountSourceError> {
    Ok(source
        .lookup(path)?
        .is_some_and(|entry| entry.node.logical_bytes == 8)
        && source.read_range(path, 0, 8)?.as_ref() == b"original")
}

fn operation(
    original: &MemoryCheckout,
    rename: bool,
    allowance: Option<u64>,
    item_budget: bool,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let setup = fuse_fixture_work();
    let cancellation = crate::CancellationToken::new();
    let checkout = original.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::native(&setup, &cancellation),
        &cancellation,
    )?;
    let source = Arc::new(
        crate::native_mount::adapter::tests::access::bind_test_access(
            MemorySource::new(
                Arc::new(crate::native_mount::SharedCheckout::new(checkout)),
                posix_config(),
            )?,
            vec![],
            None,
        )?,
    );
    let path = name("retained");
    let projection = super::super::FuseProjection {
        core: projection_core(
            source.clone(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?,
    };
    let inode = projection
        .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("retained"), &setup)?
        .attr
        .ino
        .0;
    let (handle, _) = projection.open_node(inode, libc::O_RDONLY, &setup)?;
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            allocation_operations: if item_budget {
                u64::MAX
            } else {
                allowance.unwrap_or(u64::MAX)
            },
            items_examined: if item_budget {
                allowance.unwrap_or(u64::MAX)
            } else {
                u64::MAX
            },
            ..crate::WorkBudget::UNBOUNDED
        },
        &crate::CancellationToken::new(),
    );
    let work = match work {
        Ok(work) => work,
        Err((error, _)) => {
            assert!(matches!(error,
                crate::NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded {
                    counter, maximum, ..
                }) if Some(maximum) == allowance && counter == if item_budget { "items_examined" } else { "allocation_operations" }
            ));
            assert!(
                preserved(source.as_ref(), &path)?,
                "ledger admission changed the namespace"
            );
            return Ok(None);
        }
    };
    let result = if rename {
        projection.rename_name(
            ROOT_INODE,
            std::ffi::OsStr::new("replacement"),
            ROOT_INODE,
            std::ffi::OsStr::new("retained"),
            0,
            &work,
        )
    } else {
        projection.remove_name(ROOT_INODE, std::ffi::OsStr::new("retained"), &work)
    };
    if let Err(error) = result {
        assert!(
            preserved(source.as_ref(), &path)?,
            "namespace changed before a refused completion: allowance {allowance:?}: {error:?}"
        );
        let refusal = crate::workspace::workspace_bounded_error(&error);
        // A completion reservation reduces the ceiling available to nested work.
        // Its effective ceiling cannot exceed the caller's original ceiling.
        assert!(
            matches!(refusal, Some(crate::WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                counter, maximum, observed,
            })) if allowance.is_some_and(|allowance| maximum <= allowance) && observed > maximum && counter == if item_budget { "items_examined" } else { "allocation_operations" }),
            "allowance {allowance:?}: {error:?}; source {:?}",
            std::error::Error::source(&error)
        );
        Ok(None)
    } else {
        let file = projection
            .core
            .state()
            .map_err(NativeMountError::from)?
            .file_handle(inode, handle)
            .map_err(NativeMountError::from)?
            .open_file
            .clone();
        assert_eq!(file.file().read_range(0, 8)?.as_ref(), b"original");
        Ok(Some(if item_budget {
            work.receipt().items_examined
        } else {
            work.receipt().allocation_operations
        }))
    }
}

// Measure the actual finite operation first, then exercise every allocation
// refusal prefix against replicas of that same tree. A fresh volume has fresh
// file identities and may require a different number of trie allocations.
#[test]
fn allocation_refusal_precedes_unlink_and_replacement() -> Result<(), Box<dyn std::error::Error>> {
    use crate::model::{AccessMode, CheckoutMode, ConsistencyMode, MutationMode};
    let checkout = memory_checkout(
        posix_config(),
        CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::Pinned,
            mutations: MutationMode::PrivateOverlay,
        },
        1,
    )?
    .pop()
    .ok_or("one checkout")?;
    let original = Arc::new(crate::native_mount::SharedCheckout::new(checkout));
    let source = crate::native_mount::adapter::tests::access::bind_test_access(
        MemorySource::new(Arc::clone(&original), posix_config())?,
        vec![],
        None,
    )?;
    let setup = fuse_fixture_work();
    let path = name("retained");
    source.create_file(&path, FileMetadata::default(), None, &setup)?;
    source.write_range(&path, 0, Bytes::from_static(b"original"))?;
    source.create_file(&name("replacement"), FileMetadata::default(), None, &setup)?;
    let cancellation = crate::CancellationToken::new();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let original = runtime.block_on(async {
        original.lock().await.private_candidate(
            &mut crate::kernel::allocation::KernelAccounting::native(&setup, &cancellation),
            &cancellation,
        )
    })?;
    for rename in [false, true] {
        let required =
            operation(&original, rename, None, false)?.ok_or("unbounded fixture was refused")?;
        for allowance in 0..required {
            operation(&original, rename, Some(allowance), false)?;
        }
        assert!(operation(&original, rename, Some(required), false)?.is_some());
    }
    let required = operation(&original, true, None, true)?.ok_or("unbounded rename was refused")?;
    assert!(
        operation(&original, true, Some(required - 1), true)?.is_none(),
        "rename did not reserve its completion item"
    );
    // Consumed items do not include unused admission reservations. The finite
    // allocation replay above proves progress without assuming those are equal.
    Ok(())
}

#[test]
fn post_unlink_allocation_negative_control() -> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    let setup = fuse_fixture_work();
    let path = name("retained");
    source.create_file(&path, FileMetadata::default(), None, &setup)?;
    source.write_range(&path, 0, Bytes::from_static(b"original"))?;
    let projection = super::super::FuseProjection {
        core: projection_core(
            source.clone(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?,
    };
    let inode = projection
        .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("retained"), &setup)?
        .attr
        .ino
        .0;
    projection.open_node(inode, libc::O_RDONLY, &setup)?;
    let detached = super::super::FuseOpenFile(super::super::fuse_open_file(
        source.detach_file_with_meter(&path, &setup)?,
        &setup,
    )?);
    source.remove(&path, None, &setup)?;
    let refused = fuse_fixture_work();
    // The ledger itself is already funded. Admit no further allocations.
    {
        let mut state = refused.state.lock();
        state.budget.allocation_operations = state.work.allocation_operations;
    }
    assert!(
        projection
            .core
            .state()
            .map_err(NativeMountError::from)?
            .prepare_detached_handles(inode, &detached, &refused)
            .is_err()
    );
    assert!(
        !preserved(source.as_ref(), &path)?,
        "the old ordering must violate namespace preservation on refusal"
    );
    Ok(())
}

fn rename_and_forget(resurrect: bool) -> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    let work = fuse_fixture_work();
    source.create_file(&name("before"), FileMetadata::default(), None, &work)?;
    let projection = super::super::FuseProjection {
        core: projection_core(
            source,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?,
    };
    let inode = projection
        .lookup_entry(ROOT_INODE, std::ffi::OsStr::new("before"), &work)?
        .attr
        .ino
        .0;
    let mut state = projection.core.state().map_err(NativeMountError::from)?;
    let mut install = state.prepare_rename_prefix(
        &name("before"),
        &name("after"),
        &name("after"),
        ROOT_INODE,
        &work,
    )?;
    state.release_lookup_reference(inode, 1);
    drop(state);
    let stamp = projection.core.source.rename(
        &name("before"),
        &name("after"),
        false,
        &work,
        Some(&mut install),
    )?;
    let mut state = projection.core.state().map_err(NativeMountError::from)?;
    install.install(&mut state, stamp);
    if resurrect {
        state
            .inode_by_path
            .insert(super::super::fuse_clone_path(&name("after"), &work)?, inode);
    }
    assert!(
        state
            .inode_by_path
            .values()
            .all(|inode| state.by_inode.contains_key(inode)),
        "retired inode was resurrected"
    );
    Ok(())
}

#[test]
fn prepared_rename_cannot_resurrect_a_concurrent_forget() -> Result<(), Box<dyn std::error::Error>>
{
    rename_and_forget(false)
}

#[test]
#[should_panic(expected = "retired inode was resurrected")]
fn forgotten_inode_negative_control() {
    rename_and_forget(true).unwrap();
}
