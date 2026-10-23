use super::*;

fn preserved(source: &MemorySource, path: &MountPath) -> Result<bool, MountSourceError> {
    Ok(source.lookup(path)?.is_some() && source.read_range(path, 0, 8)?.as_ref() == b"original")
}

fn operation(
    rename: bool,
    allowance: Option<u64>,
) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
    let source = Arc::new(source);
    let setup = fuse_fixture_work();
    let path = name("retained");
    source.create_file(&path, FileMetadata::default(), None, &setup)?;
    source.write_range(&path, 0, Bytes::from_static(b"original"))?;
    source.create_file(&name("replacement"), FileMetadata::default(), None, &setup)?;
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
            allocation_operations: allowance.unwrap_or(u64::MAX),
            ..crate::WorkBudget::UNBOUNDED
        },
        &crate::CancellationToken::new(),
    );
    let work = match work {
        Ok(work) => work,
        Err((error, _)) => {
            assert!(matches!(error,
                crate::NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded {
                    counter: "allocation_operations", maximum, ..
                }) if Some(maximum) == allowance
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
    if result.is_err() {
        assert!(
            preserved(source.as_ref(), &path)?,
            "allowance {allowance:?}: {result:?}"
        );
        if allowance.is_none() {
            result?;
        }
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
        Ok(Some(work.receipt().allocation_operations))
    }
}

// Measure the actual finite operation first, then exercise every allocation
// refusal prefix. The bound is the production trace, never a guessed quota.
#[test]
fn allocation_refusal_precedes_unlink_and_replacement() -> Result<(), Box<dyn std::error::Error>> {
    for rename in [false, true] {
        let required = operation(rename, None)?.ok_or("unbounded fixture was refused")?;
        for allowance in 0..required {
            operation(rename, Some(allowance))?;
        }
        assert!(operation(rename, Some(required))?.is_some());
    }
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
    let install = state.prepare_rename_prefix(
        &name("before"),
        &name("after"),
        &name("after"),
        ROOT_INODE,
        &work,
    )?;
    state.release_lookup_reference(inode, 1);
    install(&mut state);
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
