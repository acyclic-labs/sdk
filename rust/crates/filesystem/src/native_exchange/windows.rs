use super::*;
use crate::native_host::NativeHostWork;

pub(super) fn rename_durable_held<W: NativeHostWork>(
    from: &cap_std::fs::Dir,
    from_name: &Path,
    to: &cap_std::fs::Dir,
    to_name: &Path,
    expected: crate::NativeRootIdentity,
    work: &mut W,
) -> Result<(), W::Error> {
    crate::native_host::rename_no_replace_held(from, from_name, to, to_name, Some(expected), work)?;
    crate::native_host::sync_directory_held(from, work)?;
    crate::native_host::sync_directory_held(to, work)
}
pub(super) fn remove_owned_witness<W: NativeHostWork>(
    parent: &cap_std::fs::Dir,
    name: &Path,
    identity: crate::NativeRootIdentity,
    work: &mut W,
) -> Result<(), W::Error>
where
    W::Error: From<NativeExchangeError>,
{
    if crate::native_host::entry_identity(parent, name, work)? != Some(identity) {
        return Err(NativeExchangeError::IncompatibleJournal.into());
    }
    crate::native_host::with_wide_path(name, work, |name, work| {
        crate::native_host::remove_windows_leaf(parent, name, false, Some(identity), work)
    })?;
    crate::native_host::sync_directory_held(parent, work)
}
pub(super) fn create_entry_witness<W: NativeHostWork>(
    left: &cap_std::fs::Dir,
    left_name: &Path,
    right: &cap_std::fs::Dir,
    right_name: &Path,
    witness_name: &Path,
    roots: [crate::NativeRootIdentity; 2],
    work: &mut W,
) -> Result<crate::NativeRootIdentity, W::Error> {
    // Delete-on-close keeps a failed witness from authorizing recovery.
    let mut file = crate::native_host::with_wide_path(witness_name, work, |name, work| {
        crate::native_host::create_windows_wide_at(left, name, false, false, false, true, work)
    })?;
    write_all(&mut file, ENTRY_WITNESS, work)?;
    write_identity(
        &mut file,
        crate::native_host::directory_identity(left, work)?,
        work,
    )?;
    write_identity(
        &mut file,
        crate::native_host::directory_identity(right, work)?,
        work,
    )?;
    for identity in roots {
        write_identity(&mut file, identity, work)?;
    }
    for (directory, name) in [(left, left_name), (right, right_name)] {
        use std::os::windows::ffi::OsStrExt as _;
        let owned = crate::native_host::canonical_directory_wide(directory, work)?;
        let result = write_path(
            &mut file,
            owned.0.len() as u64,
            owned.0.iter().copied(),
            work,
        );
        drop(owned.0);
        work.release(owned.1)?;
        result?;
        let mut length = 0u64;
        for _ in name.as_os_str().encode_wide() {
            let examined = crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            };
            work.admit(examined)?;
            work.record(examined)?;
            length = length.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        }
        write_path(&mut file, length, name.as_os_str().encode_wide(), work)?;
    }
    attempted(
        work,
        crate::WorkCounters {
            durability_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        },
        || file.sync_all(),
    )?;
    crate::native_host::keep_windows_created_file(&file, work)?;
    crate::native_host::sync_directory_held(left, work)?;
    attempted(
        work,
        crate::WorkCounters {
            backend_read_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        },
        || crate::NativeRootIdentity::from_file(&file),
    )
}
pub(super) fn exchange_windows_held<W: NativeHostWork>(
    left: &cap_std::fs::Dir,
    left_name: &Path,
    right: &cap_std::fs::Dir,
    right_name: &Path,
    work: &mut W,
) -> Result<(), W::Error>
where
    W::Error: From<NativeExchangeError>,
{
    let scratch = service_name(left_name, ".acyclic-exchange", work)?;
    let scratch_name = scratch.0.as_path();
    let result = (|| {
        let witness = service_name(left_name, ".acyclic-exchange-witness", work)?;
        let witness_name = witness.0.as_path();
        let result = (|| {
            if crate::native_host::entry_identity(left, witness_name, work)?.is_some()
                && recover_entry_held(
                    left,
                    left_name,
                    scratch_name,
                    witness_name,
                    Some((right, right_name)),
                    work,
                )?
            {
                // A completed witness preserves its decision; rollback permits retry.
                return Ok(());
            }
            if crate::native_host::entry_identity(left, scratch_name, work)?.is_some() {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            let roots = [
                crate::native_host::entry_identity(left, left_name, work)?
                    .ok_or(NativeExchangeError::InvalidLayout)?,
                crate::native_host::entry_identity(right, right_name, work)?
                    .ok_or(NativeExchangeError::InvalidLayout)?,
            ];
            let witness = create_entry_witness(
                left,
                left_name,
                right,
                right_name,
                witness_name,
                roots,
                work,
            )?;
            // Each rename verifies its original physical preimage.
            if crate::native_host::entry_identity(left, left_name, work)? != Some(roots[0])
                || crate::native_host::entry_identity(right, right_name, work)? != Some(roots[1])
                || crate::native_host::entry_identity(left, witness_name, work)? != Some(witness)
            {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            rename_durable_held(left, left_name, left, scratch_name, roots[0], work)?;
            if crate::native_host::entry_identity(right, right_name, work)? != Some(roots[1]) {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            rename_durable_held(right, right_name, left, left_name, roots[1], work)?;
            if crate::native_host::entry_identity(left, scratch_name, work)? != Some(roots[0]) {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
            rename_durable_held(left, scratch_name, right, right_name, roots[0], work)?;
            remove_owned_witness(left, witness_name, witness, work)
        })();
        drop(witness.0);
        work.release(witness.1)?;
        result
    })();
    drop(scratch.0);
    work.release(scratch.1)?;
    result
}
pub(super) fn recover_entry_held<W: NativeHostWork>(
    left: &cap_std::fs::Dir,
    left_name: &Path,
    scratch_name: &Path,
    witness_name: &Path,
    requested_right: Option<(&cap_std::fs::Dir, &Path)>,
    work: &mut W,
) -> Result<bool, W::Error>
where
    W::Error: From<NativeExchangeError>,
{
    let mut file = crate::native_host::with_wide_path(witness_name, work, |name, work| {
        crate::native_host::open_windows_leaf_with_options(
            left,
            name,
            &crate::native_host::NativeOpenOptions::Read,
            work,
        )
    })?;
    let (witness_identity, witness_length) =
        crate::native_host::file_identity_and_length(&file, work)?;
    let mut magic = [0u8; 32];
    read_exact(&mut file, &mut magic, work)?;
    if &magic != ENTRY_WITNESS {
        return Err(NativeExchangeError::IncompatibleJournal.into());
    }
    let parents = [
        read_identity(&mut file, work)?,
        read_identity(&mut file, work)?,
    ];
    let roots = [
        read_identity(&mut file, work)?,
        read_identity(&mut file, work)?,
    ];
    if crate::native_host::directory_identity(left, work)? != parents[0] {
        return Err(NativeExchangeError::IncompatibleJournal.into());
    }
    let mut remaining = witness_length
        .checked_sub(96)
        .ok_or(NativeExchangeError::IncompatibleJournal)?;
    let (path, allocation) = read_path(&mut file, &mut remaining, work)?;
    let checked: Result<(), W::Error> = (|| {
        if !path.is_absolute() {
            return Err(NativeExchangeError::IncompatibleJournal.into());
        }
        let named_left = crate::native_host::open_root_directory(&path, work)?.0;
        if crate::native_host::directory_identity(&named_left, work)? != parents[0] {
            return Err(NativeExchangeError::IncompatibleJournal.into());
        }
        Ok(())
    })();
    drop(path);
    work.release(allocation)?;
    checked?;
    let (path, allocation) = read_path(&mut file, &mut remaining, work)?;
    let matches = path == left_name;
    drop(path);
    work.release(allocation)?;
    if !matches {
        return Err(NativeExchangeError::IncompatibleJournal.into());
    }
    let (path, allocation) = read_path(&mut file, &mut remaining, work)?;
    let opened: Result<cap_std::fs::Dir, W::Error> = (|| {
        if !path.is_absolute() {
            return Err(NativeExchangeError::IncompatibleJournal.into());
        }
        let right = crate::native_host::open_root_directory(&path, work)?.0;
        if crate::native_host::directory_identity(&right, work)? != parents[1] {
            return Err(NativeExchangeError::IncompatibleJournal.into());
        }
        Ok(right)
    })();
    drop(path);
    work.release(allocation)?;
    let right = opened?;
    let (right_name, allocation) = read_path(&mut file, &mut remaining, work)?;
    let published: Result<bool, W::Error> = (|| {
        validate_leaf(&right_name).map_err(W::Error::from)?;
        if let Some((requested_parent, requested_name)) = requested_right {
            if requested_name != right_name
                || crate::native_host::directory_identity(requested_parent, work)? != parents[1]
            {
                return Err(NativeExchangeError::IncompatibleJournal.into());
            }
        }
        let (identity, length) = crate::native_host::file_identity_and_length(&file, work)?;
        if remaining != 0 || length != witness_length || identity != witness_identity {
            return Err(NativeExchangeError::IncompatibleJournal.into());
        }
        let current = (
            crate::native_host::entry_identity(left, left_name, work)?,
            crate::native_host::entry_identity(&right, &right_name, work)?,
            crate::native_host::entry_identity(left, scratch_name, work)?,
        );
        match current {
            (Some(original), Some(replacement), None) if [original, replacement] == roots => {
                Ok(false)
            }
            (None, Some(replacement), Some(original)) if [original, replacement] == roots => {
                rename_durable_held(left, scratch_name, left, left_name, roots[0], work)?;
                Ok(false)
            }
            (Some(replacement), None, Some(original)) if [original, replacement] == roots => {
                rename_durable_held(left, scratch_name, &right, &right_name, roots[0], work)?;
                Ok(true)
            }
            (Some(replacement), Some(original), None) if [original, replacement] == roots => {
                Ok(true)
            }
            _ => Err(NativeExchangeError::IncompatibleJournal.into()),
        }
    })();
    drop(right_name);
    work.release(allocation)?;
    let published = published?;
    // On Windows the named deletion is complete only after the reader releases
    // its share-delete handle. The subsequent barrier covers actual cleanup.
    drop(file);
    remove_owned_witness(left, witness_name, witness_identity, work)?;
    Ok(published)
}
pub(super) fn scratch(live: &Path) -> Result<PathBuf, NativeExchangeError> {
    let parent = live.parent().ok_or(NativeExchangeError::InvalidLayout)?;
    let name = live.file_name().ok_or(NativeExchangeError::InvalidLayout)?;
    Ok(parent.join(format!(".{}.acyclic-exchange", name.to_string_lossy())))
}
pub(super) fn entry_exchange_witness(live: &Path) -> Result<PathBuf, NativeExchangeError> {
    let parent = live.parent().ok_or(NativeExchangeError::InvalidLayout)?;
    let name = live.file_name().ok_or(NativeExchangeError::InvalidLayout)?;
    Ok(parent.join(format!(
        ".{}.acyclic-exchange-witness",
        name.to_string_lossy()
    )))
}
