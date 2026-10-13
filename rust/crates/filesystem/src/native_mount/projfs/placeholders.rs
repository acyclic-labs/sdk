use super::*;

pub(super) fn write_entry(
    projection: Projection<'_>,
    path: &MountPath,
    entry: &ProjectedEntry,
) -> Result<Placed, String> {
    let host = host_relative_path(path).map_err(|error| error.to_string())?;
    if entry.info.IsDirectory {
        return match std::fs::create_dir(projection.runtime.root.as_path().join(&host)) {
            Ok(()) => Ok(Placed::Unowned),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::AlreadyExists
                        | std::io::ErrorKind::NotFound
                        | std::io::ErrorKind::NotADirectory
                ) =>
            {
                Ok(Placed::Nothing)
            }
            Err(error) => Err(format!("creating directory {path:?}: {error}")),
        };
    }
    let Some(target) = entry.symlink_target.as_ref() else {
        return write_ordinary(projection, path, &host, entry, None);
    };
    let mut placeholder = link_placeholder(entry.info);
    if entry.directory_link {
        placeholder.FileBasicInfo.IsDirectory = true;
        placeholder.FileBasicInfo.FileAttributes |= FILE_ATTRIBUTE_DIRECTORY.0;
    }
    let (target, extended) = symlink_extended(target.bytes())
        .ok_or_else(|| format!("link at {path:?} is unrepresentable"))?;
    let relative = HSTRING::from(host.as_os_str());
    let size = u32::try_from(size_of::<PRJ_PLACEHOLDER_INFO>()).unwrap_or(u32::MAX);
    // SAFETY: the name, placeholder and link target outlive these
    let result = unsafe {
        PrjWritePlaceholderInfo2(
            projection.context,
            &relative,
            &raw const placeholder,
            size,
            Some(&extended),
        )
    };
    match result {
        Ok(()) => Ok(Placed::Owned(Owned::Link(link_digest(
            target.iter().copied(),
        )))),
        Err(error)
            if [
                HR_ALREADY_EXISTS,
                HR_FILE_EXISTS,
                HR_FILE_NOT_FOUND,
                HR_PATH_NOT_FOUND,
                HR_DIRECTORY,
            ]
            .contains(&error.code()) =>
        {
            Ok(Placed::Nothing)
        }
        Err(error) if error.code() == HR_NOT_SUPPORTED => {
            write_link(projection, path, &host, &target, entry.directory_link)
        }
        Err(error) => Err(format!(
            "writing placeholder {path:?}: {}",
            driver_error(&error)
        )),
    }
}

pub(super) fn write_link(
    projection: Projection<'_>,
    path: &MountPath,
    host: &std::path::Path,
    target: &HSTRING,
    directory: bool,
) -> Result<Placed, String> {
    use windows::Win32::Storage::FileSystem::{
        CreateSymbolicLinkW, SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE,
        SYMBOLIC_LINK_FLAG_DIRECTORY, SYMBOLIC_LINK_FLAGS,
    };

    let name = HSTRING::from(projection.runtime.root.as_path().join(host).as_os_str());
    let mut flags = SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE.0;
    if directory {
        flags |= SYMBOLIC_LINK_FLAG_DIRECTORY.0;
    }
    // SAFETY: both names outlive this synchronous call.
    let created = unsafe { CreateSymbolicLinkW(&name, target, SYMBOLIC_LINK_FLAGS(flags)) };
    if !created {
        let error = std::io::Error::last_os_error();
        return match error.kind() {
            std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::NotFound => Ok(Placed::Nothing),
            _ => Err(format!(
                "writing link {path:?}: {error} (this volume has no link projector, so \
                 projecting a link needs the right to create one: Developer Mode)"
            )),
        };
    }
    Ok(Placed::Owned(Owned::Link(link_digest(
        target.iter().copied(),
    ))))
}

pub(super) fn link_digest(target: impl IntoIterator<Item = u16>) -> u64 {
    use std::hash::{Hash as _, Hasher as _};

    let mut hasher = std::hash::DefaultHasher::new();
    target.into_iter().for_each(|unit| unit.hash(&mut hasher));
    hasher.finish()
}

pub(super) fn release(
    projection: Projection<'_>,
    path: &MountPath,
    owned: Option<Owned>,
) -> Result<Release, String> {
    match owned {
        None => delete_unowned(projection, path),
        Some(Owned::Link(digest)) => release_link(projection, path, digest),
        Some(Owned::File(state)) => release_file(projection, path, state),
    }
}

pub(super) fn release_file(
    projection: Projection<'_>,
    path: &MountPath,
    state: HostWindowsMetadata,
) -> Result<Release, String> {
    if has_pending_capture(projection.runtime.post_operation_failure.as_ref(), path) {
        return Ok(Release::Busy);
    }
    let host = host_relative_path(path).map_err(|error| error.to_string())?;
    match delete_file_while(&projection.runtime.root.as_path().join(&host), state) {
        Ok(true) => {}
        Ok(false) => return Ok(Release::Done),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Release::Done),
        Err(error) if error.raw_os_error() == Some(SHARING_VIOLATION) => {
            return Ok(Release::Busy);
        }
        Err(error) => return Err(format!("removing {path:?}: {error}")),
    }
    clear_tombstone(projection, path, &HSTRING::from(host.as_os_str()))
}

pub(super) fn release_link(
    projection: Projection<'_>,
    path: &MountPath,
    digest: u64,
) -> Result<Release, String> {
    let host = host_relative_path(path).map_err(|error| error.to_string())?;
    let relative = HSTRING::from(host.as_os_str());
    match prj_delete(projection, &relative, PRJ_UPDATE_ALLOW_READ_ONLY) {
        Ok(()) => return Ok(Release::Done),
        Err(error) if error.code() == HR_SHARING_VIOLATION => return Ok(Release::Busy),
        Err(error) if [HR_FILE_NOT_FOUND, HR_PATH_NOT_FOUND].contains(&error.code()) => {}
        Err(error) if error.code() == HR_VIRTUALIZATION_INVALID_OPERATION => {
            match delete_link(&projection.runtime.root.as_path().join(&host), digest) {
                Ok(true) => {}
                Ok(false) => return Ok(Release::Done),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(Release::Done);
                }
                Err(error) if error.raw_os_error() == Some(SHARING_VIOLATION) => {
                    return Ok(Release::Busy);
                }
                Err(error) => return Err(format!("removing link {path:?}: {error}")),
            }
        }
        Err(error) => {
            return Err(format!("deleting link {path:?}: {}", driver_error(&error)));
        }
    }
    clear_tombstone(projection, path, &relative)
}

pub(super) fn clear_tombstone(
    projection: Projection<'_>,
    path: &MountPath,
    relative: &HSTRING,
) -> Result<Release, String> {
    match prj_delete(projection, relative, PRJ_UPDATE_ALLOW_TOMBSTONE) {
        Ok(()) => Ok(Release::Done),
        Err(error) if error.code() == HR_SHARING_VIOLATION => Ok(Release::Busy),
        Err(error)
            if [
                HR_FILE_NOT_FOUND,
                HR_PATH_NOT_FOUND,
                HR_VIRTUALIZATION_INVALID_OPERATION,
            ]
            .contains(&error.code()) =>
        {
            Ok(Release::Done)
        }
        Err(error) => Err(format!(
            "clearing link tombstone {path:?}: {}",
            driver_error(&error)
        )),
    }
}

pub(super) const SHARING_VIOLATION: i32 = 32;

pub(super) fn prj_delete(
    projection: Projection<'_>,
    relative: &HSTRING,
    update: PRJ_UPDATE_TYPES,
) -> windows::core::Result<()> {
    // SAFETY: the name outlives this synchronous call on a live context.
    unsafe { PrjDeleteFile(projection.context, relative, Some(update), None) }
}

pub(super) fn handle_state(file: &std::fs::File) -> std::io::Result<HostWindowsMetadata> {
    use std::os::windows::fs::MetadataExt as _;

    let metadata = file.metadata()?;
    let ticks = |value: u64| i64::try_from(value).map_err(std::io::Error::other);
    Ok(HostWindowsMetadata {
        identity: crate::NativeRootIdentity::from_file(file)?,
        links: None,
        size: metadata.file_size(),
        attributes: metadata.file_attributes()
            & !(FILE_ATTRIBUTE_ARCHIVE.0 | FILE_ATTRIBUTE_NORMAL.0),
        created: ticks(metadata.creation_time())?,
        modified: ticks(metadata.last_write_time())?,
    })
}

pub(super) fn basic_information(
    file: &std::fs::File,
    set: Option<&windows::Win32::Storage::FileSystem::FILE_BASIC_INFO>,
) -> std::io::Result<windows::Win32::Storage::FileSystem::FILE_BASIC_INFO> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_BASIC_INFO, FileBasicInfo, GetFileInformationByHandleEx, SetFileInformationByHandle,
    };

    let handle = HANDLE(file.as_raw_handle());
    let size = u32::try_from(size_of::<FILE_BASIC_INFO>()).unwrap_or(u32::MAX);
    if let Some(set) = set {
        // SAFETY: a live handle and a correctly sized input structure.
        unsafe {
            SetFileInformationByHandle(handle, FileBasicInfo, std::ptr::from_ref(set).cast(), size)
        }?;
        return Ok(*set);
    }
    let mut information = FILE_BASIC_INFO::default();
    // SAFETY: a live handle and a correctly sized output structure.
    unsafe {
        GetFileInformationByHandleEx(handle, FileBasicInfo, (&raw mut information).cast(), size)
    }?;
    Ok(information)
}

pub(super) fn remove_vacated(projection: Projection<'_>, path: &MountPath) {
    if let Ok(host) = host_relative_path(path)
        && !host.as_os_str().is_empty()
    {
        let _ = std::fs::remove_dir(projection.runtime.root.as_path().join(host));
    }
}

pub(super) fn write_ordinary(
    projection: Projection<'_>,
    path: &MountPath,
    host: &std::path::Path,
    entry: &ProjectedEntry,
    replacing: Option<HostWindowsMetadata>,
) -> Result<Placed, String> {
    let runtime = projection.runtime;
    let work = projection.work;
    let failed = |error: NativeMountError| format!("writing {path:?}: {error}");
    let (Some(parent), Some(name)) = (host.parent(), host.file_name()) else {
        return Err(format!("{path:?} names no file"));
    };
    let directory = crate::native_host::with_wide_path(parent, &mut &*work, |path, work| runtime.metadata_root.open_dir_held(path, work))
        .map_err(NativeMountError::from)
        .map_err(failed)?;
    let direct = projection.starting && replacing.is_none();
    let mut staging = lock_recover(&runtime.staging);
    if !direct && staging.is_none() {
        *staging = Some(staging_directory(&runtime.root, work).map_err(failed)?);
    }
    let temporary = if direct {
        name.to_owned()
    } else {
        format!(".acyclic-{}", uuid::Uuid::new_v4().simple()).into()
    };
    let from = if direct {
        &directory
    } else {
        &staging.as_ref().expect("held original staging directory").0
    };
    let mut file = match crate::native_host::with_wide_path(
        std::path::Path::new(&temporary),
        &mut &*work,
        |name, work| {
            crate::native_host::open_windows_at(from, name, windows::Win32::Storage::FileSystem::FILE_GENERIC_READ | windows::Win32::Storage::FileSystem::FILE_GENERIC_WRITE | windows::Win32::Storage::FileSystem::DELETE, windows::Wdk::Storage::FileSystem::FILE_CREATE, windows::Wdk::Storage::FileSystem::FILE_NON_DIRECTORY_FILE | windows::Wdk::Storage::FileSystem::FILE_SYNCHRONOUS_IO_NONALERT | windows::Wdk::Storage::FileSystem::FILE_DELETE_ON_CLOSE, work)
        },
    ) {
        Ok(file) => file,
        Err(crate::NativeTreeMaterializationError::Io(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::AlreadyExists
                    | std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::NotADirectory
            ) =>
        {
            return Ok(Placed::Nothing);
        }
        Err(error) => return Err(failed(error.into())),
    };
    if !write_listed_content(projection, path, entry, &mut file)? {
        return Ok(Placed::Retry);
    }
    (&mut &*work)
        .attempt_io(WriteOperations.delta(1), || {
            basic_information(&file, Some(&ordinary_basic_information(entry)))
        })
        .map_err(NativeMountError::from)
        .map_err(failed)?;
    let held = match replacing {
        Some(state) => {
            if has_pending_capture(runtime.post_operation_failure.as_ref(), path) {
                return Ok(Placed::Retry);
            }
            match (&mut &*work)
                .attempt_io(ReadOperations.delta(1), || {
                    held_while(&runtime.root.join(host), state)
                })
                .map_err(NativeMountError::from)
                .map_err(failed)?
            {
                Some(held) => Some(held),
                None => return Ok(Placed::Nothing),
            }
        }
        None => None,
    };
    let published = if direct {
        Ok(())
    } else if held.is_some() {
        crate::native_host::with_wide_path(
            std::path::Path::new(&temporary),
            &mut &*work,
            |from_name, work| {
                crate::native_host::with_wide_path(
                    std::path::Path::new(name),
                    work,
                    |to_name, _| {
                        crate::native_host::rename_held(
                            from,
                            from_name,
                            &directory,
                            to_name,
                            acyclic_native_runtime::RenameMode::Replace,
                            None,
                            &mut crate::NativeMaterializationWork::clone(
                                projection.work,
                            ))
                        .map_err(crate::materializer::native_materialization_work_error)
                    },
                )
            },
        )
    } else {
        { #[cfg(unix)] { crate::native_host::rename_held(from, std::path::Path::new(&temporary), &directory, std::path::Path::new(name), acyclic_native_runtime::RenameMode::NoReplace, None, &mut &*work) } #[cfg(windows)] { crate::native_host::with_wide_path(std::path::Path::new(&temporary), &mut &*work, |source_name, work| crate::native_host::with_wide_path(std::path::Path::new(name), work, |target, work| crate::native_host::rename_held(from, source_name, &directory, target, acyclic_native_runtime::RenameMode::NoReplace, None, work))) } }
    };
    match published {
        Ok(()) => {
            crate::native_host::keep_windows_created_file(&file, &mut &*work)
                .map_err(NativeMountError::from)
                .map_err(failed)?;
            (&mut &*work)
                .attempt_io(ReadOperations.delta(1), || handle_state(&file))
                .map(|state| Placed::Owned(Owned::File(state)))
                .map_err(NativeMountError::from)
                .map_err(failed)
        }
        Err(crate::NativeTreeMaterializationError::Io(error))
            if matches!(
                error.kind(),
                std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::NotFound
            ) =>
        {
            Ok(Placed::Nothing)
        }
        Err(crate::NativeTreeMaterializationError::Io(error))
            if error.raw_os_error() == Some(SHARING_VIOLATION) =>
        {
            Ok(Placed::Retry)
        }
        Err(error) => Err(failed(error.into())),
    }
}
