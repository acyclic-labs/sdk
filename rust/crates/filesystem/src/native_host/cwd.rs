#[cfg(any(windows, test, feature = "native-watch"))]
use super::*;

#[cfg(windows)]
pub(crate) fn canonical_directory_wide<W: NativeHostWork>(
    directory: &Dir,
    work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Storage::FileSystem::{
        GETFINALPATHNAMEBYHANDLE_FLAGS, GetFinalPathNameByHandleW,
    };
    let query = |output: &mut [u16], work: &mut W| {
        work.attempt_io(READ, || {
            // SAFETY: held handle and initialized output remain live.
            let count = unsafe {
                GetFinalPathNameByHandleW(
                    win::HANDLE(directory.as_raw_handle()),
                    output,
                    GETFINALPATHNAMEBYHANDLE_FLAGS(0),
                )
            };
            if count == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(count as usize)
            }
        })
    };
    let mut capacity = query(&mut [], work)?;
    loop {
        let (mut output, allocation) = buffer(capacity, work)?;
        output.resize(capacity, 0);
        let result = query(&mut output, work).and_then(|count| {
            work.check()?;
            Ok(count)
        });
        if let Ok(count) = result.as_ref()
            && *count < capacity
        {
            output.truncate(*count);
            return Ok((output, allocation));
        }
        drop(output);
        work.release(allocation)?;
        capacity = result?.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        if capacity > u32::MAX as usize {
            return Err(crate::WorkError::Overflow.into());
        }
    }
}

/// Folds a wide name for case with the OS's invariant uppercase mapping,
/// so names the file system treats as one name fold to one key.
#[cfg(windows)]
pub(crate) fn fold_case_wide<W: NativeHostWork>(
    name: &[u16],
    work: &mut W,
) -> Result<(Vec<u16>, W::Allocation), W::Error> {
    use windows::Win32::{
        Foundation::LPARAM,
        Globalization::{LCMAP_UPPERCASE, LCMapStringEx, LOCALE_NAME_INVARIANT},
    };
    let (mut output, allocation) = buffer(name.len(), work)?;
    if name.is_empty() {
        return Ok((output, allocation));
    }
    output.resize(name.len(), 0);
    let mapped = work.attempt_io(
        WorkCounters {
            bytes_encoded: (name.len() as u64).saturating_mul(2),
            ..WorkCounters::default()
        },
        || {
            // SAFETY: the source and destination are distinct live slices.
            let count = unsafe {
                LCMapStringEx(
                    LOCALE_NAME_INVARIANT,
                    LCMAP_UPPERCASE,
                    name,
                    Some(&mut output),
                    None,
                    None,
                    LPARAM(0),
                )
            };
            usize::try_from(count)
                .ok()
                .filter(|count| *count != 0)
                .ok_or_else(io::Error::last_os_error)
        },
    );
    match mapped {
        Ok(count) => {
            output.truncate(count);
            Ok((output, allocation))
        }
        Err(error) => {
            drop(output);
            work.release(allocation)?;
            Err(error)
        }
    }
}

/// A wide buffer and the work allocation that owns it.
#[cfg(windows)]
type WideBuffer<W> = (Vec<u16>, <W as NativeHostWork>::Allocation);

/// The long form of a full wide path whose components may be 8.3 short
/// names, or `None` when the host cannot resolve it, for example because it
/// is already gone.
#[cfg(windows)]
pub(crate) fn long_path_wide<W: NativeHostWork>(
    path: &[u16],
    work: &mut W,
) -> Result<Option<WideBuffer<W>>, W::Error> {
    use windows::{Win32::Storage::FileSystem::GetLongPathNameW, core::PCWSTR};
    let capacity = path
        .len()
        .checked_add(1)
        .ok_or(crate::WorkError::Overflow)?;
    let (mut input, input_allocation) = buffer(capacity, work)?;
    input.extend_from_slice(path);
    input.push(0);
    let query = |output: &mut [u16], work: &mut W| {
        work.started_io(READ)?;
        // SAFETY: the terminated input and the output remain live.
        let count = unsafe { GetLongPathNameW(PCWSTR(input.as_ptr()), Some(output)) };
        Ok::<_, W::Error>(count as usize)
    };
    let result = (|| {
        let mut capacity = query(&mut [], work)?;
        loop {
            if capacity == 0 {
                return Ok(None);
            }
            let (mut output, allocation) = buffer(capacity, work)?;
            output.resize(capacity, 0);
            let count = match query(&mut output, work) {
                Ok(count) => count,
                Err(error) => {
                    drop(output);
                    work.release(allocation)?;
                    return Err(error);
                }
            };
            if count != 0 && count < capacity {
                output.truncate(count);
                return Ok(Some((output, allocation)));
            }
            drop(output);
            work.release(allocation)?;
            capacity = count;
        }
    })();
    drop(input);
    work.release(input_allocation)?;
    result
}

/// The 8.3 short form of a path, for tests that reach a directory by it.
#[cfg(all(test, windows))]
pub(crate) fn short_path(path: &std::path::Path) -> io::Result<std::path::PathBuf> {
    use std::os::windows::ffi::{OsStrExt as _, OsStringExt as _};
    use windows::{Win32::Storage::FileSystem::GetShortPathNameW, core::PCWSTR};
    let input = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut output = vec![0_u16; 32_768];
    // SAFETY: the terminated input and the output remain live.
    let count = unsafe { GetShortPathNameW(PCWSTR(input.as_ptr()), Some(&mut output)) } as usize;
    if count == 0 || count >= output.len() {
        return Err(io::Error::last_os_error());
    }
    output.truncate(count);
    Ok(std::ffi::OsString::from_wide(&output).into())
}

#[cfg(unix)]
pub(super) fn cwd_metadata_flags() -> i32 {
    #[cfg(target_os = "linux")]
    let access = libc::O_PATH;
    #[cfg(target_os = "macos")]
    let access = libc::O_EVTONLY;
    access | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW
}

#[cfg(unix)]
#[cfg(any(test, feature = "native-watch"))]
pub(crate) fn normalize_root_path<W: NativeHostWork<Error = DemandError, Allocation = u64>>(
    path: &Path,
    work: &mut W,
) -> Result<std::path::PathBuf, DemandError> {
    use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
    work.check()?;
    if path.is_absolute() {
        return clone_path(path, work).map(|(path, _)| path);
    }
    let current = Dir::from_std_file(open_unix(libc::AT_FDCWD, c".", cwd_metadata_flags(), work)?);
    let (cwd, cwd_allocation) = canonical_directory_path_held(current, work)?;
    let result = (|| {
        let prefix = cwd.as_os_str().as_bytes();
        let suffix = path.as_os_str().as_bytes();
        let separator = usize::from(!prefix.ends_with(b"/"));
        let length = prefix
            .len()
            .checked_add(suffix.len())
            .and_then(|n| n.checked_add(separator))
            .ok_or(crate::WorkError::Overflow)?;
        let (mut output, allocation) = buffer(length, work)?;
        if let Err(error) = work.started_io(copy_work((length - separator) as u64)) {
            drop(output);
            work.release(allocation)?;
            return Err(error);
        }
        output.extend_from_slice(prefix);
        if separator != 0 {
            output.push(b'/');
        }
        output.extend_from_slice(suffix);
        Ok(OsString::from_vec(output).into())
    })();
    drop(cwd);
    work.release(cwd_allocation)?;
    result
}

#[cfg(unix)]
#[cfg(any(test, feature = "native-watch"))]
fn cwd_ancestor_name<W: NativeHostWork>(
    parent: &Dir,
    child: NativeRootIdentity,
    work: &mut W,
) -> Result<(OsString, W::Allocation), W::Error> {
    let (mut reader, allocation) = read_dir_held(parent, work)?;
    let result = (|| {
        while let Some((entry, allocation)) = reader.next(work)? {
            if entry
                .stat
                .as_ref()
                .is_some_and(|stat| stat.is_dir() && stat.identity() == child)
            {
                return Ok((entry.name, allocation));
            }
            drop(entry);
            work.release(allocation)?;
        }
        Err(io::Error::from_raw_os_error(libc::ENOENT).into())
    })();
    drop(reader);
    if let Err(error) = work.release(allocation) {
        if let Ok((name, allocation)) = result {
            drop(name);
            work.release(allocation)?;
        }
        return Err(error);
    }
    result
}

#[cfg(all(target_os = "linux", any(test, feature = "native-watch")))]
fn linux_directory_path_held<W: NativeHostWork>(
    directory: &Dir,
    descriptor_directory: &std::ffi::CStr,
    work: &mut W,
) -> Result<Option<(std::path::PathBuf, W::Allocation)>, W::Error> {
    use std::os::fd::FromRawFd as _;
    use std::os::unix::ffi::OsStringExt as _;
    // Only an unavailable descriptor-path capability selects the ancestry
    // implementation. Permission, cancellation and semantic failures propagate.
    // Resolve service-directory symlinks (such as /proc/self) before deciding
    // capability availability. O_NOFOLLOW | O_DIRECTORY would report ENOTDIR
    // for an available symlinked directory. The held result is verified as
    // procfs below; the caller's root descriptor is never reopened by path.
    let descriptors = work.attempt_io(READ, || {
        let descriptor = unsafe {
            // SAFETY: the static terminated path remains live for open.
            libc::open(
                descriptor_directory.as_ptr(),
                libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        if descriptor < 0 {
            let error = io::Error::last_os_error();
            return if matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ENOTDIR)) {
                Ok(None)
            } else {
                Err(error)
            };
        }
        // SAFETY: open returned a new, owned descriptor.
        Ok(Some(unsafe { File::from_raw_fd(descriptor) }))
    })?;
    let Some(descriptors) = descriptors else {
        return Ok(None);
    };
    let procfs = work.attempt_io(READ, || {
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: the descriptor and writable statfs storage remain live.
        if unsafe { libc::fstatfs(descriptors.as_raw_fd(), stat.as_mut_ptr()) } < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fstatfs initialized stat on success.
        Ok(unsafe { stat.assume_init() }.f_type == libc::PROC_SUPER_MAGIC)
    })?;
    if !procfs {
        return Ok(None);
    }
    let mut name = [0_u8; 11];
    let mut value = u32::try_from(directory.as_raw_fd()).map_err(|_| W::invalid_request())?;
    let mut start = name.len() - 1;
    loop {
        start -= 1;
        name[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    work.started_io(copy_work((name.len() - start) as u64))?;
    let mut capacity = 256_usize;
    loop {
        let (mut output, allocation) = buffer::<u8, _>(capacity, work)?;
        let result: Result<Option<bool>, W::Error> = (|| {
            work.started_io(copy_work(output.capacity() as u64))?;
            output.resize(output.capacity(), 0);
            let reading = WorkCounters {
                source_bytes_read: capacity as u64,
                ..WorkCounters::UNCHARGED
            };
            work.admit(reading)?;
            let count = work.attempt_io(READ, || {
                // SAFETY: both held descriptors, terminated name and initialized
                // bounded output remain live throughout readlinkat.
                let count = unsafe {
                    libc::readlinkat(
                        descriptors.as_raw_fd(),
                        name[start..].as_ptr().cast(),
                        output.as_mut_ptr().cast(),
                        capacity,
                    )
                };
                if count < 0 {
                    let error = io::Error::last_os_error();
                    // Linux's d_path cannot represent paths beyond its native
                    // query capacity. The held-capability walk supports them.
                    return if error.raw_os_error() == Some(libc::ENAMETOOLONG) {
                        Ok(None)
                    } else {
                        Err(error)
                    };
                }
                Ok(Some(count as usize))
            })?;
            let Some(count) = count else {
                return Ok(None);
            };
            work.record(WorkCounters {
                source_bytes_read: count as u64,
                ..WorkCounters::UNCHARGED
            })?;
            if count < capacity {
                // A deleted descriptor has no canonical namespace identity.
                // Do not trim the textual suffix: it can be a legitimate name.
                let stat = fstat(directory.as_raw_fd(), c"", work)?;
                if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "host root is not a real directory",
                    )
                    .into());
                }
                if stat.st_nlink == 0 {
                    return Err(io::Error::from_raw_os_error(libc::ENOENT).into());
                }
                work.check()?;
                output.truncate(count);
                return Ok(Some(true));
            }
            Ok(Some(false))
        })();
        if matches!(result, Ok(Some(true))) {
            return Ok(Some((OsString::from_vec(output).into(), allocation)));
        }
        drop(output);
        work.release(allocation)?;
        if result?.is_none() {
            return Ok(None);
        }
        capacity = capacity.checked_mul(2).ok_or(crate::WorkError::Overflow)?;
    }
}

#[cfg(unix)]
#[cfg(any(test, feature = "native-watch"))]
pub(crate) fn canonical_directory_path_held<W: NativeHostWork>(
    mut current: Dir,
    work: &mut W,
) -> Result<(std::path::PathBuf, W::Allocation), W::Error> {
    use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};
    #[cfg(target_os = "linux")]
    if let Some(path) = linux_directory_path_held(&current, c"/proc/self/fd", work)? {
        return Ok(path);
    }
    #[cfg(target_os = "macos")]
    {
        let mut path = [0_u8; libc::PATH_MAX as usize];
        let length = work.attempt_io(READ, || {
            // SAFETY: the held descriptor and PATH_MAX-byte output remain live.
            if unsafe { libc::fcntl(current.as_raw_fd(), libc::F_GETPATH, path.as_mut_ptr()) } < 0 {
                let error = io::Error::last_os_error();
                // F_GETPATH also reports a short output buffer as ENOSPC.
                return if matches!(
                    error.raw_os_error(),
                    Some(libc::ENAMETOOLONG | libc::ENOSPC)
                ) {
                    Ok(None)
                } else {
                    Err(error)
                };
            }
            Ok(Some(path.iter().position(|byte| *byte == 0).ok_or_else(
                || io::Error::from_raw_os_error(libc::ENAMETOOLONG),
            )?))
        })?;
        if let Some(length) = length {
            let (mut output, allocation) = buffer(length, work)?;
            if let Err(error) = work.started_io(copy_work(length as u64)) {
                drop(output);
                work.release(allocation)?;
                return Err(error);
            }
            output.extend_from_slice(path.get(..length).ok_or_else(|| W::invalid_request())?);
            return Ok((OsString::from_vec(output).into(), allocation));
        }
    }
    let root = Dir::from_std_file(open_unix(libc::AT_FDCWD, c"/", cwd_metadata_flags(), work)?);
    let root = directory_identity(&root, work)?;
    let (mut output, mut allocation) = buffer::<u8, _>(1, work)?;
    let result = (|| {
        loop {
            work.check()?;
            let child = directory_identity(&current, work)?;
            if child == root {
                break;
            }
            let parent = Dir::from_std_file(open_unix(
                current.as_raw_fd(),
                c"..",
                cwd_metadata_flags(),
                work,
            )?);
            if directory_identity(&parent, work)? == child {
                return Err(io::Error::from_raw_os_error(libc::ENOENT).into());
            }
            let (name, name_allocation) = cwd_ancestor_name(&parent, child, work)?;
            let copied = (|| {
                let size = output
                    .len()
                    .checked_add(name.len())
                    .and_then(|n| n.checked_add(1))
                    .ok_or(crate::WorkError::Overflow)?;
                if size > output.capacity() {
                    let (mut grown, next) = buffer(size, work)?;
                    if let Err(error) = work.started_io(copy_work(output.len() as u64)) {
                        drop(grown);
                        work.release(next)?;
                        return Err(error);
                    }
                    grown.append(&mut output);
                    drop(std::mem::replace(&mut output, grown));
                    work.release(std::mem::replace(&mut allocation, next))?;
                }
                work.started_io(copy_work(name.len() as u64))?;
                output.extend(name.as_bytes().iter().rev().copied());
                output.push(b'/');
                Ok(())
            })();
            drop(name);
            work.release(name_allocation)?;
            copied?;
            current = parent;
        }
        if output.is_empty() {
            output.push(b'/');
        }
        work.started_io(copy_work(output.len() as u64))?;
        output.reverse();
        Ok(())
    })();
    if let Err(error) = result {
        drop(output);
        work.release(allocation)?;
        return Err(error);
    }
    Ok((OsString::from_vec(output).into(), allocation))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};

    #[test]
    fn held_path_query_does_not_enumerate_authored_ancestors()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("held");
        std::fs::create_dir(&root)?;
        let expected = root.canonicalize()?;
        let read = || -> Result<_, Box<dyn std::error::Error>> {
            let directory = Dir::open_ambient_dir(&root, cap_std::ambient_authority())?;
            let cancellation = crate::CancellationToken::new();
            let work = NativeMaterializationWork::new(
                WorkBudget {
                    backend_read_operations: 64,
                    ..WorkBudget::UNBOUNDED
                },
                &cancellation,
            )
            .map_err(|(error, _)| error)?;
            let (path, allocation) = canonical_directory_path_held(directory, &mut &work)?;
            assert_eq!(path, expected);
            assert_eq!(work.live_allocation_bytes(), path.capacity() as u64);
            let receipt = work.receipt();
            drop(path);
            drop(allocation);
            assert_eq!(work.live_allocation_bytes(), 0);
            Ok(receipt)
        };
        let before = read()?;
        for index in 0..257 {
            std::fs::create_dir(temporary.path().join(format!("authored-{index}")))?;
        }
        assert_eq!(read()?, before);
        Ok(())
    }

    #[test]
    fn held_path_query_preserves_raw_names_and_rejects_unlinked_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let original = temporary.path().join("original");
        let moved = temporary
            .path()
            .join(OsString::from_vec(b"renamed-\xff (deleted)".to_vec()));
        std::fs::create_dir(&original)?;
        let directory = Dir::open_ambient_dir(&original, cap_std::ambient_authority())?;
        std::fs::rename(&original, &moved)?;
        std::fs::create_dir(&original)?;
        let mut work = ordinary_work();
        let (path, allocation) = canonical_directory_path_held(directory.try_clone()?, &mut work)?;
        assert_eq!(
            path.as_os_str().as_bytes(),
            moved.canonicalize()?.as_os_str().as_bytes()
        );
        drop(path);
        work.release(allocation)?;
        assert_eq!(work.live_allocation_bytes(), 0);
        std::fs::remove_dir(&moved)?;
        assert!(matches!(
            canonical_directory_path_held(directory, &mut work),
            Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::NotFound
        ));
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn held_path_query_does_not_accept_a_non_directory_descriptor()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("file");
        std::fs::write(&path, b"not a directory")?;
        let directory = Dir::from_std_file(File::open(path)?);
        let mut work = ordinary_work();
        assert!(matches!(
            canonical_directory_path_held(directory, &mut work),
            Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::InvalidInput
        ));
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn held_path_growth_charges_truncated_reads_and_retires_old_buffers()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let mut directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let mut expected = temporary.path().canonicalize()?;
        for _ in 0..3 {
            let name = OsString::from_vec(vec![b'a'; 200]);
            directory.create_dir(&name)?;
            directory = directory.open_dir(&name)?;
            expected.push(name);
        }
        let mut work = ordinary_work();
        let (path, allocation) = canonical_directory_path_held(directory, &mut work)?;
        assert_eq!(path, expected);
        assert!(
            work.receipt().source_bytes_read > path.as_os_str().len() as u64,
            "truncated native queries were not charged",
        );
        assert_eq!(work.live_allocation_bytes(), path.capacity() as u64);
        drop(path);
        work.release(allocation)?;
        assert_eq!(work.live_allocation_bytes(), 0);
        Ok(())
    }

    #[test]
    fn held_path_query_retires_buffers_at_original_work_limits()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let mut measured = ordinary_work();
        let (path, allocation) =
            canonical_directory_path_held(directory.try_clone()?, &mut measured)?;
        drop(path);
        measured.release(allocation)?;
        // Exercise every refused native read, including those after a query
        // buffer has been funded, without pinning the implementation's count.
        for read_limit in 0..measured.receipt().backend_read_operations {
            let cancellation = crate::CancellationToken::new();
            let work = NativeMaterializationWork::new(
                WorkBudget {
                    backend_read_operations: read_limit,
                    ..WorkBudget::UNBOUNDED
                },
                &cancellation,
            )
            .map_err(|(error, _)| error)?;
            let baseline = work.live_allocation_bytes();
            assert!(matches!(
                canonical_directory_path_held(directory.try_clone()?, &mut &work),
                Err(NativeTreeMaterializationError::Work(
                    crate::WorkError::BudgetExceeded {
                        counter: "backend_read_operations",
                        ..
                    }
                ))
            ));
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        Ok(())
    }

    #[test]
    fn descriptor_path_capability_absence_is_not_a_semantic_error_fallback()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let directory = Dir::open_ambient_dir(temporary.path(), cap_std::ambient_authority())?;
        let mut work = ordinary_work();
        assert!(
            linux_directory_path_held(&directory, c"/proc/self/fd/no-such-directory", &mut work)?
                .is_none()
        );
        // This is procfs, but not its descriptor table. A failed descriptor
        // query is a semantic failure, not an unsupported-capability fallback.
        assert!(matches!(
            linux_directory_path_held(&directory, c"/proc/self", &mut work),
            Err(DemandError::Io(error)) if error.kind() == io::ErrorKind::NotFound
        ));
        assert_eq!(work.live_allocation_bytes(), 0);
        let cancellation = crate::CancellationToken::new();
        cancellation.cancel();
        let mut cancelled = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        assert!(matches!(
            linux_directory_path_held(&directory, c"/proc/self/fd", &mut cancelled),
            Err(DemandError::Cancelled)
        ));
        assert_eq!(cancelled.receipt(), WorkCounters::UNCHARGED);
        Ok(())
    }
}
