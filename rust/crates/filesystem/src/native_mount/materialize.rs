//! Explicit native materialization of one authenticated checkout.

use crate::kernel::{
    ExtentKind, FileKind, FileMetadata, FilePayload, LogicalName, NameEncoding, NamespacePath,
};
use crate::native_host::HostRoot;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, ByteRange, CancellationToken, Checkout, FileId,
    OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};
use bytes::Bytes;
use std::collections::HashMap;
#[cfg(unix)]
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Exact hard bounds and destination for one materialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializeOptions {
    /// Existing empty directory owned by the caller.
    pub destination: PathBuf,
    /// Maximum directory entries fetched per authenticated page.
    pub maximum_directory_entries: u32,
    /// Maximum sparse spans fetched per authenticated range plan.
    pub maximum_extent_spans: u32,
    /// Maximum bytes read or zero-filled in one allocation.
    pub transfer_bytes: u64,
}

/// Exact result and host-side movement facts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MaterializationReceipt {
    /// Materialized regular-file or hard-link bindings.
    pub files: u64,
    /// Materialized directory bindings, excluding the supplied root.
    pub directories: u64,
    /// Materialized symbolic-link bindings.
    pub symbolic_links: u64,
    /// Materialized FIFO, socket, character-device, or block-device bindings.
    pub special_files: u64,
    /// Logical regular-file bytes represented at the destination.
    pub logical_file_bytes: u64,
    /// Bytes physically written for content and allocated-zero spans.
    pub written_bytes: u64,
    /// Complete canonical-engine and host-movement work.
    pub work: WorkCounters,
}

/// Fail-closed explicit materialization errors.
#[derive(Debug, Error)]
pub enum MaterializeError {
    /// Destination is absent, non-directory, a symlink, or non-empty.
    #[error("materialization destination must be an existing empty real directory")]
    InvalidDestination,
    /// A canonical name cannot be represented exactly on this host.
    #[error("canonical name cannot be represented exactly on this host")]
    UnrepresentableName,
    /// This host adapter cannot recreate the authenticated kind exactly.
    #[error("file kind {0:?} cannot be materialized exactly on this host")]
    UnsupportedKind(FileKind),
    /// Supplied operation bounds are zero or exceed native addressability.
    #[error("materialization options are invalid")]
    InvalidOptions,
    /// Canonical engine operation failed.
    #[error("filesystem engine failed: {0}")]
    Engine(String),
    /// Host filesystem operation failed.
    #[error("host filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    /// Exact work overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

/// Explicitly materializes one checkout without changing or publishing it.
///
/// Sparse files are pre-sized and only authenticated content or allocated-zero
/// spans are written. Hole spans issue no body I/O. Hard links are recreated by
/// stable file identity, and directory enumeration remains bounded and paged.
/// This function is never used as a fallback for failed native mounting.
///
/// # Errors
///
/// Fails before traversal for an invalid destination or zero bounds. During
/// traversal it fails closed on unrepresentable names, unsupported special
/// kinds, canonical engine errors, host I/O, cancellation, or work exhaustion.
pub async fn materialize_checkout<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    let host_root = validate_options(options).map_err(OperationFailure::before_work)?;
    let limits = checkout.volume_config().limits;
    let root = NamespacePath::new(Vec::new(), limits).map_err(|error| {
        OperationFailure::before_work(MaterializeError::Engine(error.to_string()))
    })?;
    let mut pending = vec![(root.clone(), PathBuf::new())];
    let mut deferred_directory_metadata = vec![(root, PathBuf::new())];
    let mut known_files = HashMap::<FileId, PathBuf>::new();
    let mut receipt = MaterializationReceipt::default();

    while let Some((directory, host_directory)) = pending.pop() {
        cancellation.check().map_err(|error| {
            OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
        })?;
        let mut after: Option<LogicalName> = None;
        loop {
            let remaining = receipt.work.remaining(budget).map_err(|error| {
                OperationFailure::new(MaterializeError::Work(error), receipt.work)
            })?;
            let page = checkout
                .list_directory_records(
                    &directory,
                    after.as_ref(),
                    options.maximum_directory_entries,
                    remaining,
                    cancellation,
                )
                .await
                .map_err(|failure| map_engine_failure(failure, receipt.work))?;
            receipt.work = add_work(receipt.work, page.work)?;
            if page.value.entries.is_empty() {
                break;
            }
            for entry in page.value.entries {
                let child = append_path(&directory, entry.name.clone(), limits)
                    .map_err(|error| OperationFailure::new(error, receipt.work))?;
                let host_child = host_directory.join(
                    host_name(&entry.name)
                        .map_err(|error| OperationFailure::new(error, receipt.work))?,
                );
                materialize_entry(
                    checkout,
                    &host_root,
                    &child,
                    &host_child,
                    entry.record.file_id,
                    entry.record.kind,
                    entry.record.payload,
                    options,
                    budget,
                    cancellation,
                    &mut known_files,
                    &mut pending,
                    &mut receipt,
                )
                .await?;
                if entry.record.kind == FileKind::Directory {
                    deferred_directory_metadata.push((child, host_child));
                }
                after = Some(entry.name);
            }
            if !page.value.has_more {
                break;
            }
        }
    }
    for (directory, host_directory) in deferred_directory_metadata.into_iter().rev() {
        apply_metadata(
            checkout,
            &host_root,
            &directory,
            &host_directory,
            budget,
            cancellation,
            &mut receipt,
        )
        .await?;
    }
    Ok(OperationReceipt {
        value: receipt,
        work: receipt.work,
    })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn materialize_entry<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    host_root: &HostRoot,
    path: &NamespacePath,
    host_path: &Path,
    file_id: FileId,
    kind: FileKind,
    payload: FilePayload,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    known_files: &mut HashMap<FileId, PathBuf>,
    pending: &mut Vec<(NamespacePath, PathBuf)>,
    receipt: &mut MaterializationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    if let Some(existing) = known_files.get(&file_id) {
        host_root
            .hard_link(existing, host_path)
            .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
        receipt.files = receipt.files.checked_add(1).ok_or_else(|| {
            OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
        })?;
        account_materialization(receipt, 0, budget)?;
        return Ok(());
    }

    match (kind, payload) {
        (FileKind::Directory, FilePayload::Directory { .. }) => {
            host_root
                .create_dir(host_path)
                .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            known_files.insert(file_id, host_path.to_path_buf());
            pending.push((path.clone(), host_path.to_path_buf()));
            receipt.directories = receipt.directories.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
            return Ok(());
        }
        (FileKind::Regular, FilePayload::InlineRegular(data)) => {
            let mut file = create_file(host_root, host_path, receipt.work)?;
            file.write_all(data.as_bytes())
                .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            file.sync_all()
                .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            known_files.insert(file_id, host_path.to_path_buf());
            account_file(
                receipt,
                data.as_bytes().len() as u64,
                data.as_bytes().len() as u64,
                budget,
            )?;
        }
        (FileKind::Regular, FilePayload::Regular { logical_bytes, .. }) => {
            let mut file = create_file(host_root, host_path, receipt.work)?;
            #[cfg(windows)]
            {
                mark_sparse(&file)
                    .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            }
            file.set_len(logical_bytes)
                .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            materialize_sparse_file(
                checkout,
                path,
                &mut file,
                logical_bytes,
                options,
                budget,
                cancellation,
                receipt,
            )
            .await?;
            file.sync_all()
                .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
            known_files.insert(file_id, host_path.to_path_buf());
            receipt.files = receipt.files.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            receipt.logical_file_bytes = receipt
                .logical_file_bytes
                .checked_add(logical_bytes)
                .ok_or_else(|| {
                    OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
                })?;
            account_materialization(receipt, 0, budget)?;
        }
        (FileKind::SymbolicLink, FilePayload::SymbolicLink { target_bytes, .. }) => {
            let remaining = receipt.work.remaining(budget).map_err(|error| {
                OperationFailure::new(MaterializeError::Work(error), receipt.work)
            })?;
            let target = checkout
                .read_symbolic_link(path, remaining, cancellation)
                .await
                .map_err(|failure| map_engine_failure(failure, receipt.work))?;
            receipt.work = add_work(receipt.work, target.work)?;
            if u64::try_from(target.value.len()).unwrap_or(u64::MAX) != target_bytes {
                return Err(OperationFailure::new(
                    MaterializeError::Engine("symbolic-link length mismatch".to_owned()),
                    receipt.work,
                ));
            }
            create_symlink(host_root, &target.value, host_path)
                .map_err(|error| OperationFailure::new(error, receipt.work))?;
            known_files.insert(file_id, host_path.to_path_buf());
            receipt.symbolic_links = receipt.symbolic_links.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
        }
        (kind @ (FileKind::Fifo | FileKind::Socket), FilePayload::Empty) => {
            create_special(host_root, host_path, kind, None)
                .map_err(|error| OperationFailure::new(error, receipt.work))?;
            known_files.insert(file_id, host_path.to_path_buf());
            receipt.special_files = receipt.special_files.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
        }
        (
            kind @ (FileKind::CharacterDevice | FileKind::BlockDevice),
            FilePayload::Device { major, minor },
        ) => {
            create_special(host_root, host_path, kind, Some((major, minor)))
                .map_err(|error| OperationFailure::new(error, receipt.work))?;
            known_files.insert(file_id, host_path.to_path_buf());
            receipt.special_files = receipt.special_files.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
        }
        (unsupported, _) => {
            return Err(OperationFailure::new(
                MaterializeError::UnsupportedKind(unsupported),
                receipt.work,
            ));
        }
    }
    apply_metadata(
        checkout,
        host_root,
        path,
        host_path,
        budget,
        cancellation,
        receipt,
    )
    .await
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn mark_sparse(file: &File) -> std::io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_SET_SPARSE;

    let handle = HANDLE(file.as_raw_handle());
    // SAFETY: `handle` is borrowed from the live `File` for the duration of
    // this synchronous call. FSCTL_SET_SPARSE takes no input/output buffers,
    // and every optional pointer is therefore null.
    unsafe {
        DeviceIoControl(handle, FSCTL_SET_SPARSE, None, 0, None, 0, None, None)
            .map_err(|error| std::io::Error::other(error.to_string()))
    }
}

#[allow(clippy::too_many_arguments)]
async fn materialize_sparse_file<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    path: &NamespacePath,
    file: &mut File,
    logical_bytes: u64,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    receipt: &mut MaterializationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    let mut offset = 0_u64;
    while offset < logical_bytes {
        let length = options.transfer_bytes.min(logical_bytes - offset);
        let remaining = receipt
            .work
            .remaining(budget)
            .map_err(|error| OperationFailure::new(MaterializeError::Work(error), receipt.work))?;
        let plan = checkout
            .plan_file_extents(
                path,
                ByteRange { offset, length },
                options.maximum_extent_spans,
                remaining,
                cancellation,
            )
            .await
            .map_err(|failure| map_engine_failure(failure, receipt.work))?;
        receipt.work = add_work(receipt.work, plan.work)?;
        let Some(plan) = plan.value else {
            return Err(OperationFailure::new(
                MaterializeError::Engine(
                    "non-inline file returned an inline extent plan".to_owned(),
                ),
                receipt.work,
            ));
        };
        for span in plan.spans {
            match span.kind {
                ExtentKind::Hole => {
                    #[cfg(target_os = "macos")]
                    crate::native_host::punch_hole(file, span.offset, span.length);
                }
                ExtentKind::AllocatedZero => {
                    write_zeros(
                        file,
                        span.offset,
                        span.length,
                        options.transfer_bytes,
                        receipt,
                        budget,
                    )?;
                }
                ExtentKind::Content { .. } => {
                    let remaining = receipt.work.remaining(budget).map_err(|error| {
                        OperationFailure::new(MaterializeError::Work(error), receipt.work)
                    })?;
                    let read = checkout
                        .read_file_range(
                            path,
                            ByteRange {
                                offset: span.offset,
                                length: span.length,
                            },
                            remaining,
                            cancellation,
                        )
                        .await
                        .map_err(|failure| map_engine_failure(failure, receipt.work))?;
                    receipt.work = add_work(receipt.work, read.work)?;
                    file.seek(SeekFrom::Start(span.offset))
                        .and_then(|_| file.write_all(&read.value.bytes))
                        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
                    account_written(receipt, span.length, budget)?;
                }
            }
        }
        offset = offset.checked_add(length).ok_or_else(|| {
            OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
        })?;
    }
    Ok(())
}

fn write_zeros(
    file: &mut File,
    offset: u64,
    length: u64,
    transfer_bytes: u64,
    receipt: &mut MaterializationReceipt,
    budget: WorkBudget,
) -> Result<(), OperationFailure<MaterializeError>> {
    let capacity = usize::try_from(transfer_bytes.min(length))
        .map_err(|_| OperationFailure::new(MaterializeError::InvalidOptions, receipt.work))?;
    let zeros = vec![0_u8; capacity];
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
    let mut remaining = length;
    while remaining != 0 {
        let count = usize::try_from(remaining.min(transfer_bytes))
            .map_err(|_| OperationFailure::new(MaterializeError::InvalidOptions, receipt.work))?;
        file.write_all(&zeros[..count])
            .map_err(|error| OperationFailure::new(error.into(), receipt.work))?;
        account_written(receipt, count as u64, budget)?;
        remaining -= count as u64;
    }
    Ok(())
}

async fn apply_metadata<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    host_root: &HostRoot,
    path: &NamespacePath,
    host_path: &Path,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    receipt: &mut MaterializationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    let remaining = receipt
        .work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(MaterializeError::Work(error), receipt.work))?;
    let metadata = checkout
        .read_metadata(path, remaining, cancellation)
        .await
        .map_err(|failure| map_engine_failure(failure, receipt.work))?;
    receipt.work = add_work(receipt.work, metadata.work)?;
    apply_host_metadata(host_root, host_path, metadata.value)
        .map_err(|error| OperationFailure::new(error, receipt.work))
}

fn validate_options(options: &MaterializeOptions) -> Result<HostRoot, MaterializeError> {
    if options.maximum_directory_entries == 0
        || options.maximum_extent_spans == 0
        || options.transfer_bytes == 0
        || usize::try_from(options.transfer_bytes).is_err()
    {
        return Err(MaterializeError::InvalidOptions);
    }
    let root =
        HostRoot::open(&options.destination).map_err(|_| MaterializeError::InvalidDestination)?;
    if !root
        .is_empty()
        .map_err(|_| MaterializeError::InvalidDestination)?
    {
        return Err(MaterializeError::InvalidDestination);
    }
    Ok(root)
}

fn append_path(
    parent: &NamespacePath,
    name: LogicalName,
    limits: crate::model::VolumeLimits,
) -> Result<NamespacePath, MaterializeError> {
    let mut components = parent.components().to_vec();
    components.push(name);
    NamespacePath::new(components, limits)
        .map_err(|error| MaterializeError::Engine(error.to_string()))
}

fn create_file(
    host_root: &HostRoot,
    path: &Path,
    work: WorkCounters,
) -> Result<File, OperationFailure<MaterializeError>> {
    host_root
        .create_file(path)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

#[allow(clippy::needless_pass_by_value)]
fn map_engine_failure<E: std::fmt::Display>(
    failure: OperationFailure<E>,
    prior: WorkCounters,
) -> OperationFailure<MaterializeError> {
    match prior.checked_add(*failure.work) {
        Ok(work) => {
            OperationFailure::new(MaterializeError::Engine(failure.error.to_string()), work)
        }
        Err(error) => OperationFailure::new(MaterializeError::Work(error), prior),
    }
}

fn add_work(
    left: WorkCounters,
    right: WorkCounters,
) -> Result<WorkCounters, OperationFailure<MaterializeError>> {
    left.checked_add(right)
        .map_err(|error| OperationFailure::new(error.into(), left))
}

fn account_written(
    receipt: &mut MaterializationReceipt,
    bytes: u64,
    budget: WorkBudget,
) -> Result<(), OperationFailure<MaterializeError>> {
    receipt.written_bytes = receipt.written_bytes.checked_add(bytes).ok_or_else(|| {
        OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
    })?;
    receipt.work = add_work(
        receipt.work,
        WorkCounters {
            bytes_copied: bytes,
            output_bytes: bytes,
            ..WorkCounters::default()
        },
    )?;
    receipt
        .work
        .verify(budget)
        .map_err(|error| OperationFailure::new(MaterializeError::Work(error), receipt.work))
}

fn account_materialization(
    receipt: &mut MaterializationReceipt,
    bytes: u64,
    budget: WorkBudget,
) -> Result<(), OperationFailure<MaterializeError>> {
    account_written(receipt, bytes, budget)?;
    receipt.work = add_work(
        receipt.work,
        WorkCounters {
            materializations: 1,
            ..WorkCounters::default()
        },
    )?;
    receipt
        .work
        .verify(budget)
        .map_err(|error| OperationFailure::new(MaterializeError::Work(error), receipt.work))
}

fn account_file(
    receipt: &mut MaterializationReceipt,
    logical: u64,
    written: u64,
    budget: WorkBudget,
) -> Result<(), OperationFailure<MaterializeError>> {
    receipt.files = receipt.files.checked_add(1).ok_or_else(|| {
        OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
    })?;
    receipt.logical_file_bytes =
        receipt
            .logical_file_bytes
            .checked_add(logical)
            .ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
    account_materialization(receipt, written, budget)
}

#[cfg(unix)]
pub(crate) fn host_name(name: &LogicalName) -> Result<OsString, MaterializeError> {
    use std::os::unix::ffi::OsStringExt;
    match name.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            Ok(OsString::from_vec(name.as_bytes().to_vec()))
        }
        NameEncoding::WindowsUtf16Le => Err(MaterializeError::UnrepresentableName),
    }
}

#[cfg(windows)]
pub(crate) fn host_name(name: &LogicalName) -> Result<OsString, MaterializeError> {
    use std::os::windows::ffi::OsStringExt;
    match name.encoding() {
        NameEncoding::Utf8 => std::str::from_utf8(name.as_bytes())
            .map(OsString::from)
            .map_err(|_| MaterializeError::UnrepresentableName),
        NameEncoding::WindowsUtf16Le => Ok(OsString::from_wide(
            &name
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                .collect::<Vec<_>>(),
        )),
        NameEncoding::PosixBytes => Err(MaterializeError::UnrepresentableName),
    }
}

#[cfg(unix)]
fn create_symlink(
    host_root: &HostRoot,
    target: &Bytes,
    destination: &Path,
) -> Result<(), MaterializeError> {
    use std::os::unix::ffi::OsStrExt;
    host_root
        .symlink(OsStr::from_bytes(target), destination)
        .map_err(Into::into)
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn create_special(
    host_root: &HostRoot,
    destination: &Path,
    kind: FileKind,
    device: Option<(u32, u32)>,
) -> Result<(), MaterializeError> {
    use std::os::unix::ffi::OsStrExt;
    let destination = std::ffi::CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| MaterializeError::UnrepresentableName)?;
    let result = match kind {
        FileKind::Fifo => {
            // SAFETY: `destination` is a live NUL-terminated byte string and
            // the mode contains only a conventional permission mask.
            unsafe { libc::mkfifoat(host_root.raw_directory_fd(), destination.as_ptr(), 0o600) }
        }
        FileKind::CharacterDevice | FileKind::BlockDevice => {
            let Some((major, minor)) = device else {
                return Err(MaterializeError::UnsupportedKind(kind));
            };
            let file_type = if kind == FileKind::CharacterDevice {
                libc::S_IFCHR
            } else {
                libc::S_IFBLK
            };
            let device_number = match super::device::join_device(major, minor) {
                Ok(device_number) => device_number,
                Err(errno) => return Err(std::io::Error::from_raw_os_error(errno).into()),
            };
            // SAFETY: `destination` is a live NUL-terminated byte string and
            // the mode is a value-only libc operation.
            unsafe {
                libc::mknodat(
                    host_root.raw_directory_fd(),
                    destination.as_ptr(),
                    file_type | 0o600,
                    device_number,
                )
            }
        }
        FileKind::Socket => {
            host_root.bind_unix_socket(Path::new(OsStr::from_bytes(destination.as_bytes())))?;
            return Ok(());
        }
        _ => return Err(MaterializeError::UnsupportedKind(kind)),
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

#[cfg(not(unix))]
fn create_special(
    _: &HostRoot,
    _: &Path,
    kind: FileKind,
    _: Option<(u32, u32)>,
) -> Result<(), MaterializeError> {
    Err(MaterializeError::UnsupportedKind(kind))
}

#[cfg(windows)]
fn create_symlink(
    host_root: &HostRoot,
    target: &Bytes,
    destination: &Path,
) -> Result<(), MaterializeError> {
    use std::os::windows::ffi::OsStringExt;
    if !target.len().is_multiple_of(2) {
        return Err(MaterializeError::UnrepresentableName);
    }
    let units = target
        .chunks_exact(2)
        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
        .collect::<Vec<_>>();
    host_root
        .symlink_file(Path::new(&OsString::from_wide(&units)), destination)
        .map_err(Into::into)
}

#[cfg(unix)]
fn apply_host_metadata(
    host_root: &HostRoot,
    path: &Path,
    metadata: FileMetadata,
) -> Result<(), MaterializeError> {
    use crate::kernel::MetadataField;
    use cap_std::fs::PermissionsExt;
    let file_type = host_root.symlink_metadata(path)?.file_type();
    if file_type.is_symlink() {
        return if metadata_is_unavailable(metadata) {
            Ok(())
        } else {
            Err(MaterializeError::UnsupportedKind(FileKind::SymbolicLink))
        };
    }
    if let MetadataField::Value(mode) = metadata.posix_mode {
        if file_type.is_file() || file_type.is_dir() {
            host_root.set_permissions(path, cap_std::fs::Permissions::from_mode(mode & 0o7777))?;
        } else {
            host_root.set_permissions_without_open(path, mode & 0o7777)?;
        }
    }
    Ok(())
}

#[cfg(windows)]
fn apply_host_metadata(
    host_root: &HostRoot,
    path: &Path,
    metadata: FileMetadata,
) -> Result<(), MaterializeError> {
    use crate::kernel::MetadataField;
    if host_root.symlink_metadata(path)?.file_type().is_symlink() {
        return if metadata_is_unavailable(metadata) {
            Ok(())
        } else {
            Err(MaterializeError::UnsupportedKind(FileKind::SymbolicLink))
        };
    }
    if let MetadataField::Value(attributes) = metadata.windows_attributes {
        let mut permissions = host_root.symlink_metadata(path)?.permissions();
        permissions.set_readonly(attributes & 1 != 0);
        host_root.set_permissions(path, permissions)?;
    }
    Ok(())
}

fn metadata_is_unavailable(metadata: FileMetadata) -> bool {
    use crate::kernel::MetadataField;
    matches!(metadata.posix_mode, MetadataField::Unavailable)
        && matches!(metadata.posix_uid, MetadataField::Unavailable)
        && matches!(metadata.posix_gid, MetadataField::Unavailable)
        && matches!(metadata.posix_flags, MetadataField::Unavailable)
        && matches!(metadata.windows_attributes, MetadataField::Unavailable)
        && matches!(metadata.created_ns, MetadataField::Unavailable)
        && matches!(metadata.modified_ns, MetadataField::Unavailable)
        && matches!(metadata.accessed_ns, MetadataField::Unavailable)
        && matches!(metadata.changed_ns, MetadataField::Unavailable)
        && matches!(metadata.named_attributes, MetadataField::Unavailable)
        && matches!(metadata.acl, MetadataField::Unavailable)
        && matches!(metadata.security_descriptor, MetadataField::Unavailable)
}
