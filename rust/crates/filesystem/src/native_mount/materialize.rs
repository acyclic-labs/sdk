//! Explicit native materialization of one authenticated checkout.

#[cfg(any(target_os = "macos", windows))]
use crate::ObjectId;
use crate::heap_future::in_heap;
use crate::kernel::{
    ExtentKind, FileKind, FileMetadata, FilePayload, LogicalName, NameEncoding, NamespacePath,
};
use crate::native_host::{HostDirectory, HostRoot};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, ByteRange, CancellationToken, Checkout, FileId,
    OperationFailure, OperationReceipt, PinnedReader, ResolvedFileRangeReadRequest, WorkBudget,
    WorkCounters, WorkError,
};
use bytes::Bytes;
#[cfg(unix)]
use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use crate::demand::{DemandError, DemandWork};

/// One operation-wide ledger, shared by traversal, host work and source reads.
struct GenerationReceipt {
    value: MaterializationReceipt,
    meter: Arc<GenerationNativeMeter>,
    cancellation: CancellationToken,
}

impl std::ops::Deref for GenerationReceipt {
    type Target = MaterializationReceipt;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl std::ops::DerefMut for GenerationReceipt {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl GenerationReceipt {
    fn new(
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, OperationFailure<MaterializeError>> {
        let mut meter = DemandWork::new(budget, cancellation);
        let bytes = std::mem::size_of::<std::sync::Mutex<DemandWork>>();
        meter.allocate(bytes as u64).map_err(|error| {
            OperationFailure::new(materialize_demand_error(error), meter.work)
        })?;
        Ok(Self {
            value: MaterializationReceipt { work: meter.work, ..MaterializationReceipt::default() },
            meter: Arc::new(GenerationNativeMeter(std::sync::Mutex::new(meter))),
            cancellation: cancellation.clone(),
        })
    }

    fn measured<T>(
        &mut self,
        operation: impl FnOnce(&mut DemandWork) -> Result<T, DemandError>,
    ) -> Result<T, OperationFailure<MaterializeError>> {
        let mut meter = self.meter.lock();
        meter.work = self.value.work;
        let result = operation(&mut meter);
        self.value.work = meter.work;
        result.map_err(|error| OperationFailure::new(materialize_demand_error(error), self.work))
    }

    fn remaining(&mut self) -> Result<WorkBudget, OperationFailure<MaterializeError>> {
        self.measured(|work| work.remaining())
    }

    fn merge(&mut self, work: WorkCounters) -> Result<(), OperationFailure<MaterializeError>> {
        self.measured(|meter| meter.merge(work))
    }

    fn allocate(&mut self, bytes: usize) -> Result<(), OperationFailure<MaterializeError>> {
        let bytes = u64::try_from(bytes).map_err(|_| {
            OperationFailure::new(MaterializeError::Work(WorkError::Overflow), self.work)
        })?;
        self.measured(|work| work.allocate(bytes))
    }

    fn release(&mut self, bytes: usize) -> Result<(), OperationFailure<MaterializeError>> {
        self.measured(|work| work.release(bytes as u64))
    }

    fn reserve<T>(
        &mut self,
        values: &mut Vec<T>,
        additional: usize,
    ) -> Result<(), OperationFailure<MaterializeError>> {
        let old_capacity = values.capacity();
        let required = values.len().checked_add(additional).ok_or_else(|| {
            OperationFailure::new(MaterializeError::Work(WorkError::Overflow), self.work)
        })?;
        let growth = required.saturating_sub(old_capacity);
        let bytes = growth.checked_mul(std::mem::size_of::<T>()).ok_or_else(|| {
            OperationFailure::new(MaterializeError::Work(WorkError::Overflow), self.work)
        })?;
        if growth == 0 {
            return Ok(());
        }
        let prior_peak = self.work.peak_allocation_bytes;
        self.allocate(bytes)?;
        if values.try_reserve_exact(additional).is_err() {
            self.release(bytes)?;
            self.measured(|work| {
                work.work.peak_allocation_bytes = prior_peak;
                Ok(())
            })?;
            return Err(OperationFailure::new(
                MaterializeError::Io(std::io::ErrorKind::OutOfMemory.into()), self.work,
            ));
        }
        let actual = values.capacity().checked_sub(old_capacity)
            .and_then(|growth| growth.checked_mul(std::mem::size_of::<T>()))
            .ok_or_else(|| OperationFailure::new(
                MaterializeError::Work(WorkError::Overflow), self.work,
            ))?;
        if actual > bytes {
            self.measured(|work| work.adopt((actual - bytes) as u64))?;
        }
        Ok(())
    }

    fn push<T>(
        &mut self,
        values: &mut Vec<T>,
        value: T,
    ) -> Result<(), OperationFailure<MaterializeError>> {
        if values.len() == values.capacity() {
            let additional = values.capacity().max(1);
            self.reserve(values, additional)?;
        }
        values.push(value);
        Ok(())
    }

    fn native_meter(&mut self) -> Arc<dyn acyclic_native_runtime::NativeReadMeter> {
        self.meter.lock().work = self.value.work;
        self.meter.clone()
    }

    fn runtime_result<T>(
        &mut self,
        result: std::io::Result<T>,
    ) -> Result<T, OperationFailure<MaterializeError>> {
        self.value.work = self.meter.lock().work;
        result.map_err(|error| {
            let mapped = if let Some(work) = error.get_ref().and_then(|error| error.downcast_ref::<WorkError>()) {
                MaterializeError::Work(*work)
            } else if let Some(demand) = error.get_ref().and_then(|error| error.downcast_ref::<DemandError>()) {
                match demand {
                    DemandError::Work(work) => MaterializeError::Work(*work),
                    DemandError::Cancelled => MaterializeError::Cancelled(crate::CancellationError),
                    _ => MaterializeError::Io(error),
                }
            } else {
                MaterializeError::Io(error)
            };
            OperationFailure::new(mapped, self.work)
        })
    }

    fn engine_failure(
        &mut self,
        failure: OperationFailure<crate::FsError>,
    ) -> OperationFailure<MaterializeError> {
        let error = MaterializeError::from(failure.error);
        match self.merge(*failure.work) {
            Ok(()) => OperationFailure::new(error, self.work),
            Err(failure) => failure,
        }
    }
}

fn materialize_demand_error(error: DemandError) -> MaterializeError {
    match error {
        DemandError::Work(error) => MaterializeError::Work(error),
        DemandError::Cancelled => MaterializeError::Cancelled(crate::CancellationError),
        DemandError::Io(error) => MaterializeError::Io(error),
        #[cfg(target_os = "linux")]
        DemandError::LinuxMetadata(error) => linux_metadata_error(error),
        #[cfg(target_os = "macos")]
        DemandError::MacMetadata(error) => mac_metadata_error(error),
        #[cfg(windows)]
        DemandError::WindowsMetadata(error) => windows_metadata_error(error),
        error => MaterializeError::Engine(error.to_string()),
    }
}

struct GenerationNativeMeter(std::sync::Mutex<DemandWork>);

fn generation_native_error(error: impl Into<DemandError>) -> std::io::Error {
    let error = error.into();
    if matches!(error, DemandError::Cancelled) {
        std::io::Error::new(std::io::ErrorKind::Interrupted, error)
    } else {
        std::io::Error::other(error)
    }
}

impl GenerationNativeMeter {
    fn lock(&self) -> std::sync::MutexGuard<'_, DemandWork> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn admit(&self, delta: WorkCounters) -> std::io::Result<()> {
        let work = self.lock();
        work.check().map_err(generation_native_error)?;
        work.work.admit(&delta, &work.budget).map_err(generation_native_error)
    }
}

impl acyclic_native_runtime::NativeReadMeter for GenerationNativeMeter {
    fn check(&self) -> std::io::Result<()> {
        self.lock().check().map_err(generation_native_error)
    }

    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        self.lock().allocate(bytes).map_err(generation_native_error)
    }

    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        self.lock().charge(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::UNCHARGED
        }).map_err(generation_native_error)
    }

    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        self.lock().adopt(bytes).map_err(generation_native_error)
    }

    fn release(&self, bytes: u64) {
        self.lock().release(bytes).expect("generation native allocation ownership balances");
    }

    fn examine(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.lock().charge(WorkCounters { items_examined: items, bytes_copied, ..WorkCounters::UNCHARGED })
            .map_err(generation_native_error)
    }

    fn admit_examination(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.admit(WorkCounters { items_examined: items, bytes_copied, ..WorkCounters::UNCHARGED })
    }

    fn record_examination(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.lock().record(WorkCounters { items_examined: items, bytes_copied, ..WorkCounters::UNCHARGED })
            .map_err(generation_native_error)
    }

    fn admit_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.admit(WorkCounters { source_bytes_read: maximum as u64, ..WorkCounters::UNCHARGED })
    }

    fn admit_read(&self, maximum: usize) -> std::io::Result<()> {
        self.admit_bytes(maximum)?;
        self.lock().charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })
            .map_err(generation_native_error)
    }

    fn admit_operations(&self, additional: u64) -> std::io::Result<()> {
        self.admit(WorkCounters { backend_read_operations: additional, ..WorkCounters::UNCHARGED })
    }

    fn record_operations(&self, completed: u64) -> std::io::Result<()> {
        self.lock().record(WorkCounters { backend_read_operations: completed, ..WorkCounters::UNCHARGED })
            .map_err(generation_native_error)
    }

    fn record_read(&self, bytes: usize) -> std::io::Result<()> {
        self.lock().record(WorkCounters { source_bytes_read: bytes as u64, ..WorkCounters::UNCHARGED })
            .map_err(generation_native_error)
    }

    fn admit_write(&self, maximum: usize) -> std::io::Result<()> {
        self.admit(WorkCounters {
            backend_write_operations: 1,
            output_bytes: maximum as u64,
            ..WorkCounters::UNCHARGED
        })?;
        self.lock().charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })
            .map_err(generation_native_error)
    }

    fn record_write(&self, bytes: usize) -> std::io::Result<()> {
        self.lock().record(WorkCounters { output_bytes: bytes as u64, ..WorkCounters::UNCHARGED })
            .map_err(generation_native_error)
    }

    fn admit_write_operations(&self, additional: u64) -> std::io::Result<()> {
        self.admit(WorkCounters { backend_write_operations: additional, ..WorkCounters::UNCHARGED })
    }

    fn record_write_operations(&self, completed: u64) -> std::io::Result<()> {
        self.lock().record(WorkCounters { backend_write_operations: completed, ..WorkCounters::UNCHARGED })
            .map_err(generation_native_error)
    }

    fn admit_durability(&self) -> std::io::Result<()> {
        self.lock().charge(WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED })
            .map_err(generation_native_error)
    }
}

fn clone_namespace_measured(
    path: &NamespacePath,
    extra: Option<&LogicalName>,
    limits: crate::model::VolumeLimits,
    receipt: &mut GenerationReceipt,
) -> Result<NamespacePath, OperationFailure<MaterializeError>> {
    let remaining = receipt.remaining()?;
    let result = match extra {
        Some(name) => path.child_measured(name, limits, remaining, &receipt.cancellation, 0),
        None => path.clone_measured(remaining, &receipt.cancellation, 0),
    };
    let result = match result {
        Ok(result) => result,
        Err(failure) => {
            receipt.merge(*failure.work)?;
            let error = match failure.error {
                crate::kernel::NamespacePathError::Work(error) => MaterializeError::Work(error),
                crate::kernel::NamespacePathError::Cancelled => MaterializeError::Cancelled(crate::CancellationError),
                error => MaterializeError::Engine(error.to_string()),
            };
            return Err(OperationFailure::new(error, receipt.work));
        }
    };
    receipt.merge(result.work)?;
    receipt.measured(|work| work.adopt(namespace_owned_bytes(&result.value) as u64))?;
    Ok(result.value)
}

fn namespace_owned_bytes(path: &NamespacePath) -> usize {
    path.retained_bytes() as usize
}

fn share_generation_namespace(
    path: NamespacePath,
    receipt: &mut GenerationReceipt,
) -> Result<Arc<NamespacePath>, OperationFailure<MaterializeError>> {
    receipt.allocate(std::mem::size_of::<NamespacePath>())?;
    Ok(Arc::new(path))
}

/// Native path storage whose capacity and encoding are visible to the ledger.
struct GenerationHostPath {
    #[cfg(unix)]
    bytes: Vec<u8>,
    #[cfg(windows)]
    units: Vec<u16>,
}

impl GenerationHostPath {
    fn from_namespace(
        path: &NamespacePath,
        receipt: &mut GenerationReceipt,
    ) -> Result<Arc<Self>, OperationFailure<MaterializeError>> {
        Self::from_components(None, path.components(), receipt)
    }

    fn child(
        &self,
        name: &LogicalName,
        receipt: &mut GenerationReceipt,
    ) -> Result<Arc<Self>, OperationFailure<MaterializeError>> {
        Self::from_components(Some(self), std::slice::from_ref(name), receipt)
    }

    fn from_components(
        prefix: Option<&Self>,
        names: &[LogicalName],
        receipt: &mut GenerationReceipt,
    ) -> Result<Arc<Self>, OperationFailure<MaterializeError>> {
        #[cfg(unix)]
        {
            let prefix = prefix.map_or(&[][..], |prefix| prefix.bytes.as_slice());
            let mut length = prefix.len().checked_add(names.len().saturating_sub(1))
                .and_then(|length| length.checked_add(usize::from(!prefix.is_empty() && !names.is_empty())))
                .ok_or_else(|| OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work))?;
            for name in names {
                receipt.measured(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
                if !matches!(name.encoding(), NameEncoding::Utf8 | NameEncoding::PosixBytes) {
                    return Err(OperationFailure::new(MaterializeError::UnrepresentableName, receipt.work));
                }
                length = length.checked_add(name.as_bytes().len()).ok_or_else(|| {
                    OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
                })?;
            }
            let mut bytes = Vec::new();
            receipt.reserve(&mut bytes, length)?;
            receipt.measured(|work| work.charge(WorkCounters { bytes_copied: length as u64, ..WorkCounters::UNCHARGED }))?;
            bytes.extend_from_slice(prefix);
            for name in names {
                if !bytes.is_empty() { bytes.push(b'/'); }
                bytes.extend_from_slice(name.as_bytes());
            }
            Self::share(Self { bytes }, receipt)
        }
        #[cfg(windows)]
        {
            let prefix = prefix.map_or(&[][..], |prefix| prefix.units.as_slice());
            let mut length = prefix.len().checked_add(names.len().saturating_sub(1))
                .and_then(|length| length.checked_add(usize::from(!prefix.is_empty() && !names.is_empty())))
                .ok_or_else(|| OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work))?;
            for name in names {
                receipt.measured(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
                let count = match name.encoding() {
                    NameEncoding::WindowsUtf16Le => name.as_bytes().len() / 2,
                    NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                        let text = std::str::from_utf8(name.as_bytes()).map_err(|_| {
                            OperationFailure::new(MaterializeError::UnrepresentableName, receipt.work)
                        })?;
                        let mut count = 0_usize;
                        for _ in text.encode_utf16() {
                            receipt.measured(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
                            count = count.checked_add(1).ok_or_else(|| {
                                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
                            })?;
                        }
                        count
                    }
                };
                length = length.checked_add(count).ok_or_else(|| {
                    OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
                })?;
            }
            let mut units = Vec::new();
            receipt.reserve(&mut units, length)?;
            receipt.measured(|work| work.charge(WorkCounters {
                bytes_copied: prefix.len() as u64 * 2,
                bytes_encoded: (length - prefix.len()) as u64 * 2,
                ..WorkCounters::UNCHARGED
            }))?;
            units.extend_from_slice(prefix);
            for name in names {
                if !units.is_empty() { units.push(u16::from(b'\\')); }
                match name.encoding() {
                    NameEncoding::WindowsUtf16Le => units.extend(
                        crate::kernel::types::utf16le_units(name.as_bytes()).ok_or_else(|| {
                            OperationFailure::new(MaterializeError::UnrepresentableName, receipt.work)
                        })?,
                    ),
                    NameEncoding::Utf8 | NameEncoding::PosixBytes => units.extend(
                        std::str::from_utf8(name.as_bytes()).map_err(|_| {
                            OperationFailure::new(MaterializeError::UnrepresentableName, receipt.work)
                        })?.encode_utf16(),
                    ),
                }
            }
            Self::share(Self { units }, receipt)
        }
    }

    fn share(
        path: Self,
        receipt: &mut GenerationReceipt,
    ) -> Result<Arc<Self>, OperationFailure<MaterializeError>> {
        receipt.allocate(std::mem::size_of::<Self>())?;
        Ok(Arc::new(path))
    }

    #[cfg(unix)]
    fn path(&self) -> &Path {
        use std::os::unix::ffi::OsStrExt as _;
        Path::new(OsStr::from_bytes(&self.bytes))
    }

    fn create_parents(
        &self,
        root: &HostRoot,
        work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        #[cfg(unix)]
        if let Some(parent) = self.path().parent().filter(|parent| !parent.as_os_str().is_empty()) {
            root.create_dir_all_held_measured(parent, work)?;
        }
        #[cfg(windows)]
        if let Some(separator) = self.units.iter().rposition(|unit| *unit == u16::from(b'\\')) {
            root.create_dir_all_held_wide_measured(&self.units[..separator], work)?;
        }
        Ok(())
    }

    fn create_dir(&self, root: &HostRoot, work: &mut DemandWork) -> Result<(), DemandError> {
        #[cfg(unix)]
        { root.create_dir_measured(self.path(), work) }
        #[cfg(windows)]
        { root.create_dir_wide_measured(&self.units, work) }
    }

    fn hard_link(&self, root: &HostRoot, source: &Self, work: &mut DemandWork) -> Result<(), DemandError> {
        #[cfg(unix)]
        { root.hard_link_measured(source.path(), self.path(), work) }
        #[cfg(windows)]
        { root.hard_link_wide_measured(&source.units, &self.units, work) }
    }

    #[cfg(any(target_os = "macos", windows))]
    fn clone_file(&self, root: &HostRoot, source: &Self, work: &mut DemandWork) -> Result<bool, DemandError> {
        #[cfg(target_os = "macos")]
        { root.clone_file_measured(source.path(), self.path(), work) }
        #[cfg(windows)]
        { root.clone_file_wide_measured(&source.units, &self.units, work) }
    }

    fn stat(&self, root: &HostRoot, work: &mut DemandWork) -> Result<(), DemandError> {
        #[cfg(unix)]
        root.stat_measured(self.path(), work)?;
        #[cfg(windows)]
        root.stat_wide_measured(&self.units, work)?;
        Ok(())
    }
}

static RESTORE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Exact hard bounds and destination for one materialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializeOptions {
    /// Existing empty directory owned by the caller.
    pub destination: PathBuf,
    /// Original host allocation, when attachment follows a durable preparation.
    /// Checked on the held root that performs every write, not a prior path probe.
    pub expected_root_identity: Option<crate::NativeRootIdentity>,
    /// Maximum directory entries fetched per authenticated page.
    pub maximum_directory_entries: u32,
    /// Maximum sparse spans fetched per authenticated range plan.
    pub maximum_extent_spans: u32,
    /// Maximum bytes read or zero-filled in one allocation.
    pub transfer_bytes: u64,
}

/// Both outputs preserve canonical metadata in the SDK. Host-generated Unix
/// timestamps are view-local and are not required to match the physical view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaterializeMode {
    DurableOutput,
    ReconstructibleView,
}

/// What a host applies for one record: the metadata it sets directly, and
/// the root of named attributes it sets separately, whose values live in the
/// generation rather than in the record.
struct HostMetadata {
    metadata: FileMetadata,
    named_attributes: Option<crate::ObjectId>,
}

impl MaterializeMode {
    fn host_metadata(metadata: FileMetadata) -> HostMetadata {
        #[cfg(unix)]
        let mut metadata = metadata;
        #[cfg(unix)]
        {
            metadata.changed_ns = crate::kernel::MetadataField::Unavailable;
        }
        #[cfg(target_os = "linux")]
        {
            metadata.created_ns = crate::kernel::MetadataField::Unavailable;
            // Linux files carry no BSD flags: "none" is exactly the host's
            // state, so there is nothing to apply. Any set flag still fails
            // closed as unrepresentable.
            if metadata.posix_flags == crate::kernel::MetadataField::Value(0) {
                metadata.posix_flags = crate::kernel::MetadataField::Unavailable;
            }
        }
        // Linux and macOS set extended attributes one by one after the
        // rest; Windows still refuses them as unrepresentable.
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let named_attributes = match metadata.named_attributes {
            crate::kernel::MetadataField::Value(root) => {
                metadata.named_attributes = crate::kernel::MetadataField::Unavailable;
                Some(root)
            }
            _ => None,
        };
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let named_attributes = None;
        HostMetadata {
            metadata,
            named_attributes,
        }
    }
}

impl MaterializeOptions {
    /// Creates the canonical bounded native materialization policy.
    #[must_use]
    pub fn native(destination: impl Into<PathBuf>) -> Self {
        Self {
            destination: destination.into(),
            expected_root_identity: None,
            maximum_directory_entries: 1_024,
            maximum_extent_spans: 65_536,
            transfer_bytes: 8 * 1024 * 1024,
        }
    }

    /// Requires the original physical allocation without adopting a replacement.
    #[must_use]
    pub fn expected_root_identity(mut self, identity: Option<crate::NativeRootIdentity>) -> Self {
        self.expected_root_identity = identity;
        self
    }
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

/// How a materialized host path replaces its current destination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostPathReplacement {
    /// Exchange an existing entry atomically on the same volume.
    Atomic,
    /// Route ordinary renames through a live kernel mount.
    LiveMount,
}

/// Exact result of restoring one authenticated path into a host tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HostPathRestore {
    /// The authenticated entry was materialized and published.
    Restored,
    /// The authenticated path was absent and the host entry was removed.
    Removed,
}

/// Fail-closed explicit materialization errors.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum MaterializeError {
    /// Destination is absent, non-directory, a symlink, or non-empty.
    #[error("materialization destination must be an existing empty real directory")]
    InvalidDestination,
    /// The destination path now names another physical allocation.
    #[error("materialization destination allocation changed: expected {expected:?}, actual {actual:?}")]
    AllocationChanged {
        /// Original witnessed allocation.
        expected: crate::NativeRootIdentity,
        /// Allocation actually opened for the attempted materialization.
        actual: crate::NativeRootIdentity,
    },
    /// A canonical name cannot be represented exactly on this host.
    #[error("canonical name cannot be represented exactly on this host")]
    UnrepresentableName,
    /// The requested authenticated path is absent from the checkout.
    #[error("requested path is absent from the checkout")]
    MissingPath,
    /// Host path is empty, absolute, escapes the root, or cannot be encoded.
    #[error("requested host path is not a valid relative path")]
    InvalidPath,
    /// This host adapter cannot recreate the authenticated kind exactly.
    #[error("file kind {0:?} cannot be materialized exactly on this host")]
    UnsupportedKind(FileKind),
    /// This host adapter cannot reproduce an authenticated metadata field.
    #[error("metadata field {0} cannot be materialized exactly on this host")]
    UnsupportedMetadata(&'static str),
    /// Supplied operation bounds are zero or exceed native addressability.
    #[error("materialization options are invalid")]
    InvalidOptions,
    /// Selected paths overlap, so pathwise staging would be order-dependent.
    #[error("materialization paths must be unique and non-overlapping")]
    OverlappingPaths,
    /// Canonical engine operation failed.
    #[error("filesystem engine failed: {0}")]
    Engine(String),
    /// Host filesystem operation failed.
    #[error("host filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    /// Exact work overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// The original caller cancellation stopped materialization.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
}

impl From<crate::FsError> for MaterializeError {
    fn from(error: crate::FsError) -> Self {
        match error {
            crate::FsError::Work(error)
            | crate::FsError::Authority(crate::AuthorityStoreError::Work(error))
            | crate::FsError::Object(crate::ObjectStoreError::Work(error))
            | crate::FsError::Path(crate::kernel::PathLookupError::Work(error))
            | crate::FsError::Path(crate::kernel::PathLookupError::FileTable(
                crate::kernel::FileRecordReadError::Work(error)
                | crate::kernel::FileRecordReadError::Storage(crate::ObjectStoreError::Work(error)),
            ))
            | crate::FsError::Path(crate::kernel::PathLookupError::Tree(
                crate::kernel::TreeReadError::Work(error)
                | crate::kernel::TreeReadError::Storage(crate::ObjectStoreError::Work(error)),
            )) => Self::Work(error),
            crate::FsError::Cancelled(error) => Self::Cancelled(error),
            crate::FsError::Authority(crate::AuthorityStoreError::Cancelled)
            | crate::FsError::Object(crate::ObjectStoreError::Cancelled)
            | crate::FsError::Path(crate::kernel::PathLookupError::FileTable(
                crate::kernel::FileRecordReadError::Storage(crate::ObjectStoreError::Cancelled),
            ))
            | crate::FsError::Path(crate::kernel::PathLookupError::Tree(
                crate::kernel::TreeReadError::Storage(crate::ObjectStoreError::Cancelled),
            )) => Self::Cancelled(crate::CancellationError),
            error => Self::Engine(error.to_string()),
        }
    }
}

/// Explicitly materializes one checkout without changing or publishing it.
///
/// Sparse files are pre-sized and only authenticated content or allocated-zero
/// spans are written. Hole spans issue no body I/O. Hard links are recreated by
/// stable file identity, and directory enumeration remains bounded and paged.
/// This function is only invoked by explicit materialization requests.
///
/// Source reads, native retries, live owned buffers, metadata, attributes and
/// durability use the same operation-wide budget. Failures retain completed
/// prefixes, including partial native writes. An expected root identity is
/// checked on the opened write capability before any destination mutation.
///
/// Initialize [`acyclic_native_runtime::initialize_native_io_services`] before
/// invocation. Opaque process-wide service startup is not hidden in this work.
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
    let span = crate::obs::span!(
        DEBUG,
        "acyclic.fs.materialize",
        outcome = crate::obs::Empty,
        error.kind = crate::obs::Empty,
        work.items = crate::obs::Empty,
        work.bytes = crate::obs::Empty,
        work.durability = crate::obs::Empty,
    );
    crate::obs::measured_on(
        &span,
        crate::obs::in_span(
            &span,
            materialize_heap_future(
                move |remaining| materialize_checkout_with_mode(
                    checkout, options, remaining, cancellation, MaterializeMode::DurableOutput,
                ),
                budget,
                cancellation,
            ),
        )
        .await,
        |receipt| &receipt.work,
    )
}

async fn materialize_heap_future<F>(
    create: impl FnOnce(WorkBudget) -> F,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>>
where
    F: Future<Output = Result<
        OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>,
    >>,
{
    // This box is the existing driver-stack safety boundary, not a new engine.
    let mut work = DemandWork::new(budget, cancellation);
    let bytes = std::mem::size_of::<F>() as u64;
    work.allocate(bytes).map_err(|error| {
        OperationFailure::new(materialize_demand_error(error), work.work)
    })?;
    let remaining = work.remaining().map_err(|error| {
        OperationFailure::new(materialize_demand_error(error), work.work)
    })?;
    match in_heap(move || create(remaining)).await {
        Ok(mut receipt) => {
            work.merge(receipt.work).map_err(|error| {
                OperationFailure::new(materialize_demand_error(error), work.work)
            })?;
            work.release(bytes).map_err(|error| {
                OperationFailure::new(materialize_demand_error(error), work.work)
            })?;
            receipt.work = work.work;
            receipt.value.work = work.work;
            Ok(receipt)
        }
        Err(failure) => {
            work.merge(*failure.work).map_err(|error| {
                OperationFailure::new(materialize_demand_error(error), work.work)
            })?;
            Err(OperationFailure::new(failure.error, work.work))
        }
    }
}

pub(crate) async fn materialize_checkout_with_mode<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    mode: MaterializeMode,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    let root =
        NamespacePath::new(Vec::new(), checkout.volume_config().limits).map_err(|error| {
            OperationFailure::before_work(MaterializeError::Engine(error.to_string()))
        })?;
    materialize_checkout_paths_with_mode(
        checkout,
        std::slice::from_ref(&root),
        options,
        budget,
        cancellation,
        mode,
    )
    .await
}

/// Materializes one authenticated path into an empty destination directory.
///
/// The destination receives the same relative logical path below its root;
/// for example, `src/main.rs` is written to
/// `options.destination/src/main.rs`.  This lets callers stage a path in a
/// sibling directory and perform their own atomic host-side exchange without
/// reimplementing authenticated lookup, sparse-file handling, hard links, or
/// platform name conversion.  Parent directories created for the selected
/// path are not included in the receipt; the selected node and its subtree
/// are. Hard links within that subtree retain their shared identity; a link
/// to a node outside the selected subtree becomes an independent copy.
///
/// Native I/O services must already be initialized as for [`materialize_checkout`].
pub async fn materialize_checkout_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    path: &NamespacePath,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    materialize_checkout_paths(
        checkout,
        std::slice::from_ref(path),
        options,
        budget,
        cancellation,
    )
    .await
}

/// Materializes multiple non-overlapping authenticated paths in one traversal.
///
/// All selected paths share one destination capability, cumulative work
/// receipt, and file-identity table, preserving hard links across siblings.
/// Traversal and identity-table storage are admitted before growth; a later
/// missing path or exhausted allowance retains all earlier paths' work.
pub async fn materialize_checkout_paths<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    let span = crate::obs::span!(
        DEBUG,
        "acyclic.fs.materialize_paths",
        outcome = crate::obs::Empty,
        error.kind = crate::obs::Empty,
        work.items = crate::obs::Empty,
        work.bytes = crate::obs::Empty,
        work.durability = crate::obs::Empty,
    );
    crate::obs::measured_on(
        &span,
        crate::obs::in_span(
            &span,
            materialize_checkout_paths_with_mode(
                checkout,
                paths,
                options,
                budget,
                cancellation,
                MaterializeMode::DurableOutput,
            ),
        )
        .await,
        |receipt| &receipt.work,
    )
}

#[allow(
    clippy::too_many_lines,
    reason = "one traversal materializes every requested path with shared hard-link and budget state"
)]
pub(crate) async fn materialize_checkout_paths_with_mode<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    checkout: &mut Checkout<A, O>,
    paths: &[NamespacePath],
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    mode: MaterializeMode,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    validate_bounds(options).map_err(OperationFailure::before_work)?;
    if paths.is_empty()
        || (paths.len() != 1 && paths.iter().any(|path| path.components().is_empty()))
    {
        return Err(OperationFailure::before_work(MaterializeError::InvalidOptions));
    }
    let mut receipt = GenerationReceipt::new(budget, cancellation)?;
    let reader = checkout.pinned_reader().map_err(|error| {
        OperationFailure::new(MaterializeError::Engine(error.to_string()), receipt.work)
    })?;
    let selection_bytes = paths.len().checked_mul(std::mem::size_of::<&NamespacePath>()).ok_or_else(|| {
        OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
    })?;
    let mut selected_paths = Vec::<&NamespacePath>::new();
    receipt.reserve(&mut selected_paths, paths.len())?;
    for path in paths {
        let mut insertion = selected_paths.len();
        while insertion != 0 {
            let previous = selected_paths[insertion - 1];
            receipt.measured(|work| work.charge(WorkCounters {
                items_examined: 1, ..WorkCounters::UNCHARGED
            }))?;
            if previous <= path { break; }
            insertion -= 1;
        }
        receipt.measured(|work| work.charge(WorkCounters {
            bytes_copied: ((selected_paths.len() - insertion + 1) * std::mem::size_of::<&NamespacePath>()) as u64,
            ..WorkCounters::UNCHARGED
        }))?;
        selected_paths.insert(insertion, path);
    }
    for pair in selected_paths.windows(2) {
        receipt.measured(|work| work.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        }))?;
        if matches!(pair, [ancestor, path] if path.is_within(ancestor)) {
            return Err(OperationFailure::new(MaterializeError::OverlappingPaths, receipt.work));
        }
    }
    let destination = validate_options_measured(options, &mut receipt)?;
    receipt.allocate(std::mem::size_of::<HostRoot>())?;
    let host_root = Arc::new(destination);
    let mut known_files = Vec::<(FileId, Arc<GenerationHostPath>)>::new();
    #[cfg(any(target_os = "macos", windows))]
    let mut known_payloads = Vec::<((u64, ObjectId, ObjectId), Arc<GenerationHostPath>)>::new();
    let mut pending = Vec::new();
    let mut deferred_directory_metadata = Vec::new();
    let limits = checkout.volume_config().limits;
    for path in selected_paths {
        let host_path = GenerationHostPath::from_namespace(path, &mut receipt)?;
        if path.is_root() {
            let namespace = clone_namespace_measured(path, None, limits, &mut receipt)?;
            let namespace = share_generation_namespace(namespace, &mut receipt)?;
            receipt.push(&mut pending, (namespace.clone(), host_path.clone()))?;
            receipt.push(&mut deferred_directory_metadata, (namespace, host_path))?;
            continue;
        }
        let remaining = receipt.remaining()?;
        let lookup = checkout.lookup_no_follow(path, remaining, cancellation).await
            .map_err(|failure| receipt.engine_failure(failure))?;
        receipt.merge(lookup.work)?;
        let record = lookup.value.record.ok_or_else(|| {
            OperationFailure::new(MaterializeError::MissingPath, receipt.work)
        })?;
        receipt.measured(|work| host_path.create_parents(&host_root, work))?;
        let directory = if record.kind == FileKind::Directory {
            let namespace = clone_namespace_measured(path, None, limits, &mut receipt)?;
            let namespace = share_generation_namespace(namespace, &mut receipt)?;
            receipt.push(&mut deferred_directory_metadata, (namespace.clone(), host_path.clone()))?;
            Some(namespace)
        } else {
            None
        };
        materialize_entry(
            &reader, &host_root, path, &host_path, directory,
            record.file_id, record.kind, record.payload,
            #[cfg(any(target_os = "macos", windows))]
            record.metadata,
            options, budget, cancellation, &mut known_files,
            #[cfg(any(target_os = "macos", windows))]
            &mut known_payloads,
            &mut pending, &mut receipt, mode,
        ).await?;
    }
    receipt.release(selection_bytes)?;
    while let Some((directory, host_directory)) = pending.pop() {
        let mut after: Option<LogicalName> = None;
        loop {
            let remaining = receipt.remaining()?;
            let page = checkout.list_directory_records(
                &directory, after.as_ref(), options.maximum_directory_entries, remaining, cancellation,
            ).await.map_err(|failure| receipt.engine_failure(failure))?;
            receipt.merge(page.work)?;
            if page.value.has_more && page.value.entries.is_empty() {
                return Err(OperationFailure::new(
                    MaterializeError::Engine("directory pagination made no progress".to_owned()),
                    receipt.work,
                ));
            }
            let entry_bytes = page.value.entries.capacity()
                .checked_mul(std::mem::size_of::<crate::DirectoryRecordEntry>())
                .ok_or_else(|| OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work))?;
            receipt.measured(|work| work.adopt(entry_bytes as u64))?;
            for entry in &page.value.entries {
                receipt.measured(|work| work.adopt(entry.name.retained_bytes() as u64))?;
            }
            for entry in page.value.entries {
                let child = clone_namespace_measured(&directory, Some(&entry.name), limits, &mut receipt)?;
                let child_host = host_directory.child(&entry.name, &mut receipt)?;
                let child_bytes = namespace_owned_bytes(&child);
                let mut owned_child = Some(child);
                let selected_directory = if entry.record.kind == FileKind::Directory {
                    let child = share_generation_namespace(owned_child.take().ok_or_else(|| {
                        OperationFailure::new(MaterializeError::InvalidOptions, receipt.work)
                    })?, &mut receipt)?;
                    receipt.push(&mut deferred_directory_metadata, (child.clone(), child_host.clone()))?;
                    Some(child)
                } else {
                    None
                };
                let child = selected_directory.as_deref().or(owned_child.as_ref()).ok_or_else(|| {
                    OperationFailure::new(MaterializeError::InvalidOptions, receipt.work)
                })?;
                materialize_entry(
                    &reader, &host_root, child, &child_host, selected_directory.clone(),
                    entry.record.file_id, entry.record.kind, entry.record.payload,
                    #[cfg(any(target_os = "macos", windows))]
                    entry.record.metadata,
                    options, budget, cancellation, &mut known_files,
                    #[cfg(any(target_os = "macos", windows))]
                    &mut known_payloads,
                    &mut pending, &mut receipt, mode,
                ).await?;
                if entry.record.kind != FileKind::Directory {
                    drop(owned_child);
                    receipt.release(child_bytes)?;
                }
                if let Some(previous) = after.replace(entry.name) {
                    let bytes = previous.retained_bytes();
                    drop(previous);
                    receipt.release(bytes)?;
                }
            }
            receipt.release(entry_bytes)?;
            if !page.value.has_more { break; }
        }
        if let Some(after) = after {
            let bytes = after.retained_bytes();
            drop(after);
            receipt.release(bytes)?;
        }
    }
    for (directory, host_directory) in deferred_directory_metadata.into_iter().rev() {
        apply_metadata(
            &reader, &host_root, &directory, &host_directory, budget, cancellation, &mut receipt,
        ).await?;
        let bytes = namespace_owned_bytes(&directory)
            + std::mem::size_of::<NamespacePath>();
        drop(directory);
        receipt.release(bytes)?;
    }
    let work = receipt.work;
    Ok(OperationReceipt { value: receipt.value, work })
}

/// Materializes a host-relative path using the checkout's configured name
/// profile. Native consumers do not need to construct logical names or know
/// whether this platform stores them as bytes or UTF-16 units.
pub async fn materialize_checkout_host_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    relative: &Path,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<MaterializationReceipt>, OperationFailure<MaterializeError>> {
    let span = crate::obs::span!(
        DEBUG,
        "acyclic.fs.materialize_host_path",
        outcome = crate::obs::Empty,
        error.kind = crate::obs::Empty,
        work.items = crate::obs::Empty,
        work.bytes = crate::obs::Empty,
        work.durability = crate::obs::Empty,
    );
    crate::obs::measured_on(
        &span,
        crate::obs::in_span(&span, async move {
            let limits = checkout.volume_config().limits;
            let profile = checkout.volume_config().profile;
            let mut receipt = GenerationReceipt::new(budget, cancellation)?;
            let mut count = 0_usize;
            for component in relative.components() {
                receipt.measured(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
                if !matches!(component, std::path::Component::Normal(_)) {
                    return Err(OperationFailure::new(MaterializeError::InvalidPath, receipt.work));
                }
                count = count.checked_add(1).ok_or_else(|| {
                    OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
                })?;
                if count > usize::from(limits.maximum_path_depth) {
                    return Err(OperationFailure::new(MaterializeError::InvalidPath, receipt.work));
                }
            }
            if count == 0 {
                return Err(OperationFailure::new(MaterializeError::InvalidPath, receipt.work));
            }
            let mut names = Vec::new();
            receipt.reserve(&mut names, count)?;
            for component in relative.components() {
                receipt.measured(|work| work.charge(WorkCounters {
                    items_examined: 1, source_path_components: 1, ..WorkCounters::UNCHARGED
                }))?;
                let std::path::Component::Normal(name) = component else {
                    return Err(OperationFailure::new(MaterializeError::InvalidPath, receipt.work));
                };
                let (encoding, bytes) = receipt.measured(|work| {
                    crate::native_name::host_name_bytes_measured(name, profile, limits.maximum_component_bytes, work)
                })?;
                let logical = LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
                    .map_err(|_| OperationFailure::new(MaterializeError::InvalidPath, receipt.work))?;
                names.push(logical);
            }
            let path = NamespacePath::new(names, limits)
                .map_err(|_| OperationFailure::new(MaterializeError::InvalidPath, receipt.work))?;
            let remaining = receipt.remaining()?;
            match materialize_checkout_path(checkout, &path, options, remaining, cancellation).await {
                Ok(materialized) => {
                    receipt.merge(materialized.work)?;
                    let mut value = materialized.value;
                    value.work = receipt.work;
                    Ok(OperationReceipt { value, work: receipt.work })
                }
                Err(failure) => {
                    receipt.merge(*failure.work)?;
                    Err(OperationFailure::new(failure.error, receipt.work))
                }
            }
        })
        .await,
        |receipt| &receipt.work,
    )
}

/// Restores one authenticated host-relative path without touching siblings.
///
/// Materialization occurs in a private same-volume stage. Existing entries are
/// exchanged atomically or replaced through ordinary mount-visible renames.
/// An absent authenticated path removes the current host entry.
///
/// Every allowance must be finite. Success and failure include preparation,
/// materialization, publication, durability, and attempted cleanup work.
/// The expected allocation is checked before writes and its held capability
/// remains authoritative through publication or removal.
///
/// Native I/O services must already be initialized as for [`materialize_checkout`].
pub async fn restore_checkout_host_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    relative: &Path,
    replacement: HostPathReplacement,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<HostPathRestore>, OperationFailure<MaterializeError>> {
    let span = crate::obs::span!(
        DEBUG,
        "acyclic.fs.restore_host_path",
        outcome = crate::obs::Empty,
        error.kind = crate::obs::Empty,
        work.items = crate::obs::Empty,
        work.bytes = crate::obs::Empty,
        work.durability = crate::obs::Empty,
    );
    let worker_context = span.clone();
    crate::obs::measured_on(
        &span,
        crate::obs::in_span(&span, async move {
            let work = Arc::new(std::sync::Mutex::new(crate::demand::DemandWork::new(
                budget, cancellation,
            )));
            let result = async {
                if !budget.has_finite_allowances() {
                    return Err(MaterializeError::InvalidOptions);
                }
                validate_bounds(options)?;
                let (destination_root, relative) = {
                    let mut work = restore_work(&work);
                    validate_host_relative_measured(relative, &mut work)?;
                    (
                        restore_owned_path(&options.destination, &mut work)?,
                        restore_owned_path(relative, &mut work)?,
                    )
                };
                let expected_root_identity = options.expected_root_identity;
                let prepared = restore_io(worker_context.clone(), None, Arc::clone(&work), move |work| {
                    prepare_restore(destination_root, relative, expected_root_identity, replacement, work)
                }).await?;
                let PreparedRestore {
                    root, destination_parent, stage_owner, stage_root, stage_name,
                    stage_path, destination_root, relative, lock,
                } = prepared;
                let materialize_options = MaterializeOptions {
                    destination: stage_path,
                    expected_root_identity: Some(stage_root.identity()),
                    maximum_directory_entries: options.maximum_directory_entries,
                    maximum_extent_spans: options.maximum_extent_spans,
                    transfer_bytes: options.transfer_bytes,
                };
                let materialized = async {
                    let remaining = restore_work(&work).remaining().map_err(restore_demand_error)?;
                    let materialized = materialize_checkout_host_path(
                        checkout, &relative, &materialize_options, remaining, cancellation,
                    ).await;
                    let materialization_work = match &materialized {
                        Ok(receipt) => receipt.work,
                        Err(failure) => *failure.work,
                    };
                    // The child already spent this work, including any call
                    // completing concurrently with cancellation.
                    restore_work(&work).merge(materialization_work).map_err(restore_demand_error)?;
                    materialized.map(|_| ()).map_err(|failure| failure.error)
                }.await;
                let stage_path = materialize_options.destination;
                restore_io(worker_context, Some(lock), Arc::clone(&work), move |work| {
                    // Every destination operation retains the originally bound
                    // root and parent; publication never reopens an ambient path.
                    let _root = root;
                    let _destination_root = destination_root;
                    let _stage_path = stage_path;
                    let name = relative.file_name().ok_or(MaterializeError::InvalidPath)?;
                    let result = match materialized {
                        Ok(_) => publish_restore(
                            &destination_parent, &stage_root, &relative, replacement, work,
                        ).map(|()| HostPathRestore::Restored),
                        Err(MaterializeError::MissingPath) => {
                            remove_restored_path(&destination_parent, Path::new(name), &relative, work)
                                .map(|()| HostPathRestore::Removed)
                        }
                        Err(error) => Err(error),
                    };
                    let stage_identity = stage_root.identity();
                    drop(stage_root);
                    let cleanup = stage_owner.remove_identity_measured(&stage_name, stage_identity, work)
                        .and_then(|()| stage_owner.sync_measured(work)).map_err(restore_demand_error);
                    // A failed cleanup is observable, and all cleanup work is
                    // retained even when the primary operation also failed.
                    match result {
                        Ok(value) => cleanup.map(|()| value),
                        Err(error) => { let _ = cleanup; Err(error) }
                    }
                }).await
            }.await;
            let spent = restore_work(&work).work;
            result.map(|value| OperationReceipt { value, work: spent })
                .map_err(|error| OperationFailure::new(error, spent))
        }).await,
        |receipt| &receipt.work,
    )
}

async fn restore_io<T: Send + 'static>(
    context: crate::obs::OperationSpan,
    lock: Option<File>,
    work: Arc<std::sync::Mutex<crate::demand::DemandWork>>,
    operation: impl FnOnce(&mut crate::demand::DemandWork) -> Result<T, MaterializeError> + Send + 'static,
) -> Result<T, MaterializeError> {
    let operation = move || {
        // A cancelled waiter must not release the fence while host I/O continues.
        let _restore_lock = lock;
        operation(&mut restore_work(&work))
    };
    tokio::task::spawn_blocking(crate::obs::in_context(context, operation))
        .await
        .map_err(|error| MaterializeError::Engine(error.to_string()))?
}

fn restore_work(
    work: &std::sync::Mutex<crate::demand::DemandWork>,
) -> std::sync::MutexGuard<'_, crate::demand::DemandWork> {
    work.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn restore_demand_error(error: crate::demand::DemandError) -> MaterializeError {
    match error {
        crate::demand::DemandError::Work(error) => MaterializeError::Work(error),
        crate::demand::DemandError::Io(error) => MaterializeError::Io(error),
        crate::demand::DemandError::Cancelled => MaterializeError::Cancelled(crate::CancellationError),
        _ => MaterializeError::InvalidDestination,
    }
}

struct PreparedRestore {
    root: HostRoot,
    destination_parent: HostDirectory,
    stage_owner: HostDirectory,
    stage_root: HostRoot,
    stage_name: PathBuf,
    stage_path: PathBuf,
    destination_root: PathBuf,
    relative: PathBuf,
    lock: File,
}

fn restore_owned_path(path: &Path, work: &mut crate::demand::DemandWork) -> Result<PathBuf, MaterializeError> {
    let bytes = path.as_os_str().as_encoded_bytes().len() as u64;
    let delta = WorkCounters { bytes_copied: bytes, ..WorkCounters::UNCHARGED };
    work.work.admit(&delta, &work.budget)?;
    work.allocate(bytes).map_err(restore_demand_error)?;
    let path = path.to_path_buf();
    work.record(delta).map_err(restore_demand_error)?;
    Ok(path)
}

fn restore_join_path(parent: &Path, name: &Path, work: &mut crate::demand::DemandWork) -> Result<PathBuf, MaterializeError> {
    let parent_bytes = parent.as_os_str().as_encoded_bytes();
    let separator = usize::from(!parent_bytes.is_empty()
        && !parent_bytes.last().is_some_and(|byte| std::path::is_separator(char::from(*byte))));
    let copied = parent_bytes.len().checked_add(name.as_os_str().as_encoded_bytes().len())
        .ok_or(WorkError::Overflow)?;
    let bytes = copied.checked_add(separator).ok_or(WorkError::Overflow)?;
    let delta = WorkCounters { bytes_copied: copied as u64, bytes_encoded: separator as u64, ..WorkCounters::UNCHARGED };
    work.work.admit(&delta, &work.budget)?;
    work.allocate(bytes as u64).map_err(restore_demand_error)?;
    let mut path = PathBuf::with_capacity(bytes);
    path.push(parent);
    path.push(name);
    work.record(delta).map_err(restore_demand_error)?;
    Ok(path)
}

fn prepare_restore(
    destination_root: PathBuf,
    relative: PathBuf,
    expected_root_identity: Option<crate::NativeRootIdentity>,
    replacement: HostPathReplacement,
    work: &mut crate::demand::DemandWork,
) -> Result<PreparedRestore, MaterializeError> {
    let root = HostRoot::open_measured(&destination_root, work).map_err(restore_demand_error)?;
    let actual = root.identity();
    if let Some(expected) = expected_root_identity {
        if actual != expected {
            return Err(MaterializeError::AllocationChanged { expected, actual });
        }
    }
    // No parent creation, lock creation, recovery, or stage mutation precedes
    // the identity check on the capability that owns the destination writes.
    let destination_parent = held_parent(&root, &relative, work)?;
    let destination_name = relative.file_name().ok_or(MaterializeError::InvalidPath)?;
    let lock = acquire_restore_lock(&destination_parent, &relative, true, work)?;
    cleanup_removed_restore(&destination_parent, &relative, work)?;
    match replacement {
        HostPathReplacement::Atomic => destination_parent
            .recover_entry_exchange_measured(Path::new(destination_name), work)
            .map_err(restore_demand_error)?,
        HostPathReplacement::LiveMount => recover_live_mount_replacement(
            &destination_parent, Path::new(destination_name), &relative, work,
        )?,
    }
    let stage_parent_path = match replacement {
        HostPathReplacement::Atomic => restore_owned_path(
            destination_root.parent().ok_or(MaterializeError::InvalidPath)?, work,
        )?,
        HostPathReplacement::LiveMount => restore_join_path(
            &destination_root, relative.parent().unwrap_or_else(|| Path::new("")), work,
        )?,
    };
    let stage_parent_root = match replacement {
        HostPathReplacement::Atomic => HostRoot::open_measured(&stage_parent_path, work)
            .map_err(restore_demand_error)?,
        HostPathReplacement::LiveMount => destination_parent.open_root_measured(Path::new(""), work)
            .map_err(restore_demand_error)?,
    };
    let stage_owner = stage_parent_root.create_dir_all_held_measured(Path::new(""), work)
        .map_err(restore_demand_error)?;
    let (stage_name, stage_root) = create_restore_stage(&stage_owner, work)?;
    let stage_path = match restore_join_path(&stage_parent_path, &stage_name, work) {
        Ok(path) => path,
        Err(error) => {
            let identity = stage_root.identity();
            drop(stage_root);
            let _ = stage_owner.remove_identity_measured(&stage_name, identity, work)
                .and_then(|()| stage_owner.sync_measured(work));
            return Err(error);
        }
    };
    let stage_parent_bytes = stage_parent_path.as_os_str().as_encoded_bytes().len() as u64;
    drop(stage_parent_path);
    work.release(stage_parent_bytes).map_err(restore_demand_error)?;
    Ok(PreparedRestore {
        root, destination_parent, stage_owner, stage_root, stage_name, stage_path,
        destination_root, relative, lock,
    })
}

fn publish_restore(
    destination_parent: &HostDirectory,
    stage_root: &HostRoot,
    relative: &Path,
    replacement: HostPathReplacement,
    work: &mut crate::demand::DemandWork,
) -> Result<(), MaterializeError> {
    let destination_name = relative.file_name().ok_or(MaterializeError::InvalidPath)?;
    let stage_parent = held_parent(stage_root, relative, work)?;
    let name = Path::new(destination_name);
    match destination_parent.stat_measured(name, work) {
        Ok(_) => match replacement {
            HostPathReplacement::Atomic => destination_parent
                .exchange_to_measured(name, &stage_parent, name, work).map_err(restore_demand_error)?,
            HostPathReplacement::LiveMount => replace_live_mount(
                &stage_parent, name, destination_parent, name, relative, work,
            )?,
        },
        Err(crate::demand::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            stage_parent.rename_to_measured(name, destination_parent, name, work).map_err(restore_demand_error)?;
            sync_restore_parent(destination_parent, work)?;
            sync_restore_parent(&stage_parent, work)?;
        }
        Err(error) => return Err(restore_demand_error(error)),
    }
    Ok(())
}


fn validate_host_relative_measured(relative: &Path, work: &mut crate::demand::DemandWork) -> Result<(), MaterializeError> {
    let mut any = false;
    for component in relative.components() {
        work.charge(WorkCounters { source_path_components: 1, items_examined: 1, ..WorkCounters::UNCHARGED })
            .map_err(restore_demand_error)?;
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(MaterializeError::InvalidPath);
        }
        any = true;
    }
    if any { Ok(()) } else { Err(MaterializeError::InvalidPath) }
}

fn held_parent(root: &HostRoot, relative: &Path, work: &mut crate::demand::DemandWork) -> Result<HostDirectory, MaterializeError> {
    root.create_dir_all_held_measured(relative.parent().unwrap_or_else(|| Path::new("")), work)
        .map_err(restore_demand_error)
}

fn create_restore_stage(parent: &HostDirectory, work: &mut crate::demand::DemandWork) -> Result<(PathBuf, HostRoot), MaterializeError> {
    use std::fmt::Write as _;
    for _ in 0..16 {
        let sequence = RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let process = std::process::id();
        let digits = |value: u64| if value == 0 { 1 } else { value.ilog10() as usize + 1 };
        let bytes = ".acyclic-restore-".len() + digits(u64::from(process)) + 1 + digits(sequence);
        let delta = WorkCounters { bytes_encoded: bytes as u64, ..WorkCounters::UNCHARGED };
        work.work.admit(&delta, &work.budget)?;
        work.allocate(bytes as u64).map_err(restore_demand_error)?;
        let mut name = String::with_capacity(bytes);
        write!(name, ".acyclic-restore-{process}-{sequence}").map_err(|_| MaterializeError::InvalidPath)?;
        work.record(delta).map_err(restore_demand_error)?;
        let name = PathBuf::from(name);
        match parent.create_dir_measured(&name, work) {
            Ok(()) => {
                let root = match parent.open_root_measured(&name, work) {
                    Ok(root) => root,
                    // Without the stage capability no identity authorizes
                    // deleting this name, which may already have been swapped.
                    Err(error) => return Err(restore_demand_error(error)),
                };
                if let Err(error) = sync_restore_parent(parent, work) {
                    let identity = root.identity();
                    drop(root);
                    let _ = parent.remove_identity_measured(&name, identity, work)
                        .and_then(|()| parent.sync_measured(work));
                    return Err(error);
                }
                return Ok((name, root));
            }
            Err(crate::demand::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                drop(name);
                work.release(bytes as u64).map_err(restore_demand_error)?;
            }
            Err(error) => return Err(restore_demand_error(error)),
        }
    }
    Err(MaterializeError::Io(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists, "restore stage names are exhausted",
    )))
}

fn restore_artifact_name(prefix: &str, relative: &Path, work: &mut crate::demand::DemandWork) -> Result<PathBuf, MaterializeError> {
    use std::fmt::Write as _;
    let mut hasher = blake3::Hasher::new();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        let bytes = relative.as_os_str().as_bytes();
        work.charge(WorkCounters { bytes_hashed: bytes.len() as u64, ..WorkCounters::UNCHARGED }).map_err(restore_demand_error)?;
        hasher.update(bytes);
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        for unit in relative.as_os_str().encode_wide() {
            work.charge(WorkCounters { bytes_hashed: 2, bytes_encoded: 2, ..WorkCounters::UNCHARGED }).map_err(restore_demand_error)?;
            hasher.update(&unit.to_le_bytes());
        }
    }
    let bytes = prefix.len().checked_add(64).ok_or(WorkError::Overflow)?;
    let delta = WorkCounters { bytes_encoded: bytes as u64, ..WorkCounters::UNCHARGED };
    work.work.admit(&delta, &work.budget)?;
    work.allocate(bytes as u64).map_err(restore_demand_error)?;
    let mut name = String::with_capacity(bytes);
    write!(name, "{prefix}{}", hasher.finalize().to_hex()).map_err(|_| MaterializeError::InvalidPath)?;
    work.record(delta).map_err(restore_demand_error)?;
    Ok(PathBuf::from(name))
}

fn with_restore_artifact<T>(
    prefix: &str,
    relative: &Path,
    work: &mut crate::demand::DemandWork,
    operation: impl FnOnce(&Path, &mut crate::demand::DemandWork) -> Result<T, MaterializeError>,
) -> Result<T, MaterializeError> {
    let name = restore_artifact_name(prefix, relative, work)?;
    let bytes = name.as_os_str().as_encoded_bytes().len() as u64;
    let result = operation(&name, work);
    drop(name);
    work.release(bytes).map_err(restore_demand_error)?;
    result
}

fn acquire_restore_lock(
    parent: &HostDirectory,
    relative: &Path,
    blocking: bool,
    work: &mut crate::demand::DemandWork,
) -> Result<File, MaterializeError> {
    with_restore_artifact(".acyclic-restore-lock-", relative, work, |name, work| {
        let file = parent.open_file_with_measured(
            name, &crate::native_host::NativeOpenOptions::ReadWriteCreate, work,
        ).map_err(restore_demand_error)?;
        loop {
            work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED }).map_err(restore_demand_error)?;
            match fs2::FileExt::try_lock_exclusive(&file) {
                Ok(()) => return Ok(file),
                Err(error) if blocking && error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(error) => return Err(error.into()),
            }
        }
    })
}

fn sync_restore_parent(parent: &HostDirectory, work: &mut crate::demand::DemandWork) -> Result<(), MaterializeError> {
    parent.sync_measured(work).map_err(restore_demand_error)
}

const REMOVE_RESTORE_WITNESS: &[u8] = b"acyclic-remove-restore-v1\n";
const LIVE_RESTORE_WITNESS: &[u8] = b"acyclic-live-restore-v1\n";


fn create_restore_witness(parent: &HostDirectory, name: &Path, contents: &[u8], work: &mut crate::demand::DemandWork) -> Result<(), MaterializeError> {
    use std::io::Write as _;
    let mut file = parent.open_file_with_measured(
        name, &crate::native_host::NativeOpenOptions::CreateNew, work,
    ).map_err(restore_demand_error)?;
    let mut remaining = contents;
    while !remaining.is_empty() {
        work.work.admit(&WorkCounters { output_bytes: remaining.len() as u64, backend_write_operations: 1, ..WorkCounters::UNCHARGED }, &work.budget)?;
        work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED }).map_err(restore_demand_error)?;
        match file.write(remaining) {
            Ok(0) => return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "restore witness write made no progress").into()),
            Ok(written) => {
                work.record(WorkCounters { output_bytes: written as u64, ..WorkCounters::UNCHARGED }).map_err(restore_demand_error)?;
                remaining = &remaining[written..];
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    work.charge(WorkCounters { durability_operations: 1, ..WorkCounters::UNCHARGED }).map_err(restore_demand_error)?;
    file.sync_all()?;
    sync_restore_parent(parent, work)
}

fn validate_restore_witness(parent: &HostDirectory, name: &Path, expected: &[u8], work: &mut crate::demand::DemandWork) -> Result<bool, MaterializeError> {
    use std::io::Read as _;
    let mut file = match parent.open_file_with_measured(
        name, &crate::native_host::NativeOpenOptions::Read, work,
    ) {
        Ok(file) => file,
        Err(crate::demand::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(restore_demand_error(error)),
    };
    let mut contents = [0_u8; 64];
    let bound = expected.len().checked_add(1).filter(|bound| *bound <= contents.len()).ok_or(MaterializeError::InvalidOptions)?;
    let mut read = 0;
    while read < bound {
        work.work.admit(&WorkCounters { source_bytes_read: (bound - read) as u64, backend_read_operations: 1, ..WorkCounters::UNCHARGED }, &work.budget)?;
        work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED }).map_err(restore_demand_error)?;
        match file.read(&mut contents[read..bound]) {
            Ok(0) => break,
            Ok(bytes) => {
                work.record(WorkCounters { source_bytes_read: bytes as u64, ..WorkCounters::UNCHARGED }).map_err(restore_demand_error)?;
                read += bytes;
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    if &contents[..read] == expected { Ok(true) } else {
        Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "restore artifact ownership witness is invalid").into())
    }
}

fn remove_restore_witness(parent: &HostDirectory, name: &Path, work: &mut crate::demand::DemandWork) -> Result<(), MaterializeError> {
    parent.remove_measured(name, work).map_err(restore_demand_error)?;
    sync_restore_parent(parent, work)
}

fn cleanup_removed_restore(
    parent: &HostDirectory,
    relative: &Path,
    work: &mut crate::demand::DemandWork,
) -> Result<(), MaterializeError> {
    with_restore_artifact(".acyclic-restore-removed-", relative, work, |removed, work| {
        with_restore_artifact(".acyclic-restore-remove-witness-", relative, work, |witness, work| {
            match parent.stat_measured(removed, work) {
                Ok(_) => {
                    if !validate_restore_witness(parent, witness, REMOVE_RESTORE_WITNESS, work)? {
                        return Err(MaterializeError::Engine("unowned restore-removal artifact collision".into()));
                    }
                    parent.remove_measured(removed, work).map_err(restore_demand_error)?;
                    sync_restore_parent(parent, work)?;
                    remove_restore_witness(parent, witness, work)?;
                }
                Err(crate::demand::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    if validate_restore_witness(parent, witness, REMOVE_RESTORE_WITNESS, work)? {
                        remove_restore_witness(parent, witness, work)?;
                    }
                }
                Err(error) => return Err(restore_demand_error(error)),
            }
            Ok(())
        })
    })
}

fn remove_restored_path(
    parent: &HostDirectory,
    destination_name: &Path,
    relative: &Path,
    work: &mut crate::demand::DemandWork,
) -> Result<(), MaterializeError> {
    with_restore_artifact(".acyclic-restore-removed-", relative, work, |removed, work| {
        with_restore_artifact(".acyclic-restore-remove-witness-", relative, work, |witness, work| {
            create_restore_witness(parent, witness, REMOVE_RESTORE_WITNESS, work)?;
            match parent.stat_measured(destination_name, work) {
                Ok(_) => {
                    parent.rename_to_measured(destination_name, parent, removed, work).map_err(restore_demand_error)?;
                    sync_restore_parent(parent, work)?;
                    parent.remove_measured(removed, work).map_err(restore_demand_error)?;
                    sync_restore_parent(parent, work)?;
                }
                Err(crate::demand::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(restore_demand_error(error)),
            }
            remove_restore_witness(parent, witness, work)
        })
    })
}


fn replace_live_mount(
    staged_parent: &HostDirectory,
    staged_name: &Path,
    parent: &HostDirectory,
    destination_name: &Path,
    relative: &Path,
    work: &mut crate::demand::DemandWork,
) -> Result<(), MaterializeError> {
    recover_live_mount_replacement(parent, destination_name, relative, work)?;
    with_restore_artifact(".acyclic-restore-backup-", relative, work, |backup, work| {
        with_restore_artifact(".acyclic-restore-live-witness-", relative, work, |witness, work| {
            create_restore_witness(parent, witness, LIVE_RESTORE_WITNESS, work)?;
            parent.rename_to_measured(destination_name, parent, backup, work).map_err(restore_demand_error)?;
            sync_restore_parent(parent, work)?;
            if let Err(error) = staged_parent.rename_to_measured(staged_name, parent, destination_name, work) {
                if let Err(rollback) = parent.rename_to_measured(backup, parent, destination_name, work) {
                    return Err(MaterializeError::Engine(format!(
                        "replacement failed: {error}; displaced entry remains at {} after rollback failed: {rollback}",
                        backup.display(),
                    )));
                }
                sync_restore_parent(parent, work)?;
                return Err(restore_demand_error(error));
            }
            sync_restore_parent(parent, work)?;
            sync_restore_parent(staged_parent, work)?;
            parent.remove_measured(backup, work).map_err(restore_demand_error)?;
            sync_restore_parent(parent, work)?;
            remove_restore_witness(parent, witness, work)
        })
    })
}

fn recover_live_mount_replacement(
    parent: &HostDirectory,
    destination_name: &Path,
    relative: &Path,
    work: &mut crate::demand::DemandWork,
) -> Result<(), MaterializeError> {
    with_restore_artifact(".acyclic-restore-backup-", relative, work, |backup, work| {
        with_restore_artifact(".acyclic-restore-live-witness-", relative, work, |witness, work| {
            let backup_exists = match parent.stat_measured(backup, work) {
                Ok(_) => true,
                Err(crate::demand::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => return Err(restore_demand_error(error)),
            };
            if !backup_exists {
                if validate_restore_witness(parent, witness, LIVE_RESTORE_WITNESS, work)? {
                    remove_restore_witness(parent, witness, work)?;
                }
                return Ok(());
            }
            if !validate_restore_witness(parent, witness, LIVE_RESTORE_WITNESS, work)? {
                return Err(MaterializeError::Engine("unowned live-restore backup collision".into()));
            }
            match parent.stat_measured(destination_name, work) {
                Ok(_) => parent.remove_measured(backup, work).map_err(restore_demand_error)?,
                Err(crate::demand::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    parent.rename_to_measured(backup, parent, destination_name, work).map_err(restore_demand_error)?;
                }
                Err(error) => return Err(restore_demand_error(error)),
            }
            sync_restore_parent(parent, work)?;
            remove_restore_witness(parent, witness, work)
        })
    })
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "one entry needs the reader, host root, both paths, its record, options, budget, cancellation, and the shared link and pending maps"
)]
async fn materialize_entry<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    host_root: &Arc<HostRoot>,
    path: &NamespacePath,
    host_path: &Arc<GenerationHostPath>,
    directory: Option<Arc<NamespacePath>>,
    file_id: FileId,
    kind: FileKind,
    payload: FilePayload,
    #[cfg(any(target_os = "macos", windows))] metadata_id: ObjectId,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    known_files: &mut Vec<(FileId, Arc<GenerationHostPath>)>,
    #[cfg(any(target_os = "macos", windows))]
    known_payloads: &mut Vec<((u64, ObjectId, ObjectId), Arc<GenerationHostPath>)>,
    pending: &mut Vec<(Arc<NamespacePath>, Arc<GenerationHostPath>)>,
    receipt: &mut GenerationReceipt,
    mode: MaterializeMode,
) -> Result<(), OperationFailure<MaterializeError>> {
    receipt.measured(|work| {
        work.check()?;
        work.work.admit(&WorkCounters { materializations: 1, ..WorkCounters::UNCHARGED }, &work.budget)?;
        Ok(())
    })?;
    for (identity, existing) in known_files.iter() {
        receipt.measured(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
        if *identity == file_id {
            receipt.measured(|work| host_path.hard_link(host_root, existing, work))?;
            receipt.files = receipt.files.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
            receipt.measured(|work| work.check())?;
            return Ok(());
        }
    }
    let mut authenticated_metadata = None;
    match (kind, payload) {
        (FileKind::Directory, FilePayload::Directory { .. }) => {
            receipt.measured(|work| host_path.create_dir(host_root, work))?;
            let directory = directory.ok_or_else(|| {
                OperationFailure::new(MaterializeError::InvalidOptions, receipt.work)
            })?;
            receipt.directories = receipt.directories.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
            receipt.push(pending, (directory, host_path.clone()))?;
            receipt.measured(|work| work.check())?;
            return Ok(());
        }
        (FileKind::Regular, FilePayload::InlineRegular(data)) => {
            let file = create_file(host_root, host_path, receipt)?;
            let length = data.as_bytes().len();
            receipt.allocate(length)?;
            receipt.measured(|work| work.charge(WorkCounters {
                bytes_copied: length as u64, ..WorkCounters::UNCHARGED
            }))?;
            let bytes = Bytes::copy_from_slice(data.as_bytes());
            let written = write_generation_bytes(&file, 0, bytes, receipt).await?;
            receipt.release(length)?;
            if mode == MaterializeMode::DurableOutput {
                sync_generation_file(&file, receipt).await?;
            }
            let file_bytes = file.logical_owned_bytes();
            drop(file);
            receipt.measured(|work| work.release(file_bytes))?;
            account_file(receipt, length as u64, written, budget)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (FileKind::Regular, FilePayload::Regular { logical_bytes, extents }) => {
            #[cfg(not(any(target_os = "macos", windows)))]
            let _ = extents;
            #[cfg(any(target_os = "macos", windows))]
            let mut cloned = false;
            #[cfg(any(target_os = "macos", windows))]
            let mut payload_known = false;
            #[cfg(any(target_os = "macos", windows))]
            for (key, source) in known_payloads.iter() {
                receipt.measured(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
                if *key == (logical_bytes, extents, metadata_id) {
                    payload_known = true;
                    cloned = receipt.measured(|work| host_path.clone_file(host_root, source, work))?;
                    break;
                }
            }
            #[cfg(not(any(target_os = "macos", windows)))]
            let cloned = false;
            if !cloned {
                let mut file = create_file(host_root, host_path, receipt)?;
                #[cfg(windows)]
                {
                    let meter = receipt.native_meter();
                    let control = receipt.runtime_result(file.control_bounded_async(mark_sparse_measured, meter))?;
                    receipt.runtime_result(control.await)?;
                }
                let meter = receipt.native_meter();
                let resize = receipt.runtime_result(file.set_len_bounded_async(logical_bytes, meter))?;
                receipt.runtime_result(resize.await)?;
                authenticated_metadata = Some(materialize_sparse_file(
                    reader, path, &mut file, logical_bytes, options, budget, cancellation, receipt,
                ).await?);
                if mode == MaterializeMode::DurableOutput {
                    sync_generation_file(&file, receipt).await?;
                }
                let file_bytes = file.logical_owned_bytes();
                drop(file);
                receipt.measured(|work| work.release(file_bytes))?;
            }
            #[cfg(any(target_os = "macos", windows))]
            if cloned {
                if mode == MaterializeMode::DurableOutput {
                    #[cfg(target_os = "macos")]
                    let file = receipt.measured(|work| host_root.open_file_measured(host_path.path(), work))?.into_std();
                    #[cfg(windows)]
                    let file = receipt.measured(|work| host_root.open_file_wide_measured(&host_path.units, work))?.into_std();
                    let meter = receipt.native_meter();
                    let file = receipt.runtime_result(acyclic_native_runtime::NativeFile::from_file_measured(file, meter.as_ref()))?;
                    sync_generation_file(&file, receipt).await?;
                    let file_bytes = file.logical_owned_bytes();
                    drop(file);
                    receipt.measured(|work| work.release(file_bytes))?;
                }
            }
            account_file(receipt, logical_bytes, 0, budget)?;
            #[cfg(any(target_os = "macos", windows))]
            if !payload_known {
                receipt.push(known_payloads, ((logical_bytes, extents, metadata_id), host_path.clone()))?;
            }
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (FileKind::SymbolicLink, FilePayload::SymbolicLink { target_bytes, .. }) => {
            let remaining = receipt.remaining()?;
            let target = reader.read_symbolic_link(path, remaining, cancellation).await
                .map_err(|failure| receipt.engine_failure(failure))?;
            receipt.merge(target.work)?;
            if target.value.len() as u64 != target_bytes {
                return Err(OperationFailure::new(
                    MaterializeError::Engine("symbolic-link length mismatch".to_owned()), receipt.work,
                ));
            }
            receipt.measured(|work| work.adopt(target_bytes))?;
            create_symlink_measured(host_root, &target.value, host_path, receipt)?;
            drop(target.value);
            receipt.measured(|work| work.release(target_bytes))?;
            receipt.symbolic_links = receipt.symbolic_links.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (kind @ (FileKind::Fifo | FileKind::Socket), FilePayload::Empty) => {
            create_special_measured(host_root, host_path, kind, None, receipt)?;
            receipt.special_files = receipt.special_files.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (kind @ (FileKind::CharacterDevice | FileKind::BlockDevice), FilePayload::Device { major, minor }) => {
            create_special_measured(host_root, host_path, kind, Some((major, minor)), receipt)?;
            receipt.special_files = receipt.special_files.checked_add(1).ok_or_else(|| {
                OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
            })?;
            account_materialization(receipt, 0, budget)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (unsupported, _) => return Err(OperationFailure::new(MaterializeError::UnsupportedKind(unsupported), receipt.work)),
    }
    if let Some(metadata) = authenticated_metadata {
        apply_record_metadata(reader, host_root, host_path, metadata, budget, cancellation, receipt).await?;
    } else {
        apply_metadata(reader, host_root, path, host_path, budget, cancellation, receipt).await?;
    }
    receipt.measured(|work| work.check())?;
    Ok(())
}

async fn write_generation_bytes(
    file: &acyclic_native_runtime::NativeFile,
    offset: u64,
    bytes: Bytes,
    receipt: &mut GenerationReceipt,
) -> Result<u64, OperationFailure<MaterializeError>> {
    let before = receipt.work.output_bytes;
    let meter = receipt.native_meter();
    let write = receipt.runtime_result(file.write_bounded_async(offset, bytes, meter))?;
    receipt.runtime_result(write.await)?;
    Ok(receipt.work.output_bytes - before)
}

async fn sync_generation_file(
    file: &acyclic_native_runtime::NativeFile,
    receipt: &mut GenerationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    let meter = receipt.native_meter();
    let sync = receipt.runtime_result(file.sync_bounded_async(acyclic_native_runtime::Durability::Full, meter))?;
    receipt.runtime_result(sync.await)
}

#[cfg(windows)]
fn mark_sparse_measured(file: &File, meter: &dyn acyclic_native_runtime::NativeReadMeter) -> std::io::Result<()> {
    let mut output = [];
    acyclic_native_runtime::device_control_in_place_measured_mutation(
        file, windows::Win32::System::Ioctl::FSCTL_SET_SPARSE, &[], &mut output, meter,
    )?;
    Ok(())
}


#[allow(
    clippy::too_many_arguments,
    reason = "writes one sparse file from the reader under the options, budget, cancellation, and shared receipt"
)]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered pass reads, writes, and punches every extent of the file"
)]
async fn materialize_sparse_file<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    path: &NamespacePath,
    file: &mut acyclic_native_runtime::NativeFile,
    logical_bytes: u64,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    receipt: &mut GenerationReceipt,
) -> Result<FileMetadata, OperationFailure<MaterializeError>> {
    let remaining = receipt.remaining()?;
    let resolved = reader.resolve_files(std::slice::from_ref(path), remaining, cancellation).await
        .map_err(|failure| receipt.engine_failure(failure))?;
    receipt.merge(resolved.work)?;
    let resolved = resolved.value.into_iter().next().flatten().ok_or_else(|| {
        OperationFailure::new(MaterializeError::MissingPath, receipt.work)
    })?;
    let metadata = resolved.description().metadata;
    let mut offset = 0_u64;
    while offset < logical_bytes {
        let length = options.transfer_bytes.min(logical_bytes - offset);
        let remaining = receipt.remaining()?;
        let plan = resolved.plan_extents(
            ByteRange { offset, length }, options.maximum_extent_spans, remaining, cancellation,
        ).await.map_err(|failure| receipt.engine_failure(failure))?;
        receipt.merge(plan.work)?;
        let plan = plan.value.ok_or_else(|| OperationFailure::new(
            MaterializeError::Engine("non-inline file returned an inline extent plan".to_owned()), receipt.work,
        ))?;
        let plan_bytes = generation_vec_bytes(&plan.spans, receipt)?;
        receipt.measured(|work| work.adopt(plan_bytes as u64))?;
        let mut writes = Vec::new();
        let mut reads = Vec::new();
        for span in plan.spans {
            match span.kind {
                ExtentKind::Hole => {
                    #[cfg(target_os = "macos")]
                    {
                        let meter = receipt.native_meter();
                        let control = receipt.runtime_result(file.control_bounded_async(move |handle, meter| {
                            crate::native_host::punch_hole_measured(handle, span.offset, span.length, meter)
                        }, meter))?;
                        receipt.runtime_result(control.await)?;
                    }
                }
                ExtentKind::AllocatedZero => queue_zero_writes(
                    &mut writes, span.offset, span.length, options.transfer_bytes, receipt,
                )?,
                ExtentKind::Content { .. } => receipt.push(&mut reads, ResolvedFileRangeReadRequest {
                    file: &resolved, range: ByteRange { offset: span.offset, length: span.length },
                })?,
            }
        }
        receipt.release(plan_bytes)?;
        if !reads.is_empty() {
            read_content_spans(reader, reads, options.maximum_extent_spans, budget, cancellation, receipt, &mut writes).await?;
        }
        let write_bytes = generation_vec_bytes(&writes, receipt)?;
        for write in writes {
            let written = write_generation_bytes(file, write.offset, write.bytes, receipt).await?;
            receipt.release(write.owned_bytes)?;
            account_written(receipt, written, budget)?;
        }
        receipt.release(write_bytes)?;
        offset = offset.checked_add(length).ok_or_else(|| {
            OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
        })?;
    }
    Ok(metadata)
}

async fn read_content_spans<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    reads: Vec<ResolvedFileRangeReadRequest<'_, A, O>>,
    maximum_extent_spans: u32,
    _budget: WorkBudget,
    cancellation: &CancellationToken,
    receipt: &mut GenerationReceipt,
    writes: &mut Vec<GenerationWrite>,
) -> Result<(), OperationFailure<MaterializeError>> {
    let request_bytes = generation_vec_bytes(&reads, receipt)?;
    let remaining = receipt.remaining()?;
    let read = reader.read_resolved_ranges(
        &reads, usize::try_from(maximum_extent_spans).unwrap_or(usize::MAX), remaining, cancellation,
    ).await.map_err(|failure| receipt.engine_failure(failure))?;
    receipt.merge(read.work)?;
    let result_bytes = generation_vec_bytes(&read.value, receipt)?;
    receipt.measured(|work| work.adopt(result_bytes as u64))?;
    for result in &read.value {
        receipt.measured(|work| work.adopt(result.bytes.len() as u64))?;
    }
    if reads.len() != read.value.len() {
        return Err(OperationFailure::new(
            MaterializeError::Engine("range batch returned a different number of results".to_owned()), receipt.work,
        ));
    }
    for (request, result) in reads.into_iter().zip(read.value) {
        let owned_bytes = result.bytes.len();
        receipt.push(writes, GenerationWrite { offset: request.range.offset, bytes: result.bytes, owned_bytes })?;
    }
    receipt.release(request_bytes)?;
    receipt.release(result_bytes)?;
    Ok(())
}

fn queue_zero_writes(
    writes: &mut Vec<GenerationWrite>,
    offset: u64,
    length: u64,
    transfer_bytes: u64,
    receipt: &mut GenerationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    static ZERO_BYTES: [u8; 65_536] = [0; 65_536];
    let chunk_bytes = transfer_bytes.min(ZERO_BYTES.len() as u64);
    if chunk_bytes == 0 {
        return Err(OperationFailure::new(MaterializeError::InvalidOptions, receipt.work));
    }
    let mut remaining = length;
    let mut position = offset;
    while remaining != 0 {
        let count = remaining.min(chunk_bytes) as usize;
        receipt.push(writes, GenerationWrite {
            offset: position, bytes: Bytes::from_static(&ZERO_BYTES[..count]), owned_bytes: 0,
        })?;
        position = position.checked_add(count as u64).ok_or_else(|| {
            OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
        })?;
        remaining -= count as u64;
    }
    Ok(())
}

struct GenerationWrite {
    offset: u64,
    bytes: Bytes,
    owned_bytes: usize,
}

fn generation_vec_bytes<T>(
    values: &Vec<T>,
    receipt: &GenerationReceipt,
) -> Result<usize, OperationFailure<MaterializeError>> {
    values.capacity().checked_mul(std::mem::size_of::<T>()).ok_or_else(|| {
        OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
    })
}

async fn apply_metadata<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    host_root: &Arc<HostRoot>,
    path: &NamespacePath,
    host_path: &Arc<GenerationHostPath>,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    receipt: &mut GenerationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    let remaining = receipt.remaining()?;
    let metadata = reader
        .read_metadata(path, remaining, cancellation)
        .await
        .map_err(|failure| receipt.engine_failure(failure))?;
    receipt.merge(metadata.work)?;
    apply_record_metadata(
        reader,
        host_root,
        host_path,
        metadata.value,
        budget,
        cancellation,
        receipt,
    )
    .await
}

/// Applies one record's complete metadata to its materialized host path:
/// its named attributes, then the directly settable fields. Writing an
/// attribute needs write access, which a read-only final mode would deny,
/// and changes only the ctime, which no host lets anyone set.
async fn apply_record_metadata<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    host_root: &Arc<HostRoot>,
    host_path: &Arc<GenerationHostPath>,
    metadata: FileMetadata,
    _budget: WorkBudget,
    cancellation: &CancellationToken,
    receipt: &mut GenerationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    let host = MaterializeMode::host_metadata(metadata);
    if let Some(root) = host.named_attributes {
        let remaining = receipt.remaining()?;
        let read = reader.read_named_attributes(root, remaining, cancellation).await
            .map_err(|failure| receipt.engine_failure(failure))?;
        receipt.merge(read.work)?;
        let source_array_bytes = generation_vec_bytes(&read.value, receipt)?;
        receipt.measured(|work| work.adopt(source_array_bytes as u64))?;
        for (name, value) in &read.value {
            receipt.measured(|work| {
                work.adopt(name.retained_bytes() as u64)?;
                work.adopt(value.len() as u64)
            })?;
        }
        let mut attributes = Vec::new();
        let mut payload_bytes = 0_usize;
        for (name, value) in read.value {
            if name.class() != crate::kernel::AttributeClass::PosixXattr {
                return Err(OperationFailure::new(
                    MaterializeError::UnsupportedMetadata("named_attributes"), receipt.work,
                ));
            }
            let name = name.into_bytes();
            payload_bytes = payload_bytes.checked_add(name.capacity()).and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or_else(|| OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work))?;
            receipt.push(&mut attributes, (name, value))?;
        }
        receipt.release(source_array_bytes)?;
        let host_array_bytes = generation_vec_bytes(&attributes, receipt)?;
        apply_host_attributes_offloaded(host_root, host_path, attributes, receipt).await?;
        receipt.release(host_array_bytes + payload_bytes)?;
    }
    apply_host_metadata_offloaded(host_root, host_path, host.metadata, receipt).await
}

async fn generation_blocking<T: Send + 'static>(
    receipt: &mut GenerationReceipt,
    operation: impl FnOnce(&mut DemandWork) -> Result<T, DemandError> + Send + 'static,
) -> Result<T, OperationFailure<MaterializeError>> {
    let observer = receipt.native_meter();
    let meter = receipt.meter.clone();
    let task = receipt.runtime_result(acyclic_native_runtime::run_blocking_io_measured(
        move || {
            let mut work = meter.lock();
            operation(&mut work)
        },
        observer,
    ))?;
    let result = receipt.runtime_result(task.await)?;
    result.map_err(|error| OperationFailure::new(materialize_demand_error(error), receipt.work))
}

async fn apply_host_attributes_offloaded(
    host_root: &Arc<HostRoot>,
    host_path: &Arc<GenerationHostPath>,
    attributes: Vec<(Vec<u8>, Bytes)>,
    receipt: &mut GenerationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    if attributes.is_empty() { return Ok(()); }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let root = host_root.clone();
        let path = host_path.clone();
        generation_blocking(receipt, move |work| {
            #[cfg(target_os = "linux")]
            let target = root.open_linux_metadata_target_measured(path.path(), work)?;
            #[cfg(target_os = "macos")]
            let target = root.open_macos_metadata_target_measured(path.path(), work)?;
            let result = target.set_extended_attributes_measured(&attributes, work);
            #[cfg(target_os = "macos")]
            {
                let target_bytes = target.logical_owned_bytes();
                drop(target);
                work.release(target_bytes)?;
            }
            result
        }).await
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (host_root, host_path);
        Err(OperationFailure::new(MaterializeError::UnsupportedMetadata("named_attributes"), receipt.work))
    }
}

async fn apply_host_metadata_offloaded(
    host_root: &Arc<HostRoot>,
    host_path: &Arc<GenerationHostPath>,
    metadata: FileMetadata,
    receipt: &mut GenerationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    #[cfg(unix)]
    reject_unbaselined_unix_metadata(metadata).map_err(|error| OperationFailure::new(error, receipt.work))?;
    let root = host_root.clone();
    let path = host_path.clone();
    generation_blocking(receipt, move |work| {
        if metadata == FileMetadata::default() {
            return path.stat(&root, work);
        }
        #[cfg(target_os = "linux")]
        let target = root.open_linux_metadata_target_measured(path.path(), work)?;
        #[cfg(target_os = "macos")]
        let target = root.open_macos_metadata_target_measured(path.path(), work)?;
        #[cfg(windows)]
        let target = root.open_windows_metadata_target_wide_measured(&path.units, work)?;
        target.apply_measured(metadata, work)
    }).await
}

#[cfg(windows)]
fn windows_metadata_error(error: crate::native_host::WindowsMetadataError) -> MaterializeError {
    match error {
        crate::native_host::WindowsMetadataError::Unsupported(field) => {
            MaterializeError::UnsupportedMetadata(field)
        }
        crate::native_host::WindowsMetadataError::Io(error) => MaterializeError::Io(error),
    }
}

#[cfg(unix)]
fn reject_unbaselined_unix_metadata(metadata: FileMetadata) -> Result<(), MaterializeError> {
    use crate::kernel::MetadataField;
    if matches!(metadata.changed_ns, MetadataField::Value(_)) {
        return Err(MaterializeError::UnsupportedMetadata(
            "changed_ns without native-view baseline",
        ));
    }
    #[cfg(target_os = "linux")]
    if matches!(metadata.created_ns, MetadataField::Value(_)) {
        return Err(MaterializeError::UnsupportedMetadata(
            "created_ns without native-view baseline",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn linux_metadata_error(error: crate::native_host::LinuxMetadataError) -> MaterializeError {
    match error {
        crate::native_host::LinuxMetadataError::Unsupported(field) => {
            MaterializeError::UnsupportedMetadata(field)
        }
        crate::native_host::LinuxMetadataError::Io(error) => MaterializeError::Io(error),
    }
}

#[cfg(target_os = "macos")]
fn mac_metadata_error(error: crate::native_host::MacMetadataError) -> MaterializeError {
    match error {
        crate::native_host::MacMetadataError::Unsupported(field) => {
            MaterializeError::UnsupportedMetadata(field)
        }
        crate::native_host::MacMetadataError::Io(error) => MaterializeError::Io(error),
    }
}

#[cfg(test)]
fn validate_options(
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<HostRoot>, OperationFailure<MaterializeError>> {
    let mut receipt = GenerationReceipt::new(budget, cancellation)?;
    let root = validate_options_measured(options, &mut receipt)?;
    Ok(OperationReceipt { value: root, work: receipt.work })
}

fn validate_options_measured(
    options: &MaterializeOptions,
    receipt: &mut GenerationReceipt,
) -> Result<HostRoot, OperationFailure<MaterializeError>> {
    let root = receipt.measured(|work| HostRoot::open_measured(&options.destination, work))
        .map_err(|failure| {
            let error = match failure.error {
                error @ (MaterializeError::Work(_) | MaterializeError::Cancelled(_)) => error,
                _ => MaterializeError::InvalidDestination,
            };
            OperationFailure::new(error, *failure.work)
        })?;
    if let Some(expected) = options.expected_root_identity {
        let actual = root.identity();
        if actual != expected {
            return Err(OperationFailure::new(
                MaterializeError::AllocationChanged { expected, actual },
                receipt.work,
            ));
        }
    }
    let mut entries = receipt.measured(|work| root.read_dir_stats_measured(Path::new(""), work))?;
    let reader_bytes = entries.logical_owned_bytes();
    #[cfg(unix)]
    let next = receipt.measured(|work| entries.next_measured(work));
    #[cfg(windows)]
    let next = receipt.measured(|work| entries.next_wide_measured(work));
    drop(entries);
    receipt.measured(|work| work.release(reader_bytes))?;
    if let Some(entry) = next? {
        receipt.measured(|work| work.release(entry.logical_owned_bytes()))?;
        return Err(OperationFailure::new(MaterializeError::InvalidDestination, receipt.work));
    }
    Ok(root)
}

fn validate_bounds(options: &MaterializeOptions) -> Result<(), MaterializeError> {
    if options.maximum_directory_entries == 0
        || options.maximum_extent_spans == 0
        || options.transfer_bytes == 0
        || usize::try_from(options.transfer_bytes).is_err()
    {
        return Err(MaterializeError::InvalidOptions);
    }
    Ok(())
}


fn create_file(
    host_root: &HostRoot,
    path: &GenerationHostPath,
    receipt: &mut GenerationReceipt,
) -> Result<acyclic_native_runtime::NativeFile, OperationFailure<MaterializeError>> {
    #[cfg(windows)]
    let file = receipt.measured(|work| host_root.create_overlapped_file_wide_measured(&path.units, work))?;
    #[cfg(unix)]
    let file = receipt.measured(|work| host_root.create_file_measured(path.path(), work))?;
    let meter = receipt.native_meter();
    #[cfg(windows)]
    {
        #[allow(unsafe_code, reason = "the newly created held handle is overlapped and exclusively transferred")]
        // SAFETY: the measured host constructor opens this new, held-root-relative
        // handle overlapped, without a completion-port association or other I/O.
        let native = unsafe {
            acyclic_native_runtime::NativeFile::from_overlapped_file_measured_unchecked(file, meter.as_ref())
        };
        receipt.runtime_result(native)
    }
    #[cfg(unix)]
    { receipt.runtime_result(acyclic_native_runtime::NativeFile::from_file_measured(file, meter.as_ref())) }
}


fn account_written(
    receipt: &mut GenerationReceipt,
    bytes: u64,
    budget: WorkBudget,
) -> Result<(), OperationFailure<MaterializeError>> {
    receipt.written_bytes = receipt.written_bytes.checked_add(bytes).ok_or_else(|| {
        OperationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work)
    })?;
    receipt.work.verify(budget).map_err(|error| {
        OperationFailure::new(MaterializeError::Work(error), receipt.work)
    })
}

fn account_materialization(
    receipt: &mut GenerationReceipt,
    bytes: u64,
    budget: WorkBudget,
) -> Result<(), OperationFailure<MaterializeError>> {
    account_written(receipt, bytes, budget)?;
    receipt.measured(|work| work.record(WorkCounters {
        materializations: 1,
        ..WorkCounters::UNCHARGED
    }))
}

fn account_file(
    receipt: &mut GenerationReceipt,
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


#[cfg(all(test, windows))]
mod windows_name_tests {
    use super::*;

    #[tokio::test]
    async fn overlapped_sparse_control_finishes_before_native_writes()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let directory = tempfile::tempdir()?;
        let cancellation = CancellationToken::new();
        let mut receipt = GenerationReceipt::new(
            super::restore_recovery_tests::finite_materialization_budget(), &cancellation,
        )?;
        let root = Arc::new(receipt.measured(|work| HostRoot::open_measured(directory.path(), work))?);
        let limits = crate::model::VolumeLimits::default();
        let namespace = NamespacePath::new(vec![LogicalName::new(
            NameEncoding::Utf8, b"sparse-control.bin".to_vec(), limits.maximum_component_bytes,
        )?], limits)?;
        let host_path = GenerationHostPath::from_namespace(&namespace, &mut receipt)?;
        let native = create_file(&root, &host_path, &mut receipt)?;
        let meter = receipt.native_meter();
        let control = receipt.runtime_result(native.control_bounded_async(mark_sparse_measured, meter))?;
        receipt.runtime_result(control.await)?;
        let meter = receipt.native_meter();
        let resize = receipt.runtime_result(native.set_len_bounded_async(1024 * 1024, meter))?;
        receipt.runtime_result(resize.await)?;
        write_generation_bytes(&native, 1024 * 1024 - 4, Bytes::from_static(b"tail"), &mut receipt).await?;
        sync_generation_file(&native, &mut receipt).await?;
        let path = Path::new("sparse-control.bin");
        let bytes = std::fs::read(directory.path().join(path))?;
        assert_eq!(bytes.len(), 1024 * 1024);
        assert_eq!(&bytes[bytes.len() - 4..], b"tail");
        Ok(())
    }

    #[test]
    fn posix_profile_names_materialize_through_utf8_on_windows()
    -> Result<(), Box<dyn std::error::Error>> {
        let valid = LogicalName::new(
            NameEncoding::PosixBytes,
            "uni-é中.txt".as_bytes().to_vec(),
            255,
        )?;
        let limits = crate::model::VolumeLimits::default();
        let mut receipt = GenerationReceipt::new(
            super::restore_recovery_tests::finite_materialization_budget(), &CancellationToken::new(),
        )?;
        let namespace = NamespacePath::new(vec![valid], limits)?;
        let path = GenerationHostPath::from_namespace(&namespace, &mut receipt)?;
        assert_eq!(path.units, "uni-é中.txt".encode_utf16().collect::<Vec<_>>());

        let invalid = LogicalName::new(NameEncoding::PosixBytes, vec![0xff], 255)?;
        assert!(matches!(
            GenerationHostPath::from_namespace(&NamespacePath::new(vec![invalid], limits)?, &mut receipt),
            Err(OperationFailure { error: MaterializeError::UnrepresentableName, .. })
        ));
        Ok(())
    }
}

fn create_symlink_measured(
    host_root: &HostRoot,
    target: &Bytes,
    destination: &GenerationHostPath,
    receipt: &mut GenerationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        receipt.measured(|work| host_root.symlink_measured(OsStr::from_bytes(target), destination.path(), work))
    }
    #[cfg(windows)]
    {
        let decoded = crate::kernel::types::utf16le_units(target).ok_or_else(|| {
            OperationFailure::new(MaterializeError::UnrepresentableName, receipt.work)
        })?;
        let mut units = Vec::new();
        receipt.reserve(&mut units, target.len() / 2)?;
        receipt.measured(|work| work.charge(WorkCounters {
            bytes_copied: target.len() as u64, ..WorkCounters::UNCHARGED
        }))?;
        units.extend(decoded);
        let result = receipt.measured(|work| host_root.symlink_file_wide_measured(&units, &destination.units, work));
        drop(units);
        receipt.release(target.len())?;
        result
    }
}

#[allow(clippy::useless_conversion, reason = "libc file-type constants have different widths on Linux and macOS")]
fn create_special_measured(
    host_root: &HostRoot,
    destination: &GenerationHostPath,
    kind: FileKind,
    device: Option<(u32, u32)>,
    receipt: &mut GenerationReceipt,
) -> Result<(), OperationFailure<MaterializeError>> {
    #[cfg(unix)]
    {
        match kind {
            FileKind::Fifo => receipt.measured(|work| host_root.create_fifo_held_measured(destination.path(), 0o600, work)),
            FileKind::CharacterDevice | FileKind::BlockDevice => {
                let (major, minor) = device.ok_or_else(|| {
                    OperationFailure::new(MaterializeError::UnsupportedKind(kind), receipt.work)
                })?;
                let file_type = if kind == FileKind::CharacterDevice {
                    u32::from(libc::S_IFCHR)
                } else {
                    u32::from(libc::S_IFBLK)
                };
                let device_number = super::device::join_device(major, minor).map_err(|errno| {
                    OperationFailure::new(MaterializeError::Io(std::io::Error::from_raw_os_error(errno)), receipt.work)
                })?;
                receipt.measured(|work| host_root.create_device_held_measured(destination.path(), file_type | 0o600, device_number, work))
            }
            FileKind::Socket => receipt.measured(|work| host_root.bind_unix_socket_measured(destination.path(), work)),
            _ => Err(OperationFailure::new(MaterializeError::UnsupportedKind(kind), receipt.work)),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (host_root, destination, device);
        Err(OperationFailure::new(MaterializeError::UnsupportedKind(kind), receipt.work))
    }
}

#[cfg(test)]
mod restore_recovery_tests {
    use super::*;

    pub(super) fn finite_materialization_budget() -> WorkBudget {
        WorkBudget {
            authority_records_read: 100_000,
            authority_records_appended: 100_000,
            authority_bytes_read: 64 * 1024 * 1024,
            authority_bytes_written: 64 * 1024 * 1024,
            object_probes: 100_000,
            backend_read_operations: 100_000,
            backend_write_operations: 100_000,
            durability_operations: 100_000,
            page_reads: 100_000,
            page_writes: 100_000,
            object_bytes_read: 64 * 1024 * 1024,
            object_bytes_written: 64 * 1024 * 1024,
            bytes_hashed: 64 * 1024 * 1024,
            bytes_copied: 64 * 1024 * 1024,
            bytes_encoded: 64 * 1024 * 1024,
            source_bytes_read: 64 * 1024 * 1024,
            source_path_components: 100_000,
            source_entries_visited: 100_000,
            output_bytes: 64 * 1024 * 1024,
            items_examined: 100_000,
            items_returned: 100_000,
            allocation_operations: 100_000,
            peak_allocation_bytes: 64 * 1024 * 1024,
            materializations: 100_000,
        }
    }

    #[test]
    fn replaced_root_is_rejected_before_destination_enumeration()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("root");
        std::fs::create_dir(&destination)?;
        let original = HostRoot::open(&destination)?.identity();
        std::fs::rename(&destination, temporary.path().join("original"))?;
        std::fs::create_dir(&destination)?;
        std::fs::write(destination.join("foreign"), b"untouched")?;
        let actual = HostRoot::open(&destination)?.identity();
        assert_ne!(original, actual);
        let cancellation = CancellationToken::new();
        let mut opened = GenerationReceipt::new(finite_materialization_budget(), &cancellation)?;
        let held = opened.measured(|work| HostRoot::open_measured(&destination, work))?;
        assert_eq!(held.identity(), actual);
        let failure = validate_options(
            &MaterializeOptions::native(&destination).expected_root_identity(Some(original)),
            finite_materialization_budget(),
            &CancellationToken::new(),
        ).err().ok_or("replacement was accepted")?;
        assert!(matches!(failure.error, MaterializeError::AllocationChanged { expected, actual: found }
            if expected == original && found == actual));
        assert_eq!(*failure.work, opened.work);
        assert_eq!(failure.work.backend_write_operations, 0);
        assert_eq!(std::fs::read(destination.join("foreign"))?, b"untouched");
        Ok(())
    }

    #[test]
    fn destination_validation_preserves_zero_and_exact_budget_prefixes()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let options = MaterializeOptions::native(temporary.path());
        let cancellation = CancellationToken::new();
        let zero = validate_options(&options, WorkBudget::default(), &cancellation)
            .err().ok_or("zero budget was accepted")?;
        assert!(matches!(zero.error, MaterializeError::Work(_)));
        assert_eq!(*zero.work, WorkCounters::default());
        let measured = validate_options(&options, finite_materialization_budget(), &cancellation)?;
        let exact = validate_options(&options, measured.work, &cancellation)?;
        assert_eq!(exact.work, measured.work);
        let mut short = measured.work;
        short.backend_read_operations -= 1;
        let failure = validate_options(&options, short, &cancellation)
            .err().ok_or("short budget was accepted")?;
        assert!(matches!(failure.error, MaterializeError::Work(_)));
        failure.work.verify(short)?;
        assert!(failure.work.backend_read_operations > 0);
        Ok(())
    }

    #[tokio::test]
    async fn multipath_failure_retains_completed_host_and_lookup_work()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::model::{
            AccessMode, CaseSensitivity, CheckoutMode, ConcurrencyMode, ConsistencyMode,
            FilesystemProfile, GenerationSelector, Lifecycle, MutationMode, UnicodePolicy,
            VolumeConfig, VolumeLimits,
        };
        acyclic_native_runtime::initialize_native_io_services()?;
        let cancellation = CancellationToken::new();
        let limits = VolumeLimits::default();
        let volume = crate::Fs::memory().create_volume(
            VolumeConfig {
                profile: FilesystemProfile::Portable,
                concurrency: ConcurrencyMode::Optimistic,
                lifecycle: Lifecycle::Ephemeral,
                case_sensitivity: CaseSensitivity::Sensitive,
                unicode: UnicodePolicy::Preserve,
                symbolic_links: true,
                hard_links: true,
                sparse_files: true,
                limits,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ).await?.value;
        let mut checkout = volume.checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        ).await?.value;
        let first = NamespacePath::new(vec![LogicalName::new(
            NameEncoding::Utf8, b"a-present".to_vec(), limits.maximum_component_bytes,
        )?], limits)?;
        let missing = NamespacePath::new(vec![LogicalName::new(
            NameEncoding::Utf8, b"z-missing".to_vec(), limits.maximum_component_bytes,
        )?], limits)?;
        checkout.create_file(first.clone(), Bytes::from_static(b"first"),
            WorkBudget::UNBOUNDED, &cancellation).await?;
        let zero_output = tempfile::tempdir()?;
        let zero = materialize_checkout(
            &mut checkout, &MaterializeOptions::native(zero_output.path()),
            WorkBudget::default(), &cancellation,
        ).await.err().ok_or("zero materialization budget was accepted")?;
        assert!(matches!(zero.error, MaterializeError::Work(_)));
        assert_eq!(*zero.work, WorkCounters::UNCHARGED);
        assert!(std::fs::read_dir(zero_output.path())?.next().is_none());

        let cancelled_output = tempfile::tempdir()?;
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let cancelled = materialize_checkout(
            &mut checkout, &MaterializeOptions::native(cancelled_output.path()),
            finite_materialization_budget(), &cancelled,
        ).await.err().ok_or("cancelled materialization was accepted")?;
        assert!(matches!(cancelled.error, MaterializeError::Cancelled(_)));
        assert_eq!(*cancelled.work, WorkCounters::UNCHARGED);
        assert!(std::fs::read_dir(cancelled_output.path())?.next().is_none());

        let no_write_output = tempfile::tempdir()?;
        let no_write = materialize_checkout(
            &mut checkout, &MaterializeOptions::native(no_write_output.path()),
            WorkBudget { backend_write_operations: 0, ..finite_materialization_budget() },
            &cancellation,
        ).await.err().ok_or("zero native-write budget was accepted")?;
        assert!(matches!(no_write.error, MaterializeError::Work(_)));
        assert_eq!(no_write.work.backend_write_operations, 0);
        assert_eq!(no_write.work.output_bytes, 0);
        assert!(no_write.work.backend_read_operations > 0);
        assert!(no_write.work.allocation_operations > 0);
        assert!(std::fs::read_dir(no_write_output.path())?.next().is_none());

        let full_output = tempfile::tempdir()?;
        let full = materialize_checkout(
            &mut checkout, &MaterializeOptions::native(full_output.path()),
            finite_materialization_budget(), &cancellation,
        ).await?;
        let exact_output = tempfile::tempdir()?;
        let exact = materialize_checkout(
            &mut checkout, &MaterializeOptions::native(exact_output.path()),
            full.work, &cancellation,
        ).await?;
        assert_eq!(exact.work, full.work);
        assert_eq!(exact.value.work, exact.work);
        assert_eq!(std::fs::read(exact_output.path().join("a-present"))?, b"first");
        let baseline = tempfile::tempdir()?;
        let completed = materialize_checkout_paths(
            &mut checkout, std::slice::from_ref(&first),
            &MaterializeOptions::native(baseline.path()),
            finite_materialization_budget(), &cancellation,
        ).await?;
        let output = tempfile::tempdir()?;
        let failed = materialize_checkout_paths(
            &mut checkout, &[first.clone(), missing.clone()],
            &MaterializeOptions::native(output.path()),
            finite_materialization_budget(), &cancellation,
        ).await.err().ok_or("missing second path was accepted")?;
        assert!(matches!(failed.error, MaterializeError::MissingPath));
        assert_eq!(std::fs::read(output.path().join("a-present"))?, b"first");
        assert_eq!(failed.work.output_bytes, completed.work.output_bytes);
        assert_eq!(failed.work.materializations, completed.work.materializations);
        assert_eq!(failed.work.backend_write_operations, completed.work.backend_write_operations);
        assert!(failed.work.backend_read_operations > completed.work.backend_read_operations);
        let finite = tempfile::tempdir()?;
        let finite_failure = materialize_checkout_paths(
            &mut checkout,
            &[first, missing],
            &MaterializeOptions::native(finite.path()),
            *failed.work, &cancellation,
        ).await.err().ok_or("missing second path was accepted under finite budget")?;
        assert!(matches!(finite_failure.error, MaterializeError::MissingPath));
        assert_eq!(*finite_failure.work, *failed.work);
        Ok(())
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "captures both visible and filtered restore boundaries with one real checkout"
    )]
    async fn restore_removal_records_success_after_nested_missing_path()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::model::{
            AccessMode, CaseSensitivity, CheckoutMode, ConcurrencyMode, ConsistencyMode,
            FilesystemProfile, GenerationSelector, Lifecycle, MutationMode, UnicodePolicy,
            VolumeConfig, VolumeLimits,
        };
        use std::sync::Mutex;
        use tracing_subscriber::fmt::format::FmtSpan;

        #[derive(Clone)]
        struct Output(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Output {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .map_err(|_| std::io::Error::other("capture poisoned"))?
                    .extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        acyclic_native_runtime::initialize_native_io_services()?;
        let cancellation = CancellationToken::new();
        let volume = crate::Fs::memory()
            .create_volume(
                VolumeConfig {
                    profile: FilesystemProfile::Portable,
                    concurrency: ConcurrencyMode::Optimistic,
                    lifecycle: Lifecycle::Ephemeral,
                    case_sensitivity: CaseSensitivity::Sensitive,
                    unicode: UnicodePolicy::Preserve,
                    symbolic_links: true,
                    hard_links: true,
                    sparse_files: true,
                    limits: VolumeLimits::default(),
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let mut checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let temporary = tempfile::tempdir()?;
        let relative = Path::new("private-restore-path-sentinel");
        std::fs::write(temporary.path().join(relative), b"private-content-sentinel")?;
        let output = Output(Arc::default());
        let captured = Arc::clone(&output.0);
        // A second dispatcher prevents concurrently visited callsites from
        // caching interest only from another test's absent subscriber.
        let _second = tracing::Dispatch::new(tracing_subscriber::Registry::default());
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .without_time()
            .with_span_events(FmtSpan::CLOSE)
            .with_writer(move || output.clone())
            .finish();
        let _default = tracing::subscriber::set_default(subscriber);
        let receipt = restore_checkout_host_path(
            &mut checkout,
            relative,
            HostPathReplacement::Atomic,
            &MaterializeOptions::native(temporary.path()),
            finite_materialization_budget(),
            &cancellation,
        )
        .await?;
        assert_eq!(receipt.value, HostPathRestore::Removed);
        assert!(!temporary.path().join(relative).exists());
        let bytes = captured.lock().map_err(|_| "capture poisoned")?.clone();
        let text = String::from_utf8(bytes)?;
        let parent = text
            .lines()
            .rfind(|line| line.contains("acyclic.fs.restore_host_path{") && line.contains("close"))
            .ok_or("restore close missing")?;
        assert!(parent.contains("outcome=\"ok\""), "{parent}");
        assert!(parent.contains("path.depth=1"), "{parent}");
        assert!(
            parent.contains(&format!("work.items={}", receipt.work.items_examined)),
            "{parent}"
        );
        assert!(!parent.contains("error.kind="), "{parent}");
        assert!(text.contains("error.kind=\"MissingPath\""), "{text}");
        assert!(!text.contains("private-restore-path-sentinel"));
        assert!(!text.contains("private-content-sentinel"));
        assert!(!text.contains(&temporary.path().display().to_string()));

        drop(_default);
        captured.lock().map_err(|_| "capture poisoned")?.clear();
        use tracing::Instrument as _;
        use tracing_subscriber::Layer as _;
        use tracing_subscriber::layer::SubscriberExt as _;
        let output = Output(Arc::clone(&captured));
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .without_time()
                .with_span_events(FmtSpan::CLOSE)
                .with_writer(move || output.clone())
                .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                    !metadata.name().starts_with("acyclic.fs.materialize")
                        && metadata.name() != "acyclic.fs.restore_host_path"
                })),
        );
        let _default = tracing::subscriber::set_default(subscriber);
        let caller = tracing::info_span!(
            "restore_caller",
            outcome = "caller",
            error.kind = "caller_error",
            work.items = 73
        );
        async {
            let invalid = restore_checkout_host_path(
                &mut checkout, Path::new(""), HostPathReplacement::Atomic,
                &MaterializeOptions::native(temporary.path()),
                finite_materialization_budget(), &cancellation,
            ).await;
            assert!(matches!(invalid, Err(failure) if matches!(failure.error, MaterializeError::InvalidPath)));
            std::fs::write(temporary.path().join(relative), b"old")?;
            let removed = restore_checkout_host_path(
                &mut checkout, relative, HostPathReplacement::Atomic,
                &MaterializeOptions::native(temporary.path()),
                finite_materialization_budget(), &cancellation,
            ).await?;
            assert_eq!(removed.value, HostPathRestore::Removed);
            Ok::<_, Box<dyn std::error::Error>>(())
        }.instrument(caller.clone()).await?;
        drop(caller);
        let bytes = captured.lock().map_err(|_| "capture poisoned")?.clone();
        let text = String::from_utf8(bytes)?;
        assert!(!text.contains("acyclic.fs.restore_host_path"), "{text}");
        assert!(!text.contains("acyclic.fs.materialize"), "{text}");
        let caller = text
            .lines()
            .rfind(|line| line.contains("restore_caller{") && line.contains("close"))
            .ok_or("caller close missing")?;
        assert!(caller.contains("outcome=\"caller\""), "{caller}");
        assert!(caller.contains("error.kind=\"caller_error\""), "{caller}");
        assert!(caller.contains("work.items=73"), "{caller}");
        assert_eq!(caller.matches("outcome=").count(), 1, "{caller}");
        assert_eq!(caller.matches("error.kind=").count(), 1, "{caller}");
        assert_eq!(caller.matches("work.items=").count(), 1, "{caller}");
        Ok(())
    }

    /// A file created through a Linux mount records "no flags" (and its own
    /// birth and change times); materializing it must not refuse any of
    /// those, or no fork's new file could ever reach the root.
    #[cfg(unix)]
    #[test]
    fn host_projection_drops_what_linux_cannot_or_need_not_apply() {
        use crate::kernel::MetadataField;
        let created = FileMetadata {
            posix_flags: MetadataField::Value(0),
            created_ns: MetadataField::Value(1),
            changed_ns: MetadataField::Value(2),
            ..FileMetadata::default()
        };
        let projected = MaterializeMode::host_metadata(created).metadata;
        assert_eq!(projected.changed_ns, MetadataField::Unavailable);
        #[cfg(target_os = "linux")]
        {
            assert_eq!(projected.posix_flags, MetadataField::Unavailable);
            assert_eq!(projected.created_ns, MetadataField::Unavailable);
            let flagged = FileMetadata {
                posix_flags: MetadataField::Value(1),
                ..FileMetadata::default()
            };
            assert_eq!(
                MaterializeMode::host_metadata(flagged).metadata.posix_flags,
                MetadataField::Value(1)
            );
        }
    }

    #[tokio::test]
    async fn native_materialization_rejects_unrepresentable_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::MetadataField;

        acyclic_native_runtime::initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("file"), b"data")?;
        let root = Arc::new(HostRoot::open(temporary.path())?);
        let cancellation = CancellationToken::new();
        let mut receipt = GenerationReceipt::new(finite_materialization_budget(), &cancellation)?;
        let limits = crate::model::VolumeLimits::default();
        let namespace = NamespacePath::new(vec![LogicalName::new(
            NameEncoding::Utf8, b"file".to_vec(), limits.maximum_component_bytes,
        )?], limits)?;
        let host_path = GenerationHostPath::from_namespace(&namespace, &mut receipt)?;
        #[cfg(unix)]
        for (metadata, expected) in [
            (
                FileMetadata {
                    posix_flags: MetadataField::Value(1),
                    ..FileMetadata::default()
                },
                "posix_flags",
            ),
            (
                FileMetadata {
                    changed_ns: MetadataField::Value(123),
                    ..FileMetadata::default()
                },
                "changed_ns without native-view baseline",
            ),
        ] {
            assert!(matches!(
                apply_host_metadata_offloaded(&root, &host_path, metadata, &mut receipt).await,
                Err(failure) if matches!(failure.error, MaterializeError::UnsupportedMetadata(field) if field == expected)
            ));
        }
        #[cfg(windows)]
        let metadata = FileMetadata {
            posix_mode: MetadataField::Value(0o644),
            ..FileMetadata::default()
        };
        #[cfg(windows)]
        assert!(matches!(
            apply_host_metadata_offloaded(&root, &host_path, metadata, &mut receipt).await,
            Err(failure) if matches!(failure.error, MaterializeError::UnsupportedMetadata(_))
        ));
        Ok(())
    }

    fn restoration_work() -> crate::demand::DemandWork {
        crate::demand::DemandWork::new(WorkBudget {
            backend_read_operations: 10_000,
            backend_write_operations: 10_000,
            durability_operations: 10_000,
            bytes_hashed: 1_000_000,
            bytes_copied: 1_000_000,
            bytes_encoded: 1_000_000,
            source_bytes_read: 1_000_000,
            source_path_components: 10_000,
            source_entries_visited: 10_000,
            output_bytes: 1_000_000,
            items_examined: 10_000,
            allocation_operations: 10_000,
            peak_allocation_bytes: 1_000_000,
            ..WorkCounters::UNCHARGED
        }, &CancellationToken::new())
    }

    #[test]
    fn restore_preparation_rejects_a_swapped_root_before_any_write()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("root");
        std::fs::create_dir(&destination)?;
        let expected = HostRoot::open(&destination)?.identity();
        std::fs::rename(&destination, temporary.path().join("original"))?;
        std::fs::create_dir(&destination)?;
        std::fs::write(destination.join("foreign"), b"untouched")?;
        let mut work = restoration_work();
        let failure = prepare_restore(
            destination.clone(), PathBuf::from("nested/entry"), Some(expected),
            HostPathReplacement::Atomic, &mut work,
        ).err().ok_or("swapped restore root was accepted")?;
        assert!(matches!(failure, MaterializeError::AllocationChanged { expected: found, .. } if found == expected));
        assert!(work.work.backend_read_operations > 0);
        assert_eq!(work.work.backend_write_operations, 0);
        assert_eq!(work.work.durability_operations, 0);
        assert!(!destination.join("nested").exists());
        assert_eq!(std::fs::read(destination.join("foreign"))?, b"untouched");
        Ok(())
    }

    #[test]
    fn restore_removal_retains_the_original_physical_parent()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let destination = temporary.path().join("root");
        let original = temporary.path().join("original");
        std::fs::create_dir(&destination)?;
        std::fs::write(destination.join("entry"), b"old")?;
        let mut work = restoration_work();
        let root = HostRoot::open_measured(&destination, &mut work)?;
        let parent = held_parent(&root, Path::new("entry"), &mut work)?;
        std::fs::rename(&destination, &original)?;
        std::fs::create_dir(&destination)?;
        std::fs::write(destination.join("entry"), b"foreign")?;
        remove_restored_path(&parent, Path::new("entry"), Path::new("entry"), &mut work)?;
        assert!(!original.join("entry").exists());
        assert_eq!(std::fs::read(destination.join("entry"))?, b"foreign");
        assert!(work.work.backend_write_operations > 0);
        assert!(work.work.durability_operations > 0);
        Ok(())
    }

    #[test]
    fn same_path_restores_are_serialized_by_a_process_crash_safe_lock()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        std::fs::create_dir(&root)?;
        let relative = Path::new("entry");
        let mut work = restoration_work();
        let held_root = HostRoot::open_measured(&root, &mut work)?;
        let parent = held_parent(&held_root, relative, &mut work)?;
        let first = acquire_restore_lock(&parent, relative, true, &mut work)?;

        assert!(acquire_restore_lock(&parent, relative, false, &mut work).is_err());
        drop(first);
        // Blocking: a child another test forks shares the lock's open file
        // until it execs, so a release can take a moment to be observed.
        let _next = acquire_restore_lock(&parent, relative, true, &mut work)?;
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_restore_waiter_keeps_lock_until_host_io_finishes()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let relative = Path::new("entry");
        let mut work = restoration_work();
        let held_root = HostRoot::open_measured(temporary.path(), &mut work)?;
        let parent = held_parent(&held_root, relative, &mut work)?;
        let lock = acquire_restore_lock(&parent, relative, true, &mut work)?;
        let (started, entered) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let (finished, completion) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(restore_io(
            crate::obs::caller_context(),
            Some(lock),
            Arc::new(std::sync::Mutex::new(restoration_work())),
            move |_| {
                let _ = started.send(());
                // Dropping release on an early test exit also unblocks the worker.
                blocked.recv().map_err(std::io::Error::other)?;
                let _ = finished.send(());
                Ok::<_, MaterializeError>(())
            },
        ));
        entered.await?;
        waiter.abort();
        let cancelled = waiter.await;
        let competing = acquire_restore_lock(&parent, relative, false, &mut work);
        let competing_error = match &competing {
            Err(MaterializeError::Io(error)) => Some((error.raw_os_error(), error.kind())),
            _ => None,
        };
        eprintln!("competing restore lock error: {competing_error:?}");
        let fenced = matches!(
            &competing,
            Err(MaterializeError::Io(error))
                if error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
        );
        drop(competing);
        // Release before assertions: even the broken-lock mutation must finish.
        release.send(())?;
        completion.await?;
        // Acquiring is the completion barrier for the worker's final lock drop.
        let _next = acquire_restore_lock(&parent, relative, true, &mut work)?;
        assert!(cancelled.is_err_and(|error| error.is_cancelled()));
        assert!(
            fenced,
            "cancelled waiter released a still-running restore's lock: {competing_error:?}"
        );
        Ok(())
    }

    #[test]
    fn live_mount_recovery_restores_a_displaced_entry() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        std::fs::create_dir(&root)?;
        let relative = Path::new("entry");
        let destination = root.join(relative);
        std::fs::write(&destination, b"before")?;
        let mut work = restoration_work();
        let held_root = HostRoot::open_measured(&root, &mut work)?;
        let parent = held_parent(&held_root, relative, &mut work)?;
        let backup = restore_artifact_name(".acyclic-restore-backup-", relative, &mut work)?;
        let witness = restore_artifact_name(".acyclic-restore-live-witness-", relative, &mut work)?;
        create_restore_witness(&parent, &witness, LIVE_RESTORE_WITNESS, &mut work)?;
        parent.rename_to(relative, &parent, &backup)?;

        recover_live_mount_replacement(&parent, relative, relative, &mut work)?;

        assert_eq!(std::fs::read(&destination)?, b"before");
        assert!(!root.join(backup).exists());
        Ok(())
    }

    #[test]
    fn live_mount_recovery_keeps_a_published_entry() -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        std::fs::create_dir(&root)?;
        let relative = Path::new("entry");
        let destination = root.join(relative);
        std::fs::write(&destination, b"after")?;
        let mut work = restoration_work();
        let held_root = HostRoot::open_measured(&root, &mut work)?;
        let parent = held_parent(&held_root, relative, &mut work)?;
        let backup = restore_artifact_name(".acyclic-restore-backup-", relative, &mut work)?;
        let witness = restore_artifact_name(".acyclic-restore-live-witness-", relative, &mut work)?;
        create_restore_witness(&parent, &witness, LIVE_RESTORE_WITNESS, &mut work)?;
        std::fs::write(root.join(&backup), b"before")?;

        recover_live_mount_replacement(&parent, relative, relative, &mut work)?;

        assert_eq!(std::fs::read(&destination)?, b"after");
        assert!(!root.join(backup).exists());
        Ok(())
    }

    #[test]
    fn live_mount_recovery_preserves_an_unowned_backup_collision()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        std::fs::create_dir(&root)?;
        let relative = Path::new("entry");
        let destination = root.join(relative);
        std::fs::write(&destination, b"live")?;
        let mut work = restoration_work();
        let held_root = HostRoot::open_measured(&root, &mut work)?;
        let parent = held_parent(&held_root, relative, &mut work)?;
        let backup = restore_artifact_name(".acyclic-restore-backup-", relative, &mut work)?;
        std::fs::write(root.join(&backup), b"unrelated")?;

        assert!(recover_live_mount_replacement(&parent, relative, relative, &mut work).is_err());
        assert_eq!(std::fs::read(&destination)?, b"live");
        assert_eq!(std::fs::read(root.join(backup))?, b"unrelated");
        Ok(())
    }

    #[test]
    fn absent_restore_moves_then_reclaims_the_old_entry() -> Result<(), Box<dyn std::error::Error>>
    {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("root");
        std::fs::create_dir(&root)?;
        let relative = Path::new("entry");
        std::fs::write(root.join(relative), b"old")?;
        let mut work = restoration_work();
        let held_root = HostRoot::open_measured(&root, &mut work)?;
        let parent = held_parent(&held_root, relative, &mut work)?;

        remove_restored_path(&parent, relative, relative, &mut work)?;

        assert!(!root.join(relative).exists());
        assert!(
            !root
                .join(restore_artifact_name(".acyclic-restore-removed-", relative, &mut work)?)
                .exists()
        );
        Ok(())
    }
}

#[cfg(all(test, any(target_os = "macos", windows)))]
mod clone_tests {
    use super::*;
    use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
    use crate::model::{
        AccessMode, CaseSensitivity, CheckoutMode, ConcurrencyMode, ConsistencyMode,
        FilesystemProfile, GenerationSelector, Lifecycle, MutationMode, UnicodePolicy,
        VolumeConfig, VolumeLimits,
    };
    use crate::{Fs, VolumeId};

    #[tokio::test]
    async fn identical_regular_payloads_use_native_clone_without_aliasing()
    -> Result<(), Box<dyn std::error::Error>> {
        acyclic_native_runtime::initialize_native_io_services()?;
        #[cfg(target_os = "macos")]
        let directory = tempfile::tempdir()?;
        #[cfg(windows)]
        let directory = {
            if let Some(parent) = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT") {
                tempfile::Builder::new()
                    .prefix("acyclic-materialize-clone-")
                    .tempdir_in(parent)?
            } else {
                tempfile::tempdir()?
            }
        };
        let clone_available =
            crate::probe_native_storage_capabilities(directory.path())?.block_cloning;
        let limits = VolumeLimits::default();
        let fs = Fs::memory();
        let cancellation = CancellationToken::new();
        let volume = fs
            .create_volume_with_id(
                VolumeId::from_bytes([73; 16]),
                VolumeConfig {
                    profile: FilesystemProfile::Portable,
                    concurrency: ConcurrencyMode::Optimistic,
                    lifecycle: Lifecycle::Ephemeral,
                    case_sensitivity: CaseSensitivity::Sensitive,
                    unicode: UnicodePolicy::Preserve,
                    symbolic_links: true,
                    hard_links: true,
                    sparse_files: true,
                    limits,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let mut checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode {
                    access: AccessMode::ReadWrite,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::PrivateOverlay,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        // The payload deliberately ends between cluster boundaries: Windows
        // must clone the aligned body and copy the short tail.
        let bytes = Bytes::from(vec![0xA5; 128 * 1024 + 17]);
        for name in ["first.bin", "second.bin"] {
            let path = NamespacePath::new(
                vec![LogicalName::new(
                    NameEncoding::Utf8,
                    name.as_bytes().to_vec(),
                    limits.maximum_component_bytes,
                )?],
                limits,
            )?;
            checkout
                .create_file(path, bytes.clone(), WorkBudget::UNBOUNDED, &cancellation)
                .await?;
        }
        let destination = directory.path().join("view");
        std::fs::create_dir(&destination)?;
        let receipt = materialize_checkout(
            &mut checkout,
            &MaterializeOptions {
                destination: destination.clone(),
                expected_root_identity: None,
                maximum_directory_entries: 16,
                maximum_extent_spans: 16,
                transfer_bytes: 64 * 1024,
            },
            super::restore_recovery_tests::finite_materialization_budget(),
            &cancellation,
        )
        .await?;
        assert_eq!(receipt.value.files, 2);
        assert_eq!(
            receipt.value.written_bytes,
            bytes.len() as u64 * if clone_available { 1 } else { 2 }
        );
        let first = destination.join("first.bin");
        let second = destination.join("second.bin");
        assert_eq!(std::fs::read(&first)?.as_slice(), bytes.as_ref());
        assert_eq!(std::fs::read(&second)?.as_slice(), bytes.as_ref());
        std::fs::write(&first, b"changed")?;
        assert_eq!(std::fs::read(&second)?.as_slice(), bytes.as_ref());
        Ok(())
    }
}
