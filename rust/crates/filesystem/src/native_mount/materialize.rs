//! Explicit native materialization of one authenticated checkout.

#[cfg(any(target_os = "macos", windows))]
use crate::ObjectId;
use crate::heap_future::in_heap;
use crate::kernel::{
    ExtentKind, FileKind, FileMetadata, FilePayload, LogicalName, NameEncoding, NamespacePath,
};
use crate::native_host::{HostRoot, NativeHostWork};
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
use crate::demand::{DemandError, NativeDemandReadMeter, NativeSourceWork, SourceDemandReadWork};

// Native failures carry their prefix inline; only ordinary SDK boundaries box it.
#[derive(Debug)]
struct GenerationFailure {
    error: MaterializeError,
    work: WorkCounters,
}

impl GenerationFailure {
    fn new(error: MaterializeError, work: WorkCounters) -> Self { Self { error, work } }
}

impl From<GenerationFailure> for MaterializeError {
    fn from(failure: GenerationFailure) -> Self { failure.error }
}
impl From<GenerationFailure> for OperationFailure<MaterializeError> {
    fn from(failure: GenerationFailure) -> Self { OperationFailure::new(failure.error, failure.work) }
}

impl<E: From<MaterializeError>> From<GenerationFailure> for (E, WorkCounters) {
    fn from(failure: GenerationFailure) -> Self { (failure.error.into(), failure.work) }
}

impl From<(MaterializeError, WorkCounters)> for GenerationFailure {
    fn from((error, work): (MaterializeError, WorkCounters)) -> Self { Self { error, work } }
}

/// One accepted SDK mutation and its completed physical prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeMaterializationActionPhase {
    Before,
    Completed,
}

async fn generation_action<E, F, Fut>(
    control: &mut Option<&mut F>, receipt: &GenerationReceipt,
    phase: NativeMaterializationActionPhase,
) -> Result<(), (E, WorkCounters)>
where
    F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    if let Some(control) = control {
        control(phase).await.map_err(|error| (error, receipt.meter.lock().work))?;
    }
    Ok(())
}

/// Native traversal and host work share the original ledger; standalone SDK reads contribute their measured prefixes.
struct GenerationReceipt {
    value: MaterializationReceipt,
    meter: Arc<NativeDemandReadMeter>,
    cancellation: CancellationToken,
    custody: crate::materializer::NativeAllocation,
    native_source: bool,
}

struct GenerationOwned<T> {
    value: T,
    _custody: crate::materializer::NativeAllocation,
}

impl<T> std::ops::Deref for GenerationOwned<T> {
    type Target = T;

    fn deref(&self) -> &T { &self.value }
}

type GenerationRoot = Arc<GenerationOwned<HostRoot>>;
type GenerationPath = Arc<GenerationOwned<GenerationHostPath>>;
type GenerationNamespace = Arc<GenerationOwned<NamespacePath>>;

impl GenerationReceipt {
    fn work(&self) -> WorkCounters { self.meter.lock().work }

    fn new(
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, GenerationFailure> {
        let original = crate::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(error, work)| GenerationFailure::new(materialize_native_error(error), work))?;
        Self::from_work(&original)
    }

    fn from_work(work: &crate::NativeMaterializationWork) -> Result<Self, GenerationFailure> {
        let meter = Arc::clone(work.native_read_meter());
        let custody = work.retain_owned(0).map_err(|error| {
            GenerationFailure::new(materialize_native_error(error), work.receipt())
        })?;
        let cancellation = meter.with_original_leaf(|original| original.cancellation().clone());
        Ok(Self {
            value: MaterializationReceipt::default(),
            meter,
            cancellation,
            custody,
            native_source: false,
        })
    }

    fn run<T>(
        &self,
        operation: impl FnOnce(&mut NativeSourceWork<'_>) -> Result<T, DemandError>,
    ) -> Result<T, GenerationFailure> {
        let result = operation(&mut NativeSourceWork::from_native_materialization_work(self.custody.work()));
        result.map_err(|error| GenerationFailure::new(materialize_demand_error(error), self.work()))
    }

    fn allocate(&mut self, bytes: usize) -> Result<(), GenerationFailure> {
        let bytes = bytes as u64;
        let result = self.custody.grow(bytes);
        self.custody_result(result)
    }

    fn release(&mut self, bytes: usize) -> Result<(), GenerationFailure> {
        let result = self.custody.release(bytes as u64);
        self.custody_result(result)
    }

    fn custody_result<T>(
        &self,
        result: Result<T, crate::NativeTreeMaterializationError>,
    ) -> Result<T, GenerationFailure> {
        result.map_err(|error| GenerationFailure::new(materialize_native_error(error), self.work()))
    }

    fn adopt(&mut self, bytes: usize) -> Result<(), GenerationFailure> {
        let retained = self.custody.bytes().checked_add(bytes as u64)
            .ok_or_else(|| GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), self.work()))?;
        let result = self.custody.reconcile_capacity(retained);
        self.custody_result(result)
    }

    fn take(&mut self, bytes: usize) -> Result<crate::materializer::NativeAllocation, GenerationFailure> {
        let result = self.custody.split_off(bytes as u64);
        self.custody_result(result)
    }

    fn owned_arc<T>(
        &mut self, value: T, retained: usize,
    ) -> Result<Arc<GenerationOwned<T>>, GenerationFailure> {
        let bytes = std::mem::size_of::<GenerationOwned<T>>();
        self.allocate(bytes)?;
        let bytes = retained.checked_add(bytes)
            .ok_or_else(|| GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), self.work()))?;
        let custody = self.take(bytes)?;
        Ok(Arc::new(GenerationOwned { value, _custody: custody }))
    }

    fn reserve<T>(
        &mut self,
        values: &mut Vec<T>,
        additional: usize,
    ) -> Result<(), GenerationFailure> {
        let old_capacity = values.capacity();
        let required = values.len().checked_add(additional).ok_or_else(|| {
            GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), self.work())
        })?;
        let growth = required.saturating_sub(old_capacity);
        let bytes = growth.checked_mul(std::mem::size_of::<T>()).ok_or_else(|| {
            GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), self.work())
        })?;
        if growth == 0 {
            return Ok(());
        }
        let result = self.custody.work().allocations(|ledger, work, budget, cancellation| {
            cancellation.check().map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
            let prior_peak = work.peak_allocation_bytes;
            ledger.claim_bytes(bytes as u64, 1, work, budget).map_err(|error| {
                error.into_work_or(crate::NativeTreeMaterializationError::Io(
                    std::io::ErrorKind::OutOfMemory.into(),
                ))
            })?;
            if values.try_reserve_exact(additional).is_err() {
                ledger.release(bytes as u64).map_err(|_| {
                    crate::NativeTreeMaterializationError::Work(WorkError::Overflow)
                })?;
                work.peak_allocation_bytes = prior_peak;
                return Err(crate::NativeTreeMaterializationError::Io(
                    std::io::ErrorKind::OutOfMemory.into(),
                ));
            }
            let actual = values.capacity().checked_sub(old_capacity)
                .and_then(|growth| growth.checked_mul(std::mem::size_of::<T>()))
                .ok_or(crate::NativeTreeMaterializationError::Work(WorkError::Overflow))?;
            ledger.record_owned_bytes(actual.saturating_sub(bytes) as u64, work).map_err(|_| {
                crate::NativeTreeMaterializationError::Work(WorkError::Overflow)
            })?;
            // Retain the physical capacity even when its observed surplus fails
            // the late check; the caller drops the vector before this custody.
            Ok(work.verify(budget))
        });
        let (verified, mut custody) = self.custody_result(result)?;
        let absorbed = self.custody.absorb(&mut custody);
        self.custody_result(absorbed)?;
        verified.map_err(|error| GenerationFailure::new(MaterializeError::Work(error), self.work()))
    }

    fn push<T>(
        &mut self,
        values: &mut Vec<T>,
        value: T,
    ) -> Result<(), GenerationFailure> {
        if values.len() == values.capacity() {
            let additional = values.capacity().max(1);
            self.reserve(values, additional)?;
        }
        values.push(value);
        Ok(())
    }

    fn native_meter(&self) -> Arc<dyn acyclic_native_runtime::NativeReadMeter> {
        self.custody.work().runtime_meter_owner()
    }

    fn runtime_result<T>(
        &self,
        result: std::io::Result<T>,
    ) -> Result<T, GenerationFailure> {
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
            GenerationFailure::new(mapped, self.work())
        })
    }

    fn original_result<T>(
        &self, result: Result<T, crate::FsError>,
    ) -> Result<T, GenerationFailure> {
        result.map_err(|error| {
            let error = match error {
                crate::FsError::NativeKernel(error) => materialize_native_error(error),
                crate::FsError::Work(error) => MaterializeError::Work(error),
                crate::FsError::Cancelled(error) => MaterializeError::Cancelled(error),
                error => MaterializeError::Source(error),
            };
            GenerationFailure::new(error, self.work())
        })
    }

    fn accept_source<T>(
        &mut self, value: T, custody: crate::NativeAllocation,
    ) -> Result<T, GenerationFailure> {
        let mut owned = GenerationOwned { value, _custody: custody };
        let result = self.custody.absorb(&mut owned._custody);
        self.custody_result(result)?;
        Ok(owned.value)
    }

    fn engine_failure(
        &self,
        failure: OperationFailure<crate::FsError>,
    ) -> GenerationFailure {
        let error = MaterializeError::from(failure.error);
        match self.run(|work| work.merge(*failure.work)) {
            Ok(()) => GenerationFailure::new(error, self.work()),
            Err(failure) => failure,
        }
    }
}

macro_rules! read_source {
    ($receipt:expr, $remaining:ident, $value:ident,
     $native:expr => $accept_native:expr, $legacy:expr => $accept_legacy:expr $(,)?) => {
        if $receipt.native_source {
            let $value = $receipt.original_result($native.await)?;
            $accept_native
        } else {
            let $remaining = $receipt.run(|work| work.remaining())?;
            let result = $legacy.await.map_err(|failure| $receipt.engine_failure(failure))?;
            $receipt.run(|work| work.merge(result.work))?;
            let $value = result.value;
            $accept_legacy
        }
    };
}

fn materialize_native_error(error: crate::NativeTreeMaterializationError) -> MaterializeError {
    match error {
        crate::NativeTreeMaterializationError::Work(error) => MaterializeError::Work(error),
        crate::NativeTreeMaterializationError::Cancelled => MaterializeError::Cancelled(crate::CancellationError),
        crate::NativeTreeMaterializationError::Io(error) => MaterializeError::Io(error),
        error => MaterializeError::NativeWork(error),
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
        error => MaterializeError::Demand(error),
    }
}

fn clone_namespace_measured(
    path: &NamespacePath,
    extra: Option<&LogicalName>,
    limits: crate::model::VolumeLimits,
    receipt: &mut GenerationReceipt,
) -> Result<NamespacePath, GenerationFailure> {
    let result = receipt.custody.work().allocations(|ledger, work, budget, cancellation| {
        let result = match extra {
            Some(name) => path.child_with_ledger(name, limits, ledger, work, budget, cancellation),
            None => path.clone_with_ledger(ledger, work, budget, cancellation),
        };
        result.map_err(|error| match error {
            crate::kernel::NamespacePathError::Work(error) => crate::NativeTreeMaterializationError::Work(error),
            crate::kernel::NamespacePathError::Cancelled => crate::NativeTreeMaterializationError::Cancelled,
            _ => crate::NativeTreeMaterializationError::InvalidLayout,
        })
    });
    let (path, mut custody) = receipt.custody_result(result)?;
    let result = receipt.custody.absorb(&mut custody);
    receipt.custody_result(result)?;
    Ok(path)
}

fn share_generation_namespace(
    path: NamespacePath,
    receipt: &mut GenerationReceipt,
) -> Result<GenerationNamespace, GenerationFailure> {
    let bytes = path.retained_bytes() as usize;
    receipt.owned_arc(path, bytes)
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
    ) -> Result<GenerationPath, GenerationFailure> {
        Self::from_components(None, path.components(), receipt)
    }

    fn child(
        &self,
        name: &LogicalName,
        receipt: &mut GenerationReceipt,
    ) -> Result<GenerationPath, GenerationFailure> {
        Self::from_components(Some(self), std::slice::from_ref(name), receipt)
    }

    fn from_components(
        prefix: Option<&Self>,
        names: &[LogicalName],
        receipt: &mut GenerationReceipt,
    ) -> Result<GenerationPath, GenerationFailure> {
        #[cfg(unix)]
        {
            let prefix = prefix.map_or(&[][..], |prefix| prefix.bytes.as_slice());
            let mut length = prefix.len().checked_add(names.len().saturating_sub(1))
                .and_then(|length| length.checked_add(usize::from(!prefix.is_empty() && !names.is_empty())))
                .ok_or_else(|| GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work()))?;
            for name in names {
                receipt.run(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
                if !matches!(name.encoding(), NameEncoding::Utf8 | NameEncoding::PosixBytes) {
                    return Err(GenerationFailure::new(MaterializeError::UnrepresentableName, receipt.work()));
                }
                length = length.checked_add(name.as_bytes().len()).ok_or_else(|| {
                    GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
                })?;
            }
            let mut bytes = Vec::new();
            receipt.reserve(&mut bytes, length)?;
            receipt.run(|work| work.charge(WorkCounters { bytes_copied: length as u64, ..WorkCounters::UNCHARGED }))?;
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
                .ok_or_else(|| GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work()))?;
            for name in names {
                receipt.run(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
                let count = match name.encoding() {
                    NameEncoding::WindowsUtf16Le => name.as_bytes().len() / 2,
                    NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                        let text = std::str::from_utf8(name.as_bytes()).map_err(|_| {
                            GenerationFailure::new(MaterializeError::UnrepresentableName, receipt.work())
                        })?;
                        let mut count = 0_usize;
                        for _ in text.encode_utf16() {
                            receipt.run(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
                            count = count.checked_add(1).ok_or_else(|| {
                                GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
                            })?;
                        }
                        count
                    }
                };
                length = length.checked_add(count).ok_or_else(|| {
                    GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
                })?;
            }
            let mut units = Vec::new();
            receipt.reserve(&mut units, length)?;
            receipt.run(|work| work.charge(WorkCounters {
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
                            GenerationFailure::new(MaterializeError::UnrepresentableName, receipt.work())
                        })?,
                    ),
                    NameEncoding::Utf8 | NameEncoding::PosixBytes => units.extend(
                        std::str::from_utf8(name.as_bytes()).map_err(|_| {
                            GenerationFailure::new(MaterializeError::UnrepresentableName, receipt.work())
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
    ) -> Result<GenerationPath, GenerationFailure> {
        #[cfg(unix)]
        let retained = path.bytes.capacity();
        #[cfg(windows)]
        let retained = path.units.capacity().checked_mul(std::mem::size_of::<u16>())
            .ok_or_else(|| GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work()))?;
        receipt.owned_arc(path, retained)
    }

    #[cfg(unix)]
    fn path(&self) -> &Path {
        use std::os::unix::ffi::OsStrExt as _;
        Path::new(OsStr::from_bytes(&self.bytes))
    }

    fn create_parents(
        &self, root: &HostRoot, work: &mut NativeSourceWork<'_>,
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

    fn create_dir(
        &self, root: &HostRoot, work: &mut NativeSourceWork<'_>,
    ) -> Result<(), DemandError> {
        #[cfg(unix)]
        { root.create_dir(self.path(), work) }
        #[cfg(windows)]
        { root.create_dir_wide_measured(&self.units, work) }
    }

    fn hard_link(
        &self, root: &HostRoot, source: &Self, work: &mut NativeSourceWork<'_>,
    ) -> Result<(), DemandError> {
        #[cfg(unix)]
        { root.hard_link(source.path(), self.path(), work) }
        #[cfg(windows)]
        { root.hard_link_wide_measured(&source.units, &self.units, work) }
    }

    #[cfg(any(target_os = "macos", windows))]
    fn clone_file(
        &self, root: &HostRoot, source: &Self, work: &mut NativeSourceWork<'_>,
    ) -> Result<bool, DemandError> {
        #[cfg(target_os = "macos")]
        { root.clone_file_from(root, source.path(), self.path(), work) }
        #[cfg(windows)]
        { root.clone_file_from_wide_measured(root, &source.units, &self.units, work) }
    }

    fn stat(
        &self, root: &HostRoot, work: &mut NativeSourceWork<'_>,
    ) -> Result<(), DemandError> {
        #[cfg(unix)]
        root.stat(self.path(), work)?;
        #[cfg(windows)]
        root.stat_wide_with_work(&self.units, work)?;
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
    pub expected_root_identity: Option<crate::NativeRootIdentity>,
    /// Maximum directory entries fetched per authenticated page.
    pub maximum_directory_entries: u32,
    /// Maximum sparse spans fetched per authenticated range plan.
    pub maximum_extent_spans: u32,
    /// Maximum bytes read or zero-filled in one allocation.
    pub transfer_bytes: u64,
}

/// Both outputs preserve canonical metadata in the SDK.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MaterializeMode {
    DurableOutput,
    ReconstructibleView,
}

fn host_metadata(metadata: FileMetadata) -> (FileMetadata, Option<crate::ObjectId>) {
    #[cfg(unix)]
    let mut metadata = metadata;
    #[cfg(unix)]
    {
        metadata.changed_ns = crate::kernel::MetadataField::Unavailable;
    }
    #[cfg(target_os = "linux")]
    {
        metadata.created_ns = crate::kernel::MetadataField::Unavailable;
        if metadata.posix_flags == crate::kernel::MetadataField::Value(0) {
            metadata.posix_flags = crate::kernel::MetadataField::Unavailable;
        }
    }
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
    (metadata, named_attributes)
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
    /// Original native accounting or custody failed without diagnostic copying.
    #[error(transparent)]
    NativeWork(crate::NativeTreeMaterializationError),
    /// Original SDK source failed without copying its diagnostic payload.
    #[error(transparent)]
    Source(crate::FsError),
    /// A fixed engine invariant failed without allocating diagnostic text.
    #[error("filesystem engine failed: {0}")]
    Engine(&'static str),
    /// The original host/demand cause remains typed and unboxed.
    #[error(transparent)]
    Demand(crate::DemandError),
    /// Replacement and its rollback both failed; the owned backup is retained.
    #[error("replacement failed: {primary}; displaced entry remains in the owned backup after rollback failed: {rollback}")]
    RestoreRollback {
        /// Primary actual replacement failure.
        #[source]
        primary: crate::DemandError,
        /// Actual attempted rollback failure.
        rollback: crate::DemandError,
    },
    /// Primary materialization and actual cleanup both failed.
    #[error("materialization failed: {primary}; cleanup also failed: {cleanup}")]
    RestoreCleanup {
        /// Original primary cause in its pre-admitted physical slot.
        #[source]
        primary: Box<MaterializeError>,
        /// Actual cleanup cause, without diagnostic copying.
        cleanup: crate::DemandError,
        /// Allocation released after both error payloads.
        allocation: crate::NativeAllocation,
    },
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
            error => Self::Source(error),
        }
    }
}

/// Explicitly materializes one checkout without changing or publishing it.
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
            materialize_heap_future_on_receipt(
                move |receipt| async move {
                    let root = NamespacePath::new(Vec::new(), checkout.volume_config().limits)
                        .map_err(|_| GenerationFailure::new(MaterializeError::InvalidPath, receipt.work()))?;
                    validate_bounds(options)
                        .map_err(|error| GenerationFailure::new(error, receipt.work()))?;
                    materialize_checkout_source_on_receipt(
                        GenerationSource::Standalone(checkout), std::slice::from_ref(&root), options,
                        MaterializeMode::DurableOutput, receipt,
                    ).await.map_err(Into::into)
                },
                GenerationReceipt::new(budget, cancellation)?,
            ),
        )
        .await.map_err(|(error, work)| OperationFailure::new(error, work)),
        |receipt| &receipt.work,
    )
}

async fn materialize_heap_future_on_receipt<F, E>(
    create: impl FnOnce(GenerationReceipt) -> F,
    mut receipt: GenerationReceipt,
) -> Result<OperationReceipt<MaterializationReceipt>, (E, WorkCounters)>
where
    F: Future<Output = Result<OperationReceipt<MaterializationReceipt>, (E, WorkCounters)>>,
    E: From<MaterializeError>,
{
    // Retire the stack-safety box before releasing its allocation.
    receipt.allocate(std::mem::size_of::<F>())?;
    let custody = receipt.take(std::mem::size_of::<F>())?;
    let meter = Arc::clone(&receipt.meter);
    let mut future = in_heap(move || create(receipt));
    let result = future.as_mut().await;
    drop(future);
    drop(custody);
    let work = meter.lock().work;
    match result {
        Ok(mut receipt) => {
            receipt.work = work;
            receipt.value.work = work;
            Ok(receipt)
        }
        Err((error, _)) => Err((error, work)),
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
        NamespacePath::new(Vec::new(), checkout.volume_config().limits).map_err(|_| {
            OperationFailure::before_work(MaterializeError::InvalidPath)
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
    let receipt = GenerationReceipt::new(budget, cancellation)?;
    materialize_checkout_source_on_receipt(GenerationSource::Standalone(checkout), paths, options, mode, receipt)
        .await.map_err(OperationFailure::from)
}

enum GenerationSource<'a, A, O> {
    Standalone(&'a mut Checkout<A, O>),
    Original(&'a PinnedReader<A, O>),
}

/// Materializes an original-native immutable reader on its existing ledger.
pub async fn materialize_checkout_with_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    options: &MaterializeOptions,
    work: &crate::NativeMaterializationWork,
) -> Result<MaterializationReceipt, MaterializeError> {
    let mode = MaterializeMode::DurableOutput;
    let receipt = GenerationReceipt::from_work(work)?;
    let validated = reader.validate_native_work(receipt.custody.work());
    receipt.original_result(validated)?;
    materialize_heap_future_on_receipt(move |receipt| async move {
        let root = NamespacePath::new(Vec::new(), reader.volume_config().limits)
            .map_err(|_| GenerationFailure::new(MaterializeError::InvalidPath, receipt.work()))?;
        validate_bounds(options).map_err(|error| GenerationFailure::new(error, receipt.work()))?;
        materialize_checkout_source_on_receipt(
            GenerationSource::Original(reader), std::slice::from_ref(&root), options, mode, receipt,
        ).await.map_err(Into::into)
    }, receipt).await.map(|receipt| receipt.value).map_err(|failure| failure.0)
}

/// Materializes non-overlapping original-native selections on the same ledger.
pub async fn materialize_checkout_paths_with_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    paths: &[NamespacePath],
    options: &MaterializeOptions,
    work: &crate::NativeMaterializationWork,
) -> Result<MaterializationReceipt, MaterializeError> {
    materialize_checkout_paths_with_mode_and_work(
        reader, paths, options, MaterializeMode::DurableOutput, work,
    ).await
}

pub(crate) async fn materialize_checkout_paths_with_mode_and_work<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    paths: &[NamespacePath],
    options: &MaterializeOptions,
    mode: MaterializeMode,
    work: &crate::NativeMaterializationWork,
) -> Result<MaterializationReceipt, MaterializeError> {
    let receipt = GenerationReceipt::from_work(work)?;
    let validated = reader.validate_native_work(receipt.custody.work());
    receipt.original_result(validated)?;
    validate_bounds(options).map_err(|error| GenerationFailure::new(error, receipt.work()))?;
    if paths.is_empty()
        || (paths.len() != 1 && paths.iter().any(|path| path.components().is_empty()))
    {
        return Err(MaterializeError::InvalidOptions);
    }
    materialize_checkout_source_on_receipt(
        GenerationSource::Original(reader), paths, options, mode, receipt,
    ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
}

#[allow(clippy::too_many_arguments, reason = "ordinary SDK boundary supplies no task authorization policy")]
async fn materialize_checkout_source_on_receipt<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    source: GenerationSource<'_, A, O>, paths: &[NamespacePath],
    options: &MaterializeOptions,
    mode: MaterializeMode, receipt: GenerationReceipt,
) -> Result<OperationReceipt<MaterializationReceipt>, GenerationFailure> {
    materialize_checkout_source_guarded_on_receipt(
        source, paths, options, mode, receipt,
        None::<&mut fn(NativeMaterializationActionPhase) -> std::future::Ready<Result<(), MaterializeError>>>,
    ).await.map_err(Into::into)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines, reason = "one canonical traversal dispatches ordinary or original-native source leaves")]
async fn materialize_checkout_source_guarded_on_receipt<A, O, E, F, Fut>(
    mut source: GenerationSource<'_, A, O>,
    paths: &[NamespacePath],
    options: &MaterializeOptions,
    mode: MaterializeMode,
    mut receipt: GenerationReceipt,
    mut before_effect: Option<&mut F>,
) -> Result<OperationReceipt<MaterializationReceipt>, (E, WorkCounters)>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    receipt.native_source = matches!(&source, GenerationSource::Original(_));
    let cancellation = receipt.cancellation.clone();
    let cancellation = &cancellation;
    let reader = match &source {
        GenerationSource::Standalone(checkout) => {
            let reader = checkout.pinned_reader_for_materialization_with_work(
                receipt.custody.work(), cancellation,
            );
            receipt.original_result(reader)?
        }
        GenerationSource::Original(reader) => (*reader).clone(),
    };
    let mut selected_paths = Vec::<&NamespacePath>::new();
    receipt.reserve(&mut selected_paths, paths.len())?;
    for path in paths {
        let mut insertion = selected_paths.len();
        while insertion != 0 {
            let previous = selected_paths[insertion - 1];
            receipt.run(|work| work.charge(WorkCounters {
                items_examined: 1, ..WorkCounters::UNCHARGED
            }))?;
            if previous <= path { break; }
            insertion -= 1;
        }
        receipt.run(|work| work.charge(WorkCounters {
            bytes_copied: ((selected_paths.len() - insertion + 1) * std::mem::size_of::<&NamespacePath>()) as u64,
            ..WorkCounters::UNCHARGED
        }))?;
        selected_paths.insert(insertion, path);
    }
    for pair in selected_paths.windows(2) {
        receipt.run(|work| work.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        }))?;
        if matches!(pair, [ancestor, path] if path.is_within(ancestor)) {
            return Err(GenerationFailure::new(MaterializeError::OverlappingPaths, receipt.work()).into());
        }
    }
    let selection_bytes = generation_vec_bytes(&selected_paths, &receipt)?;
    let destination = validate_options(options, &mut receipt)?;
    let host_root = receipt.owned_arc(destination, 0)?;
    let mut known_files = Vec::<(FileId, GenerationPath)>::new();
    #[cfg(any(target_os = "macos", windows))]
    let mut known_payloads = Vec::<((u64, ObjectId, ObjectId), GenerationPath)>::new();
    let mut pending = Vec::new();
    let mut deferred_directory_metadata = Vec::new();
    let limits = reader.volume_config().limits;
    for path in selected_paths {
        let host_path = GenerationHostPath::from_namespace(path, &mut receipt)?;
        if path.is_root() {
            let namespace = clone_namespace_measured(path, None, limits, &mut receipt)?;
            let namespace = share_generation_namespace(namespace, &mut receipt)?;
            receipt.push(&mut pending, (namespace.clone(), host_path.clone()))?;
            receipt.push(&mut deferred_directory_metadata, (namespace, host_path))?;
            continue;
        }
        let record = match &mut source {
            GenerationSource::Standalone(checkout) => {
                let remaining = receipt.run(|work| work.remaining())?;
                let lookup = checkout.lookup_no_follow(path, remaining, cancellation).await
                    .map_err(|failure| receipt.engine_failure(failure))?;
                receipt.run(|work| work.merge(lookup.work))?;
                lookup.value.record
            }
            GenerationSource::Original(reader) => {
                let lookup = reader.lookup_record_with_native_work(
                    path, receipt.custody.work(), cancellation,
                ).await;
                receipt.original_result(lookup)?
            }
        }.ok_or_else(|| GenerationFailure::new(MaterializeError::MissingPath, receipt.work()))?;
        generation_action(&mut before_effect, &receipt, NativeMaterializationActionPhase::Before).await?;
        receipt.run(|work| host_path.create_parents(&host_root, work))?;
        generation_action(&mut before_effect, &receipt, NativeMaterializationActionPhase::Completed).await?;
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
            options, cancellation, &mut known_files,
            #[cfg(any(target_os = "macos", windows))]
            &mut known_payloads,
            &mut pending, &mut receipt, mode, &mut before_effect,
        ).await?;
    }
    receipt.release(selection_bytes)?;
    while let Some((directory, host_directory)) = pending.pop() {
        let mut after: Option<LogicalName> = None;
        loop {
            let page = match &mut source {
                GenerationSource::Standalone(checkout) => {
                    let remaining = receipt.run(|work| work.remaining())?;
                    let page = checkout.list_directory_records(
                        &directory, after.as_ref(), options.maximum_directory_entries, remaining, cancellation,
                    ).await.map_err(|failure| receipt.engine_failure(failure))?;
                    receipt.run(|work| work.merge(page.work))?;
                    page.value
                }
                GenerationSource::Original(reader) => {
                    let page = reader.list_directory_records_with_native_work(
                        &directory, after.as_ref(), options.maximum_directory_entries,
                        receipt.custody.work(), cancellation,
                    ).await;
                    let (page, custody) = receipt.original_result(page)?;
                    receipt.accept_source(page, custody)?
                }
            };
            if page.has_more && page.entries.is_empty() {
                return Err(GenerationFailure::new(MaterializeError::Engine("directory pagination made no progress"), receipt.work()).into());
            }
            let entry_bytes = page.entries.capacity()
                .checked_mul(std::mem::size_of::<crate::DirectoryRecordEntry>())
                .ok_or_else(|| GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work()))?;
            if !receipt.native_source {
                receipt.adopt(entry_bytes)?;
                for entry in &page.entries {
                    receipt.adopt(entry.name.retained_bytes())?;
                }
            }
            for entry in page.entries {
                let child = clone_namespace_measured(&directory, Some(&entry.name), limits, &mut receipt)?;
                let child_host = host_directory.child(&entry.name, &mut receipt)?;
                let child_bytes = child.retained_bytes() as usize;
                let mut owned_child = Some(child);
                let selected_directory = if entry.record.kind == FileKind::Directory {
                    let child = share_generation_namespace(owned_child.take().ok_or_else(|| {
                        GenerationFailure::new(MaterializeError::InvalidOptions, receipt.work())
                    })?, &mut receipt)?;
                    receipt.push(&mut deferred_directory_metadata, (child.clone(), child_host.clone()))?;
                    Some(child)
                } else {
                    None
                };
                let child = selected_directory.as_deref().map(|owned| &**owned).or(owned_child.as_ref()).ok_or_else(|| {
                    GenerationFailure::new(MaterializeError::InvalidOptions, receipt.work())
                })?;
                materialize_entry(
                    &reader, &host_root, child, &child_host, selected_directory.clone(),
                    entry.record.file_id, entry.record.kind, entry.record.payload,
                    #[cfg(any(target_os = "macos", windows))]
                    entry.record.metadata,
                    options, cancellation, &mut known_files,
                    #[cfg(any(target_os = "macos", windows))]
                    &mut known_payloads,
                    &mut pending, &mut receipt, mode, &mut before_effect,
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
            if !page.has_more { break; }
        }
        if let Some(after) = after {
            let bytes = after.retained_bytes();
            drop(after);
            receipt.release(bytes)?;
        }
    }
    for (directory, host_directory) in deferred_directory_metadata.into_iter().rev() {
        apply_metadata(
            &reader, &host_root, &directory, &host_directory, cancellation, &mut receipt,
            &mut before_effect,
        ).await?;
    }
    let work = receipt.work();
    receipt.value.work = work;
    Ok(OperationReceipt { value: receipt.value, work })
}

/// Keeps all host-relative selections in one original traversal and hard-link map.
pub(crate) async fn materialize_checkout_host_paths_with_work_guarded<A, O, E, F, Fut>(
    reader: &PinnedReader<A, O>,
    relatives: &[String],
    options: &MaterializeOptions,
    work: &crate::NativeMaterializationWork,
    before_effect: &mut F,
) -> Result<MaterializationReceipt, E>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    let receipt = GenerationReceipt::from_work(work).map_err(|failure| E::from(failure.error))?;
    let validated = reader.validate_native_work(receipt.custody.work());
    receipt.original_result(validated).map_err(|failure| E::from(failure.error))?;
    materialize_heap_future_on_receipt(move |mut receipt| async move {
        validate_bounds(options).map_err(|error| GenerationFailure::new(error, receipt.work()))?;
        let mut paths = Vec::new();
        receipt.reserve(&mut paths, relatives.len())?;
        let vector_bytes = paths.capacity().checked_mul(std::mem::size_of::<NamespacePath>())
            .ok_or_else(|| GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work()))?;
        let mut paths = GenerationOwned { value: paths, _custody: receipt.take(vector_bytes)? };
        for relative in relatives {
            let mut path = generation_namespace_from_host(Path::new(relative), reader.volume_config(), &mut receipt)?;
            paths._custody.absorb(&mut path._custody)
                .map_err(|error| GenerationFailure::new(MaterializeError::NativeWork(error), receipt.work()))?;
            paths.value.push(path.value);
        }
                materialize_checkout_source_guarded_on_receipt(
            GenerationSource::Original(reader), &paths.value, options,
            MaterializeMode::DurableOutput, receipt, Some(before_effect),
        ).await
    }, receipt).await.map(|receipt| receipt.value).map_err(|failure| failure.0)
}

async fn materialize_host_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    relative: &Path,
    options: &MaterializeOptions,
    mut receipt: GenerationReceipt,
) -> Result<OperationReceipt<MaterializationReceipt>, GenerationFailure> {
    validate_bounds(options).map_err(|error| GenerationFailure::new(error, receipt.work()))?;
    let path = generation_namespace_from_host(relative, checkout.volume_config(), &mut receipt)?;
    materialize_checkout_source_on_receipt(
        GenerationSource::Standalone(checkout), std::slice::from_ref(&path.value), options, MaterializeMode::DurableOutput, receipt,
    ).await
}

fn generation_namespace_from_host(
    relative: &Path, config: crate::VolumeConfig, receipt: &mut GenerationReceipt,
) -> Result<GenerationOwned<NamespacePath>, GenerationFailure> {
    let limits = config.limits;
    let mut count = 0_usize;
    for component in relative.components() {
        receipt.run(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(GenerationFailure::new(MaterializeError::InvalidPath, receipt.work()));
        }
        count = count.checked_add(1).ok_or_else(|| {
            GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
        })?;
        if count > usize::from(limits.maximum_path_depth) {
            return Err(GenerationFailure::new(MaterializeError::InvalidPath, receipt.work()));
        }
    }
    if count == 0 {
        return Err(GenerationFailure::new(MaterializeError::InvalidPath, receipt.work()));
    }
    let mut names = Vec::new();
    receipt.reserve(&mut names, count)?;
    for component in relative.components() {
        receipt.run(|work| work.charge(WorkCounters {
            items_examined: 1, source_path_components: 1, ..WorkCounters::UNCHARGED
        }))?;
        let std::path::Component::Normal(name) = component else {
            return Err(GenerationFailure::new(MaterializeError::InvalidPath, receipt.work()));
        };
        let encoded = crate::native_name::host_name_bytes_with_native_work(
            name, config.profile, limits.maximum_component_bytes, receipt.custody.work(),
        );
        let ((encoding, bytes), custody) = receipt.custody_result(encoded)?;
        let bytes = receipt.accept_source(bytes, custody)?;
        let logical = LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
            .map_err(|_| GenerationFailure::new(MaterializeError::InvalidPath, receipt.work()))?;
        names.push(logical);
    }
    let path = NamespacePath::new(names, limits)
        .map_err(|_| GenerationFailure::new(MaterializeError::InvalidPath, receipt.work()))?;
    let retained = path.retained_bytes() as usize;
    Ok(GenerationOwned { value: path, _custody: receipt.take(retained)? })
}

/// Restores one authenticated host-relative path without touching siblings.
pub async fn restore_checkout_host_path<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    relative: &Path,
    replacement: HostPathReplacement,
    options: &MaterializeOptions,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<OperationReceipt<HostPathRestore>, OperationFailure<MaterializeError>> {
    if !budget.has_finite_allowances() {
        return Err(OperationFailure::before_work(MaterializeError::InvalidOptions));
    }
    let receipt = GenerationReceipt::new(budget, cancellation)?;
    restore_checkout_host_path_on_source(
        checkout, relative, replacement, options,
        &crate::NativeMaterializationWork::from_meter(receipt.meter),
    ).await.map_err(OperationFailure::from)
}

async fn restore_checkout_host_path_on_source<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    relative: &Path,
    replacement: HostPathReplacement,
    options: &MaterializeOptions,
    native_work: &crate::NativeMaterializationWork,
) -> Result<OperationReceipt<HostPathRestore>, GenerationFailure> {
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
    crate::obs::measured_inline_on(
        &span,
        crate::obs::in_span(&span, async move {
            let work = Arc::clone(native_work.native_read_meter());
            let admission = work.with_original_leaf(|original| {
                if original.budget.has_finite_allowances() {
                    Ok(())
                } else {
                    Err(GenerationFailure::new(MaterializeError::InvalidOptions, original.work))
                }
            });
            admission?;
            let result = async {
                validate_bounds(options)?;
                let allocation_work = native_work.clone();
                let cleanup_allocation = allocation_work.claim(std::mem::size_of::<MaterializeError>() as u64)
                    .map_err(materialize_native_error)?;
                let cleanup_slot = GenerationOwned {
                    value: Box::new(MaterializeError::InvalidOptions),
                    _custody: cleanup_allocation,
                };
                let (destination_root, relative) = {
                    let mut work = NativeSourceWork::new(&work);
                    validate_host_relative_measured(relative, &mut work)?;
                    (
                        restore_path(&options.destination, None, &allocation_work)?,
                        restore_path(relative, None, &allocation_work)?,
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
                let GenerationOwned { value: stage_path, _custody: stage_allocation } = stage_path;
                let materialize_options = MaterializeOptions {
                    destination: stage_path,
                    expected_root_identity: Some(stage_root.identity()),
                    maximum_directory_entries: options.maximum_directory_entries,
                    maximum_extent_spans: options.maximum_extent_spans,
                    transfer_bytes: options.transfer_bytes,
                };
                let materialized = match GenerationReceipt::from_work(
                    &crate::NativeMaterializationWork::from_meter(Arc::clone(&work)),
                ) {
                    Err(failure) => Err(failure.error),
                    Ok(receipt) => {
                        materialize_host_path(
                            checkout, &relative, &materialize_options, receipt,
                        ).await.map(|_| ()).map_err(|failure| failure.error)
                    }
                };
                let stage_path = GenerationOwned {
                    value: materialize_options.destination, _custody: stage_allocation,
                };
                restore_io(worker_context, Some(lock), Arc::clone(&work), move |work| {
                    let mut cleanup_slot = cleanup_slot;
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
                    let cleanup = stage_owner.remove_identity(&stage_name, stage_identity, work)
                        .and_then(|()| stage_owner.sync(work));
                    match (result, cleanup) {
                        (Ok(value), Ok(())) => Ok(value),
                        (Ok(_), Err(error)) => Err(restore_demand_error(error)),
                        (Err(error), Ok(())) => Err(error),
                        (Err(primary), Err(cleanup)) => {
                            *cleanup_slot.value = primary;
                            Err(MaterializeError::RestoreCleanup {
                                primary: cleanup_slot.value, cleanup, allocation: cleanup_slot._custody,
                            })
                        }
                    }
                }).await
            }.await;
            let spent = work.with_original_leaf(|original| original.work);
            result.map(|value| OperationReceipt { value, work: spent })
                .map_err(|error| GenerationFailure::new(error, spent))
        }).await.map_err(|failure| (failure.error, failure.work)),
        |receipt| &receipt.work,
    ).map_err(|(error, work)| GenerationFailure::new(error, work))
}

async fn restore_io<T: Send + 'static>(
    context: crate::obs::OperationSpan,
    lock: Option<File>,
    work: Arc<NativeDemandReadMeter>,
    operation: impl FnOnce(&mut NativeSourceWork<'_>) -> Result<T, MaterializeError> + Send + 'static,
) -> Result<T, MaterializeError> {
    let observer: Arc<dyn acyclic_native_runtime::NativeReadMeter> = work.clone();
    let operation = move || {
        // A cancelled waiter must not release the fence while host I/O continues.
        let _restore_lock = lock;
        operation(&mut NativeSourceWork::new(&work))
    };
    let task = acyclic_native_runtime::run_blocking_io(
        crate::obs::in_context(context, operation), Some((observer, false)),
    ).map_err(|error| materialize_native_error(crate::NativeTreeMaterializationError::from_runtime_error(error)))?;
    task.await.map_err(|error| {
        materialize_native_error(crate::NativeTreeMaterializationError::from_runtime_error(error))
    })?
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
    destination_parent: HostRoot,
    stage_owner: HostRoot,
    stage_root: HostRoot,
    stage_name: GenerationOwned<PathBuf>,
    stage_path: GenerationOwned<PathBuf>,
    destination_root: GenerationOwned<PathBuf>,
    relative: GenerationOwned<PathBuf>,
    lock: File,
}

fn restore_path(
    parent: &Path, name: Option<&Path>, work: &crate::NativeMaterializationWork,
) -> Result<GenerationOwned<PathBuf>, MaterializeError> {
    let (path, allocation) = work.path(parent, name).map_err(materialize_native_error)?;
    Ok(GenerationOwned { value: path, _custody: allocation })
}

fn prepare_restore(
    destination_root: GenerationOwned<PathBuf>,
    relative: GenerationOwned<PathBuf>,
    expected_root_identity: Option<crate::NativeRootIdentity>,
    replacement: HostPathReplacement,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<PreparedRestore, MaterializeError> {
    let allocation_work = destination_root._custody.work().clone();
    let root = HostRoot::open(&destination_root, work).map_err(restore_demand_error)?;
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
        HostPathReplacement::Atomic => restore_path(
            destination_root.parent().ok_or(MaterializeError::InvalidPath)?, None, &allocation_work,
        )?,
        HostPathReplacement::LiveMount => restore_path(
            &destination_root, Some(relative.parent().unwrap_or_else(|| Path::new(""))), &allocation_work,
        )?,
    };
    let stage_parent_root = match replacement {
        HostPathReplacement::Atomic => HostRoot::open(&stage_parent_path, work)
            .map_err(restore_demand_error)?,
        HostPathReplacement::LiveMount => destination_parent.open_child(Path::new(""), work)
            .map_err(restore_demand_error)?,
    };
    let stage_owner = stage_parent_root.create_dir_all_held_measured(Path::new(""), work)
        .map_err(restore_demand_error)?;
    let (stage_name, stage_root) = create_restore_stage(&stage_owner, &allocation_work, work)?;
    let stage_path = match restore_path(&stage_parent_path, Some(&stage_name), &allocation_work) {
        Ok(path) => path,
        Err(error) => {
            let identity = stage_root.identity();
            drop(stage_root);
            let _ = stage_owner.remove_identity(&stage_name, identity, work)
                .and_then(|()| stage_owner.sync(work));
            return Err(error);
        }
    };
    drop(stage_parent_path);
    Ok(PreparedRestore {
        root, destination_parent, stage_owner, stage_root, stage_name, stage_path,
        destination_root, relative, lock,
    })
}

fn publish_restore(
    destination_parent: &HostRoot,
    stage_root: &HostRoot,
    relative: &Path,
    replacement: HostPathReplacement,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    let destination_name = relative.file_name().ok_or(MaterializeError::InvalidPath)?;
    let stage_parent = held_parent(stage_root, relative, work)?;
    let name = Path::new(destination_name);
    match destination_parent.stat(name, work) {
        Ok(_) => match replacement {
            HostPathReplacement::Atomic => destination_parent
                .exchange_to_measured(name, &stage_parent, name, work).map_err(restore_demand_error)?,
            HostPathReplacement::LiveMount => replace_live_mount(
                &stage_parent, name, destination_parent, name, relative, work,
            )?,
        },
        Err(crate::demand::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            stage_parent.rename_to(name, destination_parent, name, work).map_err(restore_demand_error)?;
            sync_restore_parent(destination_parent, work)?;
            sync_restore_parent(&stage_parent, work)?;
        }
        Err(error) => return Err(restore_demand_error(error)),
    }
    Ok(())
}

fn validate_host_relative_measured(relative: &Path, work: &mut impl crate::demand::SourceDemandReadWork) -> Result<(), MaterializeError> {
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

fn held_parent(root: &HostRoot, relative: &Path, work: &mut impl crate::demand::SourceDemandReadWork) -> Result<HostRoot, MaterializeError> {
    root.create_dir_all_held_measured(relative.parent().unwrap_or_else(|| Path::new("")), work)
        .map_err(restore_demand_error)
}

fn create_restore_stage(parent: &HostRoot, allocation_work: &crate::NativeMaterializationWork, work: &mut impl crate::demand::SourceDemandReadWork) -> Result<(GenerationOwned<PathBuf>, HostRoot), MaterializeError> {
    use std::fmt::Write as _;
    for _ in 0..16 {
        let sequence = RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let process = std::process::id();
        let digits = |value: u64| if value == 0 { 1 } else { value.ilog10() as usize + 1 };
        let bytes = ".acyclic-restore-".len() + digits(u64::from(process)) + 1 + digits(sequence);
        let delta = WorkCounters { bytes_encoded: bytes as u64, ..WorkCounters::UNCHARGED };
        work.admit(delta).map_err(restore_demand_error)?;
        let allocation = allocation_work.claim(bytes as u64).map_err(materialize_native_error)?;
        let mut name = String::with_capacity(bytes);
        write!(name, ".acyclic-restore-{process}-{sequence}").map_err(|_| MaterializeError::InvalidPath)?;
        work.record(delta).map_err(restore_demand_error)?;
        let name = GenerationOwned { value: PathBuf::from(name), _custody: allocation };
        match parent.create_dir(&name, work) {
            Ok(()) => {
                let root = match parent.open_child(&name, work) {
                    Ok(root) => root,
                    // Without the stage capability no identity authorizes
                    // deleting this name, which may already have been swapped.
                    Err(error) => return Err(restore_demand_error(error)),
                };
                if let Err(error) = sync_restore_parent(parent, work) {
                    let identity = root.identity();
                    drop(root);
                    let _ = parent.remove_identity(&name, identity, work)
                        .and_then(|()| parent.sync(work));
                    return Err(error);
                }
                return Ok((name, root));
            }
            Err(crate::demand::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                drop(name);
            }
            Err(error) => return Err(restore_demand_error(error)),
        }
    }
    Err(MaterializeError::Io(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists, "restore stage names are exhausted",
    )))
}

fn restore_artifact_name(prefix: &str, relative: &Path, work: &mut impl crate::demand::SourceDemandReadWork) -> Result<PathBuf, MaterializeError> {
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
    work.admit(delta).map_err(restore_demand_error)?;
    work.allocate(bytes as u64).map_err(restore_demand_error)?;
    let mut name = String::with_capacity(bytes);
    if write!(name, "{prefix}{}", hasher.finalize().to_hex()).is_err() {
        drop(name);
        work.release(bytes as u64).map_err(restore_demand_error)?;
        return Err(MaterializeError::InvalidPath);
    }
    if let Err(error) = work.record(delta) {
        drop(name);
        work.release(bytes as u64).map_err(restore_demand_error)?;
        return Err(restore_demand_error(error));
    }
    Ok(PathBuf::from(name))
}

fn with_restore_artifact<T, W: crate::demand::SourceDemandReadWork>(
    prefix: &str,
    relative: &Path,
    work: &mut W,
    operation: impl FnOnce(&Path, &mut W) -> Result<T, MaterializeError>,
) -> Result<T, MaterializeError> {
    let name = restore_artifact_name(prefix, relative, work)?;
    let bytes = name.as_os_str().as_encoded_bytes().len() as u64;
    let result = operation(&name, work);
    drop(name);
    work.release(bytes).map_err(restore_demand_error)?;
    result
}

fn acquire_restore_lock(
    parent: &HostRoot,
    relative: &Path,
    blocking: bool,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<File, MaterializeError> {
    with_restore_artifact(".acyclic-restore-lock-", relative, work, |name, work| {
        let file = parent.open_file_with(
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

fn sync_restore_parent(parent: &HostRoot, work: &mut impl crate::demand::SourceDemandReadWork) -> Result<(), MaterializeError> {
    parent.sync(work).map_err(restore_demand_error)
}

const REMOVE_RESTORE_WITNESS: &[u8] = b"acyclic-remove-restore-v1\n";
const LIVE_RESTORE_WITNESS: &[u8] = b"acyclic-live-restore-v1\n";

fn create_restore_witness(parent: &HostRoot, name: &Path, contents: &[u8], work: &mut impl crate::demand::SourceDemandReadWork) -> Result<(), MaterializeError> {
    use std::io::Write as _;
    let mut file = parent.open_file_with(
        name, &crate::native_host::NativeOpenOptions::CreateNew, work,
    ).map_err(restore_demand_error)?;
    let mut remaining = contents;
    while !remaining.is_empty() {
        work.admit(WorkCounters { output_bytes: remaining.len() as u64, backend_write_operations: 1, ..WorkCounters::UNCHARGED })
            .map_err(restore_demand_error)?;
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

fn validate_restore_witness(parent: &HostRoot, name: &Path, expected: &[u8], work: &mut impl crate::demand::SourceDemandReadWork) -> Result<bool, MaterializeError> {
    use std::io::Read as _;
    let mut file = match parent.open_file_with(
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
        work.admit(WorkCounters { source_bytes_read: (bound - read) as u64, backend_read_operations: 1, ..WorkCounters::UNCHARGED })
            .map_err(restore_demand_error)?;
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

fn remove_restore_witness(parent: &HostRoot, name: &Path, work: &mut impl crate::demand::SourceDemandReadWork) -> Result<(), MaterializeError> {
    parent.remove(name, work).map_err(restore_demand_error)?;
    sync_restore_parent(parent, work)
}

fn cleanup_removed_restore(
    parent: &HostRoot,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    with_restore_artifact(".acyclic-restore-removed-", relative, work, |removed, work| {
        with_restore_artifact(".acyclic-restore-remove-witness-", relative, work, |witness, work| {
            match parent.stat(removed, work) {
                Ok(_) => {
                    if !validate_restore_witness(parent, witness, REMOVE_RESTORE_WITNESS, work)? {
                        return Err(MaterializeError::Engine("unowned restore-removal artifact collision"));
                    }
                    parent.remove(removed, work).map_err(restore_demand_error)?;
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
    parent: &HostRoot,
    destination_name: &Path,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    with_restore_artifact(".acyclic-restore-removed-", relative, work, |removed, work| {
        with_restore_artifact(".acyclic-restore-remove-witness-", relative, work, |witness, work| {
            create_restore_witness(parent, witness, REMOVE_RESTORE_WITNESS, work)?;
            match parent.stat(destination_name, work) {
                Ok(_) => {
                    parent.rename_to(destination_name, parent, removed, work).map_err(restore_demand_error)?;
                    sync_restore_parent(parent, work)?;
                    parent.remove(removed, work).map_err(restore_demand_error)?;
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
    staged_parent: &HostRoot,
    staged_name: &Path,
    parent: &HostRoot,
    destination_name: &Path,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    recover_live_mount_replacement(parent, destination_name, relative, work)?;
    with_restore_artifact(".acyclic-restore-backup-", relative, work, |backup, work| {
        with_restore_artifact(".acyclic-restore-live-witness-", relative, work, |witness, work| {
            create_restore_witness(parent, witness, LIVE_RESTORE_WITNESS, work)?;
            parent.rename_to(destination_name, parent, backup, work).map_err(restore_demand_error)?;
            sync_restore_parent(parent, work)?;
            if let Err(error) = staged_parent.rename_to(staged_name, parent, destination_name, work) {
                if let Err(rollback) = parent.rename_to(backup, parent, destination_name, work) {
                    return Err(MaterializeError::RestoreRollback { primary: error, rollback });
                }
                sync_restore_parent(parent, work)?;
                return Err(restore_demand_error(error));
            }
            sync_restore_parent(parent, work)?;
            sync_restore_parent(staged_parent, work)?;
            parent.remove(backup, work).map_err(restore_demand_error)?;
            sync_restore_parent(parent, work)?;
            remove_restore_witness(parent, witness, work)
        })
    })
}

fn recover_live_mount_replacement(
    parent: &HostRoot,
    destination_name: &Path,
    relative: &Path,
    work: &mut impl crate::demand::SourceDemandReadWork,
) -> Result<(), MaterializeError> {
    with_restore_artifact(".acyclic-restore-backup-", relative, work, |backup, work| {
        with_restore_artifact(".acyclic-restore-live-witness-", relative, work, |witness, work| {
            let backup_exists = match parent.stat(backup, work) {
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
                return Err(MaterializeError::Engine("unowned live-restore backup collision"));
            }
            match parent.stat(destination_name, work) {
                Ok(_) => parent.remove(backup, work).map_err(restore_demand_error)?,
                Err(crate::demand::DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    parent.rename_to(backup, parent, destination_name, work).map_err(restore_demand_error)?;
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
async fn materialize_entry<A, O, E, F, Fut>(
    reader: &PinnedReader<A, O>,
    host_root: &GenerationRoot,
    path: &NamespacePath,
    host_path: &GenerationPath,
    directory: Option<GenerationNamespace>,
    file_id: FileId,
    kind: FileKind,
    payload: FilePayload,
    #[cfg(any(target_os = "macos", windows))] metadata_id: ObjectId,
    options: &MaterializeOptions,
    cancellation: &CancellationToken,
    known_files: &mut Vec<(FileId, GenerationPath)>,
    #[cfg(any(target_os = "macos", windows))]
    known_payloads: &mut Vec<((u64, ObjectId, ObjectId), GenerationPath)>,
    pending: &mut Vec<(GenerationNamespace, GenerationPath)>,
    receipt: &mut GenerationReceipt,
    mode: MaterializeMode,
    before_effect: &mut Option<&mut F>,
) -> Result<(), (E, WorkCounters)>
where
    A: AsyncAuthorityStore, O: AsyncObjectStore, E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut, Fut: Future<Output = Result<(), E>>,
{
    receipt.run(|work| {
        work.with_original_leaf(|work| {
            work.check()?;
            work.work.admit(&WorkCounters { materializations: 1, ..WorkCounters::UNCHARGED }, &work.budget)?;
            Ok(())
        })
    })?;
    for (identity, existing) in known_files.iter() {
        receipt.run(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
        if *identity == file_id {
            generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
            receipt.run(|work| host_path.hard_link(host_root, existing, work))?;
            generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
            receipt.value.files = receipt.value.files.checked_add(1).ok_or_else(|| {
                GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
            })?;
            account_materialization(receipt, 0)?;
            receipt.run(|work| work.check())?;
            return Ok(());
        }
    }
    let mut authenticated_metadata = None;
    match (kind, payload) {
        (FileKind::Directory, FilePayload::Directory { .. }) => {
            generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
            receipt.run(|work| host_path.create_dir(host_root, work))?;
            generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
            let directory = directory.ok_or_else(|| {
                GenerationFailure::new(MaterializeError::InvalidOptions, receipt.work())
            })?;
            receipt.value.directories = receipt.value.directories.checked_add(1).ok_or_else(|| {
                GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
            })?;
            account_materialization(receipt, 0)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
            receipt.push(pending, (directory, host_path.clone()))?;
            receipt.run(|work| work.check())?;
            return Ok(());
        }
        (FileKind::Regular, FilePayload::InlineRegular(data)) => {
            generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
            let file = create_file(host_root, host_path, receipt)?;
            generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
            let length = data.as_bytes().len();
            let original = receipt.custody.work().clone();
            let (bytes, allocation) = receipt.custody_result(original.copy_bytes(data.as_bytes()))?;
            let bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
                bytes, allocation, original.runtime_meter_owner(),
            ).map_err(|failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => original.runtime_error(error),
                acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
                => crate::NativeTreeMaterializationError::InvalidLayout,
            });
            let bytes = receipt.custody_result(bytes)?;
            let written = write_generation_bytes(&file, 0, bytes, 0, receipt, before_effect).await?;
            if mode == MaterializeMode::DurableOutput {
                sync_generation_file(&file, receipt, before_effect).await?;
            }
            drop(file);
            account_file(receipt, length as u64, written)?;
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
                receipt.run(|work| work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }))?;
                if *key == (logical_bytes, extents, metadata_id) {
                    payload_known = true;
                    generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
                    cloned = receipt.run(|work| host_path.clone_file(host_root, source, work))?;
                    generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
                    break;
                }
            }
            #[cfg(not(any(target_os = "macos", windows)))]
            let cloned = false;
            if !cloned {
                generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
                let mut file = create_file(host_root, host_path, receipt)?;
                generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
                #[cfg(windows)]
                {
                    generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
                    let meter = receipt.native_meter();
                    let control = receipt.runtime_result(file.control_async(|file, meter| mark_sparse_measured(file, meter.expect("metered control")), Some(meter)))?;
                    receipt.runtime_result(control.await)?;
                    generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
                }
                generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
                let meter = receipt.native_meter();
                let resize = receipt.runtime_result(file.set_len_async(logical_bytes, Some(meter)))?;
                receipt.runtime_result(resize.await)?;
                generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
                authenticated_metadata = Some(materialize_sparse_file(
                    reader, path, &mut file, logical_bytes, options, cancellation, receipt, before_effect,
                ).await?);
                if mode == MaterializeMode::DurableOutput {
                    sync_generation_file(&file, receipt, before_effect).await?;
                }
                drop(file);
            }
            #[cfg(any(target_os = "macos", windows))]
            if cloned {
                if mode == MaterializeMode::DurableOutput {
                    #[cfg(target_os = "macos")]
                    let file = receipt.run(|work| host_root.open_file(host_path.path(), work))?.into_std();
                    #[cfg(windows)]
                    let file = receipt.run(|work| host_root.open_file_positional_wide_with_work(&host_path.units, work))?.into_std();
                    let meter = receipt.native_meter();
                    let file = receipt.runtime_result(
                        acyclic_native_runtime::NativeFile::from_file(file, Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)))
                            .map_err(|(error, file)| { drop(file); error }),
                    )?;
                    sync_generation_file(&file, receipt, before_effect).await?;
                    drop(file);
                }
            }
            account_file(receipt, logical_bytes, 0)?;
            #[cfg(any(target_os = "macos", windows))]
            if !payload_known {
                receipt.push(known_payloads, ((logical_bytes, extents, metadata_id), host_path.clone()))?;
            }
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (FileKind::SymbolicLink, FilePayload::SymbolicLink { target_bytes, .. }) => {
            let target = read_source!(receipt, remaining, value,
                reader.read_symbolic_link_with_native_work(path, receipt.custody.work(), cancellation) => value,
                reader.read_symbolic_link(path, remaining, cancellation) => {
                    receipt.adopt(value.len())?;
                    value
                },
            );
            if target.len() as u64 != target_bytes {
                return Err(GenerationFailure::new(MaterializeError::Engine("symbolic-link length mismatch"), receipt.work()).into());
            }
            generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
            create_symlink_measured(host_root, &target, host_path, receipt)?;
            generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
            drop(target);
            if !receipt.native_source { receipt.release(target_bytes as usize)?; }
            receipt.value.symbolic_links = receipt.value.symbolic_links.checked_add(1).ok_or_else(|| {
                GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
            })?;
            account_materialization(receipt, 0)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (kind @ (FileKind::Fifo | FileKind::Socket), FilePayload::Empty) => {
            generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
            create_special_measured(host_root, host_path, kind, None, receipt)?;
            generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
            receipt.value.special_files = receipt.value.special_files.checked_add(1).ok_or_else(|| {
                GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
            })?;
            account_materialization(receipt, 0)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (kind @ (FileKind::CharacterDevice | FileKind::BlockDevice), FilePayload::Device { major, minor }) => {
            generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
            create_special_measured(host_root, host_path, kind, Some((major, minor)), receipt)?;
            generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
            receipt.value.special_files = receipt.value.special_files.checked_add(1).ok_or_else(|| {
                GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
            })?;
            account_materialization(receipt, 0)?;
            receipt.push(known_files, (file_id, host_path.clone()))?;
        }
        (unsupported, _) => return Err(GenerationFailure::new(MaterializeError::UnsupportedKind(unsupported), receipt.work()).into()),
    }
    if let Some(metadata) = authenticated_metadata {
        apply_record_metadata(reader, host_root, host_path, metadata, cancellation, receipt, before_effect).await?;
    } else {
        apply_metadata(reader, host_root, path, host_path, cancellation, receipt, before_effect).await?;
    }
    receipt.run(|work| work.check())?;
    Ok(())
}

async fn write_generation_bytes<E, F, Fut>(
    file: &acyclic_native_runtime::NativeFile,
    offset: u64,
    bytes: acyclic_stream::OwnedBytes,
    owned_bytes: usize,
    receipt: &mut GenerationReceipt,
    before_effect: &mut Option<&mut F>,
) -> Result<u64, (E, WorkCounters)>
where
    E: From<MaterializeError>, F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
    let length = bytes.len() as u64;
    let custody = receipt.take(owned_bytes)?;
    let meter = receipt.native_meter();
    let write = receipt.runtime_result(file.write_async(offset, bytes, meter, custody))?;
    receipt.runtime_result(write.await)?;
    generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
    Ok(length)
}

async fn sync_generation_file<E, F, Fut>(
    file: &acyclic_native_runtime::NativeFile,
    receipt: &mut GenerationReceipt,
    before_effect: &mut Option<&mut F>,
) -> Result<(), (E, WorkCounters)>
where
    E: From<MaterializeError>, F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
    let meter = receipt.native_meter();
    let sync = receipt.runtime_result(file.sync_async(acyclic_native_runtime::Durability::Full, Some(meter)))?;
    receipt.runtime_result(sync.await)?;
    generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await
}

#[cfg(windows)]
fn mark_sparse_measured(file: &File, meter: &dyn acyclic_native_runtime::NativeReadMeter) -> std::io::Result<()> {
    let mut output = [];
    acyclic_native_runtime::device_control_in_place(file, windows::Win32::System::Ioctl::FSCTL_SET_SPARSE, &[], &mut output, Some((meter, acyclic_native_runtime::NativeOperationKind::Write)))?;
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
async fn materialize_sparse_file<A, O, E, F, Fut>(
    reader: &PinnedReader<A, O>,
    path: &NamespacePath,
    file: &mut acyclic_native_runtime::NativeFile,
    logical_bytes: u64,
    options: &MaterializeOptions,
    cancellation: &CancellationToken,
    receipt: &mut GenerationReceipt,
    before_effect: &mut Option<&mut F>,
) -> Result<FileMetadata, (E, WorkCounters)>
where
    A: AsyncAuthorityStore, O: AsyncObjectStore, E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut, Fut: Future<Output = Result<(), E>>,
{
    let resolved = read_source!(receipt, remaining, value,
        reader.resolve_file_ranges_with_native_work(std::slice::from_ref(path), receipt.custody.work(), cancellation)
            => receipt.accept_source(value.0, value.1)?,
        reader.resolve_files(std::slice::from_ref(path), remaining, cancellation) => {
            receipt.adopt(generation_vec_bytes(&value, receipt)?)?;
            value
        },
    );
    let resolved_bytes = generation_vec_bytes(&resolved, receipt)?;
    let resolved = resolved.into_iter().next().flatten().ok_or_else(|| {
        GenerationFailure::new(MaterializeError::MissingPath, receipt.work())
    })?;
    receipt.release(resolved_bytes)?;
    let metadata = resolved.description().metadata;
    let mut offset = 0_u64;
    while offset < logical_bytes {
        let length = options.transfer_bytes.min(logical_bytes - offset);
        let plan = read_source!(receipt, remaining, value,
            resolved.plan_extents_with_native_work(
                ByteRange { offset, length }, options.maximum_extent_spans, receipt.custody.work(), cancellation,
            ) => {
                let (plan, custody) = value.ok_or_else(|| GenerationFailure::new(MaterializeError::InvalidPath, receipt.work()))?;
                receipt.accept_source(plan, custody)?
            },
            resolved.plan_extents(ByteRange { offset, length }, options.maximum_extent_spans, remaining, cancellation) => {
                let plan = value.ok_or_else(|| GenerationFailure::new(MaterializeError::InvalidPath, receipt.work()))?;
                receipt.adopt(generation_vec_bytes(&plan.spans, receipt)?)?;
                plan
            },
        );
        let plan_bytes = generation_vec_bytes(&plan.spans, receipt)?;
        let mut writes = Vec::new();
        let mut reads = Vec::new();
        for span in plan.spans {
            match span.kind {
                ExtentKind::Hole => {
                    #[cfg(target_os = "macos")]
                    {
                        generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
                        let meter = receipt.native_meter();
                        let control = receipt.runtime_result(file.control_async(move |handle, meter| { let meter = meter.expect("metered control");
                            crate::native_host::punch_hole_measured(handle, span.offset, span.length, meter)
                        }, Some(meter)))?;
                        receipt.runtime_result(control.await)?;
                        generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
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
            read_content_spans(reader, reads, options.maximum_extent_spans, cancellation, receipt, &mut writes).await?;
        }
        let write_bytes = generation_vec_bytes(&writes, receipt)?;
        for write in writes {
            let written = write_generation_bytes(file, write.offset, write.bytes, write.owned_bytes, receipt, before_effect).await?;
            account_written(receipt, written)?;
        }
        receipt.release(write_bytes)?;
        offset = offset.checked_add(length).ok_or_else(|| {
            GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
        })?;
    }
    Ok(metadata)
}

async fn read_content_spans<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    reader: &PinnedReader<A, O>,
    reads: Vec<ResolvedFileRangeReadRequest<'_, A, O>>,
    maximum_extent_spans: u32,
    cancellation: &CancellationToken,
    receipt: &mut GenerationReceipt,
    writes: &mut Vec<GenerationWrite>,
) -> Result<(), GenerationFailure> {
    let request_bytes = generation_vec_bytes(&reads, receipt)?;
    let read = read_source!(receipt, remaining, value,
        reader.read_resolved_ranges_with_native_work(
            &reads, usize::try_from(maximum_extent_spans).unwrap_or(usize::MAX), receipt.custody.work(), cancellation,
        ) => receipt.accept_source(value.0, value.1)?,
        reader.read_resolved_ranges(
            &reads, usize::try_from(maximum_extent_spans).unwrap_or(usize::MAX), remaining, cancellation,
        ) => {
            receipt.adopt(generation_vec_bytes(&value, receipt)?)?;
            for result in &value { receipt.adopt(result.bytes.len())?; }
            value
        },
    );
    let result_bytes = generation_vec_bytes(&read, receipt)?;
    if reads.len() != read.len() {
        return Err(GenerationFailure::new(MaterializeError::Engine("range batch returned a different number of results"), receipt.work()));
    }
    for (request, result) in reads.into_iter().zip(read) {
        let owned_bytes = if receipt.native_source { 0 } else { result.bytes.len() };
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
) -> Result<(), GenerationFailure> {
    static ZERO_BYTES: [u8; 65_536] = [0; 65_536];
    let chunk_bytes = transfer_bytes.min(ZERO_BYTES.len() as u64);
    if chunk_bytes == 0 {
        return Err(GenerationFailure::new(MaterializeError::InvalidOptions, receipt.work()));
    }
    let mut remaining = length;
    let mut position = offset;
    while remaining != 0 {
        let count = remaining.min(chunk_bytes) as usize;
        receipt.push(writes, GenerationWrite {
            offset: position, bytes: acyclic_stream::OwnedBytes::from_static(&ZERO_BYTES[..count]), owned_bytes: 0,
        })?;
        position = position.checked_add(count as u64).ok_or_else(|| {
            GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
        })?;
        remaining -= count as u64;
    }
    Ok(())
}

struct GenerationWrite {
    offset: u64,
    bytes: acyclic_stream::OwnedBytes,
    owned_bytes: usize,
}

fn generation_vec_bytes<T>(
    values: &Vec<T>,
    receipt: &GenerationReceipt,
) -> Result<usize, GenerationFailure> {
    values.capacity().checked_mul(std::mem::size_of::<T>()).ok_or_else(|| {
        GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
    })
}

async fn apply_metadata<A, O, E, F, Fut>(
    reader: &PinnedReader<A, O>,
    host_root: &GenerationRoot,
    path: &NamespacePath,
    host_path: &GenerationPath,
    cancellation: &CancellationToken,
    receipt: &mut GenerationReceipt,
    before_effect: &mut Option<&mut F>,
) -> Result<(), (E, WorkCounters)>
where
    A: AsyncAuthorityStore, O: AsyncObjectStore, E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut, Fut: Future<Output = Result<(), E>>,
{
    let metadata = read_source!(receipt, remaining, value,
        reader.read_metadata_with_native_work(path, receipt.custody.work(), cancellation) => value,
        reader.read_metadata(path, remaining, cancellation) => value,
    );
    apply_record_metadata(
        reader,
        host_root,
        host_path,
        metadata,
        cancellation,
        receipt,
        before_effect,
    )
    .await
}

/// Applies one record's complete metadata to its materialized host path: its named attributes, then the directly settable fields.
async fn apply_record_metadata<A, O, E, F, Fut>(
    reader: &PinnedReader<A, O>,
    host_root: &GenerationRoot,
    host_path: &GenerationPath,
    metadata: FileMetadata,
    cancellation: &CancellationToken,
    receipt: &mut GenerationReceipt,
    before_effect: &mut Option<&mut F>,
) -> Result<(), (E, WorkCounters)>
where
    A: AsyncAuthorityStore, O: AsyncObjectStore, E: From<MaterializeError>,
    F: FnMut(NativeMaterializationActionPhase) -> Fut, Fut: Future<Output = Result<(), E>>,
{
    let (metadata, named_attributes) = host_metadata(metadata);
    if let Some(root) = named_attributes {
        let read = read_source!(receipt, remaining, value,
            reader.read_named_attributes_with_native_work(root, receipt.custody.work(), cancellation)
                => receipt.accept_source(value.0, value.1)?,
            reader.read_named_attributes(root, remaining, cancellation) => {
                receipt.adopt(generation_vec_bytes(&value, receipt)?)?;
                for (name, bytes) in &value {
                    receipt.adopt(name.retained_bytes())?;
                    receipt.adopt(bytes.len())?;
                }
                value
            },
        );
        let source_array_bytes = generation_vec_bytes(&read, receipt)?;
        let mut attributes = Vec::new();
        let mut payload_bytes = 0_usize;
        for (name, value) in read {
            if name.class() != crate::kernel::AttributeClass::PosixXattr {
                return Err(GenerationFailure::new(MaterializeError::UnsupportedMetadata("named_attributes"), receipt.work()).into());
            }
            let name = name.bytes;
            let value_bytes = if receipt.native_source { 0 } else { value.len() };
            payload_bytes = payload_bytes.checked_add(name.capacity()).and_then(|bytes| bytes.checked_add(value_bytes))
                .ok_or_else(|| GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work()))?;
            receipt.push(&mut attributes, (name, value))?;
        }
        receipt.release(source_array_bytes)?;
        let host_array_bytes = generation_vec_bytes(&attributes, receipt)?;
        let retained = host_array_bytes.checked_add(payload_bytes)
            .ok_or_else(|| GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work()))?;
        generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
        apply_host_attributes_offloaded(host_root, host_path, attributes, retained, receipt).await?;
        generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await?;
    }
    generation_action(before_effect, receipt, NativeMaterializationActionPhase::Before).await?;
    apply_host_metadata_offloaded(host_root, host_path, metadata, receipt).await?;
    generation_action(before_effect, receipt, NativeMaterializationActionPhase::Completed).await
}

async fn generation_blocking<T: Send + 'static>(
    receipt: &mut GenerationReceipt,
    operation: impl FnOnce(&mut NativeSourceWork<'_>) -> Result<T, DemandError> + Send + 'static,
) -> Result<T, GenerationFailure> {
    let observer = receipt.native_meter();
    let original = receipt.custody.work().clone();
    let task = receipt.runtime_result(acyclic_native_runtime::run_blocking_io(
        move || {
            operation(&mut NativeSourceWork::from_native_materialization_work(&original))
        },
        Some((observer, false)),
    ))?;
    let result = receipt.runtime_result(task.await)?;
    result.map_err(|error| GenerationFailure::new(materialize_demand_error(error), receipt.work()))
}

async fn apply_host_attributes_offloaded(
    host_root: &GenerationRoot,
    host_path: &GenerationPath,
    attributes: Vec<(Vec<u8>, acyclic_stream::OwnedBytes)>,
    retained: usize,
    receipt: &mut GenerationReceipt,
) -> Result<(), GenerationFailure> {
    let custody = receipt.take(retained)?;
    let attributes = GenerationOwned { value: attributes, _custody: custody };
    if attributes.is_empty() { return Ok(()); }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let root = host_root.clone();
        let path = host_path.clone();
        generation_blocking(receipt, move |work| {
            #[cfg(target_os = "linux")]
            let target = root.open_linux_metadata_target(path.path(), work)?;
            #[cfg(target_os = "macos")]
            let target = root.open_macos_metadata_target(path.path(), work)?;
            let result = target.set_extended_attributes(&attributes, work);
            #[cfg(target_os = "macos")]
            {
                let target_bytes = target.logical_owned_bytes();
                drop(target);
                crate::native_host::NativeHostWork::release(work, target_bytes)?;
            }
            result
        }).await
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (host_root, host_path);
        Err(GenerationFailure::new(MaterializeError::UnsupportedMetadata("named_attributes"), receipt.work()))
    }
}

async fn apply_host_metadata_offloaded(
    host_root: &GenerationRoot,
    host_path: &GenerationPath,
    metadata: FileMetadata,
    receipt: &mut GenerationReceipt,
) -> Result<(), GenerationFailure> {
    #[cfg(unix)]
    reject_unbaselined_unix_metadata(metadata).map_err(|error| GenerationFailure::new(error, receipt.work()))?;
    let root = host_root.clone();
    let path = host_path.clone();
    generation_blocking(receipt, move |work| {
        if metadata == FileMetadata::default() {
            return path.stat(&root, work);
        }
        #[cfg(target_os = "linux")]
        let target = root.open_linux_metadata_target(path.path(), work)?;
        #[cfg(target_os = "macos")]
        let target = root.open_macos_metadata_target(path.path(), work)?;
        #[cfg(windows)]
        let target = root.open_windows_metadata_target_wide_measured(&path.units, work)?;
        target.apply(metadata, work)
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

fn validate_options(
    options: &MaterializeOptions,
    receipt: &mut GenerationReceipt,
) -> Result<HostRoot, GenerationFailure> {
    let root = receipt.run(|work| HostRoot::open(&options.destination, work))
        .map_err(|failure| {
            let error = match failure.error {
                error @ (MaterializeError::Work(_) | MaterializeError::Cancelled(_)) => error,
                _ => MaterializeError::InvalidDestination,
            };
            GenerationFailure::new(error, failure.work)
        })?;
    if let Some(expected) = options.expected_root_identity {
        let actual = root.identity();
        if actual != expected {
            return Err(GenerationFailure::new(MaterializeError::AllocationChanged { expected, actual }, receipt.work()));
        }
    }
    let (mut entries, reader_allocation) = receipt.run(|work| {
        root.read_dir_stats(Path::new(""), work)
    })?;
    #[cfg(unix)]
    let next = receipt.run(|work| entries.next_with_work(work));
    #[cfg(windows)]
    let next = receipt.run(|work| entries.next_wide_with_work(work));
    drop(entries);
    receipt.run(|work| crate::native_host::NativeHostWork::release(work, reader_allocation))?;
    if let Some((entry, allocation)) = next? {
        drop(entry);
        receipt.run(|work| crate::native_host::NativeHostWork::release(work, allocation))?;
        return Err(GenerationFailure::new(MaterializeError::InvalidDestination, receipt.work()));
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
) -> Result<acyclic_native_runtime::NativeFile, GenerationFailure> {
    #[cfg(windows)]
    let file = receipt.run(|work| host_root.create_overlapped_file_wide_measured(&path.units, work))?;
    #[cfg(unix)]
    let file = receipt.run(|work| host_root.create_file(path.path(), work))?;
    let meter = receipt.native_meter();
    #[cfg(windows)]
    {
        #[allow(unsafe_code, reason = "the newly created held handle is overlapped and exclusively transferred")]
        // SAFETY: the measured host constructor opens this new, held-root-relative
        // handle overlapped, without a completion-port association or other I/O.
        let native = unsafe {
            acyclic_native_runtime::NativeFile::from_overlapped_file_unchecked(file, Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)))
        };
        receipt.runtime_result(native.map_err(|(error, file)| { drop(file); error }))
    }
    #[cfg(unix)]
    {
        receipt.runtime_result(
            acyclic_native_runtime::NativeFile::from_file(file, Some(acyclic_native_runtime::NativeFileMeter::Owned(meter)))
                .map_err(|(error, file)| { drop(file); error }),
        )
    }
}

fn account_written(
    receipt: &mut GenerationReceipt,
    bytes: u64,
) -> Result<(), GenerationFailure> {
    receipt.value.written_bytes = receipt.value.written_bytes.checked_add(bytes).ok_or_else(|| {
        GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
    })?;
    receipt.run(|work| work.with_original_leaf(|work| work.work.verify(work.budget).map_err(DemandError::from)))
}

fn account_materialization(
    receipt: &mut GenerationReceipt,
    bytes: u64,
) -> Result<(), GenerationFailure> {
    account_written(receipt, bytes)?;
    receipt.run(|work| work.record(WorkCounters {
        materializations: 1,
        ..WorkCounters::UNCHARGED
    }))
}

fn account_file(
    receipt: &mut GenerationReceipt,
    logical: u64,
    written: u64,
) -> Result<(), GenerationFailure> {
    receipt.value.files = receipt.value.files.checked_add(1).ok_or_else(|| {
        GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
    })?;
    receipt.value.logical_file_bytes =
        receipt.value.logical_file_bytes
            .checked_add(logical)
            .ok_or_else(|| {
                GenerationFailure::new(MaterializeError::Work(WorkError::Overflow), receipt.work())
            })?;
    account_materialization(receipt, written)
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
        let root = receipt.run(|work| HostRoot::open(directory.path(), work))?;
        let root = receipt.owned_arc(root, 0)?;
        let limits = crate::model::VolumeLimits::default();
        let namespace = NamespacePath::new(vec![LogicalName::new(
            NameEncoding::Utf8, b"sparse-control.bin".to_vec(), limits.maximum_component_bytes,
        )?], limits)?;
        let host_path = GenerationHostPath::from_namespace(&namespace, &mut receipt)?;
        let native = create_file(&root, &host_path, &mut receipt)?;
        let meter = receipt.native_meter();
        let control = receipt.runtime_result(native.control_async(|file, meter| mark_sparse_measured(file, meter.expect("metered control")), Some(meter)))?;
        receipt.runtime_result(control.await)?;
        let meter = receipt.native_meter();
        let resize = receipt.runtime_result(native.set_len_async(1024 * 1024, Some(meter)))?;
        receipt.runtime_result(resize.await)?;
        let mut ordinary_control = None::<&mut fn(NativeMaterializationActionPhase) -> std::future::Ready<Result<(), MaterializeError>>>;
        write_generation_bytes(&native, 1024 * 1024 - 4, acyclic_stream::OwnedBytes::from_static(b"tail"), 0, &mut receipt, &mut ordinary_control).await.map_err(|failure| failure.0)?;
        sync_generation_file(&native, &mut receipt, &mut ordinary_control).await.map_err(|failure| failure.0)?;
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
            Err(GenerationFailure { error: MaterializeError::UnrepresentableName, .. })
        ));
        Ok(())
    }
}

fn create_symlink_measured(
    host_root: &HostRoot,
    target: &acyclic_stream::OwnedBytes,
    destination: &GenerationHostPath,
    receipt: &mut GenerationReceipt,
) -> Result<(), GenerationFailure> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        receipt.run(|work| host_root.symlink(OsStr::from_bytes(target), destination.path(), work))
    }
    #[cfg(windows)]
    {
        let decoded = crate::kernel::types::utf16le_units(target).ok_or_else(|| {
            GenerationFailure::new(MaterializeError::UnrepresentableName, receipt.work())
        })?;
        let mut units = Vec::new();
        receipt.reserve(&mut units, target.len() / 2)?;
        receipt.run(|work| work.charge(WorkCounters {
            bytes_copied: target.len() as u64, ..WorkCounters::UNCHARGED
        }))?;
        units.extend(decoded);
        let result = receipt.run(|work| host_root.symlink_file_wide_measured(&units, &destination.units, work));
        let retained = units.capacity() * std::mem::size_of::<u16>();
        drop(units);
        receipt.release(retained)?;
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
) -> Result<(), GenerationFailure> {
    #[cfg(unix)]
    {
        match kind {
            FileKind::Fifo => receipt.run(|work| host_root.create_fifo_held(destination.path(), 0o600, work)),
            FileKind::CharacterDevice | FileKind::BlockDevice => {
                let (major, minor) = device.ok_or_else(|| {
                    GenerationFailure::new(MaterializeError::UnsupportedKind(kind), receipt.work())
                })?;
                let file_type = if kind == FileKind::CharacterDevice {
                    u32::from(libc::S_IFCHR)
                } else {
                    u32::from(libc::S_IFBLK)
                };
                let device_number = super::device::join_device(major, minor).map_err(|errno| {
                    GenerationFailure::new(MaterializeError::Io(std::io::Error::from_raw_os_error(errno)), receipt.work())
                })?;
                receipt.run(|work| host_root.create_device_held(destination.path(), file_type | 0o600, device_number, work))
            }
            FileKind::Socket => receipt.run(|work| host_root.bind_unix_socket(destination.path(), work)),
            _ => Err(GenerationFailure::new(MaterializeError::UnsupportedKind(kind), receipt.work())),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (host_root, destination, device);
        Err(GenerationFailure::new(MaterializeError::UnsupportedKind(kind), receipt.work()))
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
        let original = HostRoot::open(&destination, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?.identity();
        std::fs::rename(&destination, temporary.path().join("original"))?;
        std::fs::create_dir(&destination)?;
        std::fs::write(destination.join("foreign"), b"untouched")?;
        let actual = HostRoot::open(&destination, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?.identity();
        assert_ne!(original, actual);
        let cancellation = CancellationToken::new();
        let mut receipt = GenerationReceipt::new(finite_materialization_budget(), &cancellation)?;
        let failure = validate_options(
            &MaterializeOptions::native(&destination).expected_root_identity(Some(original)),
            &mut receipt,
        ).err().ok_or("replacement was accepted")?;
        assert!(matches!(failure.error, MaterializeError::AllocationChanged { expected, actual: found }
            if expected == original && found == actual));
        assert_eq!(std::fs::read(destination.join("foreign"))?, b"untouched");
        Ok(())
    }

    #[tokio::test]
    async fn multipath_failure_retains_completed_host_and_lookup_work()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::model::{Lifecycle, VolumeConfig, VolumeLimits};
        acyclic_native_runtime::initialize_native_io_services()?;
        let cancellation = CancellationToken::new();
        let limits = VolumeLimits::default();
        let volume = super::super::tests::memory_volume(crate::VolumeId::new(),
            VolumeConfig { limits, ..VolumeConfig::portable(Lifecycle::Ephemeral) },
            &cancellation,
        ).await?;
        let mut checkout = super::super::tests::private_checkout(&volume, &cancellation).await?;
        let first = NamespacePath::new(vec![LogicalName::new(
            NameEncoding::Utf8, b"a-present".to_vec(), limits.maximum_component_bytes,
        )?], limits)?;
        let missing = NamespacePath::new(vec![LogicalName::new(
            NameEncoding::Utf8, b"z-missing".to_vec(), limits.maximum_component_bytes,
        )?], limits)?;
        checkout.create(crate::AuthoredMutation::CreateFile { path: first.clone(), bytes: (Bytes::from_static(b"first")).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation).await?;
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
        let mut original = GenerationReceipt::new(finite_materialization_budget(), &cancellation)?;
        let candidate = checkout.private_candidate_with_native_work(original.custody.work(), &cancellation);
        let candidate = original.original_result(candidate)?;
        let reader = candidate.into_pinned_reader_with_native_work(original.custody.work(), &cancellation);
        let reader = original.original_result(reader)?;
        let prefix = original.work();
        let original_work = crate::NativeMaterializationWork::from_meter(Arc::clone(&original.meter));
        let original_output = tempfile::tempdir()?;
        let original_failed = materialize_checkout_paths_with_work(
            &reader, &[first.clone(), missing.clone()],
            &MaterializeOptions::native(original_output.path()), &original_work,
        ).await.err().ok_or("original source accepted a missing second path")?;
        assert!(matches!(original_failed, MaterializeError::MissingPath));
        assert_eq!(std::fs::read(original_output.path().join("a-present"))?, b"first");
        let original_spent = original.meter.with_original_leaf(|work| work.work);
        assert!(original_spent.allocation_operations > prefix.allocation_operations);
        assert!(original_spent.backend_read_operations > prefix.backend_read_operations);
        assert_eq!(original_spent.output_bytes, 5);

        #[derive(Debug, Error)]
        enum TaskFenceFailure {
            #[error("task fence expired before the next logical action")]
            Owner,
            #[error(transparent)]
            Materialize(#[from] MaterializeError),
        }
        for deny_before_output in [true, false] {
            let guarded_output = tempfile::tempdir()?;
            let before = original.meter.with_original_leaf(|work| work.work);
            let mut task_fence = |phase| {
                let denied = phase == NativeMaterializationActionPhase::Before
                    && (deny_before_output || original.work().output_bytes > before.output_bytes);
                std::future::ready(if denied { Err(TaskFenceFailure::Owner) } else { Ok(()) })
            };
            let error = materialize_checkout_host_paths_with_work_guarded(
                &reader, &["a-present".to_owned()],
                &MaterializeOptions::native(guarded_output.path()), &original_work, &mut task_fence,
            ).await.err().ok_or("logical action bypassed the original task fence")?;
            assert!(matches!(error, TaskFenceFailure::Owner));
            let after = original.meter.with_original_leaf(|work| work.work);
            after.verify(finite_materialization_budget())?;
            assert!(after.backend_read_operations > before.backend_read_operations);
            if deny_before_output {
                assert!(!guarded_output.path().join("a-present").exists());
                assert_eq!(after.output_bytes, before.output_bytes);
            } else {
                assert_eq!(std::fs::read(guarded_output.path().join("a-present"))?, b"first");
                assert!(after.backend_write_operations > before.backend_write_operations);
                assert!(after.output_bytes >= before.output_bytes + 5);
            }
        }

        let mut no_write = GenerationReceipt::new(
            WorkBudget { backend_write_operations: 0, ..finite_materialization_budget() }, &cancellation,
        )?;
        let candidate = checkout.private_candidate_with_native_work(no_write.custody.work(), &cancellation);
        let candidate = no_write.original_result(candidate)?;
        let reader = candidate.into_pinned_reader_with_native_work(no_write.custody.work(), &cancellation);
        let reader = no_write.original_result(reader)?;
        let no_write_output = tempfile::tempdir()?;
        let no_write_work = crate::NativeMaterializationWork::from_meter(Arc::clone(&no_write.meter));
        let original_no_write = materialize_checkout_paths_with_work(
            &reader, std::slice::from_ref(&first),
            &MaterializeOptions::native(no_write_output.path()), &no_write_work,
        ).await.err().ok_or("original source ignored zero native-write allowance")?;
        assert!(matches!(original_no_write, MaterializeError::Work(_)));
        let original_spent = no_write.meter.with_original_leaf(|work| work.work);
        assert_eq!(original_spent.backend_write_operations, 0);
        assert_eq!(original_spent.output_bytes, 0);
        assert!(std::fs::read_dir(no_write_output.path())?.next().is_none());

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
        use crate::model::{Lifecycle, VolumeConfig};
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
        let volume = super::super::tests::memory_volume(
            crate::VolumeId::new(), VolumeConfig::portable(Lifecycle::Ephemeral), &cancellation,
        ).await?;
        let mut checkout = super::super::tests::private_checkout(&volume, &cancellation).await?;
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

    /// A file created through a Linux mount records "no flags" (and its own birth and change times); materializing it must not refuse any of those, or no fork's new file could ever reach the root.
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
        let projected = host_metadata(created).0;
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
                host_metadata(flagged).0.posix_flags,
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
        let cancellation = CancellationToken::new();
        let mut receipt = GenerationReceipt::new(finite_materialization_budget(), &cancellation)?;
        let root = receipt.run(|work| HostRoot::open(temporary.path(), work))?;
        let root = receipt.owned_arc(root, 0)?;
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
        let expected = HostRoot::open(&destination, &mut crate::native_host::ordinary_work()).map_err(crate::native_host::ordinary_error)?.identity();
        std::fs::rename(&destination, temporary.path().join("original"))?;
        std::fs::create_dir(&destination)?;
        std::fs::write(destination.join("foreign"), b"untouched")?;
        let original = Arc::new(NativeDemandReadMeter(std::sync::Mutex::new(restoration_work())));
        let native_work = crate::NativeMaterializationWork::from_meter(Arc::clone(&original));
        let allocation_work = (&native_work).clone();
        let mut work = NativeSourceWork::new(&original);
        let failure = prepare_restore(
            restore_path(&destination, None, &allocation_work)?,
            restore_path(Path::new("nested/entry"), None, &allocation_work)?, Some(expected),
            HostPathReplacement::Atomic, &mut work,
        ).err().ok_or("swapped restore root was accepted")?;
        assert!(matches!(failure, MaterializeError::AllocationChanged { expected: found, .. } if found == expected));
        let spent = original.with_original_leaf(|work| work.work);
        assert!(spent.backend_read_operations > 0);
        assert_eq!(spent.backend_write_operations, 0);
        assert_eq!(spent.durability_operations, 0);
        assert_eq!(allocation_work.live_allocation_bytes(), 0);
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
        let root = HostRoot::open(&destination, &mut work)?;
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
        let held_root = HostRoot::open(&root, &mut work)?;
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
        let held_root = HostRoot::open(temporary.path(), &mut work)?;
        let parent = held_parent(&held_root, relative, &mut work)?;
        let lock = acquire_restore_lock(&parent, relative, true, &mut work)?;
        let (started, entered) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let (finished, completion) = tokio::sync::oneshot::channel();
        let original = Arc::new(NativeDemandReadMeter(std::sync::Mutex::new(restoration_work())));
        let waiter = tokio::spawn(restore_io(
            crate::obs::caller_context(),
            Some(lock),
            Arc::clone(&original),
            move |_| {
                let _ = started.send(());
                // Dropping release on an early test exit also unblocks the worker.
                blocked.recv().map_err(std::io::Error::other)?;
                let _ = finished.send(());
                Ok::<_, MaterializeError>(())
            },
        ));
        entered.await?;
        // Native waiting must not hold the original accounting leaf.
        original.with_original_leaf(|work| work.charge(WorkCounters {
            items_examined: 1, ..WorkCounters::UNCHARGED
        }))?;
        assert_eq!(original.with_original_leaf(|work| work.work.items_examined), 1);
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
        let held_root = HostRoot::open(&root, &mut work)?;
        let parent = held_parent(&held_root, relative, &mut work)?;
        let backup = restore_artifact_name(".acyclic-restore-backup-", relative, &mut work)?;
        let witness = restore_artifact_name(".acyclic-restore-live-witness-", relative, &mut work)?;
        create_restore_witness(&parent, &witness, LIVE_RESTORE_WITNESS, &mut work)?;
        parent.rename_to(relative, &parent, &backup, &mut work)?;

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
        let held_root = HostRoot::open(&root, &mut work)?;
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
        let held_root = HostRoot::open(&root, &mut work)?;
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
        let held_root = HostRoot::open(&root, &mut work)?;
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
        use crate::model::{Lifecycle, VolumeConfig, VolumeLimits};
    use crate::VolumeId;

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
        let cancellation = CancellationToken::new();
        let volume = super::super::tests::memory_volume(
                VolumeId::from_bytes([73; 16]),
                VolumeConfig { limits, ..VolumeConfig::portable(Lifecycle::Ephemeral) },
                &cancellation,
            )
            .await?;
        let mut checkout = super::super::tests::private_checkout(&volume, &cancellation).await?;
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
                .create(crate::AuthoredMutation::CreateFile { path: path, bytes: (bytes.clone()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)
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
