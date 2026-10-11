//! Native-only held-root capture, immutable archive bytes and original-fenced apply.
use crate::{NativeCheckout, NativeGeneration, NativeOperationWindowLease, NapiU32, bigint_u64, boundary_budget, fixed_16, napi_error, native_generation_buffer, native_path, publication_permit};
use acyclic_fs::kernel::FileKind;
use acyclic_fs::materializer::{MaterializationPhase, MaterializationRecovery};
use acyclic_fs::native_apply::{self, NativeDirectoryApplyReceipt as CoreApplyReceipt};
use acyclic_fs::native_archive::{self, NativeArchiveDescription, NativeDirectoryArchive as CoreArchive, NativeDirectoryCapture as CoreCapture};
use acyclic_fs::native_capture::CapturePolicy;
use acyclic_fs::native_host::HostRoot;
use acyclic_fs::{CaptureOptions, LocalCoreStateStore, MaterializeOptions, OperationId};
use napi::bindgen_prelude::{BigInt, Buffer};
use napi::Result;
use napi_derive::napi;
use std::path::PathBuf;
use std::sync::Arc;

/// Immutable actual native archive bytes, retained independently of its capture.
#[napi]
pub struct NativeDirectoryArchive { inner: Arc<CoreArchive> }
#[napi]
impl NativeDirectoryArchive {
    /// Actual compressed byte length, without JavaScript-number narrowing.
    #[napi(getter)]
    pub fn total_bytes(&self) -> BigInt { self.inner.description().total_bytes.into() }
    /// SHA256 of these exact retained bytes.
    #[napi(getter)]
    pub fn sha256(&self) -> String { self.inner.description().sha256.clone() }
    /// Bounded native upload chunk. Empty bytes occur only at exact EOF.
    #[napi]
    pub fn read_chunk(&self, offset: BigInt, maximum_bytes: NapiU32) -> Result<Buffer> {
        self.inner.read_chunk(bigint_u64(&offset, "offset")?, maximum_bytes.into_inner())
            .map(|bytes| Buffer::from(bytes.to_vec())).map_err(napi_error)
    }
}
/// Authenticate received immutable chunks using the one native archive producer contract.
#[napi]
pub fn native_directory_archive_from_chunks(chunks: Vec<Buffer>, total_bytes: BigInt, sha256: String) -> Result<NativeDirectoryArchive> {
    let description = NativeArchiveDescription { total_bytes: bigint_u64(&total_bytes, "totalBytes")?, sha256 };
    let chunks = chunks.into_iter().map(|chunk| bytes::Bytes::from(chunk.to_vec())).collect();
    let archive = CoreArchive::from_chunks(chunks, description).map_err(napi_error)?;
    Ok(NativeDirectoryArchive { inner: Arc::new(archive) })
}

/// One exact logical change relative to the original retained capture.
#[napi(object)]
pub struct NativeDirectoryApplyChange {
    /// Relative portable path; no private host path or physical inode is exposed.
    pub path: String,
    /// Original canonical entry kind, or authenticated absence.
    pub before_kind: Option<FileKind>,
    /// Desired canonical kind, or authenticated removal.
    pub after_kind: Option<FileKind>,
    /// Canonical payload changed.
    pub content_changed: bool,
    /// Canonical metadata changed.
    pub metadata_changed: bool,
    /// Stable binding identity changed.
    pub binding_changed: bool,
}
/// Full bounded preview; partial/truncated plans are never returned as complete.
#[napi(object)]
pub struct NativeDirectoryApplyPreview {
    /// Original immutable generation.
    pub from: Buffer,
    /// Exact desired immutable generation.
    pub to: Buffer,
    /// Complete changed paths.
    pub changes: Vec<NativeDirectoryApplyChange>,
}
/// Actual canonical journal outcome, not a fabricated completion acknowledgement.
#[napi(object)]
pub struct NativeDirectoryApplyReceipt {
    /// Original durable materialization operation identity.
    pub operation_id: Buffer,
    /// Captured immutable baseline.
    pub from: Buffer,
    /// Exact desired generation.
    pub to: Buffer,
    /// Observed canonical durable phase.
    pub phase: MaterializationPhase,
    /// Exact journal edit count.
    pub changed_paths: u32,
}
fn apply_receipt(receipt: CoreApplyReceipt) -> NativeDirectoryApplyReceipt {
    NativeDirectoryApplyReceipt { operation_id: receipt.operation_id.into_bytes().to_vec().into(),
        from: native_generation_buffer(receipt.from), to: native_generation_buffer(receipt.to),
        phase: receipt.phase, changed_paths: receipt.changed_paths }
}
/// Apply bounds reuse the canonical native materialization policy; destination is the captured root.
#[napi(object)]
pub struct NativeDirectoryApplyOptions {
    /// Private SDK journal storage root.
    pub state_root: String,
    /// Private operation-owned staging/backup directory.
    pub operation_directory: String,
    /// Original operation key, retained through response loss and restart.
    pub operation_id: Buffer,
    /// Existing authenticated directory-page bound.
    pub maximum_directory_entries: NapiU32,
    /// Existing sparse-range bound.
    pub maximum_extent_spans: NapiU32,
    /// Existing exact transfer bound.
    pub transfer_bytes: BigInt,
}
/// Original held native capability and durable SDK capture locator; caller JSON cannot forge it.
#[napi]
pub struct NativeDirectoryCapture { inner: CoreCapture }
#[napi]
impl NativeDirectoryCapture {
    /// Durable private-record locator.
    #[napi(getter)]
    pub fn operation_id(&self) -> Buffer { self.inner.operation_id().into_bytes().to_vec().into() }
    /// Original committed logical generation, never the later head.
    #[napi(getter)]
    pub fn generation(&self) -> Buffer { native_generation_buffer(self.inner.original_generation()) }
    /// Original logical volume.
    #[napi(getter)]
    pub fn volume_id(&self) -> Buffer { self.inner.original_volume().into_bytes().to_vec().into() }
    /// Exact original transport length retained by the private capture record.
    #[napi(getter)]
    pub fn total_bytes(&self) -> BigInt { self.inner.archive_description().total_bytes.into() }
    /// Exact original transport digest retained by the private capture record.
    #[napi(getter)]
    pub fn sha256(&self) -> String { self.inner.archive_description().sha256.clone() }
    /// Preview only the exact original-to-desired generation transition.
    #[napi]
    pub async fn preview(&self, from: &NativeGeneration, to: &NativeGeneration, maximum_changes: NapiU32) -> Result<NativeDirectoryApplyPreview> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let receipt = native_apply::preview_native_directory_apply(&self.inner, &from.inner, &to.inner,
            maximum_changes.into_inner(), boundary_budget(), &cancellation).await.map_err(napi_error)?;
        Ok(NativeDirectoryApplyPreview { from: native_generation_buffer(receipt.value.from),
            to: native_generation_buffer(receipt.value.to), changes: receipt.value.changes.into_iter().map(|change| NativeDirectoryApplyChange {
                path: change.path, before_kind: change.before_kind, after_kind: change.after_kind,
                content_changed: change.content_changed, metadata_changed: change.metadata_changed, binding_changed: change.binding_changed,
            }).collect() })
    }
    /// Apply under original physical preimages and the existing durable materialization journal.
    #[napi]
    pub async fn apply(&self, from: &NativeGeneration, to: &NativeGeneration, options: NativeDirectoryApplyOptions) -> Result<NativeDirectoryApplyReceipt> {
        let operation_id = OperationId::from_bytes(fixed_16(&options.operation_id)?);
        let state = LocalCoreStateStore::new(&options.state_root);
        let materialize = MaterializeOptions { destination: self.inner.root_path().to_path_buf(),
            maximum_directory_entries: options.maximum_directory_entries.into_inner(),
            maximum_extent_spans: options.maximum_extent_spans.into_inner(), transfer_bytes: bigint_u64(&options.transfer_bytes, "transferBytes")? };
        let cancellation = acyclic_fs::CancellationToken::new();
        native_apply::apply_native_directory_generation(&self.inner, &from.inner, &to.inner, &state,
            &PathBuf::from(options.operation_directory), operation_id, &materialize, boundary_budget(), &cancellation)
            .await.map(apply_receipt).map_err(napi_error)
    }
    /// Cold recovery resumes only the existing original journal; it never recaptures current files.
    #[napi]
    pub async fn recover(&self, state_root: String, operation_directory: String, operation_id: Buffer, roll_back: bool) -> Result<Option<NativeDirectoryApplyReceipt>> {
        let state = LocalCoreStateStore::new(state_root);
        let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
        let recovery = if roll_back { MaterializationRecovery::RollBack } else { MaterializationRecovery::Complete };
        native_apply::recover_native_directory_apply(&self.inner, &state, &PathBuf::from(operation_directory), operation_id, recovery)
            .await.map(|receipt| receipt.map(apply_receipt)).map_err(napi_error)
    }
}
/// Restore only the real original private capture record and held root, not an invented archive.
#[napi]
pub async fn restore_native_directory_capture(state_root: String, operation_id: Buffer) -> Result<NativeDirectoryCapture> {
    let state = LocalCoreStateStore::new(state_root);
    let operation_id = OperationId::from_bytes(fixed_16(&operation_id)?);
    native_archive::restore_native_directory_capture(&state, operation_id).await
        .map(|inner| NativeDirectoryCapture { inner }).map_err(napi_error)
}
/// Actual capture receipt and immutable compressed archive, with separate lifetimes.
#[napi(object)]
pub struct NativeDirectoryArchiveCapture {
    /// Original retained physical/logical receipt.
    pub capture: NativeDirectoryCapture,
    /// Actual native-produced bounded compressed bytes.
    pub archive: NativeDirectoryArchive,
}
/// Original capture configuration; exclusion prefixes are canonical Rust paths.
#[napi(object)]
pub struct NativeDirectoryCaptureOptions {
    /// Original absolute source binding, opened no-follow exactly once.
    pub source_root: String,
    /// Private durable SDK capture-record storage root.
    pub state_root: String,
    /// Original capture operation identity.
    pub operation_id: Buffer,
    /// Existing capture path bound.
    pub maximum_paths: NapiU32,
    /// Existing sparse range bound.
    pub maximum_extent_spans: NapiU32,
    /// Existing canonical capture exclusion prefixes.
    pub excluded_paths: Vec<String>,
}
#[napi]
impl NativeCheckout {
    /// Capture and seal the actual original native root, archive and private receipt.
    #[napi]
    pub async fn capture_directory_archive(&self, options: NativeDirectoryCaptureOptions, lease: Option<NativeOperationWindowLease>) -> Result<NativeDirectoryArchiveCapture> {
        let source_root = PathBuf::from(options.source_root);
        let root = Arc::new(HostRoot::open(&source_root).map_err(napi_error)?);
        let policy = CapturePolicy::excluding(options.excluded_paths.iter().map(|path| native_path(path, self.config)).collect::<Result<_>>()?).map_err(napi_error)?;
        let capture_options = CaptureOptions { source_root, expected_root_identity: root.identity(),
            maximum_paths: options.maximum_paths.into_inner(), maximum_extent_spans: options.maximum_extent_spans.into_inner() };
        let operation_id = OperationId::from_bytes(fixed_16(&options.operation_id)?);
        let permit = publication_permit(lease.as_ref())?;
        let state = LocalCoreStateStore::new(options.state_root);
        let mut checkout = self.inner.lock().await;
        let receipt = native_archive::capture_native_directory_archive(&mut checkout, root, &capture_options, &policy,
            operation_id, permit, &state, boundary_budget(), &self.cancellation).await.map_err(napi_error)?;
        Ok(NativeDirectoryArchiveCapture { capture: NativeDirectoryCapture { inner: receipt.capture },
            archive: NativeDirectoryArchive { inner: Arc::new(receipt.archive) } })
    }
    /// Import through the same canonical safe parser, never ambient untar or a JS schema.
    #[napi]
    pub async fn import_directory_archive(&self, archive: &NativeDirectoryArchive) -> Result<()> {
        let mut checkout = self.inner.lock().await;
        native_archive::import_native_directory_archive(&mut checkout, archive.inner.reader(), boundary_budget(), &self.cancellation)
            .await.map(|_| ()).map_err(napi_error)
    }
    /// Produce the desired child in the original captured volume, retaining unchanged identities and metadata.
    #[napi]
    pub async fn replace_captured_directory_archive(&self, capture: &NativeDirectoryCapture, archive: &NativeDirectoryArchive) -> Result<()> {
        let mut checkout = self.inner.lock().await;
        native_archive::replace_captured_native_directory_archive(&mut checkout, &capture.inner, archive.inner.reader(), boundary_budget(), &self.cancellation)
            .await.map(|_| ()).map_err(napi_error)
    }
    /// Re-export exact original immutable bytes after cold restoration; never manufacture an empty body.
    #[napi]
    pub async fn export_directory_archive(&self, capture: &NativeDirectoryCapture, maximum_paths: NapiU32) -> Result<NativeDirectoryArchive> {
        let checkout = self.inner.lock().await;
        if checkout.volume_id() != capture.inner.original_volume() || checkout.generation_id() != capture.inner.original_generation() {
            return Err(napi_error("archive export requires the original captured generation"));
        }
        let reader = checkout.pinned_reader().map_err(napi_error)?;
        let archive = native_archive::export_native_directory_archive(&reader, self.config, maximum_paths.into_inner(), boundary_budget(), &self.cancellation)
            .await.map_err(napi_error)?;
        if archive.description() != capture.inner.archive_description() { return Err(napi_error("restored original archive digest mismatch")); }
        Ok(NativeDirectoryArchive { inner: Arc::new(archive) })
    }
}
