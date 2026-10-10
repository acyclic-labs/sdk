//! Explicit same-graph policy backed by the original Filesystem and lazy stores.

use super::{FilesystemHost, map_error};
use crate::contract::{JsonBounds, JsonCodecCharge, canonical_json_bytes_bounded};
use crate::conversation::{VolumeClass, VolumeRef};
use crate::core::Authority;
use crate::source_authorization::{ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider};
use crate::{Error, Result, TaskId};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Digest, GenerationId, LazySnapshotRef, LazyWorkspace, WorkBudget, WorkCounters};
use serde::Deserialize;
use std::sync::Arc;

/// Creator-only project selection within one explicitly configured service graph.
///
/// The coordinator supplies authenticated creator/admission facts. This provider
/// independently verifies the original source read grant and immutable backend
/// generation. Uncreated root bootstrap seeds are deliberately rejected: a path
/// observation is not a protected successful bootstrap allocation record.
pub struct FsRootProjectSourceProvider<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    owner: Authority,
    graph_root: TaskId,
    native: Option<(VolumeRef, Arc<LazyWorkspace<A, O>>)>,
}

impl<A, O> FsRootProjectSourceProvider<A, O> {
    /// Requires the actual service owner and exact configured graph root.
    pub fn new(host: Arc<FilesystemHost<A, O>>, owner: Authority, graph_root: TaskId) -> Result<Self> {
        host.provider.validate()?;
        Ok(Self { host, owner, graph_root, native: None })
    }

    /// Binds the actual original lazy namespace; no descriptor creates a binding.
    pub fn with_native_source(mut self, project: VolumeRef, lazy: Arc<LazyWorkspace<A, O>>) -> Result<Self> {
        project.validate()?;
        if project.class() != VolumeClass::Project || project.provider() != &self.host.provider {
            return Err(Error::Unauthorized("native source is not this provider's project".into()));
        }
        self.native = Some((project, lazy));
        Ok(self)
    }
}

impl<A, O> RootProjectSourceProvider for FsRootProjectSourceProvider<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn verify<'a>(&'a self, context: RootProjectSourceContext<'a>, snapshot: &'a ProjectSourceSnapshot)
        -> acyclic_stream::BoxProviderFuture<'a, Result<()>>
    {
        Box::pin(async move {
            if context.owner != &self.owner || context.graph_root != self.graph_root
                || !context.created_project_authenticated
            {
                return Err(Error::Unauthorized("project source lacks this graph's authenticated creator".into()));
            }
            if self.native.as_ref().is_some_and(|(project, _)| project == &snapshot.project)
                && snapshot.native_snapshot.is_none()
            {
                return Err(Error::Unauthorized("bound lazy project requires its original native snapshot".into()));
            }
            snapshot.project.validate()?;
            snapshot.generation.validate()?;
            if snapshot.project.class() != VolumeClass::Project
                || snapshot.project.provider() != &self.host.provider
                || snapshot.generation.as_resource().provider() != &self.host.provider
            {
                return Err(Error::Unauthorized("project source belongs to another namespace or provider".into()));
            }
            let mut admission = SourceAdmission::new(context.maximum_work, context.maximum_bytes, context.cancellation)?;
            // Use the one bounded canonical serializer, not declaration-order JSON
            // or an unbounded temporary Value tree. These are the original namespace
            // and capability digests used by VolumeRef's ordinary conveniences.
            let bounds = JsonBounds { maximum_bytes: context.maximum_bytes, maximum_items: context.maximum_work, maximum_depth: 32 };
            let encoded = canonical_json_bytes_bounded(&snapshot.project, bounds, &mut |charge| admission.codec(charge))?;
            let encoded_owned = encoded.len() as u64;
            admission.charge(WorkCounters { bytes_hashed: encoded_owned, ..WorkCounters::default() })?;
            let digest = blake3::hash(&encoded).to_hex();
            drop(encoded);
            admission.release(encoded_owned)?;
            let mut name = [0_u8; 72];
            name[..8].copy_from_slice(b"harness-");
            name[8..].copy_from_slice(digest.as_bytes());
            let mut read = [0_u8; 76];
            read[..12].copy_from_slice(b"volume:read:");
            read[12..].copy_from_slice(digest.as_bytes());
            let name = std::str::from_utf8(&name).map_err(|_| Error::Invalid("invalid source namespace digest".into()))?;
            let read = std::str::from_utf8(&read).map_err(|_| Error::Invalid("invalid source read digest".into()))?;
            if !context.source.grants.contains(read) {
                return Err(Error::Unauthorized("selected source admission lacks original project read authority".into()));
            }
            let opened = self.host.filesystem.open_workspace_measured(name, admission.remaining()?, context.cancellation).await
                .map_err(|failure| map_error(failure.error))?;
            admission.merge(opened.work)?;
            // The measured open creates this Arc<str> payload; generation handles
            // share it, so retain it once through subsequent child operations.
            admission.retain(opened.value.name().as_str().len() as u64)?;
            let generation_bytes: [u8; 32] = snapshot.generation.as_resource().key().try_into()
                .map_err(|_| Error::Invalid("project generation key must be 32 bytes".into()))?;
            let generation_id = GenerationId::new(Digest::from_bytes(generation_bytes));
            let selected = opened.value.generation_measured(generation_id, admission.remaining()?, context.cancellation).await
                .map_err(|failure| map_error(failure.error))?;
            admission.merge(selected.work)?;
            drop(selected.value);
            if let Some(value) = &snapshot.native_snapshot {
                let Some((project, lazy)) = &self.native else {
                    return Err(Error::Unauthorized("native source has no original provider binding".into()));
                };
                if project != &snapshot.project || lazy.workspace().id() != opened.value.id() {
                    return Err(Error::Unauthorized("native source names another original workspace".into()));
                }
                // The concrete SDK descriptor is constant-size. Bound the borrowed
                // input with the same codec before typed decoding; no Value clone.
                let encoded = canonical_json_bytes_bounded(value, bounds, &mut |charge| admission.codec(charge))?;
                let owned = encoded.len() as u64;
                let frozen = LazySnapshotRef::deserialize(value)
                    .map_err(|_| Error::Invalid("native source is not a lazy snapshot reference".into()))?;
                drop(encoded);
                admission.release(owned)?;
                if frozen.workspace_id != opened.value.id() || frozen.authored_generation != generation_id {
                    return Err(Error::Unauthorized("native capture does not match the selected generation".into()));
                }
                let verified = lazy.validate_snapshot_measured(frozen, admission.remaining()?, context.cancellation).await
                    .map_err(|failure| Error::Storage(failure.error.to_string()))?;
                admission.merge(verified.work)?;
            }
            context.cancellation.check().map_err(|_| Error::Unauthorized("project source operation cancelled".into()))?;
            Ok(())
        })
    }
}

// One outer admission composes canonical codec storage and actual SDK receipts.
// Per-dimension shares conservatively bound the aggregate declared work/bytes;
// they are limits, never fabricated actual counters or allocator/RSS estimates.
struct SourceAdmission<'a> {
    work: WorkCounters,
    budget: WorkBudget,
    live: u64,
    cancellation: &'a acyclic_fs::CancellationToken,
}

impl<'a> SourceAdmission<'a> {
    fn new(maximum_work: u64, maximum_bytes: u64, cancellation: &'a acyclic_fs::CancellationToken) -> Result<Self> {
        if maximum_work == 0 || maximum_work == u64::MAX || maximum_bytes == 0 || maximum_bytes == u64::MAX {
            return Err(Error::Invalid("project source requires finite positive admission".into()));
        }
        cancellation.check().map_err(|_| Error::Unauthorized("project source operation cancelled".into()))?;
        let operations = maximum_work / 14;
        let bytes = maximum_bytes / 9;
        Ok(Self {
            work: WorkCounters::default(), live: 0, cancellation,
            budget: WorkCounters {
                authority_records_read: operations, authority_records_appended: operations,
                authority_bytes_read: bytes, authority_bytes_written: bytes,
                object_probes: operations, backend_read_operations: operations, backend_write_operations: operations,
                durability_operations: operations, page_reads: operations, page_writes: operations,
                object_bytes_read: bytes, object_bytes_written: bytes,
                bytes_hashed: bytes, bytes_copied: bytes, bytes_encoded: bytes,
                source_bytes_read: bytes, source_path_components: operations, source_entries_visited: operations,
                output_bytes: bytes, items_examined: operations, items_returned: operations,
                allocation_operations: operations, peak_allocation_bytes: maximum_bytes, materializations: operations,
            },
        })
    }

    fn remaining(&self) -> Result<WorkBudget> {
        self.cancellation.check().map_err(|_| Error::Unauthorized("project source operation cancelled".into()))?;
        let mut remaining = self.work.remaining(self.budget).map_err(quota_error)?;
        remaining.peak_allocation_bytes = self.budget.peak_allocation_bytes.checked_sub(self.live)
            .ok_or_else(|| Error::Invalid("project source storage accounting overflow".into()))?;
        Ok(remaining)
    }

    fn charge(&mut self, delta: WorkCounters) -> Result<()> {
        self.cancellation.check().map_err(|_| Error::Unauthorized("project source operation cancelled".into()))?;
        self.work.charge(&delta, &self.budget).map_err(quota_error)
    }

    fn merge(&mut self, child: WorkCounters) -> Result<()> {
        let overlap = self.live.checked_add(child.peak_allocation_bytes)
            .ok_or_else(|| Error::Invalid("project source storage accounting overflow".into()))?;
        let mut combined = self.work.checked_add(child).map_err(quota_error)?;
        combined.peak_allocation_bytes = combined.peak_allocation_bytes.max(overlap);
        WorkCounters::default().admit(&combined, &self.budget).map_err(quota_error)?;
        self.work = combined;
        Ok(())
    }

    fn retain(&mut self, bytes: u64) -> Result<()> {
        self.cancellation.check().map_err(|_| Error::Unauthorized("project source operation cancelled".into()))?;
        let live = self.live.checked_add(bytes).ok_or_else(|| Error::Invalid("project source storage accounting overflow".into()))?;
        self.work.charge_allocation(0, live, &self.budget).map_err(quota_error)?;
        self.live = live;
        Ok(())
    }

    fn release(&mut self, bytes: u64) -> Result<()> {
        self.live = self.live.checked_sub(bytes).ok_or_else(|| Error::Invalid("project source storage release mismatch".into()))?;
        Ok(())
    }

    fn codec(&mut self, charge: JsonCodecCharge) -> Result<()> {
        match charge {
            JsonCodecCharge::Work { bytes, items } => self.charge(WorkCounters { bytes_encoded: bytes, items_examined: items, ..WorkCounters::default() }),
            JsonCodecCharge::Copy { bytes } => self.charge(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() }),
            JsonCodecCharge::Read { bytes } => self.charge(WorkCounters { source_bytes_read: bytes, ..WorkCounters::default() }),
            JsonCodecCharge::Retain { bytes } => self.retain(bytes),
            JsonCodecCharge::Release { bytes } => self.release(bytes),
            JsonCodecCharge::Admission { maximum_owned_bytes } => self.retain(maximum_owned_bytes),
        }
    }
}

fn quota_error(error: acyclic_fs::WorkError) -> Error {
    Error::Invalid(format!("project source quota: {error}"))
}
