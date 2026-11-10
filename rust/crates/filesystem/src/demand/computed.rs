//! Demand-only adapters for pure projections of an immutable cut.
use super::*;
use crate::facade::FsWork;
use crate::kernel::{KernelAccounting, KernelOwnedAllocation, OwnedSourceChunk};
use std::sync::Arc;

const VERSION_DOMAIN: &[u8] = b"acyclic:computed:node:v2";

/// Immutable inputs to a view. The generation is a volume content address;
/// the sequence is a committed stream cut, never a wall-clock timestamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ViewCut {
    /// Last included stream position (exclusive).
    pub sequence: u64,
    /// Exact authored volume generation.
    pub generation: [u8; 32],
}

/// Read-only backing for a view. Implementations supply their stream/cache
/// snapshot lazily and start a follow only when `watch` is requested.
pub trait ViewProvider: Send + Sync {
    /// Read-only data access at the selected cut; may itself be a lazy reader.
    type Snapshot: Send + Sync + 'static;
    /// Committed record delivered by the backing stream's follow.
    type Record: Send + Sync;
    /// Stable namespace identity, without capturing a cut or doing IO.
    fn identity(&self) -> [u8; 16];
    /// Captures the complete provider cut on the caller's original ledger.
    fn reference(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<SourceReference, DemandError>> + Send;
    /// Resolves the requested cut without enumerating unrelated records.
    fn snapshot(
        &self,
        source: SourceReference,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(ViewCut, Self::Snapshot), DemandError>;
    /// One follow from the subscription's cut. No subscription means no task.
    fn watch(
        &self,
        sink: Arc<dyn ViewChangeSink<Self::Record>>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError>;
}

/// A committed-record callback, run on the producer's original accounting context.
pub trait ViewChangeSink<R>: Send + Sync {
    /// Ordered delivery from one follow. A record may affect no visible paths.
    fn changed(&self, record: &R, accounting: &mut KernelAccounting<'_>)
    -> Result<(), DemandError>;
    /// The stream follow has lost exact delivery.
    fn delivery_lost(&self);
}

/// One reusable projection for servers, native mounts and reader caches.
/// Methods are pure in `(cut, snapshot, query)` and perform only the requested
/// lookup, page or byte range. They must not write the authored volume.
pub trait View: Send + Sync + 'static {
    /// Read-only backing used by this view.
    type Snapshot: Send + Sync + 'static;
    /// Stream record understood by `affected`.
    type Record: Send + Sync;
    /// Stable view implementation identity, including its format version.
    fn identity(&self) -> [u8; 32];
    /// Exact lookup, without scanning a directory to locate one name.
    fn lookup(
        &self,
        cut: ViewCut,
        snapshot: &Self::Snapshot,
        path: &NamespacePath,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceNode>, DemandError>;
    /// One bounded directory page. Continuations must be tied to the query.
    fn nodes(
        &self,
        cut: ViewCut,
        snapshot: &Self::Snapshot,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<SourceDirectoryPage, DemandError>;
    /// Only the requested range, including for repeated reads of an open file.
    fn bytes(
        &self,
        cut: ViewCut,
        snapshot: &Self::Snapshot,
        path: &NamespacePath,
        offset: u64,
        length: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, DemandError>;
    /// Emit invalidations directly into the existing filesystem sink.
    fn affected(
        &self,
        record: &Self::Record,
        sink: &dyn SourceChangeSink,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError>;
}

/// A pure view served by the same demand path as a native source. Construction
/// computes nothing, reads no records and creates no background task.
pub struct Computed<V, P> {
    view: Arc<V>,
    provider: P,
}

impl<V: View, P: ViewProvider<Snapshot = V::Snapshot, Record = V::Record>> Computed<V, P> {
    /// Binds a shared view to a read-only stream or reader-cache provider.
    pub fn new(view: Arc<V>, provider: P) -> Self {
        Self { view, provider }
    }

    fn provider_source(&self, source: &SourceReference) -> Result<SourceReference, DemandError> {
        if source.identity != self.identity() {
            return Err(DemandError::StaleSource);
        }
        Ok(SourceReference {
            identity: self.provider.identity(),
            ..source.clone()
        })
    }

    fn version(
        &self,
        source: &SourceReference,
        cut: ViewCut,
        path: &NamespacePath,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<SourceVersion, DemandError> {
        self.version_names(source, cut, path.components().iter(), ctx)
    }

    fn version_names<'a>(
        &self,
        source: &SourceReference,
        cut: ViewCut,
        names: impl Iterator<Item = &'a crate::kernel::LogicalName> + Clone,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<SourceVersion, DemandError> {
        let mut hash = blake3::Hasher::new();
        let bytes = VERSION_DOMAIN.len() as u64
            + 48
            + 8
            + 32
            + source.encoded_len() as u64
            + names
                .clone()
                .map(|name| 9 + name.as_bytes().len() as u64)
                .sum::<u64>();
        ctx.charge(WorkCounters {
            bytes_hashed: bytes,
            ..WorkCounters::UNCHARGED
        })?;
        hash.update(VERSION_DOMAIN);
        hash.update(&self.view.identity());
        hash.update(&self.provider.identity());
        hash.update(&cut.sequence.to_le_bytes());
        hash.update(&cut.generation);
        source.encode(|part| {
            hash.update(part);
        });
        for name in names {
            hash.update(&[name.encoding().tag()]);
            hash.update(&(name.as_bytes().len() as u64).to_le_bytes());
            hash.update(name.as_bytes());
        }
        Ok(SourceVersion(*hash.finalize().as_bytes()))
    }

    fn selected(
        &self,
        source: &SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(ViewCut, P::Snapshot), DemandError> {
        let selected = self.provider.snapshot(self.provider_source(source)?, ctx)?;
        if self.version(source, selected.0, path, ctx)? != expected {
            return Err(DemandError::StaleVersion);
        }
        Ok(selected)
    }
}

impl<V: View, P: ViewProvider<Snapshot = V::Snapshot, Record = V::Record>> DemandSource
    for Computed<V, P>
{
    fn overlay(&self, _: &[crate::kernel::LogicalName]) -> SourceOverlay {
        SourceOverlay::Defaults
    }
    fn identity(&self) -> [u8; 16] {
        let mut hash = blake3::Hasher::new();
        hash.update(&self.view.identity());
        hash.update(&self.provider.identity());
        let mut identity = [0; 16];
        identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        identity
    }
    async fn reference(
        &self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<SourceReference, DemandError> {
        ctx.check()?;
        Ok(SourceReference {
            identity: self.identity(),
            ..self.provider.reference(ctx).await?
        })
    }

    async fn lookup<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<SourceNode>, DemandError> {
        let mut ctx = work.accounting(cancellation);
        let result = (|| {
            ctx.check()?;
            let (cut, snapshot) = self
                .provider
                .snapshot(self.provider_source(&source)?, &mut ctx)?;
            let mut node = self.view.lookup(cut, &snapshot, path, &mut ctx)?;
            if let Some(node) = &mut node {
                node.version = self.version(&source, cut, path, &mut ctx)?;
            }
            Ok(node)
        })();
        work.finish(result, &ctx)
    }

    async fn list_page<W: FsWork + Send>(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<SourceDirectoryPage, DemandError> {
        let mut ctx = work.accounting(cancellation);
        let result = (|| {
            ctx.check()?;
            if maximum_entries == 0 {
                return Err(DemandError::InvalidRequest);
            }
            let (cut, snapshot) = self
                .provider
                .snapshot(self.provider_source(&source)?, &mut ctx)?;
            let version = self.version(&source, cut, directory, &mut ctx)?;
            ctx.charge(WorkCounters {
                bytes_hashed: 48,
                ..WorkCounters::UNCHARGED
            })?;
            let mut query = blake3::Hasher::new();
            query.update(&source.identity);
            query.update(&version.0);
            let mut identity = [0; 16];
            identity.copy_from_slice(&query.finalize().as_bytes()[..16]);
            let continuation = SourceReference {
                identity,
                epoch: source.epoch,
                components: source.components.clone(),
            };
            if cursor
                .as_ref()
                .is_some_and(|cursor| cursor.source != continuation)
            {
                return Err(DemandError::StaleCursor);
            }
            let mut page =
                self.view
                    .nodes(cut, &snapshot, directory, cursor, maximum_entries, &mut ctx)?;
            if page.entries.len() > maximum_entries as usize {
                return Err(DemandError::InvalidRequest);
            }
            let entries =
                Arc::get_mut(&mut page.entries.backing).ok_or(DemandError::InvalidRequest)?;
            for entry in &mut entries.value {
                entry.node.version = self.version_names(
                    &source,
                    cut,
                    directory
                        .components()
                        .iter()
                        .chain(std::iter::once(&entry.name)),
                    &mut ctx,
                )?;
            }
            page.version = version;
            if let Some(next) = &mut page.next {
                next.source = continuation;
            }
            Ok(page)
        })();
        work.finish(result, &ctx)
    }

    async fn read_range<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        offset: u64,
        length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<OwnedSourceChunk, DemandError> {
        let mut ctx = work.accounting(cancellation);
        let result = (|| {
            ctx.check()?;
            let (cut, snapshot) = self.selected(&source, path, expected, &mut ctx)?;
            self.view
                .bytes(cut, &snapshot, path, offset, length, &mut ctx)
        })();
        work.finish(result, &ctx)
    }

    async fn open_file<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Box<dyn DemandFile>, DemandError> {
        let mut ctx = work.accounting(cancellation);
        let result = (|| {
            ctx.check()?;
            let (cut, snapshot) = self.selected(&source, path, expected, &mut ctx)?;
            let node = self
                .view
                .lookup(cut, &snapshot, path, &mut ctx)?
                .ok_or(DemandError::Absent)?;
            if node.kind != SourceNodeKind::RegularFile {
                return Err(DemandError::NotRegularFile);
            }
            let (path, mut allocation) = path.clone_with_accounting(&mut ctx, cancellation)?;
            let bytes = path.logical_owned_bytes() + std::mem::size_of::<OpenView<V>>() as u64;
            let mut header = ctx.claim_owned(std::mem::size_of::<OpenView<V>>() as u64, 1)?;
            allocation.absorb(&mut header)?;
            Ok(Box::new(OpenView {
                view: Arc::clone(&self.view),
                cut,
                snapshot,
                path,
                allocation,
                bytes,
            }) as Box<dyn DemandFile>)
        })();
        work.finish(result, &ctx)
    }

    async fn read_link<W: FsWork + Send>(
        &self,
        _: SourceReference,
        _: &NamespacePath,
        _: SourceVersion,
        _: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<OwnedSourceChunk, DemandError> {
        let ctx = work.accounting(cancellation);
        work.finish(Err(DemandError::InvalidRequest), &ctx)
    }

    fn watch<W: FsWork + Send>(
        &self,
        sink: Arc<dyn SourceChangeSink>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<Box<dyn SourceWatch>>, DemandError> {
        let mut ctx = work.accounting(cancellation);
        let result = (|| {
            ctx.check()?;
            let allocation = ctx.claim_owned(
                std::mem::size_of::<ViewChanges<V>>() as u64
                    + 2 * std::mem::size_of::<usize>() as u64,
                1,
            )?;
            self.provider.watch(
                Arc::new(ViewChanges {
                    view: Arc::clone(&self.view),
                    sink,
                    _allocation: allocation,
                }),
                &mut ctx,
            )
        })();
        work.finish(result, &ctx)
    }
}

struct OpenView<V: View> {
    view: Arc<V>,
    cut: ViewCut,
    snapshot: V::Snapshot,
    path: NamespacePath,
    allocation: KernelOwnedAllocation,
    bytes: u64,
}
impl<V: View> DemandFile for OpenView<V> {
    fn logical_owned_bytes(&self) -> u64 {
        self.bytes
    }
    fn read_range(
        &self,
        offset: u64,
        length: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, DemandError> {
        ctx.check()?;
        let _retained = &self.allocation;
        self.view
            .bytes(self.cut, &self.snapshot, &self.path, offset, length, ctx)
    }
}

struct ViewChanges<V: View> {
    view: Arc<V>,
    sink: Arc<dyn SourceChangeSink>,
    _allocation: KernelOwnedAllocation,
}
impl<V: View> ViewChangeSink<V::Record> for ViewChanges<V> {
    fn changed(
        &self,
        record: &V::Record,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        self.view.affected(record, self.sink.as_ref(), ctx)
    }
    fn delivery_lost(&self) {
        self.sink.delivery_lost();
    }
}
