//! Pure projections over provider-pinned cuts. Construction performs no work;
//! a provider acquires records only for the requested path/page/range.
use super::*;
use crate::{
    facade::FsWork,
    kernel::{KernelAccounting, KernelOwnedAllocation, Owned, OwnedSourceChunk},
    model::VolumeLimits,
};
use std::{ops::Range, sync::Arc};

/// The visible demand whose dependencies a provider must pin.
#[derive(Clone, Copy, Debug)]
pub enum ViewRequest {
    /// Exact metadata only.
    Node,
    /// One bounded directory page.
    Directory { cursor: u64, maximum: u32 },
    /// Only this byte range.
    Bytes { offset: u64, length: u64 },
    /// A file whose cut must remain available for repeated range reads.
    Open,
}

/// One pure directory page, with its backing admitted before allocation.
pub struct ViewPage {
    /// Entries, without fetching another page or subtree.
    pub entries: Owned<Vec<SourceDirectoryEntry>>,
    /// Opaque position understood only by this view.
    pub next: Option<u64>,
}

/// A pure view, shared by API, mounts and cached clients. Returned buffers use
/// the supplied accounting; evaluation never performs IO or starts tasks.
pub trait View: Copy + Send + Sync + 'static {
    /// Provider-resident immutable data required by this view.
    type Cut: Send + Sync + 'static;
    /// A followed record, including the information needed to map its paths.
    type Record: Send + Sync + 'static;
    /// Stable schema identity. Change the version when projection bytes change.
    fn identity(&self) -> (&'static str, u32);
    /// Resolve exactly one path, without listing unrelated siblings. Identity
    /// and version fields are assigned by `Computed`, not by the view.
    fn node(
        &self,
        cut: &Self::Cut,
        path: &NamespacePath,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<SourceNode>, DemandError>;
    /// Enumerate one page at the supplied cut.
    fn nodes(
        &self,
        cut: &Self::Cut,
        directory: &NamespacePath,
        cursor: u64,
        maximum: u32,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<ViewPage, DemandError>;
    /// Read only the requested range at that same cut.
    fn bytes(
        &self,
        cut: &Self::Cut,
        path: &NamespacePath,
        range: Range<u64>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, DemandError>;
    /// Emit exactly the paths affected by a record, without discovering agents
    /// or fetching linked files. The receiver copies paths on its own ledger.
    fn affected(
        &self,
        record: &Self::Record,
        emit: &mut dyn FnMut(&NamespacePath) -> Result<(), DemandError>,
    ) -> Result<(), DemandError>;
}

/// A cut and its complete retained backing, paid on the original operation.
pub struct PinnedView<C> {
    /// Immutable data already resident for the requested demand.
    pub cut: C,
    /// Content address of every stream cut and volume generation it depends on.
    pub fingerprint: [u8; 32],
    bytes: u64,
    _allocation: KernelOwnedAllocation,
}
impl<C> PinnedView<C> {
    /// Transfer already admitted backing into a pinned cut. `bytes` names its
    /// complete retained payload, excluding the inline `PinnedView` header.
    pub fn new(
        cut: C,
        fingerprint: [u8; 32],
        bytes: u64,
        allocation: KernelOwnedAllocation,
    ) -> Self {
        Self {
            cut,
            fingerprint,
            bytes,
            _allocation: allocation,
        }
    }
}

/// Receives the provider's one followed record delivery on original custody.
pub trait ViewRecordSink<R>: Send + Sync {
    /// SDK payload retained by this sink.
    fn logical_owned_bytes(&self) -> u64;
    /// Deliver one record; no timer or readiness probe is involved.
    fn record(&self, record: &R, accounting: &mut KernelAccounting<'_>) -> Result<(), DemandError>;
    /// Invalidate observers if the follow loses delivery.
    fn delivery_lost(&self);
}

/// Supplies exact historical cuts or the client's already resident reader
/// cache. A reference has stable provider identity and monotonic cut epochs;
/// `pin` must resolve the requested reference, including retained older cuts.
pub trait ViewProvider<V: View>: Send + Sync {
    /// Latest known immutable cut, with no IO. Acquiring dependencies is demand
    /// work in `pin`; creating an idle source must not read a store or follow.
    fn reference(&self) -> SourceReference;
    /// Pin only the view's dependencies for this explicit demand. A missing
    /// historical cut is a typed refusal, never a substitution of the head.
    fn pin(
        &self,
        source: SourceReference,
        view: V,
        path: &NamespacePath,
        request: ViewRequest,
        accounting: &mut KernelAccounting<'_>,
    ) -> impl Future<Output = Result<PinnedView<V::Cut>, DemandError>> + Send;
    /// Start one follow at the subscribed cut, using the returned watch's
    /// existing fence/stop lifecycle. No follow exists without a subscriber.
    fn watch(
        &self,
        sink: Arc<dyn ViewRecordSink<V::Record>>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError>;
}

/// Demand-source adapter for a pure view. It owns no cache, registry or task.
pub struct Computed<V, P> {
    view: V,
    provider: P,
    limits: VolumeLimits,
}
impl<V, P> Computed<V, P> {
    /// Construct a source without pinning data or starting a follow.
    pub fn new(view: V, provider: P, limits: VolumeLimits) -> Self {
        Self {
            view,
            provider,
            limits,
        }
    }
}

impl<V: View, P: ViewProvider<V>> Computed<V, P> {
    fn digest(
        &self,
        source: SourceReference,
        fingerprint: &[u8; 32],
        path: &NamespacePath,
    ) -> [u8; 32] {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.fs.demand.computed.v1");
        let (name, version) = self.view.identity();
        hash.update(&(name.len() as u64).to_be_bytes());
        hash.update(name.as_bytes());
        hash.update(&version.to_be_bytes());
        hash.update(&source.identity);
        hash.update(fingerprint);
        for component in path.components() {
            hash.update(&[component.encoding().tag()]);
            hash.update(&(component.as_bytes().len() as u64).to_be_bytes());
            hash.update(component.as_bytes());
        }
        *hash.finalize().as_bytes()
    }
    fn node_version(
        &self,
        source: SourceReference,
        cut: &PinnedView<V::Cut>,
        path: &NamespacePath,
        mut node: SourceNode,
    ) -> SourceNode {
        node.file_identity = self.digest(source, &[0; 32], path);
        node.version = SourceVersion(self.digest(source, &cut.fingerprint, path));
        node
    }
    fn cursor_source(
        &self,
        source: SourceReference,
        cut: &PinnedView<V::Cut>,
        directory: &NamespacePath,
    ) -> SourceReference {
        let hash = self.digest(source, &cut.fingerprint, directory);
        let mut identity = [0; 16];
        identity.copy_from_slice(&hash[..16]);
        let mut epoch = [0; 8];
        epoch.copy_from_slice(&hash[16..24]);
        SourceReference {
            components: acyclic_stream::OwnedBytes::new(),
            identity,
            epoch: u64::from_be_bytes(epoch),
        }
    }
    fn check_version(
        &self,
        source: SourceReference,
        cut: &PinnedView<V::Cut>,
        path: &NamespacePath,
        expected: SourceVersion,
    ) -> Result<(), DemandError> {
        if SourceVersion(self.digest(source, &cut.fingerprint, path)) != expected {
            return Err(DemandError::StaleVersion);
        }
        Ok(())
    }
}

impl<V: View, P: ViewProvider<V>> DemandSource for Computed<V, P> {
    fn identity(&self) -> [u8; 16] {
        self.provider.reference().identity
    }
    fn reference(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<SourceReference, DemandError> {
        accounting.check()?;
        Ok(self.provider.reference())
    }

    async fn lookup<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<SourceNode>, DemandError> {
        let mut ctx = work.accounting(cancellation);
        let result = async {
            ctx.check()?;
            let cut = self
                .provider
                .pin(source, self.view, path, ViewRequest::Node, &mut ctx)
                .await?;
            Ok(self
                .view
                .node(&cut.cut, path, &mut ctx)?
                .map(|node| self.node_version(source, &cut, path, node)))
        }
        .await;
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
        let result = async {
            ctx.check()?;
            if maximum_entries == 0 {
                return Err(DemandError::InvalidRequest);
            }
            let position = cursor.as_ref().map_or(0, |cursor| cursor.token);
            let cut = self
                .provider
                .pin(
                    source,
                    self.view,
                    directory,
                    ViewRequest::Directory {
                        cursor: position,
                        maximum: maximum_entries,
                    },
                    &mut ctx,
                )
                .await?;
            let cursor_source = self.cursor_source(source, &cut, directory);
            if cursor.is_some_and(|cursor| cursor.source != cursor_source) {
                return Err(DemandError::StaleCursor);
            }
            let mut page =
                self.view
                    .nodes(&cut.cut, directory, position, maximum_entries, &mut ctx)?;
            if page.entries.value.len() > maximum_entries as usize {
                return Err(DemandError::InvalidRequest);
            }
            for entry in &mut page.entries.value {
                let (path, _allocation) = directory.child_with_accounting(
                    &entry.name,
                    self.limits,
                    &mut ctx,
                    cancellation,
                )?;
                entry.node = self.node_version(source, &cut, &path, entry.node);
            }
            let next = page
                .next
                .map(|position| SourceCursor::detached(cursor_source, position));
            SourceDirectoryPage::stateless(
                page.entries,
                next,
                SourceVersion(self.digest(source, &cut.fingerprint, directory)),
                &mut ctx,
            )
        }
        .await;
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
        let result = async {
            ctx.check()?;
            let end = offset
                .checked_add(length)
                .ok_or(DemandError::InvalidRequest)?;
            let cut = self
                .provider
                .pin(
                    source,
                    self.view,
                    path,
                    ViewRequest::Bytes { offset, length },
                    &mut ctx,
                )
                .await?;
            self.check_version(source, &cut, path, expected)?;
            bounded(
                self.view.bytes(&cut.cut, path, offset..end, &mut ctx)?,
                length,
            )
        }
        .await;
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
        let result = async {
            ctx.check()?;
            let cut = self
                .provider
                .pin(source, self.view, path, ViewRequest::Open, &mut ctx)
                .await?;
            self.check_version(source, &cut, path, expected)?;
            let node = self
                .view
                .node(&cut.cut, path, &mut ctx)?
                .ok_or(DemandError::Absent)?;
            if node.kind != SourceNodeKind::RegularFile {
                return Err(DemandError::NotRegularFile);
            }
            let (path, mut allocation) = path.clone_with_accounting(&mut ctx, cancellation)?;
            let mut header = ctx.claim_owned(std::mem::size_of::<ComputedFile<V>>() as u64, 1)?;
            allocation.absorb(&mut header)?;
            let bytes = std::mem::size_of::<ComputedFile<V>>() as u64
                + path.logical_owned_bytes()
                + cut.bytes;
            Ok(Box::new(ComputedFile {
                view: self.view,
                cut,
                path,
                bytes,
                _allocation: allocation,
            }) as Box<dyn DemandFile>)
        }
        .await;
        work.finish(result, &ctx)
    }

    async fn read_link<W: FsWork + Send>(
        &self,
        _source: SourceReference,
        _path: &NamespacePath,
        _expected: SourceVersion,
        _maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<OwnedSourceChunk, DemandError> {
        let ctx = work.accounting(cancellation);
        let result = ctx
            .check()
            .map_err(DemandError::from)
            .and(Err(DemandError::InvalidRequest));
        work.finish(result, &ctx)
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
            let bytes =
                crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<AffectedSink<V>>())
                    .map_err(|_| crate::WorkError::Overflow)? as u64;
            let allocation = ctx.claim_owned(bytes, 1)?;
            self.provider.watch(
                Arc::new(AffectedSink {
                    view: self.view,
                    sink,
                    bytes,
                    _allocation: allocation,
                }),
                &mut ctx,
            )
        })();
        work.finish(result, &ctx)
    }
}

struct ComputedFile<V: View> {
    view: V,
    cut: PinnedView<V::Cut>,
    path: NamespacePath,
    bytes: u64,
    _allocation: KernelOwnedAllocation,
}
impl<V: View> DemandFile for ComputedFile<V> {
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
        let end = offset
            .checked_add(length)
            .ok_or(DemandError::InvalidRequest)?;
        bounded(
            self.view
                .bytes(&self.cut.cut, &self.path, offset..end, ctx)?,
            length,
        )
    }
}

fn bounded(chunk: OwnedSourceChunk, length: u64) -> Result<OwnedSourceChunk, DemandError> {
    if chunk.bytes.len() as u64 > length {
        return Err(DemandError::InvalidRequest);
    }
    Ok(chunk)
}

struct AffectedSink<V> {
    view: V,
    sink: Arc<dyn SourceChangeSink>,
    bytes: u64,
    _allocation: KernelOwnedAllocation,
}
impl<V: View> ViewRecordSink<V::Record> for AffectedSink<V> {
    fn logical_owned_bytes(&self) -> u64 {
        self.bytes + self.sink.logical_owned_bytes()
    }
    fn record(
        &self,
        record: &V::Record,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        let cancellation = ctx.cancellation().clone();
        self.view.affected(record, &mut |path| {
            let (path, allocation) = path.clone_with_accounting(ctx, &cancellation)?;
            let bytes = path.logical_owned_bytes();
            let change = Owned::new(SourceChange::Entry(path), allocation, bytes);
            let result = self
                .sink
                .source_changed(std::slice::from_ref(&change.value), ctx);
            change.retire(ctx)?;
            result
        })
    }
    fn delivery_lost(&self) {
        self.sink.delivery_lost();
    }
}
