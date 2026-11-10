use super::*;
use crate::{
    facade::FsWork,
    kernel::{KernelAccounting, KernelOwnedAllocation, LogicalName, Owned, OwnedSourceChunk},
};

/// Mount one statically typed source beneath a component. Compose these values
/// recursively for multiple prefixes or deeper paths; no registry or task is
/// created. The mounted source wins over the same name in the base.
pub struct Routed<Base, Views> {
    base: Base,
    prefix: LogicalName,
    views: Views,
}

impl<B, V> Routed<B, V> {
    /// Attach a source at an already validated namespace component.
    pub fn new(base: B, prefix: LogicalName, views: V) -> Self {
        Self {
            base,
            prefix,
            views,
        }
    }
}

impl<B: DemandSource, V: DemandSource> Routed<B, V> {
    fn combined(&self, base: SourceReference, view: SourceReference) -> [u8; 32] {
        let mut hash = blake3::Hasher::new_derive_key("acyclic.fs.demand.routed.v1");
        hash.update(&base.identity);
        hash.update(&base.epoch.to_be_bytes());
        hash.update(&view.identity);
        hash.update(&view.epoch.to_be_bytes());
        hash.update(&[self.prefix.encoding() as u8]);
        hash.update(self.prefix.as_bytes());
        *hash.finalize().as_bytes()
    }

    fn sources(
        &self,
        source: SourceReference,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(SourceReference, SourceReference), DemandError> {
        ctx.check()?;
        let base = self.base.reference();
        let view = self.views.reference();
        if source != Self::reference_of(self.combined(base, view)) {
            return Err(DemandError::StaleSource);
        }
        Ok((base, view))
    }

    fn reference_of(hash: [u8; 32]) -> SourceReference {
        let mut identity = [0; 16];
        identity.copy_from_slice(&hash[..16]);
        SourceReference { identity, epoch: 0 }
    }

    fn mounted(&self, path: &NamespacePath) -> bool {
        path.components().first() == Some(&self.prefix)
    }

    fn root(&self, base: SourceReference, view: SourceReference) -> SourceNode {
        let mut node = Empty::node();
        node.file_identity = self.combined(
            SourceReference { epoch: 0, ..base },
            SourceReference { epoch: 0, ..view },
        );
        node.version = SourceVersion(self.combined(base, view));
        node
    }

    fn push(
        entries: &mut Owned<Vec<SourceDirectoryEntry>>,
        name: &LogicalName,
        node: SourceNode,
        ctx: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), DemandError> {
        let (name, mut guard) = name.clone_with_accounting(ctx, cancellation)?;
        entries.bytes += name.logical_owned_bytes();
        entries.allocation.absorb(&mut guard)?;
        entries.ensure_for_push(usize::MAX, ctx)?;
        entries.value.push(SourceDirectoryEntry { name, node });
        Ok(())
    }
}

impl<B: DemandSource, V: DemandSource> DemandSource for Routed<B, V> {
    fn watch<W: FsWork + Send>(
        &self,
        sink: std::sync::Arc<dyn SourceChangeSink>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<Box<dyn SourceWatch>>, DemandError> {
        let mut ctx = work.accounting(cancellation);
        let result = (|| {
            ctx.check()?;
            let (prefix, mut allocation) =
                self.prefix.clone_with_accounting(&mut ctx, cancellation)?;
            let sink_bytes =
                crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<PrefixSink>())
                    .map_err(|_| crate::WorkError::Overflow)? as u64;
            let mut header = ctx.claim_owned(sink_bytes, 1)?;
            allocation.absorb(&mut header)?;
            let prefix_bytes = prefix.logical_owned_bytes();
            let prefixed = std::sync::Arc::new(PrefixSink {
                prefix,
                sink: std::sync::Arc::clone(&sink),
                bytes: sink_bytes + prefix_bytes,
                _allocation: allocation,
            });
            let base = source_sync!(&mut ctx, |work| self.base.watch(sink, work, cancellation))?;
            let view = source_sync!(&mut ctx, |work| self.views.watch(
                prefixed,
                work,
                cancellation
            ))?;
            let (Some(base), Some(view)) = (base, view) else {
                return Ok(None);
            };
            let allocation = ctx.claim_owned(std::mem::size_of::<RoutedWatch>() as u64, 1)?;
            Ok(Some(Box::new(RoutedWatch {
                base,
                view,
                _allocation: allocation,
            }) as Box<dyn SourceWatch>))
        })();
        work.finish(result, &ctx)
    }
    fn reference(&self) -> SourceReference {
        Self::reference_of(self.combined(self.base.reference(), self.views.reference()))
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
            let (base, view) = self.sources(source, &mut ctx)?;
            let node = if self.mounted(path) {
                let (relative, _allocation) =
                    path.suffix_with_accounting(1, &mut ctx, cancellation)?;
                source_call!(&mut ctx, cancellation, |work| self.views.lookup(
                    view,
                    &relative,
                    work,
                    cancellation
                ))?
            } else if path.is_root() {
                Some(self.root(base, view))
            } else {
                source_call!(&mut ctx, cancellation, |work| self.base.lookup(
                    base,
                    path,
                    work,
                    cancellation
                ))?
            };
            self.sources(source, &mut ctx)?;
            Ok(node)
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
            let (base, view) = self.sources(source, &mut ctx)?;
            if maximum_entries == 0 {
                return Err(DemandError::InvalidRequest);
            }
            let first = cursor.is_none();
            let next = match cursor {
                Some(cursor)
                    if directory.is_root() && cursor.source == source && cursor.token == 0 =>
                {
                    None
                }
                other => other,
            };
            let page = if self.mounted(directory) {
                let (relative, _allocation) =
                    directory.suffix_with_accounting(1, &mut ctx, cancellation)?;
                source_call!(&mut ctx, cancellation, |work| self.views.list_page(
                    view,
                    &relative,
                    next,
                    maximum_entries,
                    work,
                    cancellation
                ))?
            } else if !directory.is_root() {
                source_call!(&mut ctx, cancellation, |work| self.base.list_page(
                    base,
                    directory,
                    next,
                    maximum_entries,
                    work,
                    cancellation
                ))?
            } else {
                let prefix_node = if first {
                    source_call!(&mut ctx, cancellation, |work| self.views.lookup(
                        view,
                        directory,
                        work,
                        cancellation
                    ))?
                } else {
                    None
                };
                let reserved = u32::from(prefix_node.is_some());
                let mut entries = Owned::allocate(reserved as usize, &mut ctx)?;
                if let Some(node) = prefix_node {
                    Self::push(&mut entries, &self.prefix, node, &mut ctx, cancellation)?;
                }
                let mut next_page = None;
                let mut publication = None;
                if maximum_entries == reserved {
                    // The next request starts the base enumeration. No cursor
                    // registry is needed, even for a one-entry page.
                    next_page = Some(SourceCursor::detached(source, 0));
                } else {
                    let base_page =
                        source_call!(&mut ctx, cancellation, |work| self.base.list_page(
                            base,
                            directory,
                            next,
                            maximum_entries - reserved,
                            work,
                            cancellation
                        ))?;
                    for entry in &base_page.entries {
                        if entry.name != self.prefix {
                            Self::push(
                                &mut entries,
                                &entry.name,
                                entry.node,
                                &mut ctx,
                                cancellation,
                            )?;
                        }
                    }
                    next_page = base_page.next;
                    publication = base_page.publication;
                }
                let mut page = SourceDirectoryPage::stateless(
                    entries,
                    next_page,
                    self.root(base, view).version,
                    &mut ctx,
                )?;
                page.publication = publication;
                self.sources(source, &mut ctx)?;
                return Ok(page);
            };
            self.sources(source, &mut ctx)?;
            Ok(page)
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
            let (base, view) = self.sources(source, &mut ctx)?;
            if path.is_root() {
                return Err(DemandError::NotRegularFile);
            }
            if self.mounted(path) {
                let (relative, _allocation) =
                    path.suffix_with_accounting(1, &mut ctx, cancellation)?;
                source_call!(&mut ctx, cancellation, |work| self.views.read_range(
                    view,
                    &relative,
                    expected,
                    offset,
                    length,
                    work,
                    cancellation
                ))
            } else {
                source_call!(&mut ctx, cancellation, |work| self.base.read_range(
                    base,
                    path,
                    expected,
                    offset,
                    length,
                    work,
                    cancellation
                ))
            }
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
            let (base, view) = self.sources(source, &mut ctx)?;
            if path.is_root() {
                return Err(DemandError::NotRegularFile);
            }
            if self.mounted(path) {
                let (relative, _allocation) =
                    path.suffix_with_accounting(1, &mut ctx, cancellation)?;
                source_call!(&mut ctx, cancellation, |work| self.views.open_file(
                    view,
                    &relative,
                    expected,
                    work,
                    cancellation
                ))
            } else {
                source_call!(&mut ctx, cancellation, |work| self.base.open_file(
                    base,
                    path,
                    expected,
                    work,
                    cancellation
                ))
            }
        }
        .await;
        work.finish(result, &ctx)
    }

    async fn read_link<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<OwnedSourceChunk, DemandError> {
        let mut ctx = work.accounting(cancellation);
        let result = async {
            let (base, view) = self.sources(source, &mut ctx)?;
            if path.is_root() {
                return Err(DemandError::InvalidRequest);
            }
            if self.mounted(path) {
                let (relative, _allocation) =
                    path.suffix_with_accounting(1, &mut ctx, cancellation)?;
                source_call!(&mut ctx, cancellation, |work| self.views.read_link(
                    view,
                    &relative,
                    expected,
                    maximum_bytes,
                    work,
                    cancellation
                ))
            } else {
                source_call!(&mut ctx, cancellation, |work| self.base.read_link(
                    base,
                    path,
                    expected,
                    maximum_bytes,
                    work,
                    cancellation
                ))
            }
        }
        .await;
        work.finish(result, &ctx)
    }
}

struct PrefixSink {
    prefix: LogicalName,
    sink: std::sync::Arc<dyn SourceChangeSink>,
    bytes: u64,
    _allocation: KernelOwnedAllocation,
}
impl SourceChangeSink for PrefixSink {
    fn observes_birth_time(&self) -> bool {
        self.sink.observes_birth_time()
    }
    fn logical_owned_bytes(&self) -> u64 {
        self.bytes + self.sink.logical_owned_bytes()
    }
    fn source_changed(
        &self,
        changes: &[SourceChange],
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        let cancellation = ctx.cancellation().clone();
        let mut mapped = Owned::allocate(changes.len(), ctx)?;
        for change in changes {
            ctx.charge_items(1)?;
            let change = match change {
                SourceChange::Departed(path)
                | SourceChange::Name(path)
                | SourceChange::Entry(path) => {
                    let (path, mut allocation) =
                        path.prepend_with_accounting(&self.prefix, ctx, &cancellation)?;
                    mapped.bytes += path.logical_owned_bytes();
                    mapped.allocation.absorb(&mut allocation)?;
                    match change {
                        SourceChange::Departed(_) => SourceChange::Departed(path),
                        SourceChange::Name(_) => SourceChange::Name(path),
                        _ => SourceChange::Entry(path),
                    }
                }
                SourceChange::Node(node) => SourceChange::Node(*node),
                SourceChange::Everything => SourceChange::Everything,
                SourceChange::Unconfirmed => SourceChange::Unconfirmed,
            };
            mapped.value.push(change);
        }
        self.sink.source_changed(&mapped.value, ctx)
    }
    fn delivery_lost(&self) {
        self.sink.delivery_lost();
    }
}

struct RoutedWatch {
    base: Box<dyn SourceWatch>,
    view: Box<dyn SourceWatch>,
    _allocation: KernelOwnedAllocation,
}
impl SourceWatch for RoutedWatch {
    fn fence(&self, ctx: &mut KernelAccounting<'_>) -> Result<SourceFence, DemandError> {
        let base = self.base.fence(ctx)?;
        let view = self.view.fence(ctx)?;
        Ok(
            if base == SourceFence::Confirmed && view == SourceFence::Confirmed {
                SourceFence::Confirmed
            } else {
                SourceFence::Unconfirmed
            },
        )
    }
    fn stop(&self, ctx: &mut KernelAccounting<'_>) -> Result<(), DemandError> {
        let base = self.base.stop(ctx);
        let view = self.view.stop(ctx);
        base.and(view)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_with_cleanup_wait(&self, wait: &CancellationToken) -> Result<(), DemandError> {
        let base = self.base.stop_with_cleanup_wait(wait);
        let view = self.view.stop_with_cleanup_wait(wait);
        base.and(view)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        let base = self.base.stop_accepted_cleanup();
        let view = self.view.stop_accepted_cleanup();
        base.and(view)
    }
    fn is_stopped(&self) -> bool {
        self.base.is_stopped() && self.view.is_stopped()
    }
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
            + self.base.logical_owned_bytes()
            + self.view.logical_owned_bytes()
    }
    fn is_exact(&self) -> bool {
        self.base.is_exact() && self.view.is_exact()
    }
}
