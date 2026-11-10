//! Static source composition; nesting adds routes without a dynamic registry.
use super::*;
use crate::facade::FsWork;
use crate::kernel::{KernelAccounting, LogicalName, OwnedSourceChunk};
use crate::kernel::{KernelOwnedAllocation, Owned};
use std::sync::Arc;
mod watch;

macro_rules! delegate {
    ($ctx:expr, $cancel:expr, |$work:ident| $call:expr) => {{
        async {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some($work) = $ctx.native_work() {
                return $call.await;
            }
            let $work = $ctx.remaining()?;
            accept_source_result($call.await, $ctx)
        }
        .await
    }};
}

/// A source mounted at one root component, with all other paths supplied by
/// the base. Nest this combinator for siblings or for deeper prefixes.
/// A change to either child's epoch invalidates prior routed references and
/// cursors, exactly as an invalidated native source reference does.
pub struct Routed<B, V> {
    base: B,
    view: V,
    prefix: LogicalName,
    limits: crate::VolumeLimits,
}

impl<B: DemandSource, V: DemandSource> Routed<B, V> {
    /// Takes ownership of a static route. No child is queried at construction.
    pub fn new(base: B, prefix: LogicalName, view: V, limits: crate::VolumeLimits) -> Self {
        Self {
            base,
            prefix,
            view,
            limits,
        }
    }

    fn combined(&self, base: SourceReference, view: SourceReference) -> SourceReference {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic:routed:v1");
        hash.update(&base.identity);
        hash.update(&view.identity);
        hash.update(&[self.prefix.encoding().tag()]);
        hash.update(self.prefix.as_bytes());
        let (epoch, carry) = base.epoch.overflowing_add(view.epoch);
        hash.update(&[u8::from(carry)]);
        let mut identity = [0; 16];
        identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        SourceReference { identity, epoch }
    }

    fn sources(
        &self,
        source: SourceReference,
    ) -> Result<(SourceReference, SourceReference), DemandError> {
        let base = self.base.reference();
        let view = self.view.reference();
        if source != self.combined(base, view) {
            return Err(DemandError::StaleSource);
        }
        Ok((base, view))
    }

    fn routed(&self, path: &NamespacePath) -> bool {
        path.components().first() == Some(&self.prefix)
    }

    fn root_version(&self, base: SourceVersion, view: SourceVersion) -> SourceVersion {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic:routed:directory:v1");
        hash.update(&base.0);
        hash.update(&view.0);
        hash.update(self.prefix.as_bytes());
        SourceVersion(*hash.finalize().as_bytes())
    }
}

impl<B: DemandSource, V: DemandSource> DemandSource for Routed<B, V> {
    fn overlay(&self, components: &[LogicalName]) -> SourceOverlay {
        if let Some((first, rest)) = components.split_first()
            && first == &self.prefix
        {
            self.view.overlay(rest)
        } else {
            self.base.overlay(components)
        }
    }
    fn watch<W: FsWork + Send>(
        &self,
        sink: Arc<dyn SourceChangeSink>,
        work: W,
        cancel: &CancellationToken,
    ) -> W::Output<Option<Box<dyn SourceWatch>>, DemandError> {
        let mut ctx = work.accounting(cancel);
        let result = self.start_watch(sink, &mut ctx, cancel);
        work.finish(result, &ctx)
    }
    fn reference(&self) -> SourceReference {
        self.combined(self.base.reference(), self.view.reference())
    }

    async fn lookup<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        work: W,
        cancel: &CancellationToken,
    ) -> W::Output<Option<SourceNode>, DemandError> {
        let mut ctx = work.accounting(cancel);
        let result = async {
            ctx.check()?;
            let (base, view) = self.sources(source)?;
            if self.routed(path) {
                let (relative, allocation) = path.strip_first_with_accounting(&mut ctx, cancel)?;
                let result = delegate!(&mut ctx, cancel, |work| self
                    .view
                    .lookup(view, &relative, work, cancel));
                let bytes = relative.logical_owned_bytes();
                drop(relative);
                crate::kernel::retire_allocation(allocation, bytes, &mut ctx)?;
                return result.map(|node| node.map(|node| scoped_node(view.identity, node)));
            }
            let mut node = delegate!(&mut ctx, cancel, |work| self
                .base
                .lookup(base, path, work, cancel))?
            .map(|node| scoped_node(base.identity, node));
            if path.is_root() {
                let view_node = delegate!(&mut ctx, cancel, |work| self
                    .view
                    .lookup(view, path, work, cancel))?
                .ok_or(DemandError::SourceUnavailable)?;
                let root = node.as_mut().ok_or(DemandError::SourceUnavailable)?;
                if root.kind != SourceNodeKind::Directory
                    || view_node.kind != SourceNodeKind::Directory
                {
                    return Err(DemandError::NotDirectory);
                }
                root.version = self.root_version(root.version, view_node.version);
            }
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
        cancel: &CancellationToken,
    ) -> W::Output<SourceDirectoryPage, DemandError> {
        let mut ctx = work.accounting(cancel);
        let result = async {
            ctx.check()?;
            if maximum_entries == 0 {
                return Err(DemandError::InvalidRequest);
            }
            let (base, view) = self.sources(source)?;
            if self.routed(directory) {
                let (relative, allocation) =
                    directory.strip_first_with_accounting(&mut ctx, cancel)?;
                let result = delegate!(&mut ctx, cancel, |work| self.view.list_page(
                    view,
                    &relative,
                    cursor,
                    maximum_entries,
                    work,
                    cancel
                ));
                let bytes = relative.logical_owned_bytes();
                drop(relative);
                crate::kernel::retire_allocation(allocation, bytes, &mut ctx)?;
                return scope_page(result?, view.identity, &mut ctx);
            }
            if !directory.is_root() {
                return scope_page(
                    delegate!(&mut ctx, cancel, |work| self.base.list_page(
                        base,
                        directory,
                        cursor,
                        maximum_entries,
                        work,
                        cancel
                    ))?,
                    base.identity,
                    &mut ctx,
                );
            }
            let first = cursor.is_none();
            let cursor = match cursor {
                Some(cursor) if cursor.source == source && cursor.token == 0 => None,
                other => other,
            };
            let prefix_node = delegate!(&mut ctx, cancel, |work| self
                .view
                .lookup(view, directory, work, cancel))?
            .ok_or(DemandError::SourceUnavailable)?;
            if prefix_node.kind != SourceNodeKind::Directory {
                return Err(DemandError::NotDirectory);
            }
            if first && maximum_entries == 1 {
                // No base page is requested until the next demand.
                let base_node = delegate!(&mut ctx, cancel, |work| self
                    .base
                    .lookup(base, directory, work, cancel))?
                .ok_or(DemandError::SourceUnavailable)?;
                return SourceDirectoryPage::copied(
                    std::iter::once((&self.prefix, scoped_node(view.identity, prefix_node))),
                    Some(SourceCursor::detached(source, 0)),
                    self.root_version(base_node.version, prefix_node.version),
                    &mut ctx,
                );
            }
            let mut page = delegate!(&mut ctx, cancel, |work| self.base.list_page(
                base,
                directory,
                cursor,
                maximum_entries - u32::from(first),
                work,
                cancel
            ))?;
            let entries = page
                .entries
                .iter()
                .filter(|entry| entry.name != self.prefix)
                .map(|entry| (&entry.name, scoped_node(base.identity, entry.node)));
            let prefix = first.then_some((&self.prefix, scoped_node(view.identity, prefix_node)));
            let mut output = SourceDirectoryPage::copied(
                prefix.into_iter().chain(entries),
                page.next.take(),
                self.root_version(page.version, prefix_node.version),
                &mut ctx,
            )?;
            output.publication = page.publication.take();
            Ok(output)
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
        cancel: &CancellationToken,
    ) -> W::Output<OwnedSourceChunk, DemandError> {
        let mut ctx = work.accounting(cancel);
        let result = async {
            ctx.check()?;
            let (base, view) = self.sources(source)?;
            if !self.routed(path) {
                return delegate!(&mut ctx, cancel, |work| self
                    .base
                    .read_range(base, path, expected, offset, length, work, cancel));
            }
            let (relative, allocation) = path.strip_first_with_accounting(&mut ctx, cancel)?;
            let result = delegate!(&mut ctx, cancel, |work| self
                .view
                .read_range(view, &relative, expected, offset, length, work, cancel));
            let bytes = relative.logical_owned_bytes();
            drop(relative);
            crate::kernel::retire_allocation(allocation, bytes, &mut ctx)?;
            result
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
        cancel: &CancellationToken,
    ) -> W::Output<Box<dyn DemandFile>, DemandError> {
        let mut ctx = work.accounting(cancel);
        let result = async {
            ctx.check()?;
            let (base, view) = self.sources(source)?;
            if !self.routed(path) {
                return delegate!(&mut ctx, cancel, |work| self
                    .base
                    .open_file(base, path, expected, work, cancel));
            }
            let (relative, allocation) = path.strip_first_with_accounting(&mut ctx, cancel)?;
            let result = delegate!(&mut ctx, cancel, |work| self
                .view
                .open_file(view, &relative, expected, work, cancel));
            let bytes = relative.logical_owned_bytes();
            drop(relative);
            crate::kernel::retire_allocation(allocation, bytes, &mut ctx)?;
            result
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
        cancel: &CancellationToken,
    ) -> W::Output<OwnedSourceChunk, DemandError> {
        let mut ctx = work.accounting(cancel);
        let result = async {
            ctx.check()?;
            let (base, view) = self.sources(source)?;
            if !self.routed(path) {
                return delegate!(&mut ctx, cancel, |work| self.base.read_link(
                    base,
                    path,
                    expected,
                    maximum_bytes,
                    work,
                    cancel
                ));
            }
            let (relative, allocation) = path.strip_first_with_accounting(&mut ctx, cancel)?;
            let result = delegate!(&mut ctx, cancel, |work| self.view.read_link(
                view,
                &relative,
                expected,
                maximum_bytes,
                work,
                cancel
            ));
            let bytes = relative.logical_owned_bytes();
            drop(relative);
            crate::kernel::retire_allocation(allocation, bytes, &mut ctx)?;
            result
        }
        .await;
        work.finish(result, &ctx)
    }
}

fn scoped_node(source: [u8; 16], mut node: SourceNode) -> SourceNode {
    let mut hash = blake3::Hasher::new();
    hash.update(&source);
    hash.update(&node.file_identity);
    node.file_identity = *hash.finalize().as_bytes();
    node
}

fn scope_page(
    mut page: SourceDirectoryPage,
    source: [u8; 16],
    ctx: &mut KernelAccounting<'_>,
) -> Result<SourceDirectoryPage, DemandError> {
    let mut output = SourceDirectoryPage::copied(
        page.entries
            .iter()
            .map(|entry| (&entry.name, scoped_node(source, entry.node))),
        page.next.take(),
        page.version,
        ctx,
    )?;
    output.publication = page.publication.take();
    Ok(output)
}
