use super::*;
use crate::NativeMaterializationWork;
/// A composable source view that hides exact subtrees before they enter lazy
#[derive(Clone)]
pub struct FilteredDemandSource<D> {
    inner: D,
    excluded: std::sync::Arc<Vec<NamespacePath>>,
}

impl<D> FilteredDemandSource<D> {
    /// Wraps a source with canonical excluded subtree roots.
    #[must_use]
    pub fn new(inner: D, excluded: Vec<NamespacePath>) -> Self {
        Self {
            inner,
            excluded: std::sync::Arc::new(excluded),
        }
    }

    /// Returns the wrapped source capability.
    #[must_use]
    pub const fn inner(&self) -> &D {
        &self.inner
    }

    fn excludes(&self, path: &NamespacePath) -> bool {
        self.excluded.iter().any(|root| path.is_within(root))
    }

    fn excludes_child(&self, directory: &NamespacePath, name: &crate::kernel::LogicalName) -> bool {
        self.excluded.iter().any(|root| {
            root.components().len() == directory.components().len().saturating_add(1)
                && root.components().starts_with(directory.components())
                && root.components().last() == Some(name)
        })
    }
}

/// Forwards changes outside a filtered source's excluded subtrees, which no
struct FilteredChangeSink {
    excluded: std::sync::Arc<Vec<NamespacePath>>,
    sink: std::sync::Arc<dyn SourceChangeSink>,
}

impl FilteredChangeSink {
    fn visible(&self, change: &SourceChange) -> bool {
        match change {
            SourceChange::Name(path) | SourceChange::Entry(path) => {
                !self.excluded.iter().any(|root| path.is_within(root))
            }
            _ => true,
        }
    }
    fn batches<'a>(
        &'a self,
        changes: &'a [SourceChange],
    ) -> impl Iterator<Item = &'a [SourceChange]> {
        changes
            .chunk_by(|a, b| self.visible(a) == self.visible(b))
            .filter(|batch| self.visible(&batch[0]))
    }
}
impl SourceChangeSink for FilteredChangeSink {
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64 + self.sink.logical_owned_bytes()
    }
    fn source_changed(
        &self,
        changes: &[SourceChange],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> DemandResult<()> {
        let mut work = WorkCounters::UNCHARGED;
        let result = (|| {
            for batch in self.batches(changes) {
                cancellation.check().map_err(|_| DemandError::Cancelled)?;
                match self
                    .sink
                    .source_changed(batch, work.remaining(budget)?, cancellation)
                {
                    Ok(receipt) => work.try_add_assign(&receipt.work)?,
                    Err(failure) => {
                        work.try_add_assign(&failure.work)?;
                        return Err(failure.error);
                    }
                }
            }
            Ok(())
        })();
        result
            .map(|value| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn source_changed_with_native_work(
        &self,
        changes: &[SourceChange],
        work: NativeMaterializationWork,
    ) -> Result<(), DemandError> {
        crate::native_host::NativeHostWork::check(&work)?;
        for batch in self.batches(changes) {
            self.sink
                .source_changed_with_native_work(batch, work.clone())?;
        }
        Ok(())
    }
}

impl<D: DemandSource> DemandSource for FilteredDemandSource<D> {
    fn reference(&self) -> SourceReference {
        self.inner.reference()
    }
    fn watch<W: crate::facade::FsWork + Send>(
        &self,
        sink: std::sync::Arc<dyn SourceChangeSink>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<Box<dyn SourceWatch>>, DemandError> {
        self.inner.watch(
            std::sync::Arc::new(FilteredChangeSink {
                excluded: self.excluded.clone(),
                sink,
            }),
            work,
            cancellation,
        )
    }
    async fn lookup<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<SourceNode>, DemandError> {
        if self.excludes(path) {
            return work.finish(Ok(None), &work.accounting(cancellation));
        }
        self.inner.lookup(source, path, work, cancellation).await
    }
    async fn list_page<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<SourceDirectoryPage, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if self.excludes(directory) {
                return Err(DemandError::Absent);
            }
            let mut page = async {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = accounting.native_work() {
                    return self
                        .inner
                        .list_page(
                            source,
                            directory,
                            cursor,
                            maximum_entries,
                            original,
                            cancellation,
                        )
                        .await;
                }
                let budget = accounting.remaining().map_err(DemandError::from)?;
                let answer = self
                    .inner
                    .list_page(
                        source,
                        directory,
                        cursor,
                        maximum_entries,
                        budget,
                        cancellation,
                    )
                    .await;
                accept_source_result(answer, &mut accounting)
            }
            .await?;
            page.entries
                .retain(|entry| !self.excludes_child(directory, &entry.name));
            Ok(page)
        }
        .await;
        work.finish(result, &accounting)
    }
    async fn read_range<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        offset: u64,
        length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
        if self.excludes(path) {
            return work.finish(Err(DemandError::Absent), &work.accounting(cancellation));
        }
        self.inner
            .read_range(source, path, expected, offset, length, work, cancellation)
            .await
    }
    async fn open_file<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Box<dyn DemandFile>, DemandError> {
        if self.excludes(path) {
            return work.finish(Err(DemandError::Absent), &work.accounting(cancellation));
        }
        self.inner
            .open_file(source, path, expected, work, cancellation)
            .await
    }
    async fn read_link<W: crate::facade::FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
        if self.excludes(path) {
            return work.finish(Err(DemandError::Absent), &work.accounting(cancellation));
        }
        self.inner
            .read_link(source, path, expected, maximum_bytes, work, cancellation)
            .await
    }
}
