use super::*;
/// A composable source view that hides exact subtrees before they enter lazy
/// observation state. This is the SDK boundary used for host-private entries
/// such as a physical checkout's `.git` directory.
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
/// view of it can hold facts about.
struct FilteredChangeSink {
    excluded: std::sync::Arc<Vec<NamespacePath>>,
    sink: std::sync::Arc<dyn SourceChangeSink>,
}

impl SourceChangeSink for FilteredChangeSink {
    fn source_changed(&self, changes: &[SourceChange]) {
        let visible = changes
            .iter()
            .filter(|change| match change {
                SourceChange::Name(path) | SourceChange::Entry(path) => {
                    !self.excluded.iter().any(|root| path.is_within(root))
                }
                SourceChange::Node(_) | SourceChange::Everything | SourceChange::Unconfirmed => {
                    true
                }
            })
            .cloned()
            .collect::<Vec<_>>();
        if !visible.is_empty() {
            self.sink.source_changed(&visible);
        }
    }
}

#[async_trait::async_trait]
impl<D: DemandSource> DemandSource for FilteredDemandSource<D> {
    fn reference(&self) -> SourceReference {
        self.inner.reference()
    }

    fn watch(
        &self,
        sink: std::sync::Arc<dyn SourceChangeSink>,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
        self.inner.watch(std::sync::Arc::new(FilteredChangeSink {
            excluded: std::sync::Arc::clone(&self.excluded),
            sink,
        }))
    }

    async fn lookup(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        cancellation: &CancellationToken,
    ) -> DemandResult<Option<SourceNode>> {
        if self.excludes(path) {
            return Ok(OperationReceipt {
                value: None,
                work: WorkCounters::default(),
            });
        }
        self.inner.lookup(source, path, cancellation).await
    }

    async fn list_page(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        cancellation: &CancellationToken,
    ) -> DemandResult<SourceDirectoryPage> {
        if self.excludes(directory) {
            return Err(OperationFailure::new(
                DemandError::Absent,
                WorkCounters::default(),
            ));
        }
        let mut page = self
            .inner
            .list_page(source, directory, cursor, maximum_entries, cancellation)
            .await?;
        page.value
            .entries
            .retain(|entry| !self.excludes_child(directory, &entry.name));
        Ok(page)
    }

    async fn read_range(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        offset: u64,
        length: u64,
        cancellation: &CancellationToken,
    ) -> DemandResult<Bytes> {
        if self.excludes(path) {
            return Err(OperationFailure::new(
                DemandError::Absent,
                WorkCounters::default(),
            ));
        }
        self.inner
            .read_range(source, path, expected, offset, length, cancellation)
            .await
    }

    async fn open_file(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        cancellation: &CancellationToken,
    ) -> DemandResult<Box<dyn DemandFile>> {
        if self.excludes(path) {
            return Err(OperationFailure::new(
                DemandError::Absent,
                WorkCounters::default(),
            ));
        }
        self.inner
            .open_file(source, path, expected, cancellation)
            .await
    }

    async fn read_link(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        cancellation: &CancellationToken,
    ) -> DemandResult<Bytes> {
        if self.excludes(path) {
            return Err(OperationFailure::new(
                DemandError::Absent,
                WorkCounters::default(),
            ));
        }
        self.inner
            .read_link(source, path, expected, cancellation)
            .await
    }
}
