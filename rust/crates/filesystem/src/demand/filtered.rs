use super::*;
use crate::kernel::allocation::KernelAccounting;

/// A source view that hides excluded subtrees.
#[derive(Clone)]
pub struct FilteredDemandSource<D> {
    inner: D,
    excluded: std::sync::Arc<FilteredExcludedPaths>,
}

struct FilteredExcludedPaths {
    roots: Vec<NamespacePath>,
    #[cfg(not(target_arch = "wasm32"))]
    custody: Option<crate::NativeAllocation>,
}

impl FilteredExcludedPaths {
    fn excludes(&self, path: &NamespacePath, child: Option<&crate::kernel::LogicalName>,
        accounting: &mut KernelAccounting<'_>) -> Result<bool, DemandError> {
        accounting.check().map_err(SourceCursor::cursor_accounting_error)?;
        let mut count_components = false;
        #[cfg(not(target_arch = "wasm32"))]
        { count_components = accounting.native_work().is_some(); }
        for root in &self.roots {
            accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
                .map_err(SourceCursor::cursor_accounting_error)?;
            let depth = path.depth() + usize::from(child.is_some());
            if if child.is_some() { depth != root.depth() } else { depth < root.depth() } { continue; }
            let mut matched = true;
            for (actual, expected) in path.components().iter().chain(child).zip(root.components()) {
                if count_components {
                    accounting.charge(WorkCounters { items_examined: 1, source_path_components: 1,
                        ..WorkCounters::UNCHARGED }).map_err(SourceCursor::cursor_accounting_error)?;
                }
                if actual != expected { matched = false; break; }
            }
            if matched { return Ok(true); }
        }
        Ok(false)
    }
}

impl<D> FilteredDemandSource<D> {
    /// Wraps caller-owned exclusions.
    pub fn new(inner: D, roots: Vec<NamespacePath>) -> Self {
        Self { inner, excluded: std::sync::Arc::new(FilteredExcludedPaths {
            roots,
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        }) }
    }

    /// Transfers admitted exclusion data to a native view.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(inner: D, roots: Vec<NamespacePath>, mut custody: crate::NativeAllocation,
        work: &crate::NativeMaterializationWork) -> Result<Self, crate::NativeTreeMaterializationError> {
        if !custody.work().shares_ledger(work) { return Err(crate::NativeTreeMaterializationError::InvalidLayout); }
        let mut bytes = (roots.capacity() as u64).checked_mul(std::mem::size_of::<NamespacePath>() as u64)
            .ok_or(crate::WorkError::Overflow)?;
        for path in &roots {
            work.charge(WorkCounters { items_examined: 1, source_path_components: path.depth() as u64,
                ..WorkCounters::UNCHARGED })?;
            work.charge(WorkCounters { items_examined: path.depth() as u64, ..WorkCounters::UNCHARGED })?;
            bytes = bytes.checked_add(path.retained_bytes()).ok_or(crate::WorkError::Overflow)?;
        }
        if custody.bytes() < bytes { return Err(crate::NativeTreeMaterializationError::InvalidLayout); }
        let mut header = work.claim(std::mem::size_of::<FilteredExcludedPaths>() as u64, 1, WorkCounters::UNCHARGED)?;
        custody.absorb(&mut header)?;
        Ok(Self { inner, excluded: std::sync::Arc::new(FilteredExcludedPaths { roots, custody: Some(custody) }) })
    }

    /// Attaches an enclosing owner to the shared exclusions.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn retain_owner_allocation(&mut self, owner: &mut crate::NativeAllocation)
        -> Result<(), crate::NativeTreeMaterializationError> {
        std::sync::Arc::get_mut(&mut self.excluded).and_then(|paths| paths.custody.as_mut())
            .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?.absorb(owner)
    }

    /// Borrows the wrapped source.
    pub const fn inner(&self) -> &D { &self.inner }

    fn filter_page(&self, directory: &NamespacePath, mut page: SourceDirectoryPage,
        accounting: &mut KernelAccounting<'_>) -> Result<SourceDirectoryPage, DemandError> {
        if self.excluded.roots.is_empty() || page.entries.is_empty() { return Ok(page); }
        let count = page.entries.len();
        let bytes = count.checked_mul(std::mem::size_of::<usize>()).ok_or(crate::WorkError::Overflow)? as u64;
        let mut allocation = accounting.claim_owned(bytes, 1).map_err(SourceCursor::cursor_accounting_error)?;
        let mut selected = Vec::new();
        let projection = (|| {
            selected.try_reserve_exact(count).map_err(DemandError::Allocation)?;
            accounting.reconcile_owned(&mut allocation, bytes,
                (selected.capacity() * std::mem::size_of::<usize>()) as u64).map_err(SourceCursor::cursor_accounting_error)?;
            for index in 0..count {
                if !self.excluded.excludes(directory, Some(page.entries[index].name()), accounting)? {
                    selected.push(page.entries.physical_index(index).ok_or(DemandError::InvalidRequest)?);
                }
            }
            Ok(())
        })();
        if let Err(error) = projection {
            let retained = (selected.capacity() * std::mem::size_of::<usize>()) as u64;
            drop(selected);
            let cleanup = accounting.release_owned(&mut allocation, if retained == 0 { bytes } else { retained })
                .map_err(SourceCursor::cursor_accounting_error);
            return Err(error).and(cleanup.map(|()| page));
        }
        page.entries.set_selection(selected, SourcePayloadOwnership::from_kernel_allocation(allocation));
        Ok(page)
    }
}

struct FilteredChangeSink {
    excluded: std::sync::Arc<FilteredExcludedPaths>,
    sink: std::sync::Arc<dyn SourceChangeSink>,
    ownership: SourcePayloadOwnership,
}

impl FilteredChangeSink {
    fn visible(&self, change: &SourceChange, accounting: &mut KernelAccounting<'_>) -> Result<bool, DemandError> {
        accounting.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })
            .map_err(SourceCursor::cursor_accounting_error)?;
        match change {
            SourceChange::Name(path) | SourceChange::Entry(path) =>
                self.excluded.excludes(path, None, accounting).map(|hidden| !hidden),
            _ => Ok(true),
        }
    }

    fn report_changes(&self, changes: &[SourceChange], accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken, mut check: impl FnMut() -> Result<(), DemandError>) -> Result<(), DemandError> {
        check()?;
        let mut count = 0usize;
        for change in changes { check()?; count += usize::from(self.visible(change, accounting)?); }
        if count == 0 { return Ok(()); }
        let mut allocation = accounting.claim_owned(0, 0).map_err(SourceCursor::cursor_accounting_error)?;
        let mut visible = Vec::new();
        let mut retained = 0u64;
        let result = (|| {
            if count != changes.len() {
                let bytes = count.checked_mul(std::mem::size_of::<SourceChange>()).ok_or(crate::WorkError::Overflow)? as u64;
                allocation = accounting.claim_owned(bytes, 1).map_err(SourceCursor::cursor_accounting_error)?;
                retained = bytes;
                visible.try_reserve_exact(count).map_err(DemandError::Allocation)?;
                let actual = (visible.capacity() * std::mem::size_of::<SourceChange>()) as u64;
                accounting.reconcile_owned(&mut allocation, bytes, actual).map_err(SourceCursor::cursor_accounting_error)?;
                retained = actual;
                for change in changes {
                    check()?;
                    if !self.visible(change, accounting)? { continue; }
                    let projected = match change {
                        SourceChange::Name(path) | SourceChange::Entry(path) => {
                            let mut copy = path.clone_with_accounting(accounting, cancellation)
                                .map_err(SourceCursor::cursor_accounting_error)?;
                            allocation.absorb(&mut copy.1).map_err(SourceCursor::cursor_accounting_error)?;
                            retained = retained.checked_add(copy.0.retained_bytes()).ok_or(crate::WorkError::Overflow)?;
                            if matches!(change, SourceChange::Name(_)) { SourceChange::Name(copy.0) } else { SourceChange::Entry(copy.0) }
                        }
                        SourceChange::Node(identity) => {
                            let delta = WorkCounters { bytes_copied: 32, ..WorkCounters::UNCHARGED };
                            accounting.admit(delta).map_err(SourceCursor::cursor_accounting_error)?;
                            let node = SourceChange::Node(*identity);
                            accounting.record_completed(delta).map_err(SourceCursor::cursor_accounting_error)?;
                            node
                        }
                        SourceChange::Everything => SourceChange::Everything,
                        SourceChange::Unconfirmed => SourceChange::Unconfirmed,
                    };
                    visible.push(projected);
                }
            }
            check()?;
            let batch = if count == changes.len() { changes } else { &visible };
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = accounting.native_work() {
                    return self.sink.source_changed_with_native_work(batch,
                        NativeSourceWork::from_native_materialization_work(original));
                }
                let budget = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
                let answer = self.sink.source_changed(batch, budget, cancellation);
                accept_source_result(answer, accounting)
            })).map_err(|_| DemandError::WorkerUnavailable)?
        })();
        drop(visible);
        let cleanup = accounting.release_owned(&mut allocation, retained).map_err(SourceCursor::cursor_accounting_error);
        result.and(cleanup)
    }
}

impl SourceChangeSink for FilteredChangeSink {
    fn logical_owned_bytes(&self) -> u64 { std::mem::size_of::<Self>() as u64 + self.sink.logical_owned_bytes() }
    fn source_changed(&self, changes: &[SourceChange], budget: WorkBudget, cancellation: &CancellationToken) -> DemandResult<()> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = self.report_changes(changes, &mut accounting, cancellation,
            || cancellation.check().map_err(|_| DemandError::Cancelled));
        result.map(|value| OperationReceipt { value, work: accounting.spent() })
            .map_err(|error| OperationFailure::new(error, accounting.spent()))
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn source_changed_with_native_work(&self, changes: &[SourceChange], source: NativeSourceWork<'_>) -> Result<(), DemandError> {
        let original = source.materialization_work()?;
        self.report_changes(changes, &mut KernelAccounting::native(&original, original.cancellation()),
            original.cancellation(), || crate::native_host::NativeHostWork::check(&source))
    }
}

impl<D: DemandSource> DemandSource for FilteredDemandSource<D> {
    fn reference(&self) -> SourceReference { self.inner.reference() }
    fn watch<W: crate::facade::FsWork + Send>(&self, sink: std::sync::Arc<dyn SourceChangeSink>,
        work: W, cancellation: &CancellationToken) -> W::Output<Option<Box<dyn SourceWatch>>, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| {
            let bytes = std::mem::size_of::<FilteredChangeSink>() as u64;
            let allocation = accounting.claim_owned(bytes, 1).map_err(SourceCursor::cursor_accounting_error)?;
            let sink = std::sync::Arc::new(FilteredChangeSink {
                excluded: self.excluded.clone(), sink, ownership: SourcePayloadOwnership::from_kernel_allocation(allocation),
            }) as std::sync::Arc<dyn SourceChangeSink>;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { return self.inner.watch(sink, original, cancellation); }
            let remaining = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
            let answer = self.inner.watch(sink, remaining, cancellation);
            accept_source_result(answer, &mut accounting)
        })();
        work.finish(result, &accounting)
    }

    async fn lookup<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
        work: W, cancellation: &CancellationToken) -> W::Output<Option<SourceNode>, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if self.excluded.excludes(path, None, &mut accounting)? { return Ok(None); }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { return self.inner.lookup(source, path, original, cancellation).await; }
            let budget = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
            let answer = self.inner.lookup(source, path, budget, cancellation).await;
            accept_source_result(answer, &mut accounting)
        }.await;
        work.finish(result, &accounting)
    }

    async fn read_metadata<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
        expected: Option<SourceVersion>, work: W, cancellation: &CancellationToken) -> W::Output<Option<SourceNode>, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if self.excluded.excludes(path, None, &mut accounting)? { return Ok(None); }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { return self.inner.read_metadata(source, path, expected, original, cancellation).await; }
            let budget = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
            let answer = self.inner.read_metadata(source, path, expected, budget, cancellation).await;
            accept_source_result(answer, &mut accounting)
        }.await;
        work.finish(result, &accounting)
    }

    async fn list_page<W: crate::facade::FsWork + Send>(&self, source: SourceReference, directory: &NamespacePath,
        cursor: Option<SourceCursor>, maximum_entries: u32, work: W, cancellation: &CancellationToken)
        -> W::Output<SourceDirectoryPage, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if self.excluded.excludes(directory, None, &mut accounting)? { return Err(DemandError::Absent); }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                let page = self.inner.list_page(source, directory, cursor, maximum_entries, original, cancellation).await?;
                return self.filter_page(directory, page, &mut accounting);
            }
            let mut remaining = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
            let filter_items = u64::from(maximum_entries).checked_mul(self.excluded.roots.len() as u64).ok_or(crate::WorkError::Overflow)?;
            let selector_bytes = u64::from(maximum_entries).checked_mul(std::mem::size_of::<usize>() as u64).ok_or(crate::WorkError::Overflow)?;
            let selector_ops = u64::from(maximum_entries != 0 && !self.excluded.roots.is_empty());
            accounting.admit(WorkCounters { items_examined: filter_items, allocation_operations: selector_ops,
                peak_allocation_bytes: if self.excluded.roots.is_empty() { 0 } else { selector_bytes },
                ..WorkCounters::UNCHARGED }).map_err(SourceCursor::cursor_accounting_error)?;
            remaining.items_examined -= filter_items;
            remaining.allocation_operations -= selector_ops;
            if !self.excluded.roots.is_empty() { remaining.peak_allocation_bytes -= selector_bytes; }
            let answer = self.inner.list_page(source, directory, cursor, maximum_entries, remaining, cancellation).await;
            let mut page = accept_source_result(answer, &mut accounting)?;
            if self.excluded.roots.is_empty() { return Ok(page); }
            let retained = page.retained_bytes();
            let mut imported = accounting.claim_owned(retained, 0).map_err(SourceCursor::cursor_accounting_error)?;
            let publication = page.publication.take();
            let answer = self.filter_page(directory, page, &mut accounting);
            let mut publication = publication;
            accounting.release_owned(&mut imported, retained).map_err(SourceCursor::cursor_accounting_error)?;
            match answer {
                Ok(mut page) => { page.publication = publication; Ok(page) }
                Err(error) => {
                    let cleanup = publication.as_mut().map_or(Ok(()), |producer| producer.abort(&mut accounting));
                    cleanup.and(Err(error))
                }
            }
        }.await;
        work.finish(result, &accounting)
    }

    async fn read_range<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
        expected: SourceVersion, offset: u64, length: u64, work: W, cancellation: &CancellationToken) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if self.excluded.excludes(path, None, &mut accounting)? { return Err(DemandError::Absent); }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { return self.inner.read_range(source, path, expected, offset, length, original, cancellation).await; }
            let budget = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
            let answer = self.inner.read_range(source, path, expected, offset, length, budget, cancellation).await;
            accept_source_result(answer, &mut accounting)
        }.await;
        work.finish(result, &accounting)
    }

    async fn open_file<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
        expected: SourceVersion, work: W, cancellation: &CancellationToken) -> W::Output<Box<dyn DemandFile>, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if self.excluded.excludes(path, None, &mut accounting)? { return Err(DemandError::Absent); }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { return self.inner.open_file(source, path, expected, original, cancellation).await; }
            let budget = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
            let answer = self.inner.open_file(source, path, expected, budget, cancellation).await;
            accept_source_result(answer, &mut accounting)
        }.await;
        work.finish(result, &accounting)
    }

    async fn read_link<W: crate::facade::FsWork + Send>(&self, source: SourceReference, path: &NamespacePath,
        expected: SourceVersion, maximum_bytes: u64, work: W, cancellation: &CancellationToken) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if self.excluded.excludes(path, None, &mut accounting)? { return Err(DemandError::Absent); }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() { return self.inner.read_link(source, path, expected, maximum_bytes, original, cancellation).await; }
            let budget = accounting.remaining().map_err(SourceCursor::cursor_accounting_error)?;
            let answer = self.inner.read_link(source, path, expected, maximum_bytes, budget, cancellation).await;
            accept_source_result(answer, &mut accounting)
        }.await;
        work.finish(result, &accounting)
    }

}
