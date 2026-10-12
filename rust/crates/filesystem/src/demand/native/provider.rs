impl DemandSource for NativeDemandSource {
    fn reference(&self) -> SourceReference {
        SourceReference {
            identity: self.inner.identity,
            epoch: self.inner.epoch.load(Ordering::Acquire),
        }
    }
    fn watch<W: FsWork + Send>(
        &self,
        sink: Arc<dyn SourceChangeSink>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<Box<dyn SourceWatch>>, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| {
            accounting.check().map_err(DemandError::from)?;
            let mut context = callback_work(&mut accounting)?;
            let result = self.subscribe(sink, &mut context);
            finish_context(result, context, &mut accounting)
        })();
        work.finish(result, &accounting)
    }
    async fn lookup<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<SourceNode>, DemandError> {
        self.request(path, work, cancellation, move |provider, path, context| {
            provider.check(source, context)?;
            provider.observe_parent(path, context)?;
            let mut node = provider.metadata(path, context)?.as_ref().map(Self::node);
            if node.is_some_and(|node| node.kind == SourceNodeKind::Directory)
                && provider.admit_to_watches(path, context)?
            {
                node = provider.metadata(path, context)?.as_ref().map(Self::node);
            }
            provider.check(source, context)?;
            Ok(node)
        })
        .await
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
        self.request(
            directory,
            work,
            cancellation,
            move |provider, directory, context| {
                provider.directory_page(source, directory, cursor, maximum_entries, context)
            },
        )
        .await
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
    ) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
        self.request(path, work, cancellation, move |provider, path, context| {
            NativeDemandFile::open(provider, source, path, expected, context)?
                .read(offset, length, context)
        })
        .await
    }
    async fn open_file<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Box<dyn DemandFile>, DemandError> {
        self.request(path, work, cancellation, move |provider, path, context| {
            Ok(Box::new(NativeDemandFile::open(
                provider, source, path, expected, context,
            )?) as Box<dyn DemandFile>)
        })
        .await
    }
    async fn read_link<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        expected: SourceVersion,
        maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, DemandError> {
        self.request(path, work, cancellation, move |provider, path, context| {
            provider.check(source, context)?;
            provider.observe_parent(path, context)?;
            let before = provider
                .metadata(path, context)?
                .ok_or(DemandError::Absent)?;
            if !before.file_type().is_symlink() {
                return Err(DemandError::InvalidRequest);
            }
            if version(&before) != expected {
                return Err(DemandError::StaleVersion);
            }
            let (relative, allocation) = provider.relative(path, context)?;
            let answer = provider.link_bytes(&relative, maximum_bytes, context);
            drop(relative);
            context.release(allocation)?;
            let bytes = answer?;
            if provider.metadata(path, context)?.as_ref().map(version) != Some(expected) {
                return Err(DemandError::StaleVersion);
            }
            provider.check(source, context)?;
            Ok(bytes)
        })
        .await
    }
}

fn finish_context<T>(
    result: Result<T, DemandError>,
    context: CallbackWork,
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<T, DemandError> {
    match context {
        CallbackWork::Original(_) => result,
        CallbackWork::Ordinary(mut context) => {
            let completed = context.receipt();
            accept_source_result(
                result
                    .map(|value| OperationReceipt {
                        value,
                        work: completed,
                    })
                    .map_err(|error| OperationFailure::new(error, completed)),
                accounting,
            )
        }
    }
}

fn callback_work(
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<CallbackWork, DemandError> {
    if let Some(original) = accounting.native_work() {
        return Ok(CallbackWork::Original(original.clone()));
    }
    let budget = accounting.remaining().map_err(DemandError::from)?;
    let stop = match &*accounting {
        crate::kernel::KernelAccounting::Standalone { cancellation, .. }
        | crate::kernel::KernelAccounting::BorrowedStandalone { cancellation, .. }
        | crate::kernel::KernelAccounting::NativeOriginal { cancellation, .. } => cancellation,
    };
    Ok(CallbackWork::Ordinary(DemandWork::new(budget, stop)))
}
