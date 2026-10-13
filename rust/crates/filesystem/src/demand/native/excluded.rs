struct ExcludedPaths {
    paths: Vec<NamespacePath>,
    ownership: SourcePayloadOwnership,
}
impl ExcludedPaths {
    fn excludes(&self, path: &NamespacePath) -> bool {
        self.paths.iter().any(|root| path.is_within(root))
    }
    fn excludes_child(&self, directory: &NamespacePath, name: &crate::kernel::LogicalName) -> bool {
        self.paths.iter().any(|root| {
            root.depth() == directory.depth() + 1
                && root.components().starts_with(directory.components())
                && root.components().last() == Some(name)
        })
    }
    fn visible(&self, change: &SourceChange) -> bool {
        match change {
            SourceChange::Name(path) | SourceChange::Entry(path) => !self.excludes(path),
            _ => true,
        }
    }
}
impl NativeDemandSource {
    /// Hides the supplied canonical subtrees from demand and change delivery.
    pub fn exclude<W: FsWork>(
        mut self,
        paths: Vec<NamespacePath>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, DemandError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| {
            let mut context = callback_work(&mut accounting)?;
            let result = (|| {
                let ownership = SourcePayloadOwnership::new(&mut context)?;
                let bytes = crate::source_watch::arc_control_bytes::<ExcludedPaths>()
                    + (paths.capacity() * std::mem::size_of::<NamespacePath>()) as u64
                    + paths.iter().map(NamespacePath::retained_bytes).sum::<u64>();
                let allocation = context.claim(bytes)?;
                ownership.retain(allocation, &mut context)?;
                self.excluded = Arc::new(ExcludedPaths { paths, ownership });
                Ok(self)
            })();
            finish_context(result, context, &mut accounting)
        })();
        work.finish(result, &accounting)
    }
}
