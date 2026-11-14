use super::*;

impl<B: DemandSource, V: DemandSource> Routed<B, V> {
    pub(super) fn start_watch(
        &self,
        sink: Arc<dyn SourceChangeSink>,
        ctx: &mut KernelAccounting<'_>,
        cancel: &CancellationToken,
    ) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
        ctx.check()?;
        let allocation = ctx.claim_owned(std::mem::size_of::<Watches>() as u64, 1)?;
        let base_sink = self.sink(sink.clone(), self.base.identity(), false, ctx, cancel)?;
        let view_sink = self.sink(sink, self.view.identity(), true, ctx, cancel)?;
        let base = start(&self.base, base_sink, ctx, cancel)?;
        let view = match start(&self.view, view_sink, ctx, cancel) {
            Ok(view) => view,
            Err(error) => {
                if let Some(base) = base {
                    base.stop(ctx)?;
                }
                return Err(error);
            }
        };
        if base.is_none() && view.is_none() {
            return Ok(None);
        }
        Ok(Some(Box::new(Watches {
            base,
            view,
            _allocation: allocation,
        })))
    }

    fn sink(
        &self,
        sink: Arc<dyn SourceChangeSink>,
        identity: [u8; 16],
        prepend: bool,
        ctx: &mut KernelAccounting<'_>,
        cancel: &CancellationToken,
    ) -> Result<Arc<dyn SourceChangeSink>, DemandError> {
        let (prefix, mut allocation) = self.prefix.clone_with_accounting(ctx, cancel)?;
        let mut header = ctx.claim_owned(
            std::mem::size_of::<Changes>() as u64 + 2 * std::mem::size_of::<usize>() as u64,
            1,
        )?;
        allocation.absorb(&mut header)?;
        Ok(Arc::new(Changes {
            sink,
            prefix,
            identity,
            prepend,
            limits: self.limits,
            _allocation: allocation,
        }))
    }
}

fn start<S: DemandSource>(
    source: &S,
    sink: Arc<dyn SourceChangeSink>,
    ctx: &mut KernelAccounting<'_>,
    cancel: &CancellationToken,
) -> Result<Option<Box<dyn SourceWatch>>, DemandError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = ctx.native_work() {
        return source.watch(sink, work, cancel);
    }
    let work = ctx.remaining()?;
    accept_source_result(source.watch(sink, work, cancel), ctx)
}

struct Changes {
    sink: Arc<dyn SourceChangeSink>,
    prefix: LogicalName,
    identity: [u8; 16],
    prepend: bool,
    limits: crate::VolumeLimits,
    _allocation: KernelOwnedAllocation,
}
impl SourceChangeSink for Changes {
    fn observes_birth_time(&self) -> bool {
        self.sink.observes_birth_time()
    }
    fn logical_owned_bytes(&self) -> u64 {
        self.prefix.logical_owned_bytes()
            + std::mem::size_of::<Self>() as u64
            + 2 * std::mem::size_of::<usize>() as u64
    }
    fn source_changed(
        &self,
        changes: &[SourceChange],
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        let cancel = ctx.cancellation();
        let mut mapped = Owned::allocate(changes.len(), ctx)?;
        for change in changes {
            let change = match change {
                SourceChange::Name(path)
                | SourceChange::Entry(path)
                | SourceChange::Departed(path) => {
                    if !self.prepend && path.components().first() == Some(&self.prefix) {
                        continue;
                    }
                    let (path, mut allocation) = if self.prepend {
                        path.prepend_with_accounting(&self.prefix, self.limits, ctx, cancel)?
                    } else {
                        path.clone_with_accounting(ctx, cancel)?
                    };
                    mapped.bytes += path.logical_owned_bytes();
                    mapped.allocation.absorb(&mut allocation)?;
                    match change {
                        SourceChange::Name(_) => SourceChange::Name(path),
                        SourceChange::Entry(_) => SourceChange::Entry(path),
                        _ => SourceChange::Departed(path),
                    }
                }
                SourceChange::Node(id) => {
                    ctx.charge(WorkCounters {
                        bytes_hashed: 48,
                        ..WorkCounters::UNCHARGED
                    })?;
                    let mut hash = blake3::Hasher::new();
                    hash.update(&self.identity);
                    hash.update(id);
                    SourceChange::Node(*hash.finalize().as_bytes())
                }
                SourceChange::Everything | SourceChange::Unconfirmed => change.clone(),
            };
            mapped.push(change, ctx)?;
        }
        let result = self.sink.source_changed(&mapped.value, ctx);
        mapped.retire(ctx)?;
        result
    }
    fn delivery_lost(&self) {
        self.sink.delivery_lost();
    }
}

struct Watches {
    base: Option<Box<dyn SourceWatch>>,
    view: Option<Box<dyn SourceWatch>>,
    _allocation: KernelOwnedAllocation,
}
impl SourceWatch for Watches {
    fn fence(&self, ctx: &mut KernelAccounting<'_>) -> Result<SourceFence, DemandError> {
        let base = self
            .base
            .as_ref()
            .map(|watch| watch.fence(ctx))
            .transpose()?;
        let view = self
            .view
            .as_ref()
            .map(|watch| watch.fence(ctx))
            .transpose()?;
        Ok(
            if base == Some(SourceFence::Confirmed) && view == Some(SourceFence::Confirmed) {
                SourceFence::Confirmed
            } else {
                SourceFence::Unconfirmed
            },
        )
    }
    fn stop(&self, ctx: &mut KernelAccounting<'_>) -> Result<(), DemandError> {
        let base = self.base.as_ref().map(|watch| watch.stop(ctx)).transpose();
        let view = self.view.as_ref().map(|watch| watch.stop(ctx)).transpose();
        base?;
        view?;
        Ok(())
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_with_cleanup_wait(&self, cancel: &CancellationToken) -> Result<(), DemandError> {
        let base = self
            .base
            .as_ref()
            .map(|watch| watch.stop_with_cleanup_wait(cancel))
            .transpose();
        let view = self
            .view
            .as_ref()
            .map(|watch| watch.stop_with_cleanup_wait(cancel))
            .transpose();
        base?;
        view?;
        Ok(())
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        let base = self
            .base
            .as_ref()
            .map(|watch| watch.stop_accepted_cleanup())
            .transpose();
        let view = self
            .view
            .as_ref()
            .map(|watch| watch.stop_accepted_cleanup())
            .transpose();
        base?;
        view?;
        Ok(())
    }
    fn is_stopped(&self) -> bool {
        self.base.as_ref().is_none_or(|watch| watch.is_stopped())
            && self.view.as_ref().is_none_or(|watch| watch.is_stopped())
    }
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
            + self
                .base
                .as_ref()
                .map_or(0, |watch| watch.logical_owned_bytes())
            + self
                .view
                .as_ref()
                .map_or(0, |watch| watch.logical_owned_bytes())
    }
    fn is_exact(&self) -> bool {
        self.base.as_ref().is_some_and(|watch| watch.is_exact())
            && self.view.as_ref().is_some_and(|watch| watch.is_exact())
    }
}
