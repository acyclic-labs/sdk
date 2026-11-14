//! One original volume publication wait shared by directory and file queries.
use super::*;
use crate::{conversation::VolumeRef, resources::GenerationRef};

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Bound<P, O> {
    pub(super) async fn volume_generation(
        &self,
        host: &Host<P, O>,
        volume: &VolumeRef,
        after: Option<&GenerationRef>,
    ) -> Result<GenerationRef, Error> {
        let reference = super::super::bounded_io::content_workspace(&host.provider, volume, None)
            .map_err(turn)?;
        #[cfg(not(target_arch = "wasm32"))]
        let private = host.has_private_source();
        #[cfg(target_arch = "wasm32")]
        let private = false;
        if after.is_none() && !private {
            return Ok(host
                .resolve(&reference, None)
                .await
                .map_err(turn)?
                .generation);
        }
        let root = host.open(&reference, None).await.map_err(turn)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        #[cfg(not(target_arch = "wasm32"))]
        let native = if private {
            Some(
                acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(error, _)| turn(crate::Error::from(error)))?,
            )
        } else {
            None
        };
        #[cfg(not(target_arch = "wasm32"))]
        let io = match native.as_ref() {
            Some(native) => crate::filesystem::PreparationIo::from_native_work(native),
            None => crate::filesystem::PreparationIo::for_unmeasured(&cancellation),
        }
        .map_err(turn)?;
        #[cfg(target_arch = "wasm32")]
        let io = crate::filesystem::PreparationIo::for_unmeasured(&cancellation).map_err(turn)?;
        let Some(after) = after else {
            let workspace = host
                .select_current_workspace(&root, &io)
                .await
                .map_err(turn)?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(cut) = host
                .private_snapshot(&workspace, None, &io)
                .await
                .map_err(turn)?
            {
                return host.private_generation_ref(&cut, None, &io).map_err(turn);
            }
            let head = io
                .sdk(workspace.head(&io, io.cancellation()).await)
                .map_err(turn)?;
            return host.generation_ref_id(head.id(), None).map_err(turn);
        };
        #[cfg(not(target_arch = "wasm32"))]
        let private_cut = if host.is_private_generation(after) {
            let cut = host
                .private_snapshot(&root, Some(after), &io)
                .await
                .map_err(turn)?
                .ok_or(Error::Record("private generation binding is unavailable"))?;
            host.validate_private_snapshot(&cut, &io)
                .await
                .map_err(turn)?;
            Some(cut)
        } else {
            None
        };
        #[cfg(not(target_arch = "wasm32"))]
        let changes = if private_cut.is_some() {
            let work = native
                .as_ref()
                .ok_or(Error::Record("private wait lost its original work"))?;
            let allocation = work
                .claim(
                    std::mem::size_of::<SourceChanges>() as u64,
                    1,
                    Default::default(),
                )
                .map_err(|error| turn(crate::Error::from(error)))?;
            Some(Arc::new(SourceChanges {
                changed: tokio::sync::Notify::new(),
                lost: std::sync::atomic::AtomicBool::new(false),
                _allocation: allocation,
            }))
        } else {
            None
        };
        #[cfg(not(target_arch = "wasm32"))]
        let mut watch = SourceSubscription(None);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(changes) = &changes {
            let work = native
                .as_ref()
                .ok_or(Error::Record("private wait lost its original work"))?;
            let mut ctx = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work,
                cancellation: &cancellation,
            };
            watch.0 = Some(
                host.watch_private_source(&root, changes.clone(), &mut ctx)
                    .map_err(turn)?
                    .ok_or_else(|| {
                        turn(crate::Error::Unsupported(
                            "private source has no change subscription".into(),
                        ))
                    })?,
            );
        }
        // Authenticate ordinary generation addresses through the original owner.
        #[cfg(not(target_arch = "wasm32"))]
        let authored = private_cut.is_none();
        #[cfg(target_arch = "wasm32")]
        let authored = true;
        if authored {
            host.generation(&root, after, None).await.map_err(turn)?;
        }
        let selected = loop {
            let mut cursors = Vec::new();
            // Checkout can change the producer. Capture its cursor before
            // selecting that producer, then capture the producer before head.
            if let Some(path) = host.selection_changes_path(root.id()).map_err(turn)? {
                cursors.push(self.publication_cursor(path).await?);
            }
            let workspace = host
                .select_current_workspace(&root, &io)
                .await
                .map_err(turn)?;
            let path =
                acyclic_fs::session::changes_path(&host.filesystem, workspace.id().volume_id())
                    .map_err(Error::Stream)?;
            cursors.push(self.publication_cursor(path).await?);
            #[cfg(not(target_arch = "wasm32"))]
            let selected = if private_cut.is_some() {
                match host
                    .private_snapshot(&workspace, None, &io)
                    .await
                    .map_err(turn)?
                {
                    Some(cut) => host.private_generation_ref(&cut, None, &io).map_err(turn)?,
                    None => {
                        let head = io
                            .sdk(workspace.head(&io, io.cancellation()).await)
                            .map_err(turn)?;
                        host.generation_ref_id(head.id(), None).map_err(turn)?
                    }
                }
            } else {
                let head = io
                    .sdk(workspace.head(&io, io.cancellation()).await)
                    .map_err(turn)?;
                host.generation_ref_id(head.id(), None).map_err(turn)?
            };
            #[cfg(target_arch = "wasm32")]
            let selected = {
                let head = io
                    .sdk(workspace.head(&io, io.cancellation()).await)
                    .map_err(turn)?;
                host.generation_ref_id(head.id(), None).map_err(turn)?
            };
            if after != &selected {
                break selected;
            }
            let mut updates = Vec::new();
            for cursor in cursors {
                updates.push(acyclic_stream::records_only(
                    self.agents
                        .provider()
                        .follow(cursor)
                        .await
                        .map_err(Error::Stream)?,
                ));
            }
            use futures::TryStreamExt as _;
            let mut updates = futures::stream::select_all(updates);
            #[cfg(not(target_arch = "wasm32"))]
            {
                if changes
                    .as_ref()
                    .is_some_and(|changes| changes.lost.load(std::sync::atomic::Ordering::Acquire))
                {
                    return Err(Error::Stream(acyclic_stream::StreamError::Unavailable));
                }
                let source = async {
                    match changes.as_ref() {
                        Some(changes) => changes.changed.notified().await,
                        None => futures::future::pending::<()>().await,
                    }
                };
                tokio::select! {
                    _ = source => {},
                    update = updates.try_next() => {
                        update.map_err(Error::Stream)?.ok_or(Error::Stream(acyclic_stream::StreamError::Unavailable))?;
                    }
                }
            }
            #[cfg(target_arch = "wasm32")]
            updates
                .try_next()
                .await
                .map_err(Error::Stream)?
                .ok_or(Error::Stream(acyclic_stream::StreamError::Unavailable))?;
        };
        #[cfg(not(target_arch = "wasm32"))]
        watch
            .stop()
            .map_err(|error| turn(acyclic_fs::LazyWorkspaceError::Demand(error).into()))?;
        Ok(selected)
    }

    async fn publication_cursor(
        &self,
        path: acyclic_stream::StreamPath,
    ) -> Result<acyclic_stream::FollowRequest, Error> {
        let request = acyclic_stream::HeadsRequest {
            at: None,
            paths: [path.clone()].into_iter().collect(),
            retained: Default::default(),
            durable_through: None,
            projection: acyclic_stream::HeadProjection::Tail,
            children: None,
            limit: 1,
            at_least: Default::default(),
        };
        let heads = self
            .agents
            .provider()
            .heads(request.clone())
            .await
            .map_err(Error::Stream)?;
        heads.validate_for(&request).map_err(Error::Stream)?;
        Ok(acyclic_stream::FollowRequest {
            path,
            from: heads
                .heads
                .first()
                .and_then(Option::as_ref)
                .map_or(0, |head| head.tail),
            preceding: None,
            bootstrap_head: false,
            until: None,
            when_absent: acyclic_stream::WhenAbsent::Wait,
            filter: Default::default(),
            progress: false,
            at_least: Default::default(),
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct SourceChanges {
    changed: tokio::sync::Notify,
    lost: std::sync::atomic::AtomicBool,
    _allocation: acyclic_fs::NativeAllocation,
}
#[cfg(not(target_arch = "wasm32"))]
impl acyclic_fs::demand::SourceChangeSink for SourceChanges {
    fn source_changed(
        &self,
        _: &[acyclic_fs::demand::SourceChange],
        ctx: &mut acyclic_fs::kernel::KernelAccounting<'_>,
    ) -> Result<(), acyclic_fs::demand::DemandError> {
        ctx.remaining()?;
        self.changed.notify_one();
        Ok(())
    }
    fn delivery_lost(&self) {
        self.lost.store(true, std::sync::atomic::Ordering::Release);
        self.changed.notify_one();
    }
}
#[cfg(not(target_arch = "wasm32"))]
struct SourceSubscription(Option<Box<dyn acyclic_fs::demand::SourceWatch>>);
#[cfg(not(target_arch = "wasm32"))]
impl SourceSubscription {
    fn stop(&mut self) -> Result<(), acyclic_fs::demand::DemandError> {
        self.0
            .take()
            .map_or(Ok(()), |watch| watch.stop_accepted_cleanup())
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for SourceSubscription {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
