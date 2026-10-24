//! Reconcile invalidated hints before publishing any captured generation.
use super::*;
use acyclic_fs::{CaptureError, Checkout};

type Candidate = Checkout<LocalAuthorityBackend, LocalObjectBackend>;

// Bound repeated loss of continuity, rather than either publishing a stale
// candidate or allowing an actively changing source to spin forever.
pub(super) const MAXIMUM_RESCANS: usize = 4;

impl Cursor {
    fn poll(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<(WatchBatch, Option<acyclic_fs::NativeAllocation>)> {
        #[cfg(target_os = "macos")]
        {
            let (batch, owner) = self
                .watcher
                .poll_with_native_work_guarded(MAXIMUM_PATHS, &self.work, |_| Ok(()))
                .map_err(storage)?;
            let _ = cancellation;
            Ok((batch, Some(owner)))
        }
        #[cfg(not(target_os = "macos"))]
        Ok((
            self.watcher
                .poll(MAXIMUM_PATHS, WorkBudget::UNBOUNDED, cancellation)
                .map_err(storage)?
                .value,
            None,
        ))
    }

    #[inline(never)]
    fn baseline<'a>(
        &'a mut self,
        checkout: &'a mut Candidate,
        (options, policy): (&'a CaptureOptions, &'a CapturePolicy),
        cancellation: &'a CancellationToken,
        optional_before_images: bool,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, WatchBatch>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            self.watcher.begin_rescan().map_err(storage)?;
            #[cfg(all(test, not(acyclic_harness_loom)))]
            self.inject("begin")?;
            self.watcher.watch_tree(EXCLUDED).map_err(storage)?;
            #[cfg(all(test, not(acyclic_harness_loom)))]
            self.inject("watch-tree")?;
            let captured = capture_baseline(
                checkout,
                options,
                policy,
                WorkBudget::UNBOUNDED,
                cancellation,
            )
            .await;
            if let Err(failure) = captured {
                // This first import exists only to retain display before-images.
                // Failure leaves capture_baseline's private candidate unpublished.
                // Loss-of-continuity rescans remain mandatory and fail closed.
                let unreadable = matches!(&failure.error,
                    CaptureError::Kernel(cause) if matches!(cause.as_ref(),
                        acyclic_fs::FsError::NativeKernel(acyclic_fs::NativeTreeMaterializationError::Io(error))
                        if error.kind() == std::io::ErrorKind::PermissionDenied
                            || cfg!(windows) && error.raw_os_error() == Some(32)));
                if !optional_before_images || !unreadable {
                    return Err(storage(failure));
                }
                self.unreadable_diff_source = true;
            }
            #[cfg(all(test, not(acyclic_harness_loom)))]
            self.inject("baseline")?;
            let batch = self.watcher.finish_rescan().map_err(storage)?;
            #[cfg(all(test, not(acyclic_harness_loom)))]
            self.inject("finish")?;
            Ok(batch)
        })
    }
}

#[inline(never)]
pub(super) fn capture<'a>(
    cursor: &'a mut Cursor,
    checkout: &'a mut Candidate,
    physical: &'a LocalWorkspace,
    configuration: (&'a CaptureOptions, &'a CapturePolicy),
    mut baseline: bool,
    cancellation: &'a CancellationToken,
) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
    crate::conversation::BoundedContentFuture::new(None, || async move {
        let mut initial = cursor.initial.take();
        let optional_before_images =
            physical.import_source && matches!(initial, Some(WatchBatch::Changes { .. }));
        if let Some(WatchBatch::RescanRequired { reason, .. }) = initial {
            require_rescan(reason)?;
        }
        baseline |= matches!(initial, Some(WatchBatch::RescanRequired { .. }));
        if !baseline {
            cursor.watcher.fence(WATCH_FENCE_TIMEOUT).map_err(storage)?;
            #[cfg(all(test, not(acyclic_harness_loom)))]
            cursor.inject("fence")?;
        }
        let mut rescans = 0;
        loop {
            cancellation.check().map_err(storage)?;
            if baseline {
                if rescans == MAXIMUM_RESCANS {
                    return Err(Error::Conflict(
                        "checkout kept changing during capture; retry the command".into(),
                    ));
                }
                rescans += 1;
                // Discard private mutations while retaining the ORIGINAL expected
                // publication head. Reopening Exact would silently refresh that
                // fence and could overwrite a concurrent writer's generation.
                checkout
                    .discard(WorkBudget::UNBOUNDED, cancellation)
                    .await
                    .map_err(storage)?;
                initial = None;
                let batch = cursor
                    .baseline(
                        checkout,
                        configuration,
                        cancellation,
                        optional_before_images && rescans == 1,
                    )?
                    .await?;
                if !replay(
                    cursor,
                    checkout,
                    batch,
                    physical,
                    configuration,
                    cancellation,
                )?
                .await?
                {
                    continue;
                }
            } else if let Some(batch) = initial.take()
                && !replay(
                    cursor,
                    checkout,
                    batch,
                    physical,
                    configuration,
                    cancellation,
                )?
                .await?
            {
                baseline = true;
                continue;
            }
            if drain(cursor, checkout, physical, configuration, cancellation)?.await? {
                return Ok(());
            }
            baseline = true;
        }
    })
}

#[inline(never)]
fn drain<'a>(
    cursor: &'a mut Cursor,
    checkout: &'a mut Candidate,
    physical: &'a LocalWorkspace,
    configuration: (&'a CaptureOptions, &'a CapturePolicy),
    cancellation: &'a CancellationToken,
) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
    crate::conversation::BoundedContentFuture::new(None, || async move {
        let mut remaining = MAXIMUM_PATHS as usize;
        let mut pending = None;
        loop {
            // Keep the producer's vector guard alive until replay has consumed it.
            let (batch, _owner) = match pending.take() {
                Some(batch) => batch,
                None => cursor.poll(cancellation)?,
            };
            #[cfg(all(test, not(acyclic_harness_loom)))]
            cursor.inject("poll")?;
            let empty = match &batch {
                WatchBatch::Changes { changes, .. } => {
                    remaining = remaining.checked_sub(changes.len()).ok_or_else(|| {
                        Error::Conflict("checkout change batch exceeds capture bound".into())
                    })?;
                    changes.is_empty()
                }
                WatchBatch::RescanRequired { .. } => false,
            };
            if !replay(
                cursor,
                checkout,
                batch,
                physical,
                configuration,
                cancellation,
            )?
            .await?
            {
                return Ok(false);
            }
            // Recheck invalidation after replay, including an empty batch. Do not
            // fence here: FSEvents correctly cannot promise synchronous delivery.
            if empty {
                let validated = cursor.poll(cancellation)?;
                if matches!(&validated.0, WatchBatch::Changes { changes, .. } if changes.is_empty())
                {
                    return Ok(true);
                }
                pending = Some(validated);
            }
        }
    })
}

#[inline(never)]
pub(super) fn replay<'a>(
    cursor: &'a mut Cursor,
    checkout: &'a mut Candidate,
    batch: WatchBatch,
    physical: &'a LocalWorkspace,
    (options, policy): (&'a CaptureOptions, &'a CapturePolicy),
    cancellation: &'a CancellationToken,
) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
    crate::conversation::BoundedContentFuture::new(None, || async move {
        if let WatchBatch::RescanRequired { reason, .. } = batch {
            require_rescan(reason)?;
            return Ok(false);
        }
        arrivals::register(&mut cursor.watcher, &batch, physical, policy)?;
        #[cfg(all(test, not(acyclic_harness_loom)))]
        cursor.inject("register")?;
        let result = capture_watch_batch(
            checkout,
            batch,
            options,
            policy,
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await;
        #[cfg(all(test, not(acyclic_harness_loom)))]
        cursor.inject("replay")?;
        match result {
            Ok(_) => Ok(true),
            Err(failure) if matches!(failure.error, CaptureError::RescanRequired { .. }) => {
                if let CaptureError::RescanRequired { reason, .. } = failure.error {
                    require_rescan(reason)?;
                }
                Ok(false)
            }
            Err(failure) => Err(storage(failure)),
        }
    })
}

fn require_rescan(reason: acyclic_fs::WatchInvalidationReason) -> Result<()> {
    if reason == acyclic_fs::WatchInvalidationReason::RootChanged {
        return Err(Error::Unauthorized("checkout root was replaced".into()));
    }
    Ok(())
}

#[cfg(test)]
mod native_capture_storage_tests {
    use super::*;
    fn retained<Args, T>(_: impl FnOnce(Args) -> T) -> usize {
        std::mem::size_of::<T>()
    }
    #[test]
    fn capture_spine_retains_only_owned_polling_handles() {
        let measured = [
            (
                "baseline",
                retained(
                    |(cursor, checkout, _physical, options, policy, cancellation): (
                        &'static mut Cursor,
                        &'static mut Candidate,
                        &'static LocalWorkspace,
                        &'static CaptureOptions,
                        &'static CapturePolicy,
                        &'static CancellationToken,
                    )| {
                        cursor.baseline(checkout, (options, policy), cancellation, false)
                    },
                ),
            ),
            (
                "capture",
                retained(
                    |(cursor, checkout, physical, options, policy, cancellation): (
                        &'static mut Cursor,
                        &'static mut Candidate,
                        &'static LocalWorkspace,
                        &'static CaptureOptions,
                        &'static CapturePolicy,
                        &'static CancellationToken,
                    )| {
                        capture(
                            cursor,
                            checkout,
                            physical,
                            (options, policy),
                            false,
                            cancellation,
                        )
                    },
                ),
            ),
            (
                "drain",
                retained(
                    |(cursor, checkout, physical, options, policy, cancellation): (
                        &'static mut Cursor,
                        &'static mut Candidate,
                        &'static LocalWorkspace,
                        &'static CaptureOptions,
                        &'static CapturePolicy,
                        &'static CancellationToken,
                    )| {
                        drain(cursor, checkout, physical, (options, policy), cancellation)
                    },
                ),
            ),
            (
                "replay",
                retained(
                    |(cursor, checkout, physical, options, policy, cancellation): (
                        &'static mut Cursor,
                        &'static mut Candidate,
                        &'static LocalWorkspace,
                        &'static CaptureOptions,
                        &'static CapturePolicy,
                        &'static CancellationToken,
                    )| {
                        replay(
                            cursor,
                            checkout,
                            WatchBatch::Changes {
                                epoch: 0,
                                first_sequence: 0,
                                next_sequence: 0,
                                changes: Vec::new(),
                                original: Default::default(),
                            },
                            physical,
                            (options, policy),
                            cancellation,
                        )
                    },
                ),
            ),
        ];
        eprintln!("capture spine retained bytes: {measured:?}");
        assert!(
            measured.into_iter().all(|(_, bytes)| bytes <= 512),
            "capture spine embeds concrete child futures"
        );
    }
}
