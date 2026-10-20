//! Reconcile invalidated hints before publishing any captured generation.
use super::*;
use acyclic_fs::{CaptureError, Checkout, Workspace};

type Candidate = Checkout<LocalAuthorityBackend, LocalObjectBackend>;
type SourceWorkspace = Workspace<LocalAuthorityBackend, LocalObjectBackend>;

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

    async fn baseline(
        &mut self,
        checkout: &mut Candidate,
        (options, policy): (&CaptureOptions, &CapturePolicy),
        cancellation: &CancellationToken,
    ) -> Result<WatchBatch> {
        self.watcher.begin_rescan().map_err(storage)?;
        #[cfg(all(test, not(acyclic_harness_loom)))]
        self.inject("begin")?;
        self.watcher.watch_tree(EXCLUDED).map_err(storage)?;
        #[cfg(all(test, not(acyclic_harness_loom)))]
        self.inject("watch-tree")?;
        capture_baseline(
            checkout,
            options,
            policy,
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(storage)?;
        #[cfg(all(test, not(acyclic_harness_loom)))]
        self.inject("baseline")?;
        let batch = self.watcher.finish_rescan().map_err(storage)?;
        #[cfg(all(test, not(acyclic_harness_loom)))]
        self.inject("finish")?;
        Ok(batch)
    }
}

pub(super) async fn capture(
    cursor: &mut Cursor,
    checkout: &mut Candidate,
    (physical, workspace): (&LocalWorkspace, &SourceWorkspace),
    configuration: (&CaptureOptions, &CapturePolicy),
    mut baseline: bool,
    cancellation: &CancellationToken,
) -> Result<()> {
    let generation = checkout.generation_id();
    let mut initial = cursor.initial.take();
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
            // A prior replay or scan may have authored mutations. Always retry
            // from the same committed generation, never from that candidate.
            *checkout = workspace
                .checkout(
                    GenerationSelector::Exact(generation),
                    CheckoutMode::tracking_transaction(),
                    WorkBudget::UNBOUNDED,
                    cancellation,
                )
                .await
                .map_err(storage)?
                .value;
            initial = None;
            let batch = cursor
                .baseline(checkout, configuration, cancellation)
                .await?;
            if !replay(
                cursor,
                checkout,
                batch,
                physical,
                configuration,
                cancellation,
            )
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
            )
            .await?
        {
            baseline = true;
            continue;
        }
        if drain(cursor, checkout, physical, configuration, cancellation).await? {
            return Ok(());
        }
        baseline = true;
    }
}

async fn drain(
    cursor: &mut Cursor,
    checkout: &mut Candidate,
    physical: &LocalWorkspace,
    configuration: (&CaptureOptions, &CapturePolicy),
    cancellation: &CancellationToken,
) -> Result<bool> {
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
        )
        .await?
        {
            return Ok(false);
        }
        // Recheck invalidation after replay, including an empty batch. Do not
        // fence here: FSEvents correctly cannot promise synchronous delivery.
        if empty {
            let validated = cursor.poll(cancellation)?;
            if matches!(&validated.0, WatchBatch::Changes { changes, .. } if changes.is_empty()) {
                return Ok(true);
            }
            pending = Some(validated);
        }
    }
}

pub(super) async fn replay(
    cursor: &mut Cursor,
    checkout: &mut Candidate,
    batch: WatchBatch,
    physical: &LocalWorkspace,
    (options, policy): (&CaptureOptions, &CapturePolicy),
    cancellation: &CancellationToken,
) -> Result<bool> {
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
}

fn require_rescan(reason: acyclic_fs::WatchInvalidationReason) -> Result<()> {
    if reason == acyclic_fs::WatchInvalidationReason::RootChanged {
        return Err(Error::Unauthorized("checkout root was replaced".into()));
    }
    Ok(())
}
