//! Push watcher hints captured in one filesystem generation commit.
use super::*;
use {
    acyclic_fs::{
        CancellationToken, CaptureOptions, CapturePolicy, LivePublicationObservation,
        NativeWatchOptions, WatchBatch, capture_baseline, capture_watch_batch,
        model::{CheckoutMode, GenerationSelector},
    },
    acyclic_fs_work::performance::{WorkBudget, WorkCounters},
};

mod converge;
mod progress;
#[cfg(test)]
#[cfg(not(loom))]
mod tests;

pub(in crate::local) struct Cursor {
    watcher: NativeWatch,
    initial: Option<WatchBatch>,
    baseline_pending: bool,
    before_images_complete: bool,
    #[cfg(all(test, not(loom)))]
    injection: Option<tests::Injection>,
}

impl Cursor {
    /// Marks the same lost continuity as a dropped OS batch, without I/O.
    #[cfg(all(test, not(loom)))]
    pub(in crate::local) fn lose_continuity(&mut self) -> Result<()> {
        self.watcher.invalidate().map_err(storage)
    }

    fn open(workspace: &LocalWorkspace) -> Result<Self> {
        let config = workspace.view().workspace().volume_config();
        // Never write a fence cookie into the checkout. Precise backends fence
        // push delivery; FSEvents has no read-only completion proof and uses
        // the authenticated baseline instead. Keep registration persistent.
        let mut options = NativeWatchOptions::new(config.limits);
        options.permit_fence_writes = false;
        // FSEvents and ReadDirectoryChangesW observe the whole root on one
        // stream or handle: a handle per directory would grow with the checkout.
        // Inotify watches are per directory on one descriptor, so Linux registers each
        // directory as capture discovers it.
        options.recursive = cfg!(any(target_os = "macos", windows));
        #[cfg(not(target_os = "macos"))]
        let watcher = NativeWatch::open_with_profile(&workspace.path, config.profile, options)
            .map_err(storage)?;
        // FSEvents' held-source adapter requires retained native custody. Linux
        // selects its held source for a nonrecursive watch, Windows always.
        #[cfg(target_os = "macos")]
        let work = acyclic_fs::NativeMaterializationWork::new(
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .map_err(|(cause, _)| storage(cause))?;
        #[cfg(target_os = "macos")]
        let mut watcher = NativeWatch::open_with_profile_with_native_work(
            &workspace.path,
            config.profile,
            options,
            &work,
        )
        .map_err(storage)?;
        Ok(Self {
            watcher,
            initial: None,
            baseline_pending: workspace.import_source,
            before_images_complete: true,
            #[cfg(all(test, not(loom)))]
            injection: None,
        })
    }

    pub(super) fn initial(workspace: &LocalWorkspace) -> Result<Self> {
        let mut cursor = Self::open(workspace)?;
        let rescan = cursor.watcher.begin_rescan().map_err(storage)?;
        cursor.watcher.watch_tree().map_err(storage)?;
        cursor.initial = Some(cursor.watcher.finish_rescan(rescan).map_err(storage)?);
        Ok(cursor)
    }
}

impl CheckoutTools {
    #[inline(never)]
    pub(in crate::local) fn capture<'a>(
        &'a self,
        selected: (&'a LocalWorkspace, &'a workspace::View),
        slot: &'a mut Option<Cursor>,
        operation: OperationId,
        phase: u8,
        permit: PublicationPermit,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            capture(
                (&self.capture_options, &self.capture_policy),
                selected,
                slot,
                (operation, phase),
                permit,
            )
            .await
        })
    }
    /// Records, before a file publication's physical write, that the write
    /// is owed its own capture on its staged generation. A capture another
    /// write still owes is finished first.
    pub(in crate::local) async fn owe_write_capture(
        &self,
        selected: (&LocalWorkspace, &workspace::View),
        slot: &mut Option<Cursor>,
        owed: reconciliation::OwedWrite,
    ) -> Result<()> {
        let (physical, _) = selected;
        let current = reconciliation::owed_write(physical).await?;
        if current == Some(owed) {
            return Ok(());
        }
        if current.is_some() {
            self.finish_owed_write(selected, slot, PublicationPermit::Unrestricted)?
                .await?;
        }
        reconciliation::replace_owed_write(physical, None, Some(owed)).await
    }

    /// Finishes the owed write of a writer that knows the paths it wrote.
    #[inline(never)]
    pub(in crate::local) fn finish_written<'a>(
        &'a self,
        selected: (&'a LocalWorkspace, &'a workspace::View),
        slot: &'a mut Option<Cursor>,
        touched: &'a [&'a str],
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            finish_owed_write_with(
                (&self.capture_options, &self.capture_policy),
                selected,
                slot,
                PublicationPermit::Unrestricted,
                &|| async { Ok(()) },
                Some(touched),
            )
            .await
        })
    }

    /// Captures the current native state of `paths` before a writer decides
    /// their type and existence, without reading or consuming watch hints.
    /// Paths whose state cannot be captured on their own take the ordinary
    /// capture instead.
    #[inline(never)]
    pub(in crate::local) fn capture_paths<'a>(
        &'a self,
        selected: (&'a LocalWorkspace, &'a workspace::View),
        slot: &'a mut Option<Cursor>,
        paths: &'a [&'a str],
        operation: OperationId,
        phase: u8,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            let configuration = (&self.capture_options, &self.capture_policy);
            let permit = PublicationPermit::Unrestricted;
            let captured = record_paths(
                configuration,
                selected,
                paths,
                (operation, phase),
                permit,
                &|| async { Ok(()) },
                None,
            )
            .await?;
            if captured {
                return Ok(());
            }
            capture(configuration, selected, slot, (operation, phase), permit).await
        })
    }

    /// Captures a file publication's physical write under the write's own
    /// operation while its staged generation is still the head, then clears
    /// the debt. A durable companion receipt also acknowledges a clean capture,
    /// so replay cannot capture a later edit under this write's identity.
    #[inline(never)]
    pub(in crate::local) fn finish_owed_write<'a>(
        &'a self,
        selected: (&'a LocalWorkspace, &'a workspace::View),
        slot: &'a mut Option<Cursor>,
        permit: PublicationPermit,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            finish_owed_write(
                (&self.capture_options, &self.capture_policy),
                selected,
                slot,
                permit,
                &|| async { Ok(()) },
            )
            .await
        })
    }
}
/// Captures native effects of `operation` into the selected workspace.
pub(in crate::local) async fn capture(
    configuration: (&CaptureOptions, &CapturePolicy),
    selected: (&LocalWorkspace, &workspace::View),
    slot: &mut Option<Cursor>,
    operation: (OperationId, u8),
    permit: PublicationPermit,
) -> Result<()> {
    let (physical, _) = selected;
    if let Some(mount) = &physical.mounted {
        mount
            .sync_with_permit(
                permit,
                &mount.native_work()?,
                Some(capture_operation(operation.0, operation.1)),
                |_| async { Ok::<(), Error>(()) },
            )
            .await
            .map_err(|failure| failure.cause)?;
        return Ok(());
    }
    // Settle a file publication under its own operation before this capture.
    let verify = || async { Ok(()) };
    finish_owed_write(configuration, selected, slot, permit, &verify).await?;
    record_changes(
        configuration,
        selected,
        slot,
        operation,
        permit,
        &verify,
        None,
    )
    .await
}

pub(in crate::local) async fn finish_owed_write<F, Fut>(
    configuration: (&CaptureOptions, &CapturePolicy),
    selected: (&LocalWorkspace, &workspace::View),
    slot: &mut Option<Cursor>,
    permit: PublicationPermit,
    before_effect: &F,
) -> Result<()>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    finish_owed_write_with(configuration, selected, slot, permit, before_effect, None).await
}

/// Finishes the owed write. A writer that knows the paths it wrote captures
/// exactly those, so its cost is bounded by what it touched and it never
/// waits on the checkout's watch continuity. A debt found by recovery, whose
/// paths are unknown, settles through the ordinary capture.
async fn finish_owed_write_with<F, Fut>(
    configuration: (&CaptureOptions, &CapturePolicy),
    selected: (&LocalWorkspace, &workspace::View),
    slot: &mut Option<Cursor>,
    permit: PublicationPermit,
    before_effect: &F,
    touched: Option<&[&str]>,
) -> Result<()>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    let (physical, view) = selected;
    let Some(owed) = reconciliation::owed_write(physical).await? else {
        return Ok(());
    };
    let (operation, staged) = owed;
    let capture = capture_operation(OperationId::from_bytes(operation.into_bytes()), 1);
    let settled = view
        .workspace()
        .operation_generation(
            acyclic_fs::IdempotencyKey::from_bytes(capture.into_bytes()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(storage)?
        .value
        .is_some();
    let head = view
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(storage)?
        .value;
    if !settled && staged.workspace_id == view.workspace().id() && head.id() == staged.generation {
        // The debt is the write and its capture together: a write that
        // stopped before its physical publication is published first, so its
        // capture holds the write's bytes, whichever entry point finishes it.
        // A replay of an already published transition is idempotent.
        #[cfg(all(test, not(loom)))]
        let apply = !OWED_WRITE_UNAPPLIED.get();
        #[cfg(not(all(test, not(loom))))]
        let apply = true;
        if apply {
            apply_owed_write(physical, view, operation, staged.generation).await?;
        }
        #[cfg(all(test, not(loom)))]
        if KILL_BEFORE_CAPTURE.get() {
            return Err(Error::Storage("killed before the write's capture".into()));
        }
        let capture = (OperationId::from_bytes(operation.into_bytes()), 1);
        let settlement = Some(staged.generation.digest());
        let captured = match touched {
            Some(paths) => {
                record_paths(
                    configuration,
                    selected,
                    paths,
                    capture,
                    permit,
                    before_effect,
                    settlement,
                )
                .await?
            }
            None => false,
        };
        if !captured {
            record_changes(
                configuration,
                selected,
                slot,
                capture,
                permit,
                before_effect,
                settlement,
            )
            .await?;
        }
    }
    before_effect().await?;
    reconciliation::replace_owed_write(physical, Some(owed), None).await
}

#[cfg(all(test, not(loom)))]
std::thread_local! {
    /// Stops an owed write between its physical publication and its capture.
    pub(in crate::local) static KILL_BEFORE_CAPTURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Negative control: finishing a debt captures without publishing the
    /// owed write, as before the debt named the write.
    pub(in crate::local) static OWED_WRITE_UNAPPLIED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Full scans of the checkout this thread's captures performed.
    pub(in crate::local) static BASELINE_SCANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Path-scoped captures this thread's writers performed.
    pub(in crate::local) static PATH_CAPTURES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Publishes the staged generation's transition from its unique parent into
/// the checkout, under the write's publication operation.
async fn apply_owed_write(
    physical: &LocalWorkspace,
    view: &workspace::View,
    operation: acyclic_fs_format::identity::OperationId,
    staged: acyclic_fs_format::object_reference::GenerationId,
) -> Result<()> {
    let cancellation = CancellationToken::new();
    let workspace = view.workspace();
    let to = workspace
        .generation(staged, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    let (parents, _allocation) = to
        .parents(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    let parent = match parents.as_slice() {
        // A root generation is no transition: nothing to publish.
        [] => return Ok(()),
        [parent] => *parent,
        _ => {
            return Err(Error::Storage(
                "file publication has no unique parent generation".into(),
            ));
        }
    };
    let from = workspace
        .generation(parent, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    crate::tools::git::publish_checkout_transition(
        &physical.path,
        physical.identity,
        &physical.store,
        operation,
        from,
        to,
        &|| std::future::ready(Ok(())),
    )
    .await
}

pub(in crate::local) async fn record_changes<F, Fut>(
    (options, policy): (&CaptureOptions, &CapturePolicy),
    selected: (&LocalWorkspace, &workspace::View),
    slot: &mut Option<Cursor>,
    (operation, phase): (OperationId, u8),
    permit: PublicationPermit,
    before_effect: &F,
    settlement: Option<acyclic_fs_format::identity::Digest>,
) -> Result<()>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    before_effect().await?;
    let (physical, view) = selected;
    let cancellation = CancellationToken::new();
    let workspace = view.workspace();

    // On failure discard the cursor: the next command must establish a new
    // baseline rather than skip changes consumed before a failed commit.
    // Reuse the complete import publication as the durable diff baseline.
    // Eligibility survives cursor loss: recovery must scan again until this
    // exact operation has a receipt, never substitute a later checkout head.
    let baseline_pending = physical.import_source
        && phase == 0
        && slot.as_ref().is_none_or(|cursor| cursor.baseline_pending)
        && workspace
            .operation_generation(
                acyclic_fs::IdempotencyKey::from_bytes(
                    baseline_operation(workspace.id()).into_bytes(),
                ),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value
            .is_none();
    let baseline = needs_baseline(slot, physical) || baseline_pending;
    let mut cursor = match slot.take() {
        Some(cursor) => cursor,
        None => Cursor::open(physical)?,
    };
    if cursor.watcher.root_identity() != physical.identity {
        return Err(Error::Unauthorized("checkout root was replaced".into()));
    }
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(storage)?
        .value;
    converge::capture(
        &mut cursor,
        &mut checkout,
        physical,
        (options, policy),
        baseline,
        &cancellation,
    )?
    .await?;
    // A best-effort before-image refusal cannot certify an exact snapshot.
    let first_baseline = baseline_pending && cursor.before_images_complete;
    if checkout.has_pending_mutations() || settlement.is_some() || first_baseline {
        let operation = if first_baseline {
            baseline_operation(workspace.id())
        } else {
            capture_operation(operation, phase)
        };
        // Even an empty initial checkout needs an immutable baseline receipt.
        let settlement = settlement.or_else(|| {
            first_baseline.then(|| {
                acyclic_fs_format::identity::Digest::from_bytes(
                    *blake3::hash(b"acyclic.local.checkout-baseline/v1\0").as_bytes(),
                )
            })
        });
        let committed = checkout
            .commit(
                operation,
                settlement,
                None,
                None,
                permit,
                WorkBudget::UNBOUNDED,
                &cancellation,
                |_| async {
                    before_effect().await.map_err(|error| {
                        acyclic_fs_work::performance::OperationFailure::new(
                            error,
                            WorkCounters::UNCHARGED,
                        )
                    })?;
                    Ok::<_, acyclic_fs_work::performance::OperationFailure<Error>>(
                        acyclic_fs_work::performance::OperationReceipt {
                            value: (),
                            work: WorkCounters::UNCHARGED,
                        },
                    )
                },
            )
            .await
            .map_err(storage)?
            .value;
        if !matches!(
            committed,
            LivePublicationObservation::Committed { .. }
                | LivePublicationObservation::AlreadyCommitted { .. }
        ) {
            return Err(Error::Conflict(
                "checkout capture publication raced with another writer".into(),
            ));
        }
    }
    if phase == 0 {
        cursor.baseline_pending = baseline_pending && !first_baseline;
    }
    *slot = Some(cursor);
    Ok(())
}

/// Captures exactly `paths`, at their current native state, into one commit
/// on the head; capture hydrates any ancestor the head lacks. It reads no
/// watch hints and leaves them queued, so it never rescans, never waits on
/// watch continuity and never claims another command's native effects. Its
/// cost is bounded by the paths, never by the size of the checkout.
///
/// Returns `false`, committing nothing, when the paths cannot be captured on
/// their own: a native move the watch has not yet delivered still binds the
/// file's identity elsewhere, a linked file's other names are unknown, and a
/// queued hint may still describe any identity the head binds at a path. A
/// path capture therefore binds new names and updates files in place, but
/// never removes or replaces a bound identity. The caller then takes the
/// ordinary capture, which replays every hint against the state it describes
/// and is exact by construction.
pub(in crate::local) async fn record_paths<F, Fut>(
    (options, policy): (&CaptureOptions, &CapturePolicy),
    selected: (&LocalWorkspace, &workspace::View),
    paths: &[&str],
    (operation, phase): (OperationId, u8),
    permit: PublicationPermit,
    before_effect: &F,
    settlement: Option<acyclic_fs_format::identity::Digest>,
) -> Result<bool>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    before_effect().await?;
    let (_, view) = selected;
    let cancellation = CancellationToken::new();
    let workspace = view.workspace();
    let config = workspace.volume_config();
    let mut names: Vec<acyclic_fs::kernel::NamespacePath> = Vec::new();
    for path in paths {
        // Only the written paths: a hinted directory is walked on hosts whose
        // hints do not name changed children, so an ancestor hint would walk
        // unrelated files.
        let portable = acyclic_fs::path::PortablePath::parse(&format!("/{path}"), config.limits)
            .map_err(storage)?;
        let name = acyclic_fs::kernel::NamespacePath::from_portable_in_profile(
            &portable,
            config.profile,
            config.limits,
        )
        .map_err(storage)?;
        if !names.contains(&name) {
            names
                .try_reserve(1)
                .map_err(|_| Error::Storage("capture path allocation failed".into()))?;
            names.push(name);
        }
    }
    let allocation = || Error::Storage("capture path allocation failed".into());
    let mut changes: Vec<acyclic_fs::WatchChange> = Vec::new();
    changes.try_reserve(names.len()).map_err(|_| allocation())?;
    changes.extend(names.iter().cloned().map(acyclic_fs::WatchChange::Modified));
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(storage)?
        .value;
    let mut bound = Vec::new();
    bound.try_reserve(names.len()).map_err(|_| allocation())?;
    for name in &names {
        let Ok(lookup) = checkout
            .lookup_no_follow(name, WorkBudget::UNBOUNDED, &cancellation)
            .await
        else {
            return Ok(false);
        };
        bound.push(lookup.value.record.map(|record| record.file_id));
    }
    let captured = capture_watch_batch(
        &mut checkout,
        WatchBatch::Changes {
            epoch: 0,
            first_sequence: 0,
            next_sequence: 0,
            changes,
            original: Default::default(),
        },
        options,
        policy,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await;
    // Any refusal discards this candidate: the ordinary capture either
    // records the state exactly or fails with its own cause.
    if captured.is_err() {
        return Ok(false);
    }
    for (name, before) in names.iter().zip(&bound) {
        let Some(before) = before else {
            continue;
        };
        let Ok(lookup) = checkout
            .lookup_no_follow(name, WorkBudget::UNBOUNDED, &cancellation)
            .await
        else {
            return Ok(false);
        };
        if lookup.value.record.map(|record| record.file_id) != Some(*before) {
            return Ok(false);
        }
    }
    #[cfg(all(test, not(loom)))]
    PATH_CAPTURES.set(PATH_CAPTURES.get() + 1);
    if !checkout.has_pending_mutations() && settlement.is_none() {
        return Ok(true);
    }
    let committed = checkout
        .commit(
            capture_operation(operation, phase),
            settlement,
            None,
            None,
            permit,
            WorkBudget::UNBOUNDED,
            &cancellation,
            |_| async {
                before_effect().await.map_err(|error| {
                    acyclic_fs_work::performance::OperationFailure::new(
                        error,
                        WorkCounters::UNCHARGED,
                    )
                })?;
                Ok::<_, acyclic_fs_work::performance::OperationFailure<Error>>(
                    acyclic_fs_work::performance::OperationReceipt {
                        value: (),
                        work: WorkCounters::UNCHARGED,
                    },
                )
            },
        )
        .await
        .map_err(storage)?
        .value;
    if !matches!(
        committed,
        LivePublicationObservation::Committed { .. }
            | LivePublicationObservation::AlreadyCommitted { .. }
    ) {
        return Err(Error::Conflict(
            "checkout capture publication raced with another writer".into(),
        ));
    }
    Ok(true)
}

pub(super) fn needs_baseline(slot: &Option<Cursor>, physical: &LocalWorkspace) -> bool {
    slot.as_ref()
        .is_none_or(|cursor| physical.import_source && cursor.initial.is_some())
}

/// The first source import, retained in the workspace's existing operation index.
pub(in crate::local) fn baseline_operation(
    workspace: acyclic_fs::WorkspaceId,
) -> acyclic_fs_format::identity::OperationId {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.local.checkout-baseline/v1");
    hash.update(&workspace.into_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    acyclic_fs_format::identity::OperationId::from_bytes(bytes)
}

pub(in crate::local) fn capture_operation(
    operation: OperationId,
    phase: u8,
) -> acyclic_fs_format::identity::OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.local.checkout-capture/v1\0");
    hasher.update(&operation.into_bytes());
    hasher.update(&[phase]);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    acyclic_fs_format::identity::OperationId::from_bytes(bytes)
}

pub(in crate::local) fn configuration(
    workspace: &LocalWorkspace,
) -> (CaptureOptions, CapturePolicy) {
    let options = CaptureOptions {
        materialized_view: (!workspace.import_source).then(|| {
            acyclic_fs::NativeViewBaseline::new(workspace.identity, false)
                .recorded_in(&workspace.store)
        }),
        source_namespace: Some(workspace.view().source_identity()),
        source_root: workspace.path.clone(),
        expected_root_identity: workspace.identity,
        maximum_paths: None,
        maximum_extent_spans: None,
    };
    (options, CapturePolicy::default())
}
