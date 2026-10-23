//! Push watcher hints captured in one filesystem generation commit.
use super::*;
use acyclic_fs::{
    CancellationToken, CaptureOptions, CapturePolicy, LivePublicationObservation,
    NativeWatchOptions, WatchBatch, WorkBudget, WorkCounters, capture_baseline,
    capture_watch_batch,
    kernel::NamespacePath,
    model::{CheckoutMode, GenerationSelector},
};
use std::time::Duration;

mod arrivals;
mod converge;
#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;

// Exclude owned projections individually below, never their authored siblings.
const EXCLUDED: &[&str] = &[
    ".git",
    ".acyclic-sdk",
    ".system",
    crate::local::host_workspace::HOST_VIEW_ROOT,
];
const MAXIMUM_PATHS: u32 = 262_144;
const WATCH_FENCE_TIMEOUT: Duration = Duration::from_secs(1);

pub(in crate::local) struct Cursor {
    watcher: NativeWatch,
    initial: Option<WatchBatch>,
    #[cfg(target_os = "macos")]
    work: acyclic_fs::NativeMaterializationWork,
    #[cfg(all(test, not(acyclic_harness_loom)))]
    injection: Option<tests::Injection>,
}

impl Cursor {
    fn open(workspace: &LocalWorkspace, policy: &CapturePolicy) -> Result<Self> {
        let config = workspace.view().workspace().volume_config();
        // The held native source fences delivery without writing a cookie into
        // the checkout. Register directories once; subsequent commands consume
        // only push hints on this persistent watcher.
        let mut options = NativeWatchOptions::new(config.limits);
        // FSEvents observes the whole root recursively. Inotify and Windows
        // register individual directories as capture discovers them.
        options.recursive = cfg!(target_os = "macos");
        #[cfg(not(target_os = "macos"))]
        let mut watcher = NativeWatch::open_with_profile(&workspace.path, config.profile, options)
            .map_err(storage)?;
        // FSEvents' held-source adapter requires retained native custody. Linux
        // and Windows already select their held source for a nonrecursive watch.
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
        watcher
            .exclude_subtrees(policy.excluded_prefixes())
            .map_err(storage)?;
        Ok(Self {
            watcher,
            initial: None,
            #[cfg(target_os = "macos")]
            work,
            #[cfg(all(test, not(acyclic_harness_loom)))]
            injection: None,
        })
    }

    pub(super) fn initial(workspace: &LocalWorkspace, policy: &CapturePolicy) -> Result<Self> {
        let mut cursor = Self::open(workspace, policy)?;
        cursor.watcher.begin_rescan().map_err(storage)?;
        cursor.watcher.watch_tree(EXCLUDED).map_err(storage)?;
        cursor.initial = Some(cursor.watcher.finish_rescan().map_err(storage)?);
        Ok(cursor)
    }
}

impl CheckoutTools {
    pub(in crate::local) async fn capture(
        &self,
        selected: (&LocalWorkspace, &workspace::View),
        slot: &mut Option<Cursor>,
        operation: OperationId,
        phase: u8,
        permit: PublicationPermit,
        owner: Option<(
            Arc<crate::durable_host::TaskJournalOwner<LocalStream>>,
            bool,
        )>,
    ) -> Result<()> {
        capture(
            (&self.capture_options, &self.capture_policy),
            selected,
            slot,
            (operation, phase),
            permit,
            owner,
        )
        .await
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
            self.finish_owed_write(selected, slot, PublicationPermit::Unrestricted)
                .await?;
        }
        reconciliation::replace_owed_write(physical, None, Some(owed)).await
    }

    /// Captures a file publication's physical write under the write's own
    /// operation while its staged generation is still the head, then clears
    /// the debt. Once the head has moved on nothing is left to capture.
    pub(in crate::local) async fn finish_owed_write(
        &self,
        selected: (&LocalWorkspace, &workspace::View),
        slot: &mut Option<Cursor>,
        permit: PublicationPermit,
    ) -> Result<()> {
        finish_owed_write(
            (&self.capture_options, &self.capture_policy),
            selected,
            slot,
            permit,
            &|| async { Ok(()) },
        )
        .await
    }
}
/// Captures native effects with the original task journal's publication fence.
pub(in crate::local) async fn capture(
    configuration: (&CaptureOptions, &CapturePolicy),
    selected: (&LocalWorkspace, &workspace::View),
    slot: &mut Option<Cursor>,
    operation: (OperationId, u8),
    permit: PublicationPermit,
    owner: Option<(
        Arc<crate::durable_host::TaskJournalOwner<LocalStream>>,
        bool,
    )>,
) -> Result<()> {
    let (physical, _) = selected;
    if let Some(mount) = &physical.mounted {
        mount
            .sync_with_permit(permit, &mount.native_work()?, move |_| {
                let owner = owner.clone();
                async move { verify_owner(&owner).await }
            })
            .await
            .map_err(|failure| failure.cause)?;
        return Ok(());
    }
    // Settle a file publication under its own operation before this capture.
    let verify = || async { verify_owner(&owner).await };
    finish_owed_write(configuration, selected, slot, permit, &verify).await?;
    record_changes(configuration, selected, slot, operation, permit, &verify).await
}

async fn verify_owner(
    owner: &Option<(
        Arc<crate::durable_host::TaskJournalOwner<LocalStream>>,
        bool,
    )>,
) -> Result<()> {
    if let Some((owner, settlement)) = owner {
        owner.verify(*settlement, &mut None).await?;
    }
    Ok(())
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
    let (physical, view) = selected;
    let Some(owed) = reconciliation::owed_write(physical).await? else {
        return Ok(());
    };
    let (operation, staged) = owed;
    let head = view
        .workspace()
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(storage)?
        .value;
    if staged.workspace_id == view.workspace().id() && head.id() == staged.generation {
        record_changes(
            configuration,
            selected,
            slot,
            (OperationId::from_bytes(operation.into_bytes()), 1),
            permit,
            before_effect,
        )
        .await?;
    }
    before_effect().await?;
    reconciliation::replace_owed_write(physical, Some(owed), None).await
}

pub(in crate::local) async fn record_changes<F, Fut>(
    (options, policy): (&CaptureOptions, &CapturePolicy),
    selected: (&LocalWorkspace, &workspace::View),
    slot: &mut Option<Cursor>,
    (operation, phase): (OperationId, u8),
    permit: PublicationPermit,
    before_effect: &F,
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
    let baseline = slot.is_none();
    let mut cursor = match slot.take() {
        Some(cursor) => cursor,
        None => Cursor::open(physical, policy)?,
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
    )
    .await?;
    if checkout.has_pending_mutations() {
        let operation = capture_operation(operation, phase);
        let committed = checkout
            .commit(
                operation,
                None,
                None,
                None,
                permit,
                WorkBudget::UNBOUNDED,
                &cancellation,
                |_| async {
                    before_effect().await.map_err(|error| {
                        acyclic_fs::OperationFailure::new(error, WorkCounters::UNCHARGED)
                    })?;
                    Ok::<_, acyclic_fs::OperationFailure<Error>>(acyclic_fs::OperationReceipt {
                        value: (),
                        work: WorkCounters::UNCHARGED,
                    })
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
    *slot = Some(cursor);
    Ok(())
}

pub(super) fn capture_operation(operation: OperationId, phase: u8) -> acyclic_fs::OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.local.checkout-capture/v1\0");
    hasher.update(&operation.into_bytes());
    hasher.update(&[phase]);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    acyclic_fs::OperationId::from_bytes(bytes)
}

pub(in crate::local) fn configuration(
    workspace: &LocalWorkspace,
) -> Result<(CaptureOptions, CapturePolicy)> {
    let config = workspace.view().workspace().volume_config();
    let options = CaptureOptions {
        source_root: workspace.path.clone(),
        expected_root_identity: workspace.identity,
        maximum_paths: MAXIMUM_PATHS,
        maximum_extent_spans: 65_536,
    };
    let mut excluded = EXCLUDED
        .iter()
        .map(|name| {
            let path = acyclic_fs::path::PortablePath::parse(&format!("/{name}"), config.limits)
                .map_err(storage)?;
            NamespacePath::from_portable_in_profile(&path, config.profile, config.limits)
                .map_err(storage)
        })
        .collect::<Result<Vec<_>>>()?;
    excluded.push(workspace::volumes::namespace(
        &workspace.path,
        config,
        &workspace.path.join(workspace::volumes::INCARNATION_PATH),
    )?);
    for (relative, mount) in workspace.mounts.mounts() {
        if !relative.is_empty() && mount.view.source.is_some() {
            excluded.extend(workspace::volumes::excluded(
                &workspace.path,
                config,
                relative,
            )?);
        }
    }
    let policy = CapturePolicy::excluding(excluded).map_err(storage)?;
    Ok((options, policy))
}
