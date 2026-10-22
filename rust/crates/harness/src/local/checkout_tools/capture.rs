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
const EXCLUDED: &[&str] = &[".git", ".acyclic-sdk", ".system"];
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
        // FSEvents and ReadDirectoryChangesW observe the whole root on one
        // stream or handle, filtering excluded subtrees as changes arrive: a
        // handle per directory would grow with the checkout. Inotify watches
        // are per directory on one descriptor, so Linux registers each
        // directory as capture discovers it.
        options.recursive = cfg!(any(target_os = "macos", windows));
        #[cfg(not(target_os = "macos"))]
        let mut watcher = NativeWatch::open_with_profile(&workspace.path, config.profile, options)
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
        let (physical, view) = selected;
        if let Some(mount) = &physical.mounted {
            mount
                .sync_with_permit(permit, &mount.native_work()?, move |_| {
                    let owner = owner.clone();
                    async move {
                        if let Some((owner, settlement)) = owner {
                            owner.verify(settlement, &mut None).await?;
                        }
                        Ok::<(), Error>(())
                    }
                })
                .await
                .map_err(|failure| failure.cause)?;
            return Ok(());
        }
        let cancellation = CancellationToken::new();
        let workspace = view.workspace();
        let options = &self.capture_options;
        let policy = &self.capture_policy;
        // On failure discard the cursor: the next command must establish a new
        // baseline rather than skip changes consumed before a failed commit.
        let baseline = slot.is_none();
        let mut cursor = match slot.take() {
            Some(cursor) => cursor,
            None => Cursor::open(physical, &self.capture_policy)?,
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
                    |_| {
                        std::future::ready(
                            Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                                acyclic_fs::OperationReceipt {
                                    value: (),
                                    work: WorkCounters::UNCHARGED,
                                },
                            ),
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
        *slot = Some(cursor);
        Ok(())
    }
}

fn capture_operation(operation: OperationId, phase: u8) -> acyclic_fs::OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic.local.checkout-capture/v1\0");
    hasher.update(&operation.into_bytes());
    hasher.update(&[phase]);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    acyclic_fs::OperationId::from_bytes(bytes)
}

pub(super) fn configuration(workspace: &LocalWorkspace) -> Result<(CaptureOptions, CapturePolicy)> {
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
