//! Push watcher hints captured in one filesystem generation commit.
use super::*;
use acyclic_fs::{
    CancellationToken, CaptureOptions, CapturePolicy, LivePublicationObservation,
    NativeWatchOptions, PublicationPermit, WatchBatch, WorkBudget, WorkCounters, capture_baseline,
    capture_watch_batch,
    kernel::NamespacePath,
    model::{CheckoutMode, GenerationSelector},
};
use std::time::Duration;

const EXCLUDED: &[&str] = &[".git", ".acyclic-sdk"];
const MAXIMUM_PATHS: u32 = 262_144;
const WATCH_FENCE_TIMEOUT: Duration = Duration::from_secs(1);

impl CheckoutTools {
    pub(super) async fn capture(
        &self,
        view: &workspace::View,
        slot: &mut Option<NativeWatch>,
        operation: OperationId,
        phase: u8,
        owner: &crate::durable_host::TaskJournalOwner<LocalStream>,
    ) -> Result<()> {
        let cancellation = CancellationToken::new();
        let workspace = view.workspace();
        let options = &self.capture_options;
        let policy = &self.capture_policy;
        // On failure discard the cursor: the next command must establish a new
        // baseline rather than skip changes consumed before a failed commit.
        let baseline = slot.is_none();
        let mut watcher = if let Some(watcher) = slot.take() {
            watcher
        } else {
            self.open_watcher()?
        };
        if watcher.root_identity() != self.workspace.identity {
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
        let batch = if baseline {
            None
        } else {
            watcher.fence(WATCH_FENCE_TIMEOUT).map_err(storage)?;
            Some(
                watcher
                    .poll(MAXIMUM_PATHS, WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(storage)?
                    .value,
            )
        };
        let batch = match batch {
            Some(batch @ WatchBatch::Changes { .. }) => batch,
            None | Some(WatchBatch::RescanRequired { .. }) => {
                // A reset is an explicit requirement to replace the baseline.
                // Register push watches first, then include changes delivered
                // during the scan before committing the captured generation.
                watcher.begin_rescan().map_err(storage)?;
                watcher.watch_tree(EXCLUDED).map_err(storage)?;
                capture_baseline(
                    &mut checkout,
                    options,
                    policy,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(storage)?;
                watcher.finish_rescan().map_err(storage)?
            }
        };
        capture_watch_batch(
            &mut checkout,
            batch,
            options,
            policy,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(storage)?;
        if checkout.has_pending_mutations() {
            let operation = capture_operation(operation, phase);
            let committed = checkout
                .commit(
                    operation,
                    None,
                    None,
                    None,
                    PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                    |_| async {
                        owner
                            .verify(false, &mut None)
                            .await
                            .map_err(acyclic_fs::OperationFailure::before_work)?;
                        Ok::<_, acyclic_fs::OperationFailure<Error>>(acyclic_fs::OperationReceipt {
                            value: (),
                            work: WorkCounters::UNCHARGED,
                        })
                    },
                )
                .await
                .map_err(|failure| failure.error)?
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
        *slot = Some(watcher);
        Ok(())
    }

    fn open_watcher(&self) -> Result<NativeWatch> {
        let config = self.workspace.view().workspace().volume_config();
        let mut watcher = NativeWatch::open_with_profile(
            &self.workspace.path,
            config.profile,
            NativeWatchOptions::new(config.limits),
        )
        .map_err(storage)?;
        watcher
            .exclude_subtrees(self.capture_policy.excluded_prefixes())
            .map_err(storage)?;
        Ok(watcher)
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
    let policy = CapturePolicy::excluding(
        EXCLUDED
            .iter()
            .map(|name| {
                let path =
                    acyclic_fs::path::PortablePath::parse(&format!("/{name}"), config.limits)
                        .map_err(storage)?;
                NamespacePath::from_portable_in_profile(&path, config.profile, config.limits)
                    .map_err(storage)
            })
            .collect::<Result<Vec<_>>>()?,
    )
    .map_err(storage)?;
    Ok((options, policy))
}
