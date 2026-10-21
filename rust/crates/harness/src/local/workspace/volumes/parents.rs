//! Bind auxiliary container directories before the first command's watch cut.
use super::*;

pub(in crate::local::workspace) async fn bind(
    view: &View,
    root: &acyclic_fs::HeldRoot,
    mounts: &Mounts,
    cancellation: &CancellationToken,
) -> Result<()> {
    use acyclic_fs::{
        CaptureOptions,
        model::{CheckoutMode, GenerationSelector},
    };
    let config = view.workspace().volume_config();
    let work = WorkBudget::UNBOUNDED;
    let mut paths = Vec::new();
    for (relative, mount) in mounts.mounts() {
        if mount.view.source.is_none() {
            continue;
        }
        let Some(parent) = Path::new(relative).parent() else {
            continue;
        };
        if parent.as_os_str().is_empty() {
            continue;
        }
        let parent = parent
            .to_str()
            .ok_or_else(|| Error::Invalid("mount parent is not UTF-8".into()))?;
        let portable = acyclic_fs::path::PortablePath::parse(
            &format!("/{}", parent.replace('\\', "/")),
            config.limits,
        )
        .map_err(storage)?;
        paths.push(
            NamespacePath::from_portable_in_profile(&portable, config.profile, config.limits)
                .map_err(storage)?,
        );
    }
    paths.sort_unstable();
    paths.dedup();
    if paths.is_empty() {
        return Ok(());
    }
    let options = CaptureOptions {
        source_root: root.path().to_path_buf(),
        expected_root_identity: root.identity(),
        maximum_paths: u32::try_from(paths.len()).map_err(storage)?,
        maximum_extent_spans: 1,
    };
    let mut checkout = view
        .workspace()
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            work,
            cancellation,
        )
        .await
        .map_err(storage)?
        .value;
    // Exact directory paths capture their metadata, never enumerate or read
    // their children. Authored siblings remain demand-backed and watchable.
    // The held root is the source: a replaced checkout path never redirects it.
    acyclic_fs::capture_held_paths(&mut checkout, &paths, root, &options, work, cancellation)
        .await
        .map_err(storage)?;
    if !checkout.has_pending_mutations() {
        return Ok(());
    }
    // Reopening can capture a new parent state under the same workspace-open
    // identity. Each new publication needs its own key; the commit retains
    // that key for any retries of this one captured transaction.
    let publication = acyclic_fs::OperationId::new();
    let committed = checkout
        .commit(
            publication,
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            work,
            cancellation,
            |_| {
                std::future::ready(Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                    },
                ))
            },
        )
        .await
        .map_err(storage)?
        .value;
    if !matches!(
        committed,
        acyclic_fs::LivePublicationObservation::Committed { .. }
            | acyclic_fs::LivePublicationObservation::AlreadyCommitted { .. }
    ) {
        return Err(Error::Conflict(
            "auxiliary parent binding raced with another writer".into(),
        ));
    }
    Ok(())
}
