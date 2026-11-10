//! File controls for one admitted shell. Native notifications drive observation;
//! there is no status probe, timer or task for an idle agent.
use super::*;

pub(super) struct Cancellation {
    watcher: acyclic_fs::NativeWatch,
    path: std::path::PathBuf,
    work: acyclic_fs::NativeMaterializationWork,
}

impl Cancellation {
    pub(super) fn open(root: &std::path::Path, operation: crate::OperationId) -> Result<Self> {
        // A mounted home projection is reached through the SDK directory
        // alias. Native watches refuse a linked root, so watch its target.
        let root = &workspace::volumes::alias_target(root)?.unwrap_or_else(|| root.to_path_buf());
        let options = acyclic_fs::NativeWatchOptions::new(acyclic_fs::VolumeLimits::default());
        let mut watcher = acyclic_fs::NativeWatch::open(root, options).map_err(storage)?;
        let rescan = watcher.begin_rescan().map_err(storage)?;
        let root_path =
            acyclic_fs::kernel::NamespacePath::new(Vec::new(), acyclic_fs::VolumeLimits::default())
                .map_err(storage)?;
        watcher.watch_directory(&root_path).map_err(storage)?;
        watcher.finish_rescan(rescan).map_err(storage)?;
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        Ok(Self {
            watcher,
            path: root.join(format!("shell-{operation}.cancel")),
            work,
        })
    }

    pub(super) async fn wait(mut self) -> Result<()> {
        loop {
            if self.path.try_exists().map_err(storage)? {
                return Ok(());
            }
            self.watcher
                .changed_with_native_work(&self.work)
                .await
                .map_err(storage)?;
            self.watcher
                .poll_with_native_work_guarded(u32::MAX, &self.work, |_| Ok(()))
                .map_err(storage)?;
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests;
