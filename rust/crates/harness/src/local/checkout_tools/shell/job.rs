//! File controls for one admitted shell, over the shared native input watch.
use super::*;

pub(super) struct Cancellation {
    watcher: crate::input_watch::InputWatch,
    path: std::path::PathBuf,
}

impl Cancellation {
    pub(super) fn open(root: &std::path::Path, operation: crate::OperationId) -> Result<Self> {
        let path = root.join(format!("shell-{operation}.cancel"));
        let watcher = crate::input_watch::InputWatch::new(vec![path.clone()])?;
        Ok(Self { watcher, path })
    }

    pub(super) async fn wait(self) -> Result<()> {
        loop {
            // Subscribe before observing the predicate. Each re-read follows
            // a new input event; no sticky tree invalidation can spin here.
            let changed = self.watcher.changed();
            self.watcher.arm()?;
            if self.path.try_exists().map_err(storage)? {
                return Ok(());
            }
            changed.await?;
        }
    }
}

#[cfg(test)]
#[cfg(not(loom))]
mod tests;
