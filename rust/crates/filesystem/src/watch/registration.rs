//! One held, funded traversal for baseline and arriving-subtree registration.
use super::*;
use crate::native_capture::CapturePolicy;

impl NativeWatch {
    /// Registers directories below an arriving namespace path, respecting the
    /// same exclusions and original work context as baseline registration.
    /// Backends which already cover descendants require no enumeration.
    pub fn watch_arrivals(
        &mut self,
        batch: &WatchBatch,
        policy: &CapturePolicy,
        maximum_paths: u32,
    ) -> Result<(), NativeWatchError> {
        let WatchBatch::Changes { changes, .. } = batch else {
            return Ok(());
        };
        let mut remaining = maximum_paths;
        for change in changes {
            let directory = match change {
                WatchChange::Created(path) | WatchChange::Arrived(path) => path,
                WatchChange::Renamed { to, .. } => to,
                _ => continue,
            };
            self.register_tree(directory, policy, &mut remaining, &[])?;
        }
        Ok(())
    }

    /// Adds every directory beneath the root except the excluded root names.
    pub fn watch_tree(&mut self, excluded: &[&str]) -> Result<(), NativeWatchError> {
        let root = NamespacePath::new(Vec::new(), self.limits)
            .map_err(|_| NativeWatchError::UnrepresentablePath)?;
        let mut remaining = u32::MAX;
        self.register_tree(&root, &CapturePolicy::default(), &mut remaining, excluded)
    }

    fn register_tree(
        &mut self,
        directory: &NamespacePath,
        policy: &CapturePolicy,
        remaining: &mut u32,
        excluded: &[&str],
    ) -> Result<(), NativeWatchError> {
        if self.has_subtree_registration() {
            return Ok(());
        }
        let original = self
            .original_work
            .as_ref()
            .ok_or(NativeWatchError::NativeWorkUnavailable)?
            .clone();
        original.admit(WorkCounters::UNCHARGED)?;
        if *remaining == 0 {
            return Err(NativeWatchError::InvalidOptions);
        }
        let root = self
            .held_root
            .held(&mut &original)?
            .ok_or(NativeWatchError::SourceUnavailable)?
            .try_clone(&mut &original)?;
        crate::native_capture::walk_watch_tree(
            &root,
            (self.profile, self.limits),
            directory,
            policy,
            &original,
            |path, stat, meter| {
                *remaining = remaining
                    .checked_sub(1)
                    .ok_or(NativeWatchError::InvalidOptions)?;
                if stat.file_type() != crate::demand::SourceNodeKind::Directory {
                    return Ok(false);
                }
                let relative =
                    crate::native_capture::namespace_to_host_path_with_work(path, &mut &*meter)?;
                let omitted = (path.depth() == 1
                    && excluded.iter().any(|name| relative.0 == Path::new(name)))
                    || self
                        .shared
                        .lock()
                        .map_err(|_| NativeWatchError::StatePoisoned)?
                        .excluded
                        .iter()
                        .any(|omitted| {
                            omitted
                                .strip_prefix(&self.root)
                                .is_ok_and(|omitted| relative.0.starts_with(omitted))
                        });
                drop(relative);
                if omitted {
                    return Ok(false);
                }
                // Install before the held walker reads this directory's entries.
                // Registration may outlive this traversal. Its storage belongs
                // to the original watch, never the transient walk scope.
                self.watch_directory_with_work(path, &mut original.clone())
                    .map_err(demand_watch_error)?;
                Ok(true)
            },
        )
    }
}
