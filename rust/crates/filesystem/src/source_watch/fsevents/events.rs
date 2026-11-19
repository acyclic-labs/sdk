use super::*;

impl Shared {
    pub(super) fn root_unchanged<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        work.started_io(WorkCounters {
            items_examined: 1,
            ..READ
        })?;
        Ok(std::fs::symlink_metadata(&self.root)
            .is_ok_and(|metadata| identity(&metadata) == self.held))
    }

    pub(super) fn events<'a, W: SourceDemandReadWork>(
        &self,
        events: impl ExactSizeIterator<Item = (&'a Path, u32)>,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let count = events.len() as u64;
        let items = crate::materializer::NativeResourceCredit::Items;
        work.check()?;
        work.admit(WorkCounters {
            items_examined: count,
            ..WorkCounters::UNCHARGED
        })?;
        work.with_original_leaf(|work| work.reserve(items.delta(count)))?;
        let (mut changes, slots) = match buffer::<HostChange, _>(events.len(), work) {
            Ok(buffer) => buffer,
            Err(error) => {
                work.with_original_leaf(|work| work.refund(items.delta(count)))?;
                return Err(error);
            }
        };
        let mut payload = 0_u64;
        let result = (|| {
            let mut root_uncertain = false;
            for (path, flags) in events {
                work.started_io(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                // A volume mounted or unmounted at a path beneath the root
                // rebinds only that path; one above the root, or lost
                // notifications, may have changed anything.
                let dropped = flags & (USER_DROPPED | KERNEL_DROPPED) != 0;
                root_uncertain |= dropped
                    || (flags & (MUST_SCAN_SUBDIRS | MOUNT | UNMOUNT) != 0
                        && self.root.starts_with(path));
                if flags & ROOT_CHANGED != 0 {
                    self.delivery.notify_lost();
                    changes.push(HostChange::Everything);
                } else if dropped {
                    changes.push(HostChange::Everything);
                } else if let Some(relative) = relative_path(path, &self.root, work)? {
                    let (relative, bytes) = clone_path(relative, work)?;
                    payload = payload
                        .checked_add(bytes)
                        .ok_or(crate::WorkError::Overflow)?;
                    changes.push(change(relative, flags));
                } else if flags & (MUST_SCAN_SUBDIRS | MOUNT | UNMOUNT) != 0 {
                    changes.push(HostChange::Everything);
                }
            }
            // A gap may have swallowed the original root's rename. Check the
            // binding before handing off the rescan, even without a fence.
            if root_uncertain && self.delivery.is_exact() && !self.root_unchanged(work)? {
                self.delivery.notify_lost();
            }
            self.delivery.deliver(&changes, work)
        })();
        let mut remaining = count;
        let mut cleaned = Ok(());
        while let Some(change) = changes.pop() {
            remaining -= 1;
            drop(change);
            cleaned = cleaned
                .and(work.with_original_leaf(|work| work.complete(items.delta(1), items.delta(1))));
        }
        cleaned = cleaned.and(work.with_original_leaf(|work| work.refund(items.delta(remaining))));
        drop(changes);
        work.release(payload + slots)?;
        result.and(cleaned)
    }
}

fn relative_path<'a, W: SourceDemandReadWork>(
    path: &'a Path,
    root: &Path,
    work: &mut W,
) -> Result<Option<&'a Path>, DemandError> {
    let mut path = path.components();
    for component in root.components() {
        work.started_io(WorkCounters {
            source_path_components: 1,
            ..WorkCounters::UNCHARGED
        })?;
        if path.next() != Some(component) {
            return Ok(None);
        }
    }
    Ok(Some(path.as_path()))
}

pub(super) fn change(relative: PathBuf, flags: u32) -> HostChange {
    let unspecific = flags & ITEM_FLAGS == 0 || flags & (MUST_SCAN_SUBDIRS | MOUNT | UNMOUNT) != 0;
    if relative.as_os_str().is_empty() {
        return if unspecific {
            HostChange::Everything
        } else {
            HostChange::Altered(relative)
        };
    }
    if unspecific || flags & (ITEM_CREATED | ITEM_REMOVED | ITEM_RENAMED) != 0 {
        HostChange::Rebound(relative)
    } else {
        HostChange::Altered(relative)
    }
}
