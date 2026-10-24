use super::*;

impl Shared {
    pub(super) fn events<'a, W: SourceDemandReadWork>(
        &self,
        events: impl ExactSizeIterator<Item = (&'a Path, u32)>,
        work: &mut W,
    ) -> Result<(), DemandError> {
        let count = events.len() as u64;
        work.check()?;
        work.admit(WorkCounters {
            items_examined: count,
            ..WorkCounters::UNCHARGED
        })?;
        work.with_original_leaf(|work| {
            work.reserve(crate::materializer::NativeResourceCredit::Items.delta(count))
        })?;
        let (mut changes, slots) = match buffer::<HostChange, _>(events.len(), work) {
            Ok(buffer) => buffer,
            Err(error) => {
                work.with_original_leaf(|work| {
                    work.refund(crate::materializer::NativeResourceCredit::Items.delta(count))
                        .expect("balanced cleanup reservation")
                });
                return Err(error);
            }
        };
        let mut payload = 0_u64;
        let result = (|| {
            for (path, flags) in events {
                work.started_io(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                // A volume mounted or unmounted at a path beneath the root
                // rebinds only that path; one above the root, or lost
                // notifications, may have changed anything.
                if flags & (USER_DROPPED | KERNEL_DROPPED) != 0 {
                    changes.push(HostChange::Everything);
                } else if let Some(relative) = relative_path(path, &self.root, work)? {
                    let (relative, bytes) = clone_path(relative, work)?;
                    payload = payload
                        .checked_add(bytes)
                        .ok_or(crate::WorkError::Overflow)?;
                    changes.push(change(relative, flags));
                } else if flags & (MOUNT | UNMOUNT) != 0 {
                    changes.push(HostChange::Everything);
                }
            }
            self.delivery.deliver(&changes, work)
        })();
        let mut remaining = count;
        let mut cleaned = Ok(());
        while let Some(change) = changes.pop() {
            remaining -= 1;
            drop(change);
            cleaned = cleaned.and(work.with_original_leaf(|work| {
                work.complete(
                    crate::materializer::NativeResourceCredit::Items.delta(1),
                    crate::materializer::NativeResourceCredit::Items.delta(1),
                )
            }));
        }
        work.with_original_leaf(|work| {
            work.refund(crate::materializer::NativeResourceCredit::Items.delta(remaining))
                .expect("balanced cleanup reservation")
        });
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

pub(super) fn identity(metadata: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt as _;
    (metadata.dev(), metadata.ino())
}
