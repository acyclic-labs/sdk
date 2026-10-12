use super::*;

pub(super) struct DirectoryHandle {
    pub(super) path: DarwinOwned<MountPath>,
    pub(super) binding_epoch: Option<u64>,
    pub(super) epochs: Option<CacheEpochs>,
    pub(super) cursor: Option<Vec<u8>>,
    pub(super) entries: VecDeque<MountDirectoryEntry>,
    pub(super) emitted: i64,
    pub(super) page_source_ownership: Option<crate::demand::SourcePayloadOwnership>,
}

impl DirectoryHandle {
    pub(super) fn new(
        path: DarwinOwned<MountPath>,
        binding_epoch: Option<u64>,
        epochs: Option<CacheEpochs>,
    ) -> Self {
        Self {
            path,
            binding_epoch,
            epochs,
            cursor: None,
            entries: VecDeque::new(),
            emitted: 0,
            page_source_ownership: None,
        }
    }

    pub(super) fn clear_page(&mut self) {
        drop(self.cursor.take());
        drop(std::mem::take(&mut self.entries));
        drop(self.page_source_ownership.take());
    }

    pub(super) fn rewind(&mut self) {
        self.clear_page();
        self.emitted = 0;
    }

    pub(super) fn pop_entry(&mut self) -> bool {
        let Some(entry) = self.entries.pop_front() else {
            return false;
        };
        let bytes = entry.name.capacity() as u64;
        let owner = self
            .page_source_ownership
            .as_mut()
            .filter(|owner| owner.has_native_custody())
            .map(|owner| {
                owner
                    .split_owned_payload(bytes)
                    .expect("source page owns each name")
            });
        drop(entry);
        drop(owner);
        true
    }

    pub(super) fn is_current(
        &self,
        context: &DarwinMountContext,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        Ok(match self.epochs {
            Some(epochs) => {
                context.source.view_is_stable_with_meter(work)?
                    && context.source.binding_epoch_with_meter(work)? == Some(epochs.binding)
                    && context.unchanged_since(&self.path, None, epochs.view, work)?
            }
            None => context.cache_epochs(work)?.is_none(),
        })
    }

    pub(super) fn keep_current(
        &mut self,
        context: &DarwinMountContext,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        for _ in 0..MAXIMUM_LISTING_RESTARTS {
            if self.epochs.is_none() || self.is_current(context, work)? {
                return Ok(());
            }
            if self.emitted > 2
                || context.source.binding_epoch_with_meter(work)? != self.binding_epoch
            {
                return Err(driver_errno(libc::ESTALE));
            }
            self.epochs = context.cache_epochs(work)?;
            self.clear_page();
        }
        Err(driver_errno(libc::ESTALE))
    }
}
