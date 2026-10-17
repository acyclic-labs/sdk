use super::*;

pub(super) type FoldedKeys = crate::kernel::Owned<Vec<Vec<u8>>>;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CheckoutMountSource<A, O> {
    pub(super) async fn require_unique_folded_path(
        &self,
        checkout: &mut Checkout<A, O>,
        path: &NamespacePath,
        include_directory: bool,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Option<folded_cache::Shared<FoldedKeys>>, MountSourceError> {
        let mut accounting = crate::kernel::KernelAccounting::native(work, cancellation);
        let mut parent = crate::kernel::Owned::new(
            NamespacePath::new(Vec::new(), self.config.limits).map_err(engine_error)?,
            accounting.claim_owned(0, 0)?,
            0,
        );
        for index in 0..=path.components().len() {
            if index == path.components().len() && !include_directory {
                break;
            }
            let record = checkout
                .lookup_no_follow(&parent.value, work, cancellation)
                .await?
                .record
                .ok_or(MountSourceError::NotFound)?;
            let FilePayload::Directory { entries } = record.payload else {
                return Err(MountSourceError::NotDirectory);
            };
            let keys = match self.folded_directories.get(entries) {
                Some(folded_cache::DirectoryNames::Unique) => None,
                Some(folded_cache::DirectoryNames::Collisions(keys)) => Some(keys),
                None => {
                    let mut keys = checkout
                        .folded_collision_keys(entries, &mut accounting, cancellation)
                        .await?;
                    if keys.value.is_empty() {
                        keys.retire(&mut accounting)?;
                        self.folded_directories
                            .insert(entries, folded_cache::DirectoryNames::Unique);
                        None
                    } else {
                        let bytes = (std::mem::size_of_val(&keys)
                            + 2 * std::mem::size_of::<usize>())
                            as u64;
                        let mut allocation = accounting.claim_owned(bytes, 1)?;
                        keys.allocation.absorb(&mut allocation)?;
                        keys.bytes = keys.bytes.checked_add(bytes).ok_or(WorkError::Overflow)?;
                        let keys = folded_cache::Shared::new(keys);
                        self.folded_directories.insert(
                            entries,
                            folded_cache::DirectoryNames::Collisions(folded_cache::Shared::clone(
                                &keys,
                            )),
                        );
                        Some(keys)
                    }
                }
            };
            if let Some(name) = path.components().get(index) {
                if let Some(keys) = &keys
                    && crate::facade::ambiguous_folded_name(&keys.value, name, &mut accounting)?
                {
                    return Err(MountSourceError::Filesystem(FsError::NameCollision));
                }
                let (value, allocation) = parent
                    .value
                    .child_with_accounting(name, self.config.limits, &mut accounting, cancellation)
                    .map_err(engine_error)?;
                let bytes = value.retained_bytes();
                let next = crate::kernel::Owned::new(value, allocation, bytes);
                std::mem::replace(&mut parent, next).retire(&mut accounting)?;
            } else {
                parent.retire(&mut accounting)?;
                return Ok(keys);
            }
        }
        parent.retire(&mut accounting)?;
        Ok(None)
    }
}
