//! Operation-local roots for objects whose parent has not been installed yet.
use super::*;
use crate::kernel::allocation::fixed_index::FixedIndex;
use crate::kernel::allocation::retained::{Retained, RetainedArc};

/// Construction custody carried by an original accounting operation.
#[doc(hidden)]
#[derive(Default)]
pub struct ConstructionRoots {
    groups: Option<FixedIndex<Construction, { std::mem::size_of::<usize>() }>>,
}

/// A frame either owns its construction roots or borrows its original owner.
#[doc(hidden)]
pub enum ConstructionScope<'a> {
    /// An outer operation's lazily populated roots.
    Owned(ConstructionRoots),
    /// A nested provider call retains the same roots.
    Borrowed(&'a mut ConstructionRoots),
}

impl Default for ConstructionScope<'_> {
    fn default() -> Self {
        Self::Owned(ConstructionRoots::default())
    }
}

impl ConstructionScope<'_> {
    pub(crate) fn roots(&mut self) -> &mut ConstructionRoots {
        match self {
            Self::Owned(roots) => roots,
            Self::Borrowed(roots) => roots,
        }
    }
}

struct Construction {
    // Remove the registry's observation before releasing the original owner.
    _hold: Hold,
    group: RetainedArc<ConstructionGroup>,
}

pub(crate) struct ConstructionGroup {
    objects: Mutex<FixedIndex<ObjectId, { ObjectId::ENCODED_LEN }>>,
}

impl std::fmt::Debug for ConstructionGroup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConstructionGroup")
            .finish_non_exhaustive()
    }
}

impl ConstructionRoots {
    pub(crate) fn is_empty(&self) -> bool {
        self.groups.is_none()
    }

    pub(crate) fn pin(
        &mut self,
        collection: &Arc<Collection>,
        object: ObjectId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        // A live group owns a Hold, which keeps this collection allocated;
        // another provider cannot reuse its address while the key is present.
        let key = (Arc::as_ptr(collection) as usize).to_be_bytes();
        let owner = self
            .groups
            .get_or_insert_with(FixedIndex::default)
            .get_or_try_insert_with(key, accounting, |accounting| {
                let group = RetainedArc::admit(accounting)?.into_value(ConstructionGroup {
                    objects: Mutex::new(FixedIndex::default()),
                });
                let hold = collection.register_with_accounting(
                    Held::Construction {
                        group: group.clone(),
                    },
                    accounting,
                )?;
                Ok(Construction { _hold: hold, group })
            })?;
        owner
            .group
            .objects
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_try_insert_with(object.index_key(), accounting, |_| Ok(object))?;
        Ok(())
    }
}

impl ConstructionGroup {
    /// Copy one bounded page without decoding an incomplete parent object.
    pub(crate) fn page(
        &self,
        after: Option<[u8; ObjectId::ENCODED_LEN]>,
        limit: usize,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Retained<Vec<ObjectId>>, crate::FsError> {
        let mut page = Retained::allocate(limit, accounting)?;
        let objects = self.objects.lock().unwrap_or_else(PoisonError::into_inner);
        let mut after = after;
        while page.value.len() < limit {
            let next = match after {
                Some(key) => objects.seek_after(key, accounting)?,
                None => objects.first(accounting)?,
            };
            let Some((key, object)) = next else {
                break;
            };
            after = Some(*key);
            page.value.push(*object);
        }
        Ok(page)
    }
}
