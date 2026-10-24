use super::entries::{Entries, Entry, share_values};
use super::{Child, Format, Page, PageRef};
use crate::FsError;
use crate::kernel::allocation::KernelAccounting;
use crate::kernel::persistent_point::Owned;
use std::sync::Arc;

pub(crate) enum SharedPage<F: Format> {
    Leaf {
        entries: Vec<Arc<Entry<F::Value>>>,
        logical_bytes: u64,
    },
    Internal {
        children: Vec<Arc<Entry<Child<F::Key>>>>,
        logical_bytes: u64,
    },
}

impl<F: Format> SharedPage<F> {
    pub(crate) fn from_owned(
        page: Owned<Page<F>>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Owned<Self>, FsError> {
        let Owned {
            value,
            allocation,
            bytes,
        } = page;
        match value {
            Page::Leaf(values) => {
                let (owned, logical_bytes) = share_values(
                    Owned::new(values, allocation, bytes),
                    F::value_owned_bytes,
                    accounting,
                )?;
                let Owned {
                    value: entries,
                    allocation,
                    bytes,
                } = owned;
                Ok(Owned::new(
                    Self::Leaf {
                        entries,
                        logical_bytes,
                    },
                    allocation,
                    bytes,
                ))
            }
            Page::Internal(values) => {
                let (owned, logical_bytes) = share_values(
                    Owned::new(values, allocation, bytes),
                    |child| F::key_owned_bytes(&child.first),
                    accounting,
                )?;
                let Owned {
                    value: children,
                    allocation,
                    bytes,
                } = owned;
                Ok(Owned::new(
                    Self::Internal {
                        children,
                        logical_bytes,
                    },
                    allocation,
                    bytes,
                ))
            }
        }
    }

    pub(crate) fn view(&self) -> PageRef<'_, F> {
        match self {
            Self::Leaf { entries, .. } => PageRef::Leaf(Entries::Shared(entries)),
            Self::Internal { children, .. } => PageRef::Internal(Entries::Shared(children)),
        }
    }

    pub(crate) fn logical_bytes(&self) -> u64 {
        match self {
            Self::Leaf { logical_bytes, .. } | Self::Internal { logical_bytes, .. } => {
                *logical_bytes
            }
        }
    }
}
