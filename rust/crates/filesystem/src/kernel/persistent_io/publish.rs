use super::*;
use crate::kernel::persistent_btree::entries::{Entry, copy_entries};
use crate::kernel::persistent_point::Owned;

// Call only after put_hashed succeeds: a decoded cache hit must never make an
// object visible before its authenticated bytes exist in the object store.
pub(crate) fn retain_written_leaf<S: AsyncObjectStore, F: Format>(
    store: &S,
    object: ObjectId,
    limits: DecodeLimits,
    entries: &[Arc<Entry<F::Value>>],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    retain_written(
        store,
        object,
        limits,
        entries,
        accounting,
        |entries, logical_bytes| SharedPage::<F>::Leaf {
            entries,
            logical_bytes,
        },
    )
}

pub(crate) fn retain_written_internal<S: AsyncObjectStore, F: Format>(
    store: &S,
    object: ObjectId,
    limits: DecodeLimits,
    children: &[Arc<Entry<Child<F::Key>>>],
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    retain_written(
        store,
        object,
        limits,
        children,
        accounting,
        |children, logical_bytes| SharedPage::<F>::Internal {
            children,
            logical_bytes,
        },
    )
}

fn retain_written<S: AsyncObjectStore, F: Format, T>(
    store: &S,
    object: ObjectId,
    limits: DecodeLimits,
    entries: &[Arc<Entry<T>>],
    accounting: &mut KernelAccounting<'_>,
    page: impl FnOnce(Vec<Arc<Entry<T>>>, u64) -> SharedPage<F>,
) -> Result<(), FsError> {
    #[cfg(all(
        test,
        feature = "memory",
        not(acyclic_fs_loom),
        not(target_arch = "wasm32")
    ))]
    if tests::omit_written_cache() {
        return Ok(());
    }
    if !store.supports_decoded_cache() {
        return Ok(());
    }
    let empty = accounting.claim_owned(0, 0)?;
    let (copied, logical_bytes) = copy_entries(entries, accounting)?;
    let shared = Owned::new(
        page(copied.value, logical_bytes),
        copied.allocation,
        copied.bytes,
    );
    let shared = super::cache::reserve_shared_controls(shared, accounting)?;
    let (page, allocation) = super::cache::admit_shared_page(
        store,
        DecodedCacheKey::new::<SharedPage<F>>(object, limits),
        shared,
        empty,
        accounting,
    )?;
    let bytes = page.logical_bytes;
    drop(page);
    crate::kernel::persistent_point::retire_allocation(allocation, bytes, accounting)
}

#[cfg(all(
    test,
    feature = "memory",
    not(acyclic_fs_loom),
    not(target_arch = "wasm32")
))]
#[path = "../tests/persistent_io/publish.rs"]
mod tests;
