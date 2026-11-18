mod extent_clone;
mod extent_mutation;
mod file_read;
mod range;

#[path = "../../support/extent_objects.rs"]
mod extent_objects;
#[path = "../../support/sparse_extents.rs"]
mod sparse_extents;

use acyclic_fs::kernel::{
    CanonicalDecodeError, DecodeLimits, Extent, ExtentPage, decode_extent_page,
};
use acyclic_fs::{AsyncObjectStore, AuthorityWork, CancellationToken, MemoryObjectStore};
use acyclic_fs_format::object_reference::ObjectId;
use acyclic_fs_work::performance::WorkBudget;

async fn read_extent_page(
    store: &MemoryObjectStore,
    page: ObjectId,
    limits: DecodeLimits,
) -> Result<ExtentPage, Box<dyn std::error::Error>> {
    let bytes = store
        .read(
            page,
            u64::MAX,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?;
    Ok(decode_extent_page(&bytes, limits, &mut |_| {
        Ok::<_, CanonicalDecodeError>(())
    })?)
}

async fn leaf_extents(
    store: &MemoryObjectStore,
    page: ObjectId,
) -> Result<Vec<Extent>, Box<dyn std::error::Error>> {
    let ExtentPage::Leaf(extents) = read_extent_page(store, page, DecodeLimits::default()).await?
    else {
        return Err("extent page is not a leaf".into());
    };
    Ok(extents)
}
