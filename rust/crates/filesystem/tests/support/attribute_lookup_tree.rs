use acyclic_fs::MemoryObjectStore;
use acyclic_fs::kernel::{AttributeEntry, AttributeName, AttributePage, Child};
use acyclic_fs_format::object_reference::ObjectId;

use super::{attribute_entries, put};

pub(crate) fn name(byte: u8) -> Result<AttributeName, Box<dyn std::error::Error>> {
    attribute_entries::name([byte])
}

pub(crate) fn entry(byte: u8) -> Result<AttributeEntry, Box<dyn std::error::Error>> {
    attribute_entries::entry([byte], u64::from(byte), byte)
}

pub(crate) async fn two_level_tree(
    store: &MemoryObjectStore,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let first = put(store, &AttributePage::Leaf(vec![entry(1)?]), 8).await?;
    let second = put(store, &AttributePage::Leaf(vec![entry(3)?, entry(4)?]), 8).await?;
    put(
        store,
        &AttributePage::Internal(vec![
            Child {
                first: name(1)?,
                page: first,
            },
            Child {
                first: name(3)?,
                page: second,
            },
        ]),
        8,
    )
    .await
}
