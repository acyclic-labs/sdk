use acyclic_fs::MemoryObjectStore;
use acyclic_fs::kernel::{Child, FileKind, TreeEntry, TreePage};
use acyclic_fs_format::object_reference::ObjectId;

use super::{name, put, tree_entry};

pub(super) fn entry(value: &str, byte: u8) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    tree_entry(value, byte, FileKind::Regular)
}

pub(super) async fn tree(
    store: &MemoryObjectStore,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let a = put(
        store,
        &TreePage::Leaf(vec![entry("a", 1)?, entry("b", 2)?]),
        8,
    )
    .await?;
    let c = put(
        store,
        &TreePage::Leaf(vec![entry("c", 3)?, entry("d", 4)?]),
        8,
    )
    .await?;
    let e = put(
        store,
        &TreePage::Leaf(vec![entry("e", 5)?, entry("f", 6)?]),
        8,
    )
    .await?;
    put(
        store,
        &TreePage::Internal(vec![
            Child {
                first: name("a")?,
                page: a,
            },
            Child {
                first: name("c")?,
                page: c,
            },
            Child {
                first: name("e")?,
                page: e,
            },
        ]),
        8,
    )
    .await
}
