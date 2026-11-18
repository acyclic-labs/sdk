use acyclic_fs::kernel::{
    CanonicalDecodeError, DecodeLimits, FileKind, TreeMutation, TreePage, apply_tree_mutations,
    decode_tree_page,
};
use acyclic_fs::{AsyncObjectStore, AuthorityWork, CancellationToken, MemoryObjectStore};
use acyclic_fs_format::object_reference::ObjectId;
use acyclic_fs_work::performance::WorkBudget;

use super::{entry, put_tree_page};

/// Two-item pages: the item limit closes each page at its second item, so
/// canonical pages are pairs, whatever the keys hash to.
pub(super) fn pair_limits() -> DecodeLimits {
    DecodeLimits {
        maximum_page_items: 2,
        ..DecodeLimits::default()
    }
}

/// The canonical tree of `a m n x`: leaves `[a m]` and `[n x]` under one root.
pub(super) async fn two_leaf_tree(
    store: &MemoryObjectStore,
) -> Result<(ObjectId, [ObjectId; 2]), Box<dyn std::error::Error>> {
    let empty = put_tree_page(store, &TreePage::Leaf(Vec::new()), 8).await?;
    let root = apply_tree_mutations(
        store,
        empty,
        vec![
            TreeMutation::Insert(entry("a", 1, FileKind::Regular)?),
            TreeMutation::Insert(entry("m", 2, FileKind::Regular)?),
            TreeMutation::Insert(entry("n", 3, FileKind::Regular)?),
            TreeMutation::Insert(entry("x", 4, FileKind::Regular)?),
        ],
        pair_limits(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .value;
    let bytes = store
        .read(
            root,
            u64::MAX,
            &mut AuthorityWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new()),
        )
        .await?;
    let TreePage::Internal(children) = decode_tree_page(&bytes, pair_limits(), &mut |_| {
        Ok::<_, CanonicalDecodeError>(())
    })?
    else {
        return Err("two-leaf root is not internal".into());
    };
    let [left, right] = children.as_slice() else {
        return Err("two-leaf root does not hold two leaves".into());
    };
    Ok((root, [left.page, right.page]))
}
