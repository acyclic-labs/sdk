use acyclic_fs::MemoryObjectStore;
use acyclic_fs::kernel::{Child, TreeEntry, TreePage};

use super::put_tree_page;

pub(super) async fn packed_tree(
    store: &MemoryObjectStore,
    entries: impl IntoIterator<Item = Result<TreeEntry, Box<dyn std::error::Error>>>,
    width: usize,
) -> Result<Option<Child<acyclic_fs::kernel::LogicalName>>, Box<dyn std::error::Error>> {
    if width < 2 {
        return Err("fixture page width must admit at least two children".into());
    }
    let limit = u32::try_from(width)?;
    let mut entries = entries.into_iter();
    let mut level = Vec::new();
    loop {
        let values = entries
            .by_ref()
            .take(width)
            .collect::<Result<Vec<_>, _>>()?;
        let Some(first) = values.first().map(|entry| entry.name.clone()) else {
            break;
        };
        let page = put_tree_page(store, &TreePage::Leaf(values), limit).await?;
        level.push(Child { first, page });
    }
    while level.len() > 1 {
        let mut parents = Vec::new();
        let mut children = level.into_iter();
        while children.len() != 0 {
            let batch: Vec<_> = children.by_ref().take(width).collect();
            let first = batch.first().ok_or("missing fixture child")?.first.clone();
            let page = put_tree_page(store, &TreePage::Internal(batch), limit).await?;
            parents.push(Child { first, page });
        }
        level = parents;
    }
    Ok(level.pop())
}
