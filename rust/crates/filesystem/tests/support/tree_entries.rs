use acyclic_fs::kernel::{
    CanonicalDecodeError, DecodeLimits, FileKind, LogicalName, NameEncoding, TreeEntry, TreePage,
    decode_tree_page,
};
use acyclic_fs::{AsyncObjectStore, AuthorityWork, CancellationToken, MemoryObjectStore};
use acyclic_fs_format::identity::FileId;
use acyclic_fs_format::object_reference::ObjectId;
use acyclic_fs_work::performance::WorkBudget;

pub(crate) fn name(value: &str) -> Result<LogicalName, Box<dyn std::error::Error>> {
    Ok(LogicalName::new(
        NameEncoding::Utf8,
        value.as_bytes().to_vec(),
    )?)
}

pub(crate) fn entry(
    value: &str,
    byte: u8,
    kind: FileKind,
) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    Ok(TreeEntry {
        name: name(value)?,
        file_id: FileId::from_bytes([byte; 16]),
        kind,
    })
}

pub(crate) fn wide_entry(
    value: u16,
    kind: FileKind,
) -> Result<TreeEntry, Box<dyn std::error::Error>> {
    entry(
        &format!("wide-{value:04}"),
        u8::try_from(value & 0xff)?,
        kind,
    )
}

pub(crate) async fn flatten_tree(
    store: &MemoryObjectStore,
    root: ObjectId,
) -> Result<Vec<TreeEntry>, Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let mut pages = vec![root];
    let mut entries = Vec::new();
    while let Some(page) = pages.pop() {
        let bytes = store.read(page, u64::MAX, &mut work).await?;
        match decode_tree_page(&bytes, DecodeLimits::default(), &mut |_| {
            Ok::<_, CanonicalDecodeError>(())
        })? {
            TreePage::Leaf(leaf) => entries.extend(leaf),
            TreePage::Internal(children) => {
                pages.extend(children.into_iter().rev().map(|child| child.page));
            }
        }
    }
    Ok(entries)
}
