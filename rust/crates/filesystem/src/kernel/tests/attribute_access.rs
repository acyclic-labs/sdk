use super::*;
use crate::test_support::{OwnedReadObjectStore, put_attribute_page as put};

#[path = "../../../tests/support/attribute_entries.rs"]
mod attribute_entries;
#[path = "../../../tests/support/attribute_lookup_tree.rs"]
mod attribute_lookup_tree;
use attribute_lookup_tree::{entry, name, two_level_tree};

#[tokio::test]
async fn owned_attribute_pages_are_admitted_and_released_across_the_frontier()
-> Result<(), Box<dyn std::error::Error>> {
    let store = OwnedReadObjectStore::default();
    let root = two_level_tree(&store.inner).await?;
    let lookup = lookup_attribute(
        &store,
        root,
        &name(4)?,
        DecodeLimits::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(lookup.value, Some(entry(4)?));
    assert_eq!(lookup.work.page_reads, 2);
    assert!(lookup.work.bytes_copied > 0);
    assert!(lookup.work.peak_allocation_bytes > 0);
    Ok(())
}
