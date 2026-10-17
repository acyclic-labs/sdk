use super::*;
use proptest::prelude::*;

fn options(bytes: u64, chunk: u32, width: u32) -> BlobBuildOptions {
    BlobBuildOptions {
        maximum_blob_bytes: bytes,
        chunk_bytes: chunk,
        page_items: width,
        page_bytes: u32::MAX,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // A page reserves exactly the references allowed by the source bound;
    // repeated ceiling division agrees with independent wide arithmetic.
    #[test]
    fn capacity_matches_the_maximum_possible_references(
        bytes in 1..=u64::MAX, chunk in 1..=u32::MAX,
        width in 2u32..=1024, level in 0usize..=6,
    ) {
        let index = BlobIndexBuilder::new(options(bytes, chunk, width));
        let divisor = u128::from(chunk) * u128::from(width).pow(level as u32);
        let expected = u128::from(bytes).div_ceil(divisor).min(u128::from(width));
        prop_assert_eq!(index.capacity(level) as u128, expected);
        prop_assert!((1..=width as usize).contains(&index.capacity(level)));
    }

    // Tight reservation bounds must preserve every authenticated page and byte,
    // including short sources and multi-level page-boundary crossings.
    #[test]
    fn source_bounds_do_not_change_blob_identity(
        bytes in prop::collection::vec(any::<u8>(), 0..256),
        chunk in 1u32..=16, width in 2u32..=8,
    ) {
        let store = crate::memory::MemoryObjectStore::default();
        let cancellation = CancellationToken::new();
        let build = |maximum| futures::executor::block_on(build_blob(
            &store, &mut std::io::Cursor::new(&bytes),
            options(maximum, chunk, width), WorkBudget::UNBOUNDED, &cancellation,
        ));
        let tight = build((bytes.len() as u64).max(1)).unwrap();
        let loose = build(1024 * 1024).unwrap();
        prop_assert_eq!(tight.root, loose.root);
        prop_assert_eq!(tight.logical_bytes, bytes.len() as u64);
        prop_assert_eq!(tight.work.source_bytes_read, loose.work.source_bytes_read);
    }
}

#[test]
fn small_blob_reservations_do_not_depend_on_page_width() -> Result<(), Box<dyn std::error::Error>> {
    fn build(overreserve: bool) -> Result<(ObjectId, u64, usize, usize), FsError> {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let store = crate::memory::MemoryObjectStore::default();
        let mut batching = BlobBatchStore::new(&store, &mut accounting)?;
        let mut index = BlobIndexBuilder::new(options(1, 1024 * 1024, 1024));
        if overreserve {
            // Fault the production allocation path with its former reservation.
            reserve_blob_vec(&mut index.leaf, index.width, &mut accounting)?;
        }
        let chunk = HashedObject::new(ObjectKind::BlobChunk, vec![b'x'].into());
        let chunk = BlobChunkRef {
            first_offset: 0,
            end_offset: 1,
            chunk: chunk.object_id(),
        };
        futures::executor::block_on(index.push_chunk(
            &mut batching,
            chunk,
            &mut accounting,
            &cancellation,
        ))?;
        let root = futures::executor::block_on(index.finish(
            &mut batching,
            &mut accounting,
            &cancellation,
        ))?;
        let leaf = index.leaf.value.capacity();
        let parent = index.levels.value[0].value.capacity();
        let allocated = accounting.spent().peak_allocation_bytes;
        index.release(&mut accounting)?;
        batching.release(&mut accounting)?;
        Ok((root, allocated, leaf, parent))
    }
    let normal = build(false)?;
    let broken = build(true)?;
    assert_eq!(
        normal.0, broken.0,
        "allocation policy changed canonical bytes"
    );
    let bounded = |result: &(ObjectId, u64, usize, usize)| result.2 == 1 && result.3 == 1;
    assert!(bounded(&normal));
    assert!(!bounded(&broken), "full-page reservation escaped the bound");
    assert!(broken.1 > normal.1);
    println!("blob index normal={normal:?}, full-page control={broken:?}");
    Ok(())
}
