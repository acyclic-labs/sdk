//! A bounded hot cache of pages reachable from the current authenticated roots.
use super::*;
use crate::wire_codec::{OwnedIndexAllocation, OwnedIndexWork};
use acyclic_collections::FixedIndex;

const HOT_PAGES: usize = 128;

struct CachedNode {
    sequence: u64,
    scalar_digest: Option<[u8; 32]>,
    bytes: OwnedBytes,
    io: Option<PreparationIo>,
    referenced: std::sync::atomic::AtomicBool,
}

impl CachedNode {
    fn new(
        sequence: u64,
        bytes: &OwnedBytes,
        retained: u64,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let scalar_digest = (!node::is_branch_bundle(bytes))
            .then(|| crate::model::hash_bytes(bytes, io))
            .transpose()?;
        let mut node = Self {
            sequence,
            scalar_digest,
            bytes: OwnedBytes::from_static(b""),
            referenced: std::sync::atomic::AtomicBool::new(false),
            io: io
                .filter(|_| retained != 0)
                .map(PreparationIo::scoped)
                .transpose()?,
        };
        if let Some(io) = &node.io {
            io.retain_owned(retained)?;
        }
        node.bytes = bytes.clone();
        Ok(node)
    }
}

impl Drop for CachedNode {
    fn drop(&mut self) {
        self.bytes = OwnedBytes::from_static(b"");
        drop(self.io.take());
    }
}

#[derive(Default)]
pub(in crate::filesystem::execution_journal) struct NodeCache {
    values: FixedIndex<CachedNode, OwnedIndexAllocation, 8>,
    len: usize,
    replacement: Option<[u8; 8]>,
    io: Option<PreparationIo>,
}

impl NodeCache {
    pub(in crate::filesystem::execution_journal) fn bind(
        &mut self,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        match (&mut self.io, io) {
            (Some(retained), Some(current)) if retained.shares_ledger(current) => {
                retained.use_current_context(current)?;
            }
            (None, Some(current)) if self.len == 0 => {
                self.io = Some(current.scoped()?);
            }
            (None, None) => (),
            _ => {
                return Err(invalid(
                    io,
                    "execution page cache belongs to another ledger",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn get(
        &self,
        reference: NodeRef,
        io: Option<&PreparationIo>,
    ) -> Result<Option<OwnedBytes>> {
        let node = self
            .values
            .get(reference.sequence.to_be_bytes(), &mut OwnedIndexWork(io))?;
        if let Some(node) = node {
            if node
                .scalar_digest
                .is_some_and(|digest| digest != reference.digest)
            {
                return Err(invalid(io, "execution page cache digest differs"));
            }
            node.referenced
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(node.map(|node| node.bytes.clone()))
    }

    pub(super) fn insert(&mut self, sequence: u64, bytes: &OwnedBytes) -> Result<()> {
        self.insert_retained(
            sequence,
            bytes,
            bytes.retained_capacity().unwrap_or(0) as u64,
        )
    }

    pub(super) fn insert_retained(
        &mut self,
        sequence: u64,
        bytes: &OwnedBytes,
        retained: u64,
    ) -> Result<()> {
        let key = sequence.to_be_bytes();
        if let Some(node) = self
            .values
            .get(key, &mut OwnedIndexWork(self.io.as_ref()))?
        {
            if node.bytes.as_ref() != bytes.as_ref() {
                return Err(invalid(
                    self.io.as_ref(),
                    "execution page cache record differs",
                ));
            }
            node.referenced
                .store(true, std::sync::atomic::Ordering::Relaxed);
            return Ok(());
        }
        if self.len == HOT_PAGES {
            // One second chance per observed page keeps hot immutable roots
            // through unrelated publications. At most HOT_PAGES + 1 probes:
            // this exclusive borrow prevents another get from resetting a bit.
            loop {
                crate::model::charge_items(self.io.as_ref(), 1)?;
                let mut work = OwnedIndexWork(self.io.as_ref());
                let next = match self.replacement {
                    Some(after) => self.values.seek_after(after, &mut work)?,
                    None => self.values.first(&mut work)?,
                };
                let (key, node) = match next {
                    Some(node) => node,
                    None => self.values.first(&mut work)?.ok_or_else(|| {
                        invalid(self.io.as_ref(), "execution page eviction slot is missing")
                    })?,
                };
                let referenced = node
                    .referenced
                    .swap(false, std::sync::atomic::Ordering::Relaxed);
                let sequence = node.sequence;
                self.replacement = Some(*key);
                if !referenced {
                    self.remove(sequence)?;
                    break;
                }
            }
        }
        self.values
            .get_or_try_insert_with(key, &mut OwnedIndexWork(self.io.as_ref()), |_| {
                CachedNode::new(sequence, bytes, retained, self.io.as_ref())
            })?;
        self.len += 1;
        Ok(())
    }

    pub(super) fn remove(&mut self, sequence: u64) -> Result<()> {
        let removed = self.values.remove(
            sequence.to_be_bytes(),
            &mut OwnedIndexWork(self.io.as_ref()),
        )?;
        if removed.is_some() {
            self.len -= 1;
        }
        drop(removed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::StreamProviderExt as _;

    #[derive(Default)]
    struct Settled {
        retained: [Option<u64>; 32],
        staging: Option<(usize, usize, usize, usize)>,
    }

    impl Settled {
        fn check(&mut self, update: u32, cache: &NodeCache, staging: &Scratch) -> Result<()> {
            assert!(
                staging.records.values.is_empty()
                    && staging.retired.values.is_empty()
                    && staging.live.values.is_empty()
                    && staging.path.values.is_empty(),
                "recycled staging retained old publication values"
            );
            let sizes = (
                staging.records.values.capacity(),
                staging.retired.values.capacity(),
                staging.live.values.capacity(),
                staging.path.values.capacity(),
            );
            assert!(
                cache.len < 2 * (update as usize + 1).min(32),
                "cache retained more records than reachable logical nodes"
            );
            if update == 63 {
                self.staging = Some(sizes);
            }
            if update >= 64 {
                assert_eq!(
                    Some(sizes),
                    self.staging,
                    "settled staging capacities grew with history"
                );
            }
            // Once every live sequence uses the same varint width, each pass
            // through the key set rewrites the same bundle shapes in order.
            if update >= 128 {
                let retained = cache.io.as_ref().expect("funded cache").owned_bytes()?;
                let previous = &mut self.retained[update as usize % 32];
                assert_eq!(
                    retained,
                    *previous.get_or_insert(retained),
                    "settled page metadata grew with update count"
                );
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn hot_page_updates_do_not_read_history_or_retain_replaced_pages() -> Result<()> {
        for _native in [
            false,
            #[cfg(not(target_arch = "wasm32"))]
            true,
        ] {
            check_pages(_native, 32).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn distinct_historical_keys_do_not_grow_the_hot_page_cache() -> Result<()> {
        for _native in [
            false,
            #[cfg(not(target_arch = "wasm32"))]
            true,
        ] {
            check_pages(_native, 1_000).await?;
        }
        Ok(())
    }

    async fn check_pages(_native: bool, distinct: u32) -> Result<()> {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let path = StreamPath::new("hot-index/pages")?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs_work::performance::work_budget(256 * 1024 * 1024, 4 * 1024 * 1024);
        #[cfg(not(target_arch = "wasm32"))]
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        #[cfg(not(target_arch = "wasm32"))]
        let native_baseline = work.live_allocation_bytes()?;
        #[cfg(target_arch = "wasm32")]
        let io = PreparationIo::from_budget(budget, &cancellation)?;
        #[cfg(not(target_arch = "wasm32"))]
        let io = if _native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let baseline = io.remaining()?.peak_allocation_bytes;
        let mut cache = NodeCache::default();
        let mut root = None;
        let mut tail = 0;
        let mut scratch = None;
        let mut settled = Settled::default();
        for update in 0..1_000_u32 {
            let before = io.receipt()?;
            let mut writer = Writer::new(&provider, &path, tail, Some(&io), scratch)?;
            writer.set_cache(cache, Some(&io))?;
            let key = *blake3::hash(&(update % distinct).to_be_bytes()).as_bytes();
            root = Some(writer.set(root, key, &update.to_be_bytes(), false).await?);
            cache = writer.prepare_cache()?;
            let outcome = provider
                .append(acyclic_stream::AppendRequest {
                    path: path.clone(),
                    records: writer.records().iter().cloned().map(Into::into).collect(),
                    if_tail: Some(tail),
                    idempotency_key: acyclic_stream::IdempotencyKey::unique(),
                })
                .await?;
            assert!(matches!(outcome, AppendOutcome::Committed(_)));
            tail = writer.tail()?;
            scratch = Some(writer.take_scratch());
            drop(writer);
            let reads = io.receipt()?.authority_records_read - before.authority_records_read;
            if distinct == 32 {
                settled.check(
                    update,
                    &cache,
                    scratch.as_ref().expect("recycled index staging"),
                )?;
                assert_eq!(reads, 0, "hot index update reread committed pages");
            } else {
                assert!(cache.len <= HOT_PAGES, "historical pages stayed resident");
                assert!(
                    reads <= 2 * 257,
                    "update exceeded two fixed-width key paths"
                );
            }
        }
        let reader = Reader {
            provider: &provider,
            path: &path,
            base: tail,
            staged: &[],
            io: Some(&io),
            cache: Some(&cache),
        };
        let before = io.receipt()?;
        for value in 968..1_000_u32 {
            let key = *blake3::hash(&(value % distinct).to_be_bytes()).as_bytes();
            let read = reader.get(root, &key).await?.expect("live cached leaf");
            assert!(
                matches!(read.node(Some(&io))?, Node::Leaf { value: bytes, .. }
                if bytes == value.to_be_bytes())
            );
        }
        if distinct == 32 {
            let mut reachable = std::collections::BTreeSet::new();
            let mut pending = vec![root.expect("live root")];
            while let Some(reference) = pending.pop() {
                reachable.insert(reference.sequence);
                let page = reader.read(reference).await?;
                if let Node::Branch { children, .. } = page.node(Some(&io))? {
                    pending.extend(children);
                }
            }
            assert_eq!(
                cache.len,
                reachable.len(),
                "a replaced record stayed cached"
            );
            assert_eq!(
                io.receipt()?.authority_records_read - before.authority_records_read,
                0
            );
        } else {
            assert!(
                2 * distinct as usize - 1 > HOT_PAGES,
                "unbounded reachable-page negative control did not exceed the cache bound"
            );
            let read = reader
                .get(root, blake3::hash(&0_u32.to_be_bytes()).as_bytes())
                .await?
                .expect("evicted historical leaf");
            assert!(matches!(read.node(Some(&io))?, Node::Leaf { value, .. }
                if value == 0_u32.to_be_bytes()));
            assert!(
                io.receipt()?.authority_records_read > before.authority_records_read,
                "historical lookup never exercised a cold authenticated page"
            );
        }
        let mut forged = root.expect("live root");
        forged.digest[0] ^= 1;
        assert!(
            reader.read(forged).await.is_err(),
            "cache accepted a different page digest"
        );
        drop(cache);
        drop(scratch);
        drop(provider);
        assert_eq!(
            io.remaining()?.peak_allocation_bytes,
            baseline,
            "page cache retained allocation after drop"
        );
        #[cfg(not(target_arch = "wasm32"))]
        if _native {
            assert_eq!(
                work.live_allocation_bytes()?,
                native_baseline,
                "page cache or staging retained original native allocations"
            );
        }
        Ok(())
    }

    #[test]
    fn frequently_read_page_survives_unrelated_publication_churn() -> Result<()> {
        for native in [
            false,
            #[cfg(not(target_arch = "wasm32"))]
            true,
        ] {
            check_churn(native)?;
        }
        Ok(())
    }

    fn check_churn(_native: bool) -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs_work::performance::work_budget(256 * 1024 * 1024, 4 * 1024 * 1024);
        #[cfg(not(target_arch = "wasm32"))]
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        #[cfg(target_arch = "wasm32")]
        let io = PreparationIo::from_budget(budget, &cancellation)?;
        #[cfg(not(target_arch = "wasm32"))]
        let io = if _native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let baseline = io.remaining()?.peak_allocation_bytes;
        let mut cache = NodeCache::default();
        cache.bind(Some(&io))?;
        // Capture an actual page encoder output; cache admission never decodes it.
        let (bytes, _) = Node::Leaf {
            key: [0; 32],
            value: b"immutable fixture page",
            segmented: false,
        }
        .encode(None)?;
        let hot = NodeRef {
            sequence: 0,
            digest: *blake3::hash(&bytes).as_bytes(),
        };
        cache.insert(hot.sequence, &bytes)?;
        let mut misses = 0;
        let mut maximum_items = 0;
        let mut maximum_allocations = 0;
        let mut settled = None;
        for sequence in 1..=1_000_u64 {
            if cache.get(hot, Some(&io))?.is_none() {
                misses += 1;
            }
            let before = io.receipt()?;
            cache.insert(sequence.reverse_bits(), &bytes)?;
            let after = io.receipt()?;
            maximum_items = maximum_items.max(after.items_examined - before.items_examined);
            maximum_allocations =
                maximum_allocations.max(after.allocation_operations - before.allocation_operations);
            assert_eq!(
                after.bytes_copied, before.bytes_copied,
                "page cache insertion copied prior backing"
            );
            assert!(cache.len <= HOT_PAGES);
            if cache.len == HOT_PAGES {
                let remaining = io.remaining()?.peak_allocation_bytes;
                assert_eq!(
                    remaining,
                    *settled.get_or_insert(remaining),
                    "fixed page residency grew its original node or payload funding"
                );
            }
        }
        eprintln!(
            "publication churn=1000 native={_native} hot_page_misses={misses} maximum_items={maximum_items} maximum_allocations={maximum_allocations}"
        );
        assert_eq!(
            misses, 0,
            "frequently read page was evicted by publication order"
        );
        // Each cursor probe uses at most four 64-bit paths (including wrap),
        // plus the initial lookup, extraction and one shared insertion.
        let walk = 8 * 8 + 1;
        let insertion = acyclic_collections::FixedIndex::<
            CachedNode,
            crate::wire_codec::OwnedIndexAllocation,
            8,
        >::insertion_bound(1)
        .expect("one fixed-width insertion")
        .examined_items;
        let maximum =
            (HOT_PAGES as u64 + 1) * (1 + 4 * walk + 8) + 2 * (walk + 8) + 2 * walk + 8 + insertion;
        assert!(
            maximum_items <= maximum,
            "cache insertion exceeded bounded key/credit work: {maximum_items} > {maximum}"
        );
        assert!(
            maximum_allocations <= 2,
            "cache insertion allocated more than its leaf and branch"
        );
        drop(cache);
        assert_eq!(
            io.remaining()?.peak_allocation_bytes,
            baseline,
            "page cache retained its original node or payload funding"
        );
        Ok(())
    }
}
