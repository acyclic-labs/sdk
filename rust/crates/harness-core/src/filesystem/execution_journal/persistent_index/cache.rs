//! A bounded hot cache of pages reachable from the current authenticated roots.
use super::*;
use crate::wire_codec::{OwnedIndexAllocation, OwnedIndexWork};
use acyclic_collections::FixedIndex;

const HOT_PAGES: usize = 128;

struct CachedNode {
    reference: NodeRef,
    bytes: OwnedBytes,
    io: Option<PreparationIo>,
    referenced: std::sync::atomic::AtomicBool,
}

impl CachedNode {
    fn new(reference: NodeRef, bytes: &OwnedBytes, io: Option<&PreparationIo>) -> Result<Self> {
        let mut node = Self {
            reference,
            bytes: OwnedBytes::from_static(b""),
            referenced: std::sync::atomic::AtomicBool::new(false),
            io: io
                .filter(|_| bytes.retained_capacity().unwrap_or(0) != 0)
                .map(PreparationIo::scoped)
                .transpose()?,
        };
        if let Some(io) = &node.io {
            io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?;
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
    pub(super) fn bind(&mut self, io: Option<&PreparationIo>) -> Result<()> {
        match (&mut self.io, io) {
            (Some(retained), Some(current)) if retained.shares_ledger(current) => {
                retained.use_current_context(current);
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
        let node = self.node(reference, io)?;
        if let Some(node) = node {
            node.referenced
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(node.map(|node| node.bytes.clone()))
    }

    fn node(&self, reference: NodeRef, io: Option<&PreparationIo>) -> Result<Option<&CachedNode>> {
        let node = self
            .values
            .get(reference.sequence.to_be_bytes(), &mut OwnedIndexWork(io))?;
        if node.is_some_and(|node| node.reference != reference) {
            return Err(invalid(io, "execution page cache digest differs"));
        }
        Ok(node)
    }

    pub(super) fn insert(&mut self, reference: NodeRef, bytes: &OwnedBytes) -> Result<()> {
        if let Some(node) = self.node(reference, self.io.as_ref())? {
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
                let reference = node.reference;
                self.replacement = Some(*key);
                if !referenced {
                    self.remove(reference)?;
                    break;
                }
            }
        }
        self.values.get_or_try_insert_with(
            reference.sequence.to_be_bytes(),
            &mut OwnedIndexWork(self.io.as_ref()),
            |_| CachedNode::new(reference, bytes, self.io.as_ref()),
        )?;
        self.len += 1;
        Ok(())
    }

    pub(super) fn remove(&mut self, reference: NodeRef) -> Result<()> {
        if self.node(reference, self.io.as_ref())?.is_none() {
            return Ok(());
        }
        let removed = self.values.remove(
            reference.sequence.to_be_bytes(),
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
        retained: u64,
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
            assert_eq!(
                cache.len,
                2 * (update as usize + 1).min(32) - 1,
                "cache retained a replaced tree version"
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
            if update == 99 {
                self.retained = cache
                    .io
                    .as_ref()
                    .expect("funded cache")
                    .remaining()?
                    .peak_allocation_bytes;
            }
            if update >= 100 {
                assert_eq!(
                    cache
                        .io
                        .as_ref()
                        .expect("funded cache")
                        .remaining()?
                        .peak_allocation_bytes,
                    self.retained,
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
        let budget = acyclic_fs::performance::work_budget(256 * 1024 * 1024, 4 * 1024 * 1024);
        #[cfg(not(target_arch = "wasm32"))]
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        #[cfg(not(target_arch = "wasm32"))]
        let native_baseline = work.live_allocation_bytes();
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
            let before = io.receipt();
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
            let reads = io.receipt().authority_records_read - before.authority_records_read;
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
        let before = io.receipt();
        for value in 968..1_000_u32 {
            let key = *blake3::hash(&(value % distinct).to_be_bytes()).as_bytes();
            let read = reader.get(root, &key).await?.expect("live cached leaf");
            assert!(
                matches!(read.node(Some(&io))?, Node::Leaf { value: bytes, .. }
                if bytes == value.to_be_bytes())
            );
        }
        if distinct == 32 {
            assert_eq!(
                io.receipt().authority_records_read - before.authority_records_read,
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
                io.receipt().authority_records_read > before.authority_records_read,
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
                work.live_allocation_bytes(),
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
        let budget = acyclic_fs::performance::work_budget(256 * 1024 * 1024, 4 * 1024 * 1024);
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
        cache.insert(hot, &bytes)?;
        let mut misses = 0;
        let mut maximum_items = 0;
        let mut maximum_allocations = 0;
        for sequence in 1..=1_000_u64 {
            if cache.get(hot, Some(&io))?.is_none() {
                misses += 1;
            }
            let before = io.receipt();
            cache.insert(
                NodeRef {
                    sequence: sequence.reverse_bits(),
                    ..hot
                },
                &bytes,
            )?;
            let after = io.receipt();
            maximum_items = maximum_items.max(after.items_examined - before.items_examined);
            maximum_allocations =
                maximum_allocations.max(after.allocation_operations - before.allocation_operations);
            assert_eq!(
                after.bytes_copied, before.bytes_copied,
                "page cache insertion copied prior backing"
            );
            assert!(cache.len <= HOT_PAGES);
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
