//! A bounded hot cache of pages reachable from the current authenticated roots.
use super::*;

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
    keys: crate::model::DigestIndex<usize>,
    values: Vec<Option<CachedNode>>,
    vacant: Vec<usize>,
    replacement: usize,
    io: Option<PreparationIo>,
}

fn key(sequence: u64) -> [u8; 32] {
    let mut key = [0; 32];
    key[..8].copy_from_slice(&sequence.to_be_bytes());
    key
}

impl NodeCache {
    pub(super) fn bind(&mut self, io: Option<&PreparationIo>) -> Result<()> {
        match (&mut self.io, io) {
            (Some(retained), Some(current)) if retained.shares_ledger(current) => {
                retained.use_current_context(current);
            }
            (None, Some(current)) if self.values.capacity() == 0 && self.vacant.capacity() == 0 => {
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
        let Some(position) = self.keys.get(0, &key(reference.sequence), io)? else {
            return Ok(None);
        };
        let node = self
            .values
            .get(position)
            .and_then(Option::as_ref)
            .ok_or_else(|| invalid(io, "execution page cache slot is missing"))?;
        if node.reference != reference {
            return Err(invalid(io, "execution page cache digest differs"));
        }
        node.referenced
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(Some(node.bytes.clone()))
    }

    pub(super) fn insert(&mut self, reference: NodeRef, bytes: &OwnedBytes) -> Result<()> {
        if self.get(reference, self.io.as_ref())?.is_some() {
            return Ok(());
        }
        if self.vacant.is_empty() && self.values.len() == HOT_PAGES {
            // One second chance per observed page keeps hot immutable roots
            // through unrelated publications. At most HOT_PAGES + 1 probes:
            // this exclusive borrow prevents another get from resetting a bit.
            loop {
                crate::model::charge_items(self.io.as_ref(), 1)?;
                let node = self
                    .values
                    .get(self.replacement)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        invalid(self.io.as_ref(), "execution page eviction slot is missing")
                    })?;
                let referenced = node
                    .referenced
                    .swap(false, std::sync::atomic::Ordering::Relaxed);
                let reference = node.reference;
                self.replacement = (self.replacement + 1) % HOT_PAGES;
                if !referenced {
                    self.remove(reference)?;
                    break;
                }
            }
        }
        self.keys.reserve_key(self.io.as_ref())?;
        if self.vacant.is_empty() && self.values.len() == self.values.capacity() {
            let capacity = self
                .values
                .capacity()
                .checked_mul(2)
                .map(|n| n.max(1))
                .ok_or_else(|| invalid(self.io.as_ref(), "execution page cache overflows"))?;
            crate::executor::stock_vec_reserve(&mut self.values, capacity, self.io.as_ref())?;
        }
        let node = CachedNode::new(reference, bytes, self.io.as_ref())?;
        let position = self.vacant.last().copied().unwrap_or(self.values.len());
        self.keys
            .set(0, key(reference.sequence), Some(position), self.io.as_ref())?;
        if self.vacant.pop().is_some() {
            *self.values.get_mut(position).ok_or_else(|| {
                invalid(self.io.as_ref(), "execution page cache slot is missing")
            })? = Some(node);
        } else {
            self.values.push(Some(node));
        }
        Ok(())
    }

    pub(super) fn remove(&mut self, reference: NodeRef) -> Result<()> {
        if self
            .keys
            .get(0, &key(reference.sequence), self.io.as_ref())?
            .is_none()
        {
            return Ok(());
        }
        if self.vacant.len() == self.vacant.capacity() {
            let capacity = self
                .vacant
                .capacity()
                .checked_mul(2)
                .map(|n| n.max(1))
                .ok_or_else(|| {
                    invalid(self.io.as_ref(), "execution page cache free slots overflow")
                })?;
            crate::executor::stock_vec_reserve(&mut self.vacant, capacity, self.io.as_ref())?;
        }
        let position = self
            .keys
            .remove(0, &key(reference.sequence), self.io.as_ref())?
            .ok_or_else(|| invalid(self.io.as_ref(), "execution page cache key disappeared"))?;
        drop(
            self.values
                .get_mut(position)
                .ok_or_else(|| invalid(self.io.as_ref(), "execution page cache slot is missing"))?
                .take(),
        );
        self.vacant.push(position);
        Ok(())
    }
}

impl Drop for NodeCache {
    fn drop(&mut self) {
        self.keys = Default::default();
        self.values = Vec::new();
        self.vacant = Vec::new();
        drop(self.io.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Settled {
        slots: usize,
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
                cache.keys.entries().count(),
                2 * (update as usize + 1).min(32) - 1,
                "cache retained a replaced tree version"
            );
            if update == 63 {
                self.staging = Some(sizes);
                self.slots = cache.values.capacity();
            }
            if update >= 64 {
                assert_eq!(
                    Some(sizes),
                    self.staging,
                    "settled staging capacities grew with history"
                );
                assert_eq!(
                    cache.values.capacity(),
                    self.slots,
                    "replaced pages grew cache slots"
                );
            }
            if update == 99 {
                self.retained = cache.io.as_ref().expect("funded cache").owned_bytes()?;
            }
            if update >= 100 {
                assert_eq!(
                    cache.io.as_ref().expect("funded cache").owned_bytes()?,
                    self.retained,
                    "settled page metadata grew with update count"
                );
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn hot_page_updates_do_not_read_history_or_retain_replaced_pages() -> Result<()> {
        for native in [false, true] {
            check_pages(native, 32).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn distinct_historical_keys_do_not_grow_the_hot_page_cache() -> Result<()> {
        for native in [false, true] {
            check_pages(native, 1_000).await?;
        }
        Ok(())
    }

    async fn check_pages(native: bool, distinct: u32) -> Result<()> {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let path = StreamPath::new("hot-index/pages")?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(256 * 1024 * 1024, 4 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let native_baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let baseline = io.owned_bytes()?;
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
            root = Some(writer.set(root, key, &update.to_be_bytes()).await?);
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
                assert!(
                    cache.keys.entries().count() <= HOT_PAGES,
                    "historical pages stayed resident"
                );
                assert!(
                    cache.values.capacity() <= HOT_PAGES,
                    "historical page slots stayed resident"
                );
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
            io.owned_bytes()?,
            baseline,
            "page cache retained allocation after drop"
        );
        if native {
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
        let mut cache = NodeCache::default();
        let bytes = OwnedBytes::from_static(b"immutable fixture page");
        let hot = NodeRef {
            sequence: 0,
            digest: *blake3::hash(&bytes).as_bytes(),
        };
        cache.insert(hot, &bytes)?;
        let mut misses = 0;
        for sequence in 1..=1000 {
            if cache.get(hot, None)?.is_none() {
                misses += 1;
            }
            cache.insert(NodeRef { sequence, ..hot }, &bytes)?;
            assert!(cache.keys.entries().count() <= HOT_PAGES);
            assert!(cache.values.capacity() <= HOT_PAGES);
        }
        eprintln!("publication churn=1000 hot_page_misses={misses}");
        assert_eq!(
            misses, 0,
            "frequently read page was evicted by publication order"
        );
        Ok(())
    }
}
