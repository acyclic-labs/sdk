//! Bounded radix lookup: at most 256 branches, independent of history length.
use super::*;

pub(super) type Position = (usize, usize);
struct Node<T> {
    key: [u8; 32],
    value: Option<T>,
    bit: u16,
    children: [usize; 2],
}
pub(crate) struct Index<T = Position> {
    nodes: Vec<Node<T>>,
    roots: [Option<usize>; 2],
    vacant: Vec<usize>,
}
impl<T> Default for Index<T> {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            roots: [None; 2],
            vacant: Vec::new(),
        }
    }
}
impl<T: Copy> Index<T> {
    /// Visits live leaf entries when building an initial durable snapshot.
    pub(crate) fn entries(&self) -> impl Iterator<Item = ([u8; 32], T)> + '_ {
        self.nodes
            .iter()
            .filter_map(|node| node.value.map(|value| (node.key, value)))
    }

    /// Funds two nodes for another key without rebuilding the retained prefix.
    pub(crate) fn reserve_key(&mut self, io: Option<&PreparationIo>) -> Result<()> {
        let required = self
            .nodes
            .len()
            .checked_add(2_usize.saturating_sub(self.vacant.len()))
            .ok_or_else(super::inventory::overflow)?;
        if required > self.nodes.capacity() {
            let capacity = self
                .nodes
                .capacity()
                .max(2)
                .checked_mul(2)
                .ok_or_else(super::inventory::overflow)?;
            crate::executor::stock_vec_reserve(&mut self.nodes, capacity, io)?;
        }
        Ok(())
    }
    pub(super) fn new(
        parts: usize,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        let capacity = parts
            .checked_mul(2)
            .ok_or_else(super::inventory::overflow)?;
        Ok(Self {
            nodes: crate::core::reserve_copy_vector(capacity, quota)?,
            roots: [None; 2],
            vacant: Vec::new(),
        })
    }
    pub(crate) fn get(
        &self,
        domain: usize,
        key: &[u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<Option<T>> {
        self.get_with(domain, key, |items| step(io, items))
    }

    /// Removes a key and its redundant branch without retaining tombstones.
    pub(crate) fn remove(
        &mut self,
        domain: usize,
        key: &[u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<Option<T>> {
        step(io, 1)?;
        let Some(mut current) = *self
            .roots
            .get(domain)
            .ok_or_else(super::inventory::overflow)?
        else {
            return Ok(None);
        };
        let mut parent = None;
        let mut grandparent = None;
        loop {
            step(io, 1)?;
            let node = self
                .nodes
                .get(current)
                .ok_or_else(super::inventory::overflow)?;
            if node.bit == 256 {
                if node.key != *key || node.value.is_none() {
                    return Ok(None);
                }
                break;
            }
            let side = direction(key, node.bit);
            grandparent = parent;
            parent = Some((current, side));
            current = *node
                .children
                .get(side)
                .ok_or_else(super::inventory::overflow)?;
        }
        let count = self
            .vacant
            .len()
            .checked_add(2)
            .ok_or_else(super::inventory::overflow)?;
        if count > self.vacant.capacity() {
            let capacity = self
                .vacant
                .capacity()
                .checked_mul(2)
                .map(|n| n.max(count))
                .ok_or_else(super::inventory::overflow)?;
            crate::executor::stock_vec_reserve(&mut self.vacant, capacity, io)?;
        }
        let value = self
            .nodes
            .get_mut(current)
            .ok_or_else(super::inventory::overflow)?
            .value
            .take();
        if let Some((parent, side)) = parent {
            let sibling = *self
                .nodes
                .get(parent)
                .and_then(|node| node.children.get(1 - side))
                .ok_or_else(super::inventory::overflow)?;
            if let Some((grandparent, side)) = grandparent {
                *self
                    .nodes
                    .get_mut(grandparent)
                    .and_then(|node| node.children.get_mut(side))
                    .ok_or_else(super::inventory::overflow)? = sibling;
            } else {
                *self
                    .roots
                    .get_mut(domain)
                    .ok_or_else(super::inventory::overflow)? = Some(sibling);
            }
            self.vacant.push(parent);
        } else {
            *self
                .roots
                .get_mut(domain)
                .ok_or_else(super::inventory::overflow)? = None;
        }
        self.vacant.push(current);
        Ok(value)
    }
    pub(super) fn get_history(
        &self,
        domain: usize,
        key: &[u8; 32],
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<T>> {
        self.get_with(domain, key, |items| history_step_traced(work, items))
    }
    fn get_with(
        &self,
        domain: usize,
        key: &[u8; 32],
        mut charge: impl FnMut(u64) -> Result<()>,
    ) -> Result<Option<T>> {
        charge(1)?;
        let Some(mut node) = self.roots[domain] else {
            return Ok(None);
        };
        loop {
            charge(1)?;
            let current = &self.nodes[node];
            if current.bit == 256 {
                charge(32)?;
                return Ok((current.key == *key).then_some(current.value).flatten());
            }
            node = current.children[direction(key, current.bit)];
        }
    }
    pub(crate) fn set(
        &mut self,
        domain: usize,
        key: [u8; 32],
        value: Option<T>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        self.set_with(domain, key, value, |items| step(io, items))
    }
    pub(super) fn set_history(
        &mut self,
        domain: usize,
        key: [u8; 32],
        value: Option<T>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        self.set_with(domain, key, value, |items| history_step_traced(work, items))
    }
    fn set_with(
        &mut self,
        domain: usize,
        key: [u8; 32],
        value: Option<T>,
        mut charge: impl FnMut(u64) -> Result<()>,
    ) -> Result<()> {
        charge(1)?;
        let Some(mut node) = self.roots[domain] else {
            self.roots[domain] = Some(self.leaf(key, value)?);
            return Ok(());
        };
        while self.nodes[node].bit < 256 {
            charge(1)?;
            node = self.nodes[node].children[direction(&key, self.nodes[node].bit)];
        }
        charge(33)?;
        let old = self.nodes[node].key;
        let difference = old.iter().zip(key).enumerate().find_map(|(i, (a, b))| {
            let xor = a ^ b;
            (xor != 0).then(|| (i * 8 + xor.leading_zeros() as usize) as u16)
        });
        let Some(bit) = difference else {
            self.nodes[node].value = value;
            return Ok(());
        };
        let leaf = self.leaf(key, value)?;
        let mut current = self.roots[domain].unwrap();
        let mut parent = None;
        while self.nodes[current].bit < bit {
            charge(1)?;
            let side = direction(&key, self.nodes[current].bit);
            parent = Some((current, side));
            current = self.nodes[current].children[side];
        }
        let mut children = [current; 2];
        children[direction(&key, bit)] = leaf;
        let branch = self.insert_node(Node {
            key: [0; 32],
            value: None,
            bit,
            children,
        })?;
        match parent {
            Some((parent, side)) => self.nodes[parent].children[side] = branch,
            None => self.roots[domain] = Some(branch),
        }
        Ok(())
    }
    fn leaf(&mut self, key: [u8; 32], value: Option<T>) -> Result<usize> {
        self.insert_node(Node {
            key,
            value,
            bit: 256,
            children: [0; 2],
        })
    }

    fn insert_node(&mut self, value: Node<T>) -> Result<usize> {
        if let Some(node) = self.vacant.pop() {
            *self
                .nodes
                .get_mut(node)
                .ok_or_else(super::inventory::overflow)? = value;
            return Ok(node);
        }
        let node = self.nodes.len();
        assert!(
            node < self.nodes.capacity(),
            "preflighted instruction index capacity"
        );
        self.nodes.push(value);
        Ok(node)
    }
}
fn direction(key: &[u8; 32], bit: u16) -> usize {
    usize::from((key[usize::from(bit / 8)] >> (7 - bit % 8)) & 1)
}

pub(super) fn source_key(
    volume: &crate::conversation::VolumeRef,
    path: &str,
    io: Option<&PreparationIo>,
) -> Result<[u8; 32]> {
    with_history_io(io, |work| source_key_history(volume, path, work))
}

pub(super) fn source_key_history(
    volume: &crate::conversation::VolumeRef,
    path: &str,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<[u8; 32]> {
    // Keys are request-local; the unobserved path hashes the borrowed wire.
    if work.is_none() {
        return crate::executor::json::serialized_json_digest(&(volume, path));
    }
    #[cfg(feature = "filesystem")]
    let maximum = work
        .as_deref()
        .map(|work| work.remaining().map(|b| b.bytes_encoded))
        .transpose()?
        .unwrap_or(u64::MAX);
    #[cfg(not(feature = "filesystem"))]
    let maximum = u64::MAX;
    crate::executor::json::canonical_json_digest(&(volume, path), maximum, work)
}

pub(super) fn history_step_traced(work: &mut Option<&mut dyn HistoryIo>, items: u64) -> Result<()> {
    history_step(work, items)?;
    trace_step(items);
    Ok(())
}

pub(super) fn step(io: Option<&PreparationIo>, items: u64) -> Result<()> {
    charge_items(io, items)?;
    trace_step(items);
    Ok(())
}
fn trace_step(items: u64) {
    trace_operation("instruction-step", items);
}
pub(super) fn trace_operation(name: &'static str, items: u64) {
    let _ = (name, items);
    #[cfg(all(
        test,
        feature = "test-support",
        feature = "filesystem",
        not(target_arch = "wasm32")
    ))]
    TRACE_STEPS.with(|active| {
        if active.get() {
            acyclic_test_support::differential::record((name, items));
        }
    });
}

#[cfg(all(
    test,
    feature = "test-support",
    feature = "filesystem",
    not(target_arch = "wasm32")
))]
thread_local! { static TRACE_STEPS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(all(
    test,
    feature = "test-support",
    feature = "filesystem",
    not(target_arch = "wasm32")
))]
pub(super) fn traced<T>(run: impl FnOnce() -> T) -> T {
    TRACE_STEPS.with(|active| assert!(!active.replace(true)));
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            TRACE_STEPS.with(|active| active.set(false));
        }
    }
    let _reset = Reset;
    run()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(index: u64) -> [u8; 32] {
        let mut key = [0; 32];
        key[..8].copy_from_slice(&index.to_be_bytes());
        key
    }

    #[test]
    fn removing_radix_keys_reuses_storage_and_preserves_both_domains() -> Result<()> {
        let mut index = Index::<u64>::default();
        for domain in 0..2 {
            for value in 0..32 {
                index.reserve_key(None)?;
                index.set(domain, key(value), Some(value), None)?;
            }
        }
        let nodes = index.nodes.len();
        let capacity = index.nodes.capacity();
        for cycle in 0..1_000 {
            for value in 0..32 {
                assert_eq!(
                    index.remove(0, &key(value + cycle * 32), None)?,
                    Some(value)
                );
                assert_eq!(index.remove(0, &key(value + cycle * 32), None)?, None);
                assert_eq!(index.get(1, &key(value), None)?, Some(value));
            }
            assert!(index.roots[0].is_none());
            for value in 0..32 {
                index.reserve_key(None)?;
                index.set(0, key(value + (cycle + 1) * 32), Some(value), None)?;
                assert_eq!(index.get(0, &key(value + cycle * 32), None)?, None);
            }
            assert_eq!(index.nodes.len(), nodes, "retired nodes accumulated");
            assert_eq!(
                index.nodes.capacity(),
                capacity,
                "churn grew the radix arena"
            );
            assert_eq!(index.entries().count(), 64);
        }
        Ok(())
    }
}
