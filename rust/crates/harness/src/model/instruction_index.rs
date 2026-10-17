//! Bounded radix lookup: at most 256 branches, independent of history length.
use super::*;

pub(super) type Position = (usize, usize);
struct Node {
    key: [u8; 32],
    value: Option<Position>,
    bit: u16,
    children: [usize; 2],
}
pub(super) struct Index {
    nodes: Vec<Node>,
    roots: [Option<usize>; 2],
}
impl Index {
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
        })
    }
    pub(super) fn get(
        &self,
        domain: usize,
        key: &[u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<Option<Position>> {
        step(io, 1)?;
        let Some(mut node) = self.roots[domain] else {
            return Ok(None);
        };
        loop {
            step(io, 1)?;
            let current = &self.nodes[node];
            if current.bit == 256 {
                step(io, 32)?;
                return Ok((current.key == *key).then_some(current.value).flatten());
            }
            node = current.children[direction(key, current.bit)];
        }
    }
    pub(super) fn set(
        &mut self,
        domain: usize,
        key: [u8; 32],
        value: Option<Position>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        step(io, 1)?;
        let Some(mut node) = self.roots[domain] else {
            self.roots[domain] = Some(self.leaf(key, value));
            return Ok(());
        };
        while self.nodes[node].bit < 256 {
            step(io, 1)?;
            node = self.nodes[node].children[direction(&key, self.nodes[node].bit)];
        }
        step(io, 33)?;
        let old = self.nodes[node].key;
        let difference = old.iter().zip(key).enumerate().find_map(|(i, (a, b))| {
            let xor = a ^ b;
            (xor != 0).then(|| (i * 8 + xor.leading_zeros() as usize) as u16)
        });
        let Some(bit) = difference else {
            self.nodes[node].value = value;
            return Ok(());
        };
        let leaf = self.leaf(key, value);
        let mut current = self.roots[domain].unwrap();
        let mut parent = None;
        while self.nodes[current].bit < bit {
            step(io, 1)?;
            let side = direction(&key, self.nodes[current].bit);
            parent = Some((current, side));
            current = self.nodes[current].children[side];
        }
        let mut children = [current; 2];
        children[direction(&key, bit)] = leaf;
        let branch = self.nodes.len();
        self.nodes.push(Node {
            key: [0; 32],
            value: None,
            bit,
            children,
        });
        match parent {
            Some((parent, side)) => self.nodes[parent].children[side] = branch,
            None => self.roots[domain] = Some(branch),
        }
        Ok(())
    }
    fn leaf(&mut self, key: [u8; 32], value: Option<Position>) -> usize {
        let node = self.nodes.len();
        assert!(
            node < self.nodes.capacity(),
            "preflighted instruction index capacity"
        );
        self.nodes.push(Node {
            key,
            value,
            bit: 256,
            children: [0; 2],
        });
        node
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
    // These keys are request-local. The unobserved path can hash the borrowed
    // wire directly without materializing a JSON Value merely to sort fields.
    if io.is_none() {
        return crate::executor::json::serialized_json_digest(&(volume, path));
    }
    with_history_io(io, |work| {
        #[cfg(feature = "filesystem")]
        let maximum = io
            .map(|io| io.remaining().map(|budget| budget.bytes_encoded))
            .transpose()?
            .unwrap_or(u64::MAX);
        #[cfg(not(feature = "filesystem"))]
        let maximum = u64::MAX;
        crate::executor::json::canonical_json_digest(&(volume, path), maximum, work)
    })
}

pub(super) fn step(io: Option<&PreparationIo>, items: u64) -> Result<()> {
    charge_items(io, items)?;
    #[cfg(all(test, feature = "test-support"))]
    TRACE_STEPS.with(|active| {
        if active.get() {
            acyclic_test_support::differential::record(("instruction-step", items));
        }
    });
    Ok(())
}

#[cfg(all(test, feature = "test-support"))]
thread_local! { static TRACE_STEPS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(all(test, feature = "test-support"))]
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
