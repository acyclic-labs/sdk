use super::*;

// A compressed byte trie has one leaf and one branch allocation per insertion.
// Unlike a standard collection's private nodes, every allocation has an exact
// layout that the original history owner can admit before construction.
#[derive(Default)]
pub(super) struct IntentIndex {
    root: Option<Box<Node>>,
}

struct Node {
    bit: usize,
    children: [Option<Box<Node>>; 2],
    key: String,
    sequence: u64,
    _custody: crate::NativeCustody,
    _key_custody: crate::NativeCustody,
}

struct Pending {
    leaf: Box<Node>,
    branch: Option<Box<Node>>,
    portable_bytes: u64,
}

// Nine-bit symbols reserve zero for end-of-key, distinguishing every prefix
// from its extensions without hashing or a collision-dependent fallback.
fn direction(key: &str, bit: usize) -> usize {
    let symbol = key
        .as_bytes()
        .get(bit / 9)
        .map_or(0, |byte| u16::from(*byte) + 1);
    usize::from((symbol >> (8 - bit % 9)) & 1)
}

impl IntentIndex {
    pub(super) fn get(&self, key: &str) -> Option<u64> {
        #[cfg(test)]
        if tests::intent_index::omit_index_lookup() {
            return None;
        }
        let mut node = self.root.as_deref()?;
        while node.bit != usize::MAX {
            node = node.children[direction(key, node.bit)].as_deref()?;
        }
        (node.key == key).then_some(node.sequence)
    }

    pub(super) fn insert_with<'w, T>(
        &mut self,
        key: &str,
        sequence: u64,
        quota: &mut CodecQuota<'_, 'w>,
        apply: impl FnOnce(&mut Option<&'w mut dyn HistoryIo>) -> Result<T>,
    ) -> Result<T> {
        let pending = self.prepare(key, sequence, quota)?;
        let result = apply(quota.work())?;
        self.install(pending, quota)?;
        Ok(result)
    }

    fn prepare(&self, key: &str, sequence: u64, quota: &mut CodecQuota<'_, '_>) -> Result<Pending> {
        let mut prior = self.root.as_deref();
        while let Some(node) = prior.filter(|node| node.bit != usize::MAX) {
            prior = node.children[direction(key, node.bit)].as_deref();
        }
        let bit = if let Some(prior) = prior {
            if prior.key == key {
                return Err(guarded_boundary(
                    quota.work(),
                    wire::ErrorCode::Conflict,
                    "coordinator history repeats an idempotency key",
                ));
            }
            let offset = prior
                .key
                .bytes()
                .zip(key.bytes())
                .take_while(|(a, b)| a == b)
                .count();
            let symbol = |text: &str| text.as_bytes().get(offset).map_or(0, |b| u16::from(*b) + 1);
            offset
                .checked_mul(9)
                .and_then(|bit| {
                    bit.checked_add((symbol(&prior.key) ^ symbol(key)).leading_zeros() as usize - 7)
                })
                .ok_or_else(|| {
                    guarded_boundary(
                        quota.work(),
                        wire::ErrorCode::Invalid,
                        "coordinator key index overflows",
                    )
                })?
        } else {
            usize::MAX
        };
        #[cfg(all(test, not(acyclic_harness_loom)))]
        let early_copy = if tests::intent_index::copy_before_admission() {
            Some(copy_key(key, quota)?)
        } else {
            None
        };
        let mut portable_bytes = 0;
        let leaf_custody = claim_node(quota, &mut portable_bytes)?;
        let branch_custody = if bit == usize::MAX {
            None
        } else {
            Some(claim_node(quota, &mut portable_bytes)?)
        };
        #[cfg(all(test, not(acyclic_harness_loom)))]
        let copied = match early_copy {
            Some(copied) => copied,
            None => copy_key(key, quota)?,
        };
        #[cfg(not(all(test, not(acyclic_harness_loom))))]
        let copied = copy_key(key, quota)?;
        let key_custody = detach(copied.capacity() as u64, quota, &mut portable_bytes)?;
        let leaf = Box::new(Node {
            bit: usize::MAX,
            children: [None, None],
            key: copied,
            sequence,
            _custody: leaf_custody,
            _key_custody: key_custody,
        });
        let branch = branch_custody.map(|custody| {
            Box::new(Node {
                bit,
                children: [None, None],
                key: String::new(),
                sequence: 0,
                _custody: custody,
                _key_custody: None,
            })
        });
        Ok(Pending {
            leaf,
            branch,
            portable_bytes,
        })
    }

    // All fallible admission and allocation precedes the projection update.
    fn install(&mut self, mut pending: Pending, quota: &mut CodecQuota<'_, '_>) -> Result<()> {
        quota.transfer_bytes(pending.portable_bytes)?;
        let Some(mut branch) = pending.branch.take() else {
            self.root = Some(pending.leaf);
            return Ok(());
        };
        let bit = branch.bit;
        let mut slot = &mut self.root;
        loop {
            if slot.as_ref().is_some_and(|node| node.bit < bit) {
                let node = slot.as_mut().expect("observed branch");
                slot = &mut node.children[direction(&pending.leaf.key, node.bit)];
            } else {
                let side = direction(&pending.leaf.key, bit);
                branch.children[1 - side] = slot.take();
                branch.children[side] = Some(pending.leaf);
                *slot = Some(branch);
                return Ok(());
            }
        }
    }
}

fn copy_key(key: &str, quota: &mut CodecQuota<'_, '_>) -> Result<String> {
    match quota.work().as_deref_mut() {
        Some(io) => {
            let copied = io.clone_text(key)?;
            quota.adopt(copied.capacity() as u64)?;
            Ok(copied)
        }
        None => Ok(key.to_owned()),
    }
}

fn claim_node(
    quota: &mut CodecQuota<'_, '_>,
    portable_bytes: &mut u64,
) -> Result<crate::NativeCustody> {
    let bytes = std::mem::size_of::<Node>() as u64;
    if let Some(io) = quota.work().as_deref_mut() {
        io.admit_owned(bytes)?;
        io.admit_allocation(bytes)?;
        io.retain_owned(bytes)?;
        quota.adopt(bytes)?;
    }
    detach(bytes, quota, portable_bytes)
}

fn detach(
    bytes: u64,
    quota: &mut CodecQuota<'_, '_>,
    portable_bytes: &mut u64,
) -> Result<crate::NativeCustody> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = quota
        .work()
        .as_deref_mut()
        .filter(|io| io.original_native_work().is_some())
    {
        let custody = io.take_native_custody(bytes)?;
        quota.transfer_bytes(bytes)?;
        return Ok(Some(custody));
    }
    if quota.work().is_some() {
        *portable_bytes += bytes;
    }
    Ok(None)
}

impl Drop for IntentIndex {
    fn drop(&mut self) {
        // Rotate left children into the root until each node is childless.
        // Untrusted key lengths never turn destruction into stack recursion.
        while let Some(mut node) = self.root.take() {
            if let Some(mut left) = node.children[0].take() {
                node.children[0] = left.children[1].take();
                left.children[1] = Some(node);
                self.root = Some(left);
            } else {
                self.root = node.children[1].take();
            }
        }
    }
}
