// Persistent (key, sequence) AVL index. Each edge carries an inherited prefix
// cut, so a fork pins the index in O(1), including cuts inside a record batch.
// Insertion copies only the search path and funds every new control node.
#[derive(Clone)]
struct KeyTree {
    root: Option<Arc<KeyNode>>,
    before: u64,
}

impl Default for KeyTree {
    fn default() -> Self {
        Self {
            root: None,
            before: u64::MAX,
        }
    }
}

fn key_node_bytes() -> usize {
    #[cfg(not(target_arch = "wasm32"))]
    {
        acyclic_native_runtime::arc_allocation_bytes::<KeyNode>()
    }
    #[cfg(target_arch = "wasm32")]
    {
        std::mem::size_of::<KeyNode>() + 2 * std::mem::size_of::<usize>()
    }
}

struct KeyNode {
    parts: KeyParts,
    height: u32,
    _allocation: Allocation,
}

#[derive(Clone)]
struct KeyParts {
    key: Bytes,
    sequence: u64,
    present: bool,
    left: KeyTree,
    right: KeyTree,
}

impl KeyTree {
    fn cap(&self, before: u64) -> Self {
        Self {
            root: self.root.clone(),
            before: before.min(self.before),
        }
    }

    fn height(&self) -> u32 {
        self.root.as_ref().map_or(0, |node| node.height)
    }

    fn parts(&self) -> Option<KeyParts> {
        let mut parts = self.root.as_ref()?.parts.clone();
        parts.left = parts.left.cap(self.before);
        parts.right = parts.right.cap(self.before);
        if parts.sequence >= self.before {
            parts.present = false;
        }
        Some(parts)
    }

    fn node(
        parts: KeyParts,
        allocate: &mut impl FnMut() -> Result<Allocation, StreamError>,
    ) -> Result<Self, StreamError> {
        let height = 1 + parts.left.height().max(parts.right.height());
        let allocation = allocate()?;
        Ok(Self {
            root: Some(Arc::new(KeyNode {
                parts,
                height,
                _allocation: allocation,
            })),
            before: u64::MAX,
        })
    }

    fn balance(
        mut parts: KeyParts,
        allocate: &mut impl FnMut() -> Result<Allocation, StreamError>,
    ) -> Result<Self, StreamError> {
        if parts.left.height() > parts.right.height() + 1 {
            let mut left = parts.left.parts().ok_or(StreamError::Unavailable)?;
            if left.left.height() < left.right.height() {
                let mut pivot = left.right.parts().ok_or(StreamError::Unavailable)?;
                left.right = pivot.left;
                parts.left = pivot.right;
                pivot.left = Self::node(left, allocate)?;
                pivot.right = Self::node(parts, allocate)?;
                Self::node(pivot, allocate)
            } else {
                parts.left = left.right;
                left.right = Self::node(parts, allocate)?;
                Self::node(left, allocate)
            }
        } else if parts.right.height() > parts.left.height() + 1 {
            let mut right = parts.right.parts().ok_or(StreamError::Unavailable)?;
            if right.right.height() < right.left.height() {
                let mut pivot = right.left.parts().ok_or(StreamError::Unavailable)?;
                right.left = pivot.right;
                parts.right = pivot.left;
                pivot.right = Self::node(right, allocate)?;
                pivot.left = Self::node(parts, allocate)?;
                Self::node(pivot, allocate)
            } else {
                parts.right = right.left;
                right.left = Self::node(parts, allocate)?;
                Self::node(right, allocate)
            }
        } else {
            Self::node(parts, allocate)
        }
    }

    fn insert(
        &self,
        entry: KeyParts,
        allocate: &mut impl FnMut() -> Result<Allocation, StreamError>,
    ) -> Result<Self, StreamError> {
        let Some(mut parts) = self.parts() else {
            return Self::node(entry, allocate);
        };
        match (&entry.key, entry.sequence).cmp(&(&parts.key, parts.sequence)) {
            std::cmp::Ordering::Less => parts.left = parts.left.insert(entry, allocate)?,
            std::cmp::Ordering::Greater => parts.right = parts.right.insert(entry, allocate)?,
            std::cmp::Ordering::Equal => parts.present = entry.present,
        }
        Self::balance(parts, allocate)
    }

    fn append(
        &self,
        records: &Arc<RecordBatch>,
        provider: &MemoryStream,
    ) -> Result<Self, StreamError> {
        self.append_reserved(records, &mut || provider.allocate(key_node_bytes()))
    }

    fn append_reserved(
        &self,
        records: &Arc<RecordBatch>,
        allocate: &mut impl FnMut() -> Result<Allocation, StreamError>,
    ) -> Result<Self, StreamError> {
        let mut tree = self.clone();
        for record in &records.records {
            for key in record.headers.keys() {
                tree = tree.insert(
                    KeyParts {
                        key: key.clone(),
                        sequence: record.sequence,
                        present: true,
                        left: Self::default(),
                        right: Self::default(),
                    },
                    allocate,
                )?;
            }
        }
        Ok(tree)
    }

    fn seek<E>(
        &self,
        key: &[u8],
        from: u64,
        before: u64,
        examine: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Option<u64>, E> {
        let before = before.min(self.before);
        if from >= before {
            return Ok(None);
        }
        let Some(node) = &self.root else {
            return Ok(None);
        };
        examine()?;
        let parts = &node.parts;
        match parts.key.as_ref().cmp(key) {
            std::cmp::Ordering::Less => parts.right.seek(key, from, before, examine),
            std::cmp::Ordering::Greater => parts.left.seek(key, from, before, examine),
            std::cmp::Ordering::Equal if parts.sequence < from => {
                parts.right.seek(key, from, before, examine)
            }
            std::cmp::Ordering::Equal if parts.sequence >= before => {
                parts.left.seek(key, from, before, examine)
            }
            std::cmp::Ordering::Equal => {
                if let Some(sequence) = parts.left.seek(key, from, before, examine)? {
                    return Ok(Some(sequence));
                }
                if parts.present {
                    return Ok(Some(parts.sequence));
                }
                parts.right.seek(key, from, before, examine)
            }
        }
    }
}
