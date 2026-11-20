use super::*;

pub(super) struct Ancestor {
    reference: NodeRef,
    bit: u16,
    side: usize,
    children: [NodeRef; 2],
}

impl<'a, P: crate::store::JournalProvider + ?Sized> Writer<'a, P> {
    pub(in crate::filesystem::execution_journal) fn new(
        provider: &'a acyclic_stream::ProviderOwner<P>,
        path: &'a StreamPath,
        base: u64,
        io: Option<&PreparationIo>,
        scratch: Option<Scratch>,
    ) -> Result<Self> {
        let mut scratch = match scratch {
            Some(scratch) => scratch,
            None => Scratch::new(io)?,
        };
        scratch.bind(io)?;
        Ok(Self {
            provider,
            path,
            base,
            scratch,
            content: Funded::new(io)?,
            cache: NodeCache::default(),
        })
    }

    pub(in crate::filesystem::execution_journal) fn take_scratch(&mut self) -> Scratch {
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        scratch
    }

    pub(in crate::filesystem::execution_journal) fn reader(&self) -> Reader<'_, P> {
        Reader {
            provider: self.provider,
            path: self.path,
            base: self.base,
            staged: &self.scratch.records.values,
            io: self.content.io.as_ref(),
            cache: Some(&self.cache),
        }
    }

    pub(in crate::filesystem::execution_journal) fn set_cache(
        &mut self,
        mut cache: NodeCache,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        cache.bind(io)?;
        self.cache = cache;
        Ok(())
    }

    pub(in crate::filesystem::execution_journal) fn prepare_cache(&mut self) -> Result<NodeCache> {
        self.cache.bind(self.content.io.as_ref())?;
        // Retire committed pages before admitting their replacements. Staged
        // intermediate roots are filtered below and never enter the cache.
        for sequence in &self.scratch.retired.values {
            self.cache.remove(*sequence)?;
        }
        for (index, bytes) in self.scratch.records.values.iter().enumerate() {
            let sequence = self.base + index as u64;
            if !self
                .scratch
                .live
                .values
                .get(index)
                .copied()
                .ok_or_else(|| {
                    invalid(
                        self.content.io.as_ref(),
                        "execution index page state is missing",
                    )
                })?
            {
                continue;
            }
            self.cache.insert(sequence, bytes)?;
        }
        Ok(std::mem::take(&mut self.cache))
    }

    pub(in crate::filesystem::execution_journal) fn records(&self) -> &[OwnedBytes] {
        &self.scratch.records.values
    }

    pub(in crate::filesystem::execution_journal) fn tail(&self) -> Result<u64> {
        self.base
            .checked_add(self.scratch.records.values.len() as u64)
            .ok_or_else(|| invalid(self.content.io.as_ref(), "execution index tail overflows"))
    }

    pub(super) fn stage(&mut self, node: Node<'_>) -> Result<NodeRef> {
        self.scratch.records.reserve_slot()?;
        self.scratch.live.reserve_slot()?;
        let sequence = self.tail()?;
        let io = self.content.io.as_ref();
        let (bytes, held) = node.encode(io)?;
        // Finite buffers already belong to the writer's pool; native buffers
        // carry their own custody. Retire either before releasing its charge.
        match crate::model::hash_bytes(&bytes, io) {
            Ok(digest) => {
                self.scratch.records.values.push(bytes);
                self.scratch.live.values.push(true);
                Ok(NodeRef { sequence, digest })
            }
            Err(error) => {
                drop(bytes);
                if let Some(io) = io {
                    io.release(held)?;
                }
                Err(error)
            }
        }
    }

    pub(in crate::filesystem::execution_journal) async fn set(
        &mut self,
        root: Option<NodeRef>,
        key: [u8; 32],
        value: &[u8],
        segmented: bool,
    ) -> Result<NodeRef> {
        let Some(mut current) = root else {
            return self.stage(Node::Leaf {
                key,
                value,
                segmented,
            });
        };
        let mut path = std::mem::take(&mut self.scratch.path);
        path.values.clear();
        let old_key;
        let mut minimum_bit = 0;
        let reader = self.reader();
        let mut read = reader.read(current).await?;
        loop {
            match read.node(reader.io)? {
                Node::Leaf { key, .. } => {
                    old_key = key;
                    break;
                }
                Node::Branch { bit, children } => {
                    if bit < minimum_bit || path.values.len() >= 256 {
                        return Err(invalid(
                            reader.io,
                            "execution index branches are not ordered",
                        ));
                    }
                    minimum_bit = bit + 1;
                    let side = direction(&key, bit, reader.io)?;
                    path.push(Ancestor {
                        reference: current,
                        bit,
                        side,
                        children,
                    })?;
                    current = *children
                        .get(side)
                        .ok_or_else(|| invalid(reader.io, "execution index child is missing"))?;
                    read = reader.follow(read, current).await?;
                }
            }
        }
        drop(read);
        let difference = old_key
            .iter()
            .zip(key)
            .enumerate()
            .find_map(|(index, (old, new))| {
                let xor = old ^ new;
                (xor != 0).then(|| index * 8 + xor.leading_zeros() as usize)
            });
        let prefix = difference.map_or(path.values.len(), |difference| {
            path.values
                .iter()
                .position(|ancestor| usize::from(ancestor.bit) >= difference)
                .unwrap_or(path.values.len())
        });
        let leaf = Node::Leaf {
            key,
            value,
            segmented,
        };
        let split = if let Some(difference) = difference {
            let bit = u16::try_from(difference).map_err(|_| {
                invalid(
                    self.content.io.as_ref(),
                    "execution index key bit overflows",
                )
            })?;
            let old = path
                .values
                .get(prefix)
                .map_or(current, |ancestor| ancestor.reference);
            Some((
                bit,
                direction(&key, bit, self.content.io.as_ref())?,
                [old; 2],
            ))
        } else {
            None
        };
        let branch_count = prefix + usize::from(split.is_some());
        let replacement = if branch_count == 0 {
            self.stage(leaf)?
        } else {
            let leaf = if node::fits_bundle(branch_count, value) {
                node::BundleLeaf::Inline {
                    key,
                    value,
                    segmented,
                }
            } else {
                node::BundleLeaf::Separate(self.stage(leaf)?)
            };
            self.scratch.records.reserve_slot()?;
            self.scratch.live.reserve_slot()?;
            let branches = split.into_iter().chain(
                path.values
                    .iter()
                    .take(prefix)
                    .rev()
                    .map(|ancestor| (ancestor.bit, ancestor.side, ancestor.children)),
            );
            let (bytes, root) = node::encode_branch_bundle(
                self.tail()?,
                &leaf,
                branches,
                self.content.io.as_ref(),
            )?;
            self.scratch.records.values.push(bytes);
            self.scratch.live.values.push(true);
            root
        };
        for ancestor in path.values.iter().take(prefix) {
            // A bundle stays reachable until its deepest branch is replaced.
            // Copying a prefix can leave its same-record child in the tree.
            if ancestor
                .children
                .iter()
                .all(|child| child.sequence != ancestor.reference.sequence)
            {
                self.retire(ancestor.reference.sequence)?;
            }
        }
        if difference.is_none() {
            self.retire(current.sequence)?;
        }
        self.scratch.path = path;
        Ok(replacement)
    }

    fn retire(&mut self, sequence: u64) -> Result<()> {
        if sequence < self.base {
            return self.scratch.retired.push(sequence);
        }
        let position = usize::try_from(sequence - self.base).map_err(|_| {
            invalid(
                self.content.io.as_ref(),
                "retired execution page offset overflows",
            )
        })?;
        let live = self.scratch.live.values.get_mut(position).ok_or_else(|| {
            invalid(
                self.content.io.as_ref(),
                "retired execution page is missing",
            )
        })?;
        *live = false;
        Ok(())
    }
}
