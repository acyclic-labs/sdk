use super::*;

enum Storage {
    Committed(crate::store::operations::ObservedRecord),
    Staged(OwnedBytes),
    Cached(OwnedBytes),
}

pub(in crate::filesystem::execution_journal) struct ReadNode<'a> {
    storage: Storage,
    // The committed SDK value owns native custody intrinsically.
    _hold: Option<crate::filesystem::bounded_io::DecodeReservation<'a>>,
    slot: Option<node::BundleSlot>,
    reference: NodeRef,
}

impl ReadNode<'_> {
    pub(in crate::filesystem::execution_journal) fn reference(&self) -> NodeRef {
        self.reference
    }
    pub(super) fn sequence(&self) -> u64 {
        self.reference.sequence
    }

    fn bytes(&self) -> &[u8] {
        match &self.storage {
            Storage::Committed(record) => &record.value,
            Storage::Staged(bytes) | Storage::Cached(bytes) => bytes,
        }
    }

    pub(in crate::filesystem::execution_journal) fn node(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<Node<'_>> {
        match &self.slot {
            Some(slot) => Node::decode_at(
                slot.bytes(self.bytes(), io)?,
                self.reference.sequence,
                slot.previous(),
                io,
            ),
            None => Node::decode(self.bytes(), self.reference.sequence, io),
        }
    }

    fn authenticate(&self, io: Option<&PreparationIo>) -> Result<()> {
        let bytes = self.bytes();
        let selected = match &self.slot {
            Some(slot) => slot.bytes(bytes, io)?,
            None => bytes,
        };
        if (self.slot.is_some() || !matches!(&self.storage, Storage::Cached(_)))
            && crate::model::hash_bytes(selected, io)? != self.reference.digest
        {
            return Err(invalid(io, "execution index node digest differs"));
        }
        self.node(io)?;
        Ok(())
    }
}

impl<'a, P: crate::store::JournalProvider + ?Sized> Reader<'a, P> {
    pub(in crate::filesystem::execution_journal) async fn read(
        &self,
        reference: NodeRef,
    ) -> Result<ReadNode<'a>> {
        self.read_with_cache(reference, None).await
    }

    async fn read_with_cache(
        &self,
        reference: NodeRef,
        cache: Option<&mut NodeCache>,
    ) -> Result<ReadNode<'a>> {
        let (storage, hold, retained) = if reference.sequence >= self.base {
            let index = usize::try_from(reference.sequence - self.base)
                .map_err(|_| invalid(self.io, "execution index node offset overflows"))?;
            let bytes = self.staged.get(index).ok_or_else(|| {
                invalid(self.io, "execution index node is outside its checkpoint")
            })?;
            (Storage::Staged(bytes.clone()), None, 0)
        } else if let Some(bytes) = self
            .cache
            .or(cache.as_deref())
            .map(|cache| cache.get(reference, self.io))
            .transpose()?
            .flatten()
        {
            (Storage::Cached(bytes), None, 0)
        } else {
            let mut history = self.io.map(PreparationIo::history_io);
            let mut work = history
                .as_mut()
                .map(|io| io as &mut dyn crate::store::HistoryIo);
            #[cfg(feature = "test-support")]
            tracing::trace!(target: "acyclic_harness::journal_work",
                journal_reads = 1_u64, "execution index record requested");
            let record = crate::store::operations::one_record(
                self.provider.as_ref(),
                self.path,
                reference.sequence,
                &mut work,
            )
            .await?
            .ok_or_else(|| invalid(self.io, "execution index node is missing"))?;
            #[cfg(feature = "test-support")]
            tracing::trace!(target: "acyclic_harness::journal_work",
                journal_records = 1_u64, journal_read_bytes = record.value.len() as u64,
                "execution index record returned");
            let held = crate::store::operations::sdk_record_retained_bytes(&record, &work);
            let hold = self.io.map(|io| io.guard_owned(held)).transpose()?;
            (Storage::Committed(record), hold, held)
        };
        let mut read = ReadNode {
            storage,
            _hold: hold,
            slot: None,
            reference,
        };
        if node::is_branch_bundle(read.bytes()) {
            read.slot = Some(node::BundleSlot::find(
                read.bytes(),
                reference.digest,
                self.io,
            )?);
            read.node(self.io)?;
        } else {
            read.authenticate(self.io)?;
        }
        if let Some(cache) = cache
            && let Storage::Committed(record) = &read.storage
        {
            cache.insert_retained(reference.sequence, &record.value, retained)?;
        }
        Ok(read)
    }

    pub(super) async fn follow(
        &self,
        parent: ReadNode<'a>,
        reference: NodeRef,
    ) -> Result<ReadNode<'a>> {
        self.follow_with_cache(parent, reference, None).await
    }

    async fn follow_with_cache(
        &self,
        mut parent: ReadNode<'a>,
        reference: NodeRef,
        cache: Option<&mut NodeCache>,
    ) -> Result<ReadNode<'a>> {
        if parent.sequence() != reference.sequence {
            drop(parent);
            return self.read_with_cache(reference, cache).await;
        }
        let slot = parent
            .slot
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    self.io,
                    "execution index scalar node has a same-record link",
                )
            })?
            .preceding(parent.bytes(), reference.digest, self.io)?;
        parent.slot = Some(slot);
        parent.reference = reference;
        parent.authenticate(self.io)?;
        Ok(parent)
    }

    pub(in crate::filesystem::execution_journal) async fn get(
        &self,
        root: Option<NodeRef>,
        key: &[u8; 32],
    ) -> Result<Option<ReadNode<'a>>> {
        self.get_with_cache(root, key, None).await
    }

    pub(in crate::filesystem::execution_journal) async fn get_with_cache(
        &self,
        root: Option<NodeRef>,
        key: &[u8; 32],
        mut cache: Option<&mut NodeCache>,
    ) -> Result<Option<ReadNode<'a>>> {
        let Some(reference) = root else {
            return Ok(None);
        };
        let mut minimum_bit = 0;
        crate::model::charge_items(self.io, 1)?;
        let mut read = self
            .read_with_cache(reference, cache.as_deref_mut())
            .await?;
        loop {
            match read.node(self.io)? {
                Node::Leaf { key: found, .. } => return Ok((found == *key).then_some(read)),
                Node::Branch { bit, children } => {
                    if bit < minimum_bit {
                        return Err(invalid(self.io, "execution index branches are not ordered"));
                    }
                    minimum_bit = bit + 1;
                    let reference = *children
                        .get(direction(key, bit, self.io)?)
                        .ok_or_else(|| invalid(self.io, "execution index child is missing"))?;
                    crate::model::charge_items(self.io, 1)?;
                    read = self
                        .follow_with_cache(read, reference, cache.as_deref_mut())
                        .await?;
                }
            }
        }
    }
}
