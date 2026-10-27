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
    sequence: u64,
    reference: NodeRef,
}

impl ReadNode<'_> {
    pub(in crate::filesystem::execution_journal) fn reference(&self) -> NodeRef {
        self.reference
    }
    pub(super) fn sequence(&self) -> u64 {
        self.sequence
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
        Node::decode(self.bytes(), self.sequence, io)
    }
}

impl<'a, P: StreamProvider> Reader<'a, P> {
    pub(in crate::filesystem::execution_journal) async fn read(
        &self,
        reference: NodeRef,
    ) -> Result<ReadNode<'a>> {
        let (storage, hold) = if reference.sequence >= self.base {
            let index = usize::try_from(reference.sequence - self.base)
                .map_err(|_| invalid(self.io, "execution index node offset overflows"))?;
            let bytes = self.staged.get(index).ok_or_else(|| {
                invalid(self.io, "execution index node is outside its checkpoint")
            })?;
            (Storage::Staged(bytes.clone()), None)
        } else if let Some(bytes) = self
            .cache
            .map(|cache| cache.get(reference, self.io))
            .transpose()?
            .flatten()
        {
            (Storage::Cached(bytes), None)
        } else {
            let mut history = self.io.map(PreparationIo::history_io);
            let mut work = history
                .as_mut()
                .map(|io| io as &mut dyn crate::store::HistoryIo);
            #[cfg(feature = "test-support")]
            tracing::trace!(target: "acyclic_harness::journal_work",
                journal_reads = 1_u64, "execution index record requested");
            let record = crate::store::operations::one_record(
                self.provider,
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
            (Storage::Committed(record), hold)
        };
        let read = ReadNode {
            storage,
            _hold: hold,
            sequence: reference.sequence,
            reference,
        };
        if !matches!(&read.storage, Storage::Cached(_))
            && crate::model::hash_bytes(read.bytes(), self.io)? != reference.digest
        {
            return Err(invalid(self.io, "execution index node digest differs"));
        }
        read.node(self.io)?;
        Ok(read)
    }

    pub(in crate::filesystem::execution_journal) async fn get(
        &self,
        root: Option<NodeRef>,
        key: &[u8; 32],
    ) -> Result<Option<ReadNode<'a>>> {
        let Some(mut reference) = root else {
            return Ok(None);
        };
        let mut minimum_bit = 0;
        loop {
            crate::model::charge_items(self.io, 1)?;
            let read = self.read(reference).await?;
            match read.node(self.io)? {
                Node::Leaf { key: found, .. } => return Ok((found == *key).then_some(read)),
                Node::Branch { bit, children } => {
                    if bit < minimum_bit {
                        return Err(invalid(self.io, "execution index branches are not ordered"));
                    }
                    minimum_bit = bit + 1;
                    reference = *children
                        .get(direction(key, bit, self.io)?)
                        .ok_or_else(|| invalid(self.io, "execution index child is missing"))?;
                }
            }
        }
    }
}
