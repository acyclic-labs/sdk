//! Large summary cells span authenticated pages without changing record limits.
use super::*;
use read::ReadNode;

const LINK_BYTES: usize = 41;
const HEADER_BYTES: usize = 52;
const CHUNK_BYTES: usize = acyclic_stream::MAX_RECORD_BYTES - node::LEAF_HEADER_BYTES - LINK_BYTES;

struct Header {
    length: usize,
    last: NodeRef,
}

impl Header {
    fn parse(value: &[u8], sequence: u64, io: Option<&PreparationIo>) -> Result<Option<Self>> {
        if value.get(..4) != Some(b"EXV1") {
            return Ok(None);
        }
        let malformed = || invalid(io, "invalid segmented value header");
        let value: &[u8; HEADER_BYTES] = value.try_into().map_err(|_| malformed())?;
        let length = u64::from_le_bytes(value[4..12].try_into().map_err(|_| malformed())?);
        let length = usize::try_from(length).map_err(|_| malformed())?;
        let last = NodeRef {
            sequence: u64::from_le_bytes(value[12..20].try_into().map_err(|_| malformed())?),
            digest: value[20..].try_into().map_err(|_| malformed())?,
        };
        // A chain cannot contain more pages than precede its root. Reject an
        // impossible advertised allocation before reserving its output buffer.
        if length <= acyclic_stream::MAX_RECORD_BYTES - node::LEAF_HEADER_BYTES
            || last.sequence >= sequence
            || length.div_ceil(CHUNK_BYTES) as u64 > last.sequence.saturating_add(1)
        {
            return Err(malformed());
        }
        Ok(Some(Self { length, last }))
    }
}

pub(in crate::filesystem::execution_journal) enum Value<'a> {
    Inline(ReadNode<'a>),
    Segmented {
        bytes: OwnedBytes,
        _hold: Option<crate::filesystem::bounded_io::DecodeReservation<'a>>,
    },
}

impl Value<'_> {
    pub(in crate::filesystem::execution_journal) fn bytes(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<&[u8]> {
        match self {
            Self::Inline(read) => leaf_value(read, io),
            Self::Segmented { bytes, .. } => Ok(bytes),
        }
    }
}

fn leaf_value<'a>(read: &'a ReadNode<'_>, io: Option<&PreparationIo>) -> Result<&'a [u8]> {
    match read.node(io)? {
        Node::Leaf { value, .. } => Ok(value),
        _ => Err(invalid(io, "execution index value is not a leaf")),
    }
}

fn link_bytes(reference: Option<NodeRef>) -> [u8; LINK_BYTES] {
    let mut bytes = [0; LINK_BYTES];
    if let Some(reference) = reference {
        bytes[0] = 1;
        bytes[1..9].copy_from_slice(&reference.sequence.to_le_bytes());
        bytes[9..41].copy_from_slice(&reference.digest);
    }
    bytes
}

fn read_link(bytes: &[u8], io: Option<&PreparationIo>) -> Result<Option<NodeRef>> {
    let malformed = || invalid(io, "execution index value link is malformed");
    let bytes: &[u8; LINK_BYTES] = bytes.try_into().map_err(|_| malformed())?;
    match bytes[0] {
        0 if bytes.iter().all(|byte| *byte == 0) => Ok(None),
        1 => Ok(Some(NodeRef {
            sequence: u64::from_le_bytes(bytes[1..9].try_into().map_err(|_| malformed())?),
            digest: bytes[9..41].try_into().map_err(|_| malformed())?,
        })),
        _ => Err(malformed()),
    }
}

impl<P: StreamProvider + ?Sized> Writer<'_, P> {
    pub(in crate::filesystem::execution_journal) async fn set_value(
        &mut self,
        root: Option<NodeRef>,
        key: [u8; 32],
        value: &[u8],
        io: Option<&PreparationIo>,
    ) -> Result<NodeRef> {
        if value.len() <= acyclic_stream::MAX_RECORD_BYTES - node::LEAF_HEADER_BYTES {
            return self.set(root, key, value).await;
        }
        let mut previous = None;
        for chunk in value.chunks(CHUNK_BYTES) {
            let (bytes, held) = crate::executor::with_history_io(io, |work| {
                let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                let mut bytes =
                    crate::core::reserve_copy_vector(LINK_BYTES + chunk.len(), &mut quota)?;
                if let Some(work) = quota.work().as_deref_mut() {
                    work.copy_bytes((LINK_BYTES + chunk.len()) as u64)?;
                }
                bytes.extend_from_slice(&link_bytes(previous));
                bytes.extend_from_slice(chunk);
                quota.transfer();
                crate::store::operations::owned_bytes_from_retained_vec(bytes, quota.work())
            })?;
            let hold = io.map(|io| io.guard_owned(held)).transpose()?;
            previous = Some(self.stage(Node::Leaf { key, value: &bytes })?);
            // These immutable value segments are cold, rather than retained
            // forever when a later summary replaces their referring leaf.
            *self
                .scratch
                .live
                .values
                .last_mut()
                .ok_or_else(|| invalid(None, "missing value page"))? = false;
            drop(bytes);
            drop(hold);
        }
        let last =
            previous.ok_or_else(|| invalid(self.content.io.as_ref(), "missing value segments"))?;
        let mut header = [0; HEADER_BYTES];
        header[..4].copy_from_slice(b"EXV1");
        header[4..12].copy_from_slice(&(value.len() as u64).to_le_bytes());
        header[12..20].copy_from_slice(&last.sequence.to_le_bytes());
        header[20..].copy_from_slice(&last.digest);
        self.set(root, key, &header).await
    }
}

impl<'a, P: StreamProvider + ?Sized> Reader<'a, P> {
    pub(in crate::filesystem::execution_journal) async fn value(
        &self,
        read: ReadNode<'a>,
    ) -> Result<Value<'a>> {
        let value = leaf_value(&read, self.io)?;
        let Some(Header { length, last }) = Header::parse(value, read.sequence(), self.io)? else {
            return Ok(Value::Inline(read));
        };
        let mut reference = last;
        let Node::Leaf { key, .. } = read.node(self.io)? else {
            return Err(invalid(self.io, "missing value key"));
        };
        let mut history = self.io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        let mut bytes = crate::core::reserve_copy_vector(length, &mut quota)?;
        if let Some(work) = quota.work().as_deref_mut() {
            work.copy_bytes(crate::core::owned_sum([length as u64, length as u64])?)?;
        }
        bytes.resize(length, 0);
        let mut remaining = length;
        loop {
            let page = self.read(reference).await?;
            let Node::Leaf { key: found, value } = page.node(self.io)? else {
                return Err(invalid(self.io, "invalid value segment"));
            };
            if found != key || value.len() <= LINK_BYTES {
                return Err(invalid(self.io, "invalid value segment key or length"));
            }
            let (link, chunk) = value.split_at(LINK_BYTES);
            let start = remaining
                .checked_sub(chunk.len())
                .ok_or_else(|| invalid(self.io, "value segments exceed length"))?;
            if remaining != length && chunk.len() != CHUNK_BYTES {
                return Err(invalid(self.io, "short interior value segment"));
            }
            bytes
                .get_mut(start..remaining)
                .ok_or_else(|| invalid(self.io, "invalid value segment range"))?
                .copy_from_slice(chunk);
            remaining = start;
            match read_link(link, self.io)? {
                Some(previous) if previous.sequence < reference.sequence && remaining != 0 => {
                    reference = previous;
                }
                None if remaining == 0 => break,
                _ => return Err(invalid(self.io, "invalid value segment chain")),
            }
        }
        quota.transfer();
        let (bytes, held) =
            crate::store::operations::owned_bytes_from_retained_vec(bytes, quota.work())?;
        let hold = self.io.map(|io| io.guard_owned(held)).transpose()?;
        Ok(Value::Segmented { bytes, _hold: hold })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segmented_header_rejects_impossible_allocations_before_reading_pages() -> Result<()> {
        let mut header = [0; HEADER_BYTES];
        header[..4].copy_from_slice(b"EXV1");
        header[4..12].copy_from_slice(&((CHUNK_BYTES * 2) as u64).to_le_bytes());
        header[12..20].copy_from_slice(&1_u64.to_le_bytes());
        assert!(Header::parse(&header, 2, None)?.is_some());
        header[4..12].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(Header::parse(&header, 2, None).is_err());
        header[4..12].copy_from_slice(&((CHUNK_BYTES * 2) as u64).to_le_bytes());
        assert!(Header::parse(&header, 1, None).is_err());
        assert!(Header::parse(&header[..HEADER_BYTES - 1], 2, None).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn oversized_cells_reopen_through_bounded_authenticated_segments() -> Result<()> {
        for native in [false, true] {
            check_cell(native).await?;
        }
        Ok(())
    }

    async fn check_cell(native: bool) -> Result<()> {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let path = StreamPath::new("segmented-index/pages")?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(256 * 1024 * 1024, 16 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let native_baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let input: Vec<_> = (0_u8..251)
            .cycle()
            .take(acyclic_stream::MAX_RECORD_BYTES + 123)
            .collect();
        let key = [9; 32];
        // The original one-leaf path refuses this valid aggregate cell.
        let mut old = Writer::new(&provider, &path, 0, Some(&io), None)?;
        assert!(
            old.set(None, key, &input).await.is_err(),
            "one-leaf negative control accepted an oversized record"
        );
        drop(old);
        let mut writer = Writer::new(&provider, &path, 0, Some(&io), None)?;
        let root = writer.set_value(None, key, &input, Some(&io)).await?;
        assert!(writer.records().len() > 1);
        assert!(
            writer
                .records()
                .iter()
                .all(|bytes| bytes.len() <= acyclic_stream::MAX_RECORD_BYTES)
        );
        let cache = writer.prepare_cache()?;
        assert!(cache.get(root, Some(&io))?.is_some());
        for sequence in 0..root.sequence {
            let index = usize::try_from(sequence)
                .map_err(|_| invalid(Some(&io), "fixture index overflows"))?;
            let digest = crate::model::hash_bytes(&writer.records()[index], Some(&io))?;
            assert!(
                cache
                    .get(NodeRef { sequence, digest }, Some(&io))?
                    .is_none(),
                "large cold segments entered the hot page cache"
            );
        }
        let tail = writer.tail()?;
        let outcome = provider
            .append(acyclic_stream::AppendRequest {
                path: path.clone(),
                records: writer.records().iter().cloned().map(Into::into).collect(),
                if_tail: Some(0),
                idempotency_key: acyclic_stream::IdempotencyKey::unique(),
            })
            .await?;
        assert!(matches!(outcome, AppendOutcome::Committed(_)));
        drop(cache);
        drop(writer);
        let reader = Reader {
            provider: &provider,
            path: &path,
            base: tail,
            staged: &[],
            io: Some(&io),
            cache: None,
        };
        let read = reader.get(Some(root), &key).await?.expect("segmented root");
        let value = reader.value(read).await?;
        assert_eq!(value.bytes(Some(&io))?, input);
        drop(value);
        drop(provider);
        assert_eq!(
            io.owned_bytes()?,
            0,
            "segmented cell retained decode custody"
        );
        if native {
            assert_eq!(
                work.live_allocation_bytes(),
                native_baseline,
                "segmented cell retained native allocations"
            );
        }
        Ok(())
    }
}
