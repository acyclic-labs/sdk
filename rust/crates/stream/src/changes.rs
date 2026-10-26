//! Canonical provider-authored change lists with entry-level follow projection.
//!
//! A change record stores each indexed key exactly once, in its value. Keys
//! are sorted and unique, so each one is stored as the length it shares with
//! its predecessor followed by the rest of its bytes. In memory the record's
//! headers carry the full keys for indexing and filtering; no wire or
//! persisted form carries them a second time ([`wire_keys`]), and every
//! ingress derives them from the value ([`record_headers`]).
//!
//! The value is, with every integer a minimal LEB128 varint:
//! `context_len context count (shared suffix_len suffix op_len op meta_len meta)*`,
//! where `shared` is the longest common prefix with the previous key, so one
//! list has exactly one encoding.
use crate::{AppendRecord, Attribute, OwnedBytes, RecordHeaders, StreamError, wire};

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod golden;

/// Borrowed path-local facts used by a provider's funded change producer.
pub struct ChangeEntryRef<'a> {
    /// Canonical indexed key, with the same order as the resulting headers.
    pub key: &'a [u8],
    /// Nonempty semantic operation name.
    pub operation: &'a str,
    /// Metadata belonging only to this key; never file contents.
    pub metadata: &'a [u8],
}

const ATTRIBUTE: &str = "acyclic.record";
/// Marker emitted by the current change producer. Publication must be fenced
/// until every applied voter advertises this marker in `READER_FORMATS`.
/// A reader upgrade alone does not authorize a writer upgrade.
pub const WRITER_FORMAT: &[u8] = b"changes/v2";

/// Exact reserved markers this canonical reader validates and projects.
/// Persist these capabilities with the applied voter selection; intersect the
/// voters' supported markers before admitting `WRITER_FORMAT` publication.
pub const READER_FORMATS: &[&[u8]] = &[V1, WRITER_FORMAT];

const FORMAT: &[u8] = WRITER_FORMAT;
const V1: &[u8] = b"changes/v1";

fn marks(name: &str, value: &OwnedBytes) -> bool {
    name == ATTRIBUTE && READER_FORMATS.contains(&value.as_ref())
}

fn compact(headers: &RecordHeaders) -> bool {
    headers.attributes().iter().any(|attribute| {
        attribute.name == ATTRIBUTE && attribute.value.as_ref() == FORMAT
    })
}

pub(crate) fn unknown(headers: &RecordHeaders) -> bool {
    headers.attributes().iter().any(|attribute| {
        attribute.name == ATTRIBUTE && !marks(attribute.name, attribute.value)
    })
}

pub(crate) fn marked(headers: &RecordHeaders) -> bool {
    headers
        .attributes()
        .iter()
        .any(|attribute| marks(attribute.name, attribute.value))
}

/// Whether `headers` belong to a change record.
pub fn is_changes(headers: &RecordHeaders) -> bool {
    marked(headers)
}

/// V2 derives its index from the compact value. V1 retains its original wire
/// keys, so reading and retrying old records never changes their identity.
pub(crate) fn wire_keys(headers: &RecordHeaders) -> &[OwnedBytes] {
    if compact(headers) { &[] } else { headers.keys() }
}

fn varint_len(value: usize) -> usize {
    prost::encoding::encoded_len_varint(value as u64)
}

fn shared(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

/// Running size of a canonical list, entry by entry.
struct Size<'a> {
    value: usize,
    keys: usize,
    count: usize,
    previous: Option<&'a [u8]>,
}
impl<'a> Size<'a> {
    fn new(context: &[u8]) -> Self {
        Self {
            value: varint_len(context.len()) + context.len(),
            keys: ATTRIBUTE.len() + FORMAT.len(),
            count: 0,
            previous: None,
        }
    }
    /// Adds `entry`, or says why it cannot follow the entries so far.
    fn push(&mut self, entry: &ChangeEntryRef<'a>) -> Result<(), StreamError> {
        if entry.key.len() > crate::MAX_RECORD_KEY_BYTES
            || entry.operation.is_empty()
            || entry.operation.chars().any(char::is_control)
            || self.previous.is_some_and(|previous| previous >= entry.key)
        {
            return Err(StreamError::InvalidArgument);
        }
        if self.count == crate::MAX_RECORD_KEYS {
            return Err(StreamError::LimitExceeded);
        }
        let common = self
            .previous
            .map_or(0, |previous| shared(previous, entry.key));
        let suffix = entry.key.len() - common;
        let value = self
            .value
            .saturating_add(varint_len(common) + varint_len(suffix) + suffix)
            .saturating_add(varint_len(entry.operation.len()) + entry.operation.len())
            .saturating_add(varint_len(entry.metadata.len()) + entry.metadata.len());
        let keys = self.keys.saturating_add(entry.key.len());
        if self.total_with(value, self.count + 1).saturating_add(keys) > crate::MAX_RECORD_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        (self.value, self.keys, self.count, self.previous) =
            (value, keys, self.count + 1, Some(entry.key));
        Ok(())
    }
    fn total_with(&self, value: usize, count: usize) -> usize {
        value + varint_len(count)
    }
    fn total(&self) -> usize {
        self.total_with(self.value, self.count)
    }
}

/// Exact encoded value length of a canonical list, or why it is not one.
pub fn changes_encoded_len(
    context: &[u8],
    entries: &[ChangeEntryRef<'_>],
) -> Result<usize, StreamError> {
    if entries.is_empty() {
        return Err(StreamError::InvalidArgument);
    }
    let mut size = Size::new(context);
    for entry in entries {
        size.push(entry)?;
    }
    Ok(size.total())
}

/// How many leading entries of a sorted list fit one record: never zero, and
/// never two equal keys in one record. Producers split a longer list into
/// consecutive records of the same commit.
pub fn changes_chunk_len(
    context: &[u8],
    entries: &[ChangeEntryRef<'_>],
) -> Result<usize, StreamError> {
    let mut size = Size::new(context);
    for entry in entries {
        match size.push(entry) {
            Ok(()) => {}
            Err(_) if size.count > 0 && size.previous.is_some_and(|key| key == entry.key) => break,
            Err(StreamError::LimitExceeded) if size.count > 0 => break,
            Err(error) => return Err(error),
        }
    }
    if size.count == 0 {
        return Err(StreamError::LimitExceeded);
    }
    Ok(size.count)
}

/// Writes a list `changes_encoded_len` accepted, `length` bytes long.
fn encode_into(buffer: &mut Vec<u8>, context: &[u8], entries: &[ChangeEntryRef<'_>]) {
    let put = |buffer: &mut Vec<u8>, bytes: &[u8]| {
        prost::encoding::encode_varint(bytes.len() as u64, buffer);
        buffer.extend_from_slice(bytes);
    };
    put(buffer, context);
    prost::encoding::encode_varint(entries.len() as u64, buffer);
    let mut previous: &[u8] = &[];
    for entry in entries {
        let common = shared(previous, entry.key);
        prost::encoding::encode_varint(common as u64, buffer);
        put(buffer, entry.key.get(common..).unwrap_or_default());
        put(buffer, entry.operation.as_bytes());
        put(buffer, entry.metadata);
        previous = entry.key;
    }
}

struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    /// A minimally encoded varint; any other encoding would change identity.
    fn number(&mut self) -> Result<usize, StreamError> {
        let before = self.0.len();
        let value = prost::encoding::decode_varint(&mut self.0)
            .map_err(|_| StreamError::InvalidArgument)?;
        if prost::encoding::encoded_len_varint(value) != before - self.0.len() {
            return Err(StreamError::InvalidArgument);
        }
        usize::try_from(value).map_err(|_| StreamError::LimitExceeded)
    }
    fn bytes(&mut self) -> Result<&'a [u8], StreamError> {
        let length = self.number()?;
        let (value, rest) = self
            .0
            .split_at_checked(length)
            .ok_or(StreamError::InvalidArgument)?;
        self.0 = rest;
        Ok(value)
    }

    // The v1 producer used canonical protobuf: ordered length-delimited fields,
    // minimal lengths and omitted empty scalar fields. Keep the retained bytes.
    fn field(&mut self, tag: u8) -> Result<Option<&'a [u8]>, StreamError> {
        if self.0.first() != Some(&tag) {
            return Ok(None);
        }
        self.0 = self.0.get(1..).ok_or(StreamError::InvalidArgument)?;
        let value = self.bytes()?;
        if value.is_empty() {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Some(value))
    }
}

fn parse_v1<'a>(
    bytes: &'a [u8],
    mut visit: impl FnMut(&[u8], &'a str, &'a [u8]) -> Result<(), StreamError>,
) -> Result<&'a [u8], StreamError> {
    let mut reader = Reader(bytes);
    let context = reader.field(10)?.unwrap_or_default();
    let mut previous: Option<&[u8]> = None;
    let mut count = 0;
    while let Some(bytes) = reader.field(18)? {
        let mut entry = Reader(bytes);
        let key = entry.field(10)?.unwrap_or_default();
        let operation = entry.field(18)?.ok_or(StreamError::InvalidArgument)?;
        let operation = std::str::from_utf8(operation).map_err(|_| StreamError::InvalidArgument)?;
        let metadata = entry.field(26)?.unwrap_or_default();
        count += 1;
        if count > crate::MAX_RECORD_KEYS || key.len() > crate::MAX_RECORD_KEY_BYTES
            || previous.is_some_and(|previous| previous >= key)
            || operation.chars().any(char::is_control) || !entry.0.is_empty()
        {
            return Err(StreamError::InvalidArgument);
        }
        visit(key, operation, metadata)?;
        previous = Some(key);
    }
    if count == 0 || !reader.0.is_empty() {
        return Err(StreamError::InvalidArgument);
    }
    Ok(context)
}

fn parse_record<'a>(
    bytes: &'a [u8],
    headers: &RecordHeaders,
    visit: impl FnMut(&[u8], &'a str, &'a [u8]) -> Result<(), StreamError>,
) -> Result<&'a [u8], StreamError> {
    if compact(headers) { parse(bytes, visit) } else { parse_v1(bytes, visit) }
}

/// Visits each entry of a canonical value with its full key, rejecting every
/// noncanonical encoding.
fn parse<'a>(
    bytes: &'a [u8],
    mut visit: impl FnMut(&[u8], &'a str, &'a [u8]) -> Result<(), StreamError>,
) -> Result<&'a [u8], StreamError> {
    let mut reader = Reader(bytes);
    let context = reader.bytes()?;
    let count = reader.number()?;
    if count == 0 || count > crate::MAX_RECORD_KEYS {
        return Err(StreamError::InvalidArgument);
    }
    let mut key = [0_u8; crate::MAX_RECORD_KEY_BYTES];
    let mut length = 0;
    for index in 0..count {
        let common = reader.number()?;
        let suffix = reader.bytes()?;
        let operation = reader.bytes()?;
        let metadata = reader.bytes()?;
        let total = common
            .checked_add(suffix.len())
            .filter(|total| *total <= crate::MAX_RECORD_KEY_BYTES)
            .ok_or(StreamError::InvalidArgument)?;
        // The shared length is exactly the common prefix, so the suffix's
        // first byte must differ from the predecessor's next one; together
        // with that byte's order this makes keys strictly increasing.
        let canonical = if index == 0 {
            common == 0
        } else {
            common <= length
                && match (
                    suffix.first(),
                    key.get(common..length).and_then(<[u8]>::first),
                ) {
                    (Some(next), Some(previous)) => next > previous,
                    (Some(_), None) => true,
                    (None, _) => false,
                }
        };
        let operation = std::str::from_utf8(operation).map_err(|_| StreamError::InvalidArgument)?;
        if !canonical || operation.is_empty() || operation.chars().any(char::is_control) {
            return Err(StreamError::InvalidArgument);
        }
        key.get_mut(common..total)
            .ok_or(StreamError::InvalidArgument)?
            .copy_from_slice(suffix);
        length = total;
        visit(
            key.get(..length).ok_or(StreamError::InvalidArgument)?,
            operation,
            metadata,
        )?;
    }
    if !reader.0.is_empty() {
        return Err(StreamError::InvalidArgument);
    }
    Ok(context)
}

/// Decodes a canonical change value into its entries with full keys.
pub fn decode_changes(bytes: &[u8]) -> Result<wire::ChangeRecord, StreamError> {
    let mut entries = Vec::new();
    let context = parse(bytes, |key, operation, metadata| {
        entries.push(wire::ChangeEntry {
            key: bytes::Bytes::copy_from_slice(key),
            operation: operation.into(),
            metadata: bytes::Bytes::copy_from_slice(metadata),
        });
        Ok(())
    })?;
    Ok(wire::ChangeRecord {
        context: bytes::Bytes::copy_from_slice(context),
        entries,
    })
}

pub(crate) fn decode(
    bytes: &[u8],
    headers: &RecordHeaders,
) -> Result<wire::ChangeRecord, StreamError> {
    validate(bytes, headers)?;
    let mut entries = Vec::new();
    let context = parse_record(bytes, headers, |key, operation, metadata| {
        entries.push(wire::ChangeEntry {
            key: bytes::Bytes::copy_from_slice(key),
            operation: operation.into(),
            metadata: bytes::Bytes::copy_from_slice(metadata),
        });
        Ok(())
    })?;
    Ok(wire::ChangeRecord { context: bytes::Bytes::copy_from_slice(context), entries })
}

/// Rejects a change value whose keys differ from its in-memory headers, and
/// any record claiming a change format other than this one.
pub(crate) fn validate(bytes: &[u8], headers: &RecordHeaders) -> Result<(), StreamError> {
    if !marked(headers) {
        if headers
            .attributes()
            .iter()
            .any(|attribute| attribute.name == ATTRIBUTE)
        {
            return Err(StreamError::InvalidArgument);
        }
        return Ok(());
    }
    if bytes.len().saturating_add(headers.payload_bytes()) > crate::MAX_RECORD_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    let mut expected = headers.keys().iter();
    parse_record(bytes, headers, |key, _, _| {
        if expected
            .next()
            .is_none_or(|expected| expected.as_ref() != key)
        {
            return Err(StreamError::InvalidArgument);
        }
        Ok(())
    })?;
    if expected.next().is_some() {
        return Err(StreamError::InvalidArgument);
    }
    Ok(())
}

/// Headers for a record arriving from any wire or persisted form. A change
/// record carries no keys there: they are derived from its value, so a
/// second, disagreeing copy cannot exist.
pub(crate) fn record_headers(
    value: &[u8],
    keys: Vec<OwnedBytes>,
    attributes: Vec<Attribute>,
) -> Result<RecordHeaders, StreamError> {
    if !attributes
        .iter()
        .any(|attribute| attribute.name == ATTRIBUTE && attribute.value.as_ref() == FORMAT)
    {
        let headers = RecordHeaders::new(keys, attributes)?;
        validate(value, &headers)?;
        return Ok(headers);
    }
    if !keys.is_empty() {
        return Err(StreamError::InvalidArgument);
    }
    let mut ranges = Vec::new();
    let mut owned = Vec::new();
    parse(value, |key, _, _| {
        let start = owned.len();
        owned.extend_from_slice(key);
        ranges.push(start..owned.len());
        Ok(())
    })?;
    let owned = OwnedBytes::from(owned);
    let headers = RecordHeaders::new(
        ranges.into_iter().map(|range| owned.slice(range)).collect(),
        attributes,
    )?;
    if value.len().saturating_add(headers.payload_bytes()) > crate::MAX_RECORD_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    Ok(headers)
}

impl AppendRecord {
    /// Encodes a typed change list. Entries must have sorted unique keys; the
    /// shared context contains publication identity, never path-specific facts.
    /// Large publications use consecutive chunks in the same atomic commit.
    pub fn changes(
        context: &[u8],
        entries: &[wire::ChangeEntry],
        mut attributes: Vec<Attribute>,
    ) -> Result<Self, StreamError> {
        if attributes
            .iter()
            .any(|attribute| attribute.name == ATTRIBUTE)
        {
            return Err(StreamError::InvalidArgument);
        }
        attributes.push(Attribute {
            name: ATTRIBUTE.into(),
            value: FORMAT.into(),
        });
        attributes.sort_by(|left, right| left.name.cmp(&right.name));
        let references = entries
            .iter()
            .map(|entry| ChangeEntryRef {
                key: &entry.key,
                operation: &entry.operation,
                metadata: &entry.metadata,
            })
            .collect::<Vec<_>>();
        let length = changes_encoded_len(context, &references)?;
        let mut value = Vec::with_capacity(length);
        encode_into(&mut value, context, &references);
        let headers = RecordHeaders::new(
            entries
                .iter()
                .map(|entry| entry.key.clone().into())
                .collect(),
            attributes,
        )?;
        let value: OwnedBytes = value.into();
        validate(&value, &headers)?;
        Ok(Self { value, headers })
    }

    /// The same list restricted to the entries `keep` accepts, or `None` when
    /// it keeps none. Projection re-encodes, so the result stays canonical.
    pub(crate) fn retain_changes(
        &self,
        mut keep: impl FnMut(&[u8]) -> bool,
    ) -> Result<Option<Self>, StreamError> {
        let mut changes = decode(&self.value, &self.headers)?;
        let before = changes.entries.len();
        changes.entries.retain(|entry| keep(&entry.key));
        if changes.entries.len() == before {
            return Ok(Some(self.clone()));
        }
        if changes.entries.is_empty() {
            return Ok(None);
        }
        if !compact(&self.headers) {
            use prost::Message;
            let headers = RecordHeaders::new(
                changes.entries.iter().map(|entry| entry.key.clone().into()).collect(),
                self.headers.attributes().iter().map(crate::RecordAttribute::to_owned).collect(),
            )?;
            let value = changes.encode_to_vec().into();
            validate(&value, &headers)?;
            return Ok(Some(Self { value, headers }));
        }
        Self::changes(
            &changes.context,
            &changes.entries,
            self.headers
                .attributes()
                .iter()
                .filter(|attribute| attribute.name != ATTRIBUTE)
                .map(crate::RecordAttribute::to_owned)
                .collect(),
        )
        .map(Some)
    }
}
