//! Canonical provider-authored change lists with entry-level follow projection.
//!
//! A v2 change record stores each indexed key exactly once, in its value. Keys
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
//!
//! Retained v1 records use the original canonical protobuf ChangeRecord and
//! indexed headers. Reading never rewrites either format or its retry identity.
use crate::{AppendRecord, Attribute, OwnedBytes, RecordHeaders, StreamCategory, StreamError};
use acyclic_stream_wire::{
    changes::{
        ChangeDecodeError, READER_FORMATS, RECORD_FORMAT_ATTRIBUTE as ATTRIBUTE,
        WRITER_FORMAT as FORMAT, visit_v1, visit_v2,
    },
    wire,
};

#[cfg(not(target_arch = "wasm32"))]
mod native;

#[cfg(any(test, feature = "test-support"))]
std::thread_local! {
    static PARSED_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static MARKER_PROBES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(any(test, feature = "test-support"))]
pub fn take_parsed_bytes() -> usize {
    PARSED_BYTES.with(|count| count.replace(0))
}

#[cfg(any(test, feature = "test-support"))]
pub fn take_marker_probes() -> usize {
    MARKER_PROBES.with(|count| count.replace(0))
}

/// Borrowed path-local facts used by a provider's funded change producer.
pub struct ChangeEntryRef<'a> {
    /// Canonical indexed key, with the same order as the resulting headers.
    pub key: &'a [u8],
    /// Nonempty semantic operation name.
    pub operation: &'a str,
    /// Metadata belonging only to this key; never file contents.
    pub metadata: &'a [u8],
}

fn marker(headers: &RecordHeaders) -> Option<&OwnedBytes> {
    headers
        .attributes()
        .iter()
        .find(|attribute| {
            #[cfg(any(test, feature = "test-support"))]
            MARKER_PROBES.with(|count| count.set(count.get() + 1));
            attribute.name == ATTRIBUTE
        })
        .map(|attribute| attribute.value)
}

fn compact(headers: &RecordHeaders) -> bool {
    marker(headers).is_some_and(|value| value.as_ref() == FORMAT)
}

/// Classifies the reserved marker in one pass; an unknown claim is never opaque.
pub fn reader_format(headers: &RecordHeaders) -> Result<Option<&[u8]>, StreamError> {
    match marker(headers) {
        None => Ok(None),
        Some(value) if READER_FORMATS.contains(&value.as_ref()) => Ok(Some(value.as_ref())),
        Some(_) => Err(StreamError::InvalidArgument),
    }
}

/// Whether `headers` belong to a change record.
pub fn is_changes(headers: &RecordHeaders) -> bool {
    reader_format(headers).is_ok_and(|format| format.is_some())
}

/// V2 derives its index from the compact value. V1 retains its original wire
/// keys, so reading and retrying old records never changes their identity.
pub fn wire_keys(headers: &RecordHeaders) -> &[OwnedBytes] {
    if compact(headers) {
        &[]
    } else {
        headers.keys()
    }
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
        if entry.key.len() > acyclic_stream_wire::headers::MAX_RECORD_KEY_BYTES
            || entry.operation.is_empty()
            || entry.operation.chars().any(char::is_control)
            || self.previous.is_some_and(|previous| previous >= entry.key)
        {
            return Err(StreamError::InvalidArgument);
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
            Err(error) if error.category() == StreamCategory::LimitExceeded && size.count > 0 => {
                break;
            }
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

fn parse_record<'a>(
    bytes: &'a [u8],
    format: Option<&[u8]>,
    expected: Option<&[OwnedBytes]>,
    remaining_keys: Option<usize>,
    visit: impl FnMut(&[u8], &'a str, &'a [u8]) -> Result<(), ChangeDecodeError>,
) -> Result<&'a [u8], StreamError> {
    #[cfg(any(test, feature = "test-support"))]
    PARSED_BYTES.with(|count| count.set(count.get() + bytes.len()));
    if format == Some(FORMAT) {
        visit_v2(bytes, expected, remaining_keys, visit)
    } else {
        visit_v1(bytes, expected, visit)
    }
    .map_err(|error| match error {
        ChangeDecodeError::Invalid => StreamError::InvalidArgument,
        ChangeDecodeError::Limit => StreamError::LimitExceeded,
    })
}

/// Decodes a canonical change value into its entries with full keys.
pub fn decode_changes(bytes: &[u8]) -> Result<wire::ChangeRecord, StreamError> {
    decode_record(bytes, Some(FORMAT), None, 0, |_| true)
}

/// Borrow a typed publication's context after authenticating its complete
/// body/index association. Opaque records return None. This performs no heap
/// allocation and does not decode or copy the change entries.
pub fn changes_context<'a>(
    bytes: &'a [u8],
    headers: &'a RecordHeaders,
) -> Result<Option<&'a [u8]>, StreamError> {
    Ok(AdmittedRecord::new(bytes, headers)?.context)
}

#[cfg(any(test, feature = "test-support"))]
pub fn decode(bytes: &[u8], headers: &RecordHeaders) -> Result<wire::ChangeRecord, StreamError> {
    let admitted = AdmittedRecord::new(bytes, headers)?;
    // Only a fully validated typed index establishes the body's entry count.
    let capacity = admitted.format.map_or(0, |_| headers.keys().len());
    admitted.decode(capacity, |_| true)
}

fn decode_record(
    bytes: &[u8],
    format: Option<&[u8]>,
    keys: Option<&[OwnedBytes]>,
    capacity: usize,
    keep: impl Fn(&[u8]) -> bool,
) -> Result<wire::ChangeRecord, StreamError> {
    let mut entries = Vec::with_capacity(capacity);
    let remaining = crate::MAX_RECORD_BYTES
        .checked_sub(bytes.len())
        .ok_or(StreamError::LimitExceeded)?;
    let context = parse_record(
        bytes,
        format,
        keys,
        Some(remaining),
        |key, operation, metadata| {
            if keep(key) {
                entries.push(wire::ChangeEntry {
                    key: bytes::Bytes::copy_from_slice(key),
                    operation: operation.into(),
                    metadata: bytes::Bytes::copy_from_slice(metadata),
                });
            }
            Ok(())
        },
    )?;
    Ok(wire::ChangeRecord {
        context: bytes::Bytes::copy_from_slice(context),
        entries,
    })
}

/// Rejects a change value whose keys differ from its in-memory headers, and
/// any record claiming a change format outside the supported readers.
pub fn validate(bytes: &[u8], headers: &RecordHeaders) -> Result<(), StreamError> {
    AdmittedRecord::new(bytes, headers).map(|_| ())
}

/// Complete admission binds the exact immutable body, index and reader. The
/// original parser can then materialize selected entries without revalidation.
pub struct AdmittedRecord<'a> {
    bytes: &'a [u8],
    format: Option<&'a [u8]>,
    context: Option<&'a [u8]>,
    keys: &'a [OwnedBytes],
}

impl<'a> AdmittedRecord<'a> {
    pub fn new(bytes: &'a [u8], headers: &'a RecordHeaders) -> Result<Self, StreamError> {
        let format = reader_format(headers)?;
        let mut context = None;
        if format.is_some() {
            if bytes.len().saturating_add(headers.payload_bytes()) > crate::MAX_RECORD_BYTES {
                return Err(StreamError::LimitExceeded);
            }
            context = Some(parse_record(
                bytes,
                format,
                Some(headers.keys()),
                None,
                |_, _, _| Ok(()),
            )?);
        }
        Ok(Self {
            bytes,
            format,
            context,
            keys: if format.is_some() {
                headers.keys()
            } else {
                &[]
            },
        })
    }

    pub fn is_changes(&self) -> bool {
        self.format.is_some()
    }

    fn decode(
        &self,
        capacity: usize,
        keep: impl Fn(&[u8]) -> bool,
    ) -> Result<wire::ChangeRecord, StreamError> {
        decode_record(
            self.bytes,
            self.format,
            self.format.map(|_| self.keys),
            capacity,
            keep,
        )
    }
}

/// Headers for a record arriving from any wire or persisted form. A change
/// record carries no keys there: they are derived from its value, so a
/// second, disagreeing copy cannot exist.
pub fn record_headers(
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
    let overhead = attributes.iter().fold(value.len(), |bytes, attribute| {
        bytes
            .saturating_add(attribute.name.len())
            .saturating_add(attribute.value.len())
    });
    let remaining = crate::MAX_RECORD_BYTES
        .checked_sub(overhead)
        .ok_or(StreamError::LimitExceeded)?;
    parse_record(value, Some(FORMAT), None, Some(remaining), |key, _, _| {
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

    /// Validates once before selecting entries from the admitted index. Whole
    /// records transfer their original owners; only selected entries are copied
    /// for partial projection. Opaque records use their original header policy.
    pub fn project(
        self,
        unrestricted: bool,
        opaque: impl FnOnce(&RecordHeaders) -> bool,
        keep: impl Fn(&[u8], &RecordHeaders) -> bool,
    ) -> Result<Option<Self>, StreamError> {
        let admitted = AdmittedRecord::new(&self.value, &self.headers)?;
        if !admitted.is_changes() {
            return Ok((unrestricted || opaque(&self.headers)).then_some(self));
        }
        let count = if unrestricted {
            self.headers.keys().len()
        } else {
            self.headers
                .keys()
                .iter()
                .filter(|key| keep(key, &self.headers))
                .count()
        };
        if count == self.headers.keys().len() {
            return Ok(Some(self));
        }
        if count == 0 {
            return Ok(None);
        }
        let changes = admitted.decode(count, |key| keep(key, &self.headers))?;
        if admitted.format != Some(FORMAT) {
            use prost::Message;
            let headers = RecordHeaders::new(
                changes
                    .entries
                    .iter()
                    .map(|entry| entry.key.clone().into())
                    .collect(),
                self.headers
                    .attributes()
                    .iter()
                    .map(crate::RecordAttribute::to_owned)
                    .collect(),
            )?;
            let value: OwnedBytes = changes.encode_to_vec().into();
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
