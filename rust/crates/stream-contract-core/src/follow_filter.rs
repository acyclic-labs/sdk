use crate::{RecordHeaders, StreamError};
use acyclic_stream_wire::filter::{FilterError, canonical, contains_key, exclusive_key_end};
use acyclic_stream_wire::wire;
use prost::Message;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Canonical requested filter intersected with every capability filter.
/// Requested ranges select any key; each authority ceiling covers every key.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FollowFilter(pub Option<Arc<Vec<Clause>>>);

#[derive(Clone, Debug, PartialEq)]
pub struct Clause {
    pub all_keys: bool,
    pub filter: wire::SessionFilter,
    pub projection: Option<wire::KeyRange>,
}

impl FollowFilter {
    /// Select one exact key, including empty and maximum-length keys. This
    /// shares the canonical range representation used by remote sessions.
    pub fn exact_key(key: bytes::Bytes) -> Result<Self, StreamError> {
        let end = exclusive_key_end(&key)?;
        Self::new(wire::SessionFilter {
            keys: vec![wire::KeyRange {
                start: key,
                end: end.into(),
            }],
            ..Default::default()
        })
    }

    /// Whether selection requires an authenticated record index key. Such a
    /// selection cannot match an unkeyed record, including with attribute clauses.
    pub fn requires_key(&self) -> bool {
        self.ranges().is_some()
    }

    pub fn clauses(&self) -> &[Clause] {
        self.0.as_deref().map_or(&[], Vec::as_slice)
    }

    /// Requires both admitted selections, preserving every authority ceiling.
    /// An unrestricted selection contributes no additional clause.
    pub fn intersection(&self, other: &Self) -> Self {
        if self.0.is_none() {
            return other.clone();
        }
        if other.0.is_none() {
            return self.clone();
        }
        let mut clauses = Vec::with_capacity(self.clauses().len() + other.clauses().len());
        clauses.extend_from_slice(self.clauses());
        clauses.extend_from_slice(other.clauses());
        Self(Some(Arc::new(clauses)))
    }

    /// Keeps an indirect record as a candidate through every key clause.
    /// Attributes still apply. This is only an index selection: the caller
    /// must project the indirect record and authorize its exact resulting keys
    /// before disclosure. It must never be used as an authority ceiling.
    pub fn including_projection_key(&self, key: &[u8]) -> Result<Self, StreamError> {
        if key.len() >= acyclic_stream_wire::headers::MAX_RECORD_KEY_BYTES {
            return Err(StreamError::InvalidArgument);
        }
        let mut clauses = self.clauses().to_vec();
        for clause in &mut clauses {
            if !clause.filter.keys.is_empty() {
                clause.projection = Some(projection_range(key)?);
            }
        }
        Ok(Self::from_clauses(clauses))
    }
    /// Validates and canonicalizes a requested subscription filter.
    pub fn new(requested: wire::SessionFilter) -> Result<Self, StreamError> {
        Ok(Self::from_clauses(Self::requested_clauses(requested)?))
    }

    fn from_clauses(clauses: Vec<Clause>) -> Self {
        Self((!clauses.is_empty()).then(|| Arc::new(clauses)))
    }

    fn requested_clauses(mut requested: wire::SessionFilter) -> Result<Vec<Clause>, StreamError> {
        if requested.encoded_len() > crate::MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        let conjunction = std::mem::take(&mut requested.all_of);
        let filter = canonical(requested)?;
        let mut clauses = Vec::new();
        if !filter.keys.is_empty() || !filter.attributes.is_empty() {
            clauses.push(Clause {
                all_keys: false,
                filter,
                projection: None,
            });
        }
        for clause in conjunction {
            let projection = clause
                .projection_key
                .as_deref()
                .map(projection_range)
                .transpose()?;
            let filter = canonical(wire::SessionFilter {
                keys: clause.keys,
                attributes: clause.attributes,
                all_of: vec![],
            })?;
            if !filter.keys.is_empty() || !filter.attributes.is_empty() {
                clauses.push(Clause {
                    all_keys: clause.all_keys,
                    filter,
                    projection,
                });
            }
        }
        Ok(clauses)
    }

    /// Intersects a request with authenticated capability clauses. Subject
    /// references bind at their declaring block, before any child lineage.
    pub fn authorized(
        requested: wire::SessionFilter,
        capability: &acyclic_auth::verify::VerifiedCapability,
    ) -> Result<Self, StreamError> {
        use acyclic_auth::contract::SubjectReferenceKind;
        let mut clauses = Self::requested_clauses(requested)?;
        for scoped in capability.effective().filters() {
            let source = scoped.filter();
            let attributes = source
                .attributes
                .iter()
                .map(|attribute| {
                    let mut any_of: Vec<bytes::Bytes> =
                        attribute.any_of.iter().cloned().map(Into::into).collect();
                    for subject in &attribute.subjects {
                        match subject.kind {
                            SubjectReferenceKind::Id(_) => any_of.push(
                                bytes::Bytes::copy_from_slice(scoped.subject_id().as_bytes()),
                            ),
                            SubjectReferenceKind::Lineage(_) => any_of.extend(
                                scoped
                                    .lineage()
                                    .map(|id| bytes::Bytes::copy_from_slice(id.as_bytes())),
                            ),
                        }
                    }
                    wire::AttributeMatch {
                        name: attribute.name.clone(),
                        any_of,
                    }
                })
                .collect();
            if source.keys.is_empty() && source.attributes.is_empty() {
                continue;
            }
            clauses.push(Clause {
                all_keys: true,
                projection: None,
                filter: canonical(wire::SessionFilter {
                    all_of: vec![],
                    keys: source
                        .keys
                        .iter()
                        .map(|range| wire::KeyRange {
                            start: range.start.clone().into(),
                            end: range.end.clone().into(),
                        })
                        .collect(),
                    attributes,
                })?,
            });
        }
        for scoped in capability.effective().pins() {
            let pin = scoped.pin();
            let value = match &pin.value {
                acyclic_auth::contract::PinValue::Literal(value) => value.as_slice(),
                acyclic_auth::contract::PinValue::SubjectId(_) => scoped.subject_id().as_bytes(),
            };
            clauses.push(Clause {
                all_keys: true,
                projection: None,
                filter: canonical(wire::SessionFilter {
                    all_of: vec![],
                    keys: vec![],
                    attributes: vec![wire::AttributeMatch {
                        name: pin.name.clone(),
                        any_of: vec![bytes::Bytes::copy_from_slice(value)],
                    }],
                })?,
            });
        }
        Ok(Self::from_clauses(clauses))
    }

    /// Returns whether opaque record headers satisfy the full intersection.
    pub fn matches(&self, headers: &RecordHeaders) -> bool {
        crate::changes::reader_format(headers)
            .is_ok_and(|format| self.matches_format(headers, format))
    }

    fn matches_format(&self, headers: &RecordHeaders, format: Option<&[u8]>) -> bool {
        match format {
            Some(_) => headers
                .keys()
                .iter()
                .any(|key| self.matches_entry(key, headers)),
            None => self.clauses().iter().all(|clause| clause.matches(headers)),
        }
    }

    /// Checks one expanded typed change against the original selection.
    /// Indirect candidate selection must not replace this disclosure check.
    pub fn matches_entry(&self, key: &[u8], headers: &RecordHeaders) -> bool {
        self.clauses().iter().all(|clause| {
            (clause.filter.keys.is_empty() || clause.contains(key))
                && clause.matches_attributes(headers)
        })
    }

    /// Original attribute ceilings also apply to indirect candidates whose
    /// concrete keys can only be checked after expansion.
    pub fn matches_attributes(&self, headers: &RecordHeaders) -> bool {
        self.clauses()
            .iter()
            .all(|clause| clause.matches_attributes(headers))
    }

    /// Validates a record and checks whether projection would preserve it whole.
    /// Typed changes require every entry to pass, while [`Self::matches`] only
    /// requires an entry that can survive projection. No record owner is cloned
    /// and no projected value is constructed.
    pub fn preserves(&self, record: &crate::Record) -> Result<bool, StreamError> {
        let admitted = crate::changes::AdmittedRecord::new(&record.value, &record.headers)?;
        Ok(if admitted.is_changes() {
            record
                .headers
                .keys()
                .iter()
                .all(|key| self.matches_entry(key, &record.headers))
        } else {
            self.matches_format(&record.headers, None)
        })
    }

    /// Projects typed provider changes before delivery. Opaque values retain
    /// the stronger rule that every key must satisfy every authority ceiling.
    pub fn project(&self, mut record: crate::Record) -> Result<Option<crate::Record>, StreamError> {
        let Some(body) = (crate::AppendRecord {
            value: record.value,
            headers: record.headers,
        })
        .project(
            self.clauses().is_empty(),
            |headers| self.matches_format(headers, None),
            |key, headers| self.matches_entry(key, headers),
        )?
        else {
            return Ok(None);
        };
        record.value = body.value;
        record.headers = body.headers;
        Ok(Some(record))
    }

    #[cfg(any(
        test,
        not(target_arch = "wasm32"),
        all(feature = "wasm", target_arch = "wasm32")
    ))]
    pub fn transport_filter(&self) -> Result<wire::SessionFilter, StreamError> {
        let filter = wire::SessionFilter {
            keys: vec![],
            attributes: vec![],
            all_of: self
                .clauses()
                .iter()
                .map(|clause| wire::SessionFilterClause {
                    keys: clause.filter.keys.clone(),
                    attributes: clause.filter.attributes.clone(),
                    all_keys: clause.all_keys,
                    projection_key: clause.projection.as_ref().map(|range| range.start.clone()),
                })
                .collect(),
        };
        if filter.encoded_len() > crate::MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        Ok(filter)
    }

    /// Stable identity of the canonical effective conjunction.
    pub fn digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"acyclic.stream.follow-filter.v1\0");
        hash.update((self.clauses().len() as u64).to_le_bytes());
        let mut bytes = Vec::new();
        for clause in self.clauses().iter() {
            hash.update([u8::from(clause.all_keys)]);
            hash.update([u8::from(clause.projection.is_some())]);
            if let Some(range) = &clause.projection {
                hash.update((range.start.len() as u64).to_le_bytes());
                hash.update(&range.start);
            }
            let length = clause.filter.encoded_len();
            bytes.clear();
            if bytes.capacity() < length {
                // Release before reserving: no old bytes need copying or overlap.
                bytes = Vec::new();
                bytes.reserve_exact(length);
            }
            clause.filter.encode_raw(&mut bytes);
            hash.update((bytes.len() as u64).to_le_bytes());
            hash.update(&bytes);
        }
        hash.finalize().into()
    }

    pub fn ranges(&self) -> Option<impl Iterator<Item = &wire::KeyRange>> {
        self.clauses()
            .iter()
            .find(|clause| !clause.filter.keys.is_empty())
            .map(|clause| clause.filter.keys.iter().chain(clause.projection.iter()))
    }

    pub fn attribute(&self) -> Option<&wire::AttributeMatch> {
        self.clauses()
            .iter()
            .find_map(|clause| clause.filter.attributes.first())
    }
}

impl Clause {
    fn contains(&self, key: &[u8]) -> bool {
        self.projection
            .as_ref()
            .is_some_and(|range| range.start.as_ref() == key)
            || contains_key(&self.filter.keys, key)
    }

    fn matches_attributes(&self, headers: &RecordHeaders) -> bool {
        self.filter.attributes.iter().all(|clause| {
            headers
                .attributes()
                .binary_search_by(|attribute| attribute.name.cmp(clause.name.as_str()))
                .ok()
                .and_then(|index| headers.attributes().get(index))
                .is_some_and(|attribute| {
                    clause
                        .any_of
                        .binary_search_by(|value| value.as_ref().cmp(attribute.value.as_ref()))
                        .is_ok()
                })
        })
    }
    fn matches(&self, headers: &RecordHeaders) -> bool {
        let in_range = |key: &crate::OwnedBytes| self.contains(key);
        (self.filter.keys.is_empty()
            || !headers.keys().is_empty()
                && if self.all_keys {
                    headers.keys().iter().all(in_range)
                } else {
                    headers.keys().iter().any(in_range)
                })
            && self.matches_attributes(headers)
    }
}

fn projection_range(key: &[u8]) -> Result<wire::KeyRange, StreamError> {
    if key.len() >= acyclic_stream_wire::headers::MAX_RECORD_KEY_BYTES {
        return Err(StreamError::InvalidArgument);
    }
    let end = exclusive_key_end(key)?;
    Ok(wire::KeyRange {
        start: bytes::Bytes::copy_from_slice(key),
        end: end.into(),
    })
}

impl From<FilterError> for StreamError {
    fn from(error: FilterError) -> Self {
        match error {
            FilterError::Invalid => Self::InvalidArgument,
            FilterError::Limit => Self::LimitExceeded,
        }
    }
}

// Check selection against full decoding by the same canonical format reader.
// The oracle does not reproduce parsing or the production projection path.
#[cfg(any(all(test, not(target_arch = "wasm32")), feature = "test-support"))]
pub fn projection_contract(record: &crate::Record) -> Result<(), StreamError> {
    if FollowFilter::default().project(record.clone())? != Some(record.clone()) {
        return Err(StreamError::InvalidResponse);
    }
    let keys = record.headers.keys();
    let Some(pivot) = keys.get(keys.len() / 2) else {
        return Ok(());
    };
    let changes = crate::changes::is_changes(&record.headers)
        .then(|| crate::changes::decode(&record.value, &record.headers))
        .transpose()?;
    let range = |prefix| wire::KeyRange {
        start: if prefix {
            bytes::Bytes::new()
        } else {
            bytes::Bytes::copy_from_slice(pivot)
        },
        end: if prefix {
            bytes::Bytes::copy_from_slice(pivot)
        } else {
            bytes::Bytes::new()
        },
    };
    for (prefix, conflicting) in [(true, false), (false, false), (true, true)] {
        // An empty end denotes infinity, so it cannot express the empty prefix.
        if prefix && pivot.is_empty() {
            continue;
        }
        let keep = |key: &[u8]| !conflicting && (key < pivot.as_ref()) == prefix;
        let filter = FollowFilter::new(wire::SessionFilter {
            keys: if conflicting {
                vec![range(prefix)]
            } else {
                Vec::new()
            },
            all_of: vec![wire::SessionFilterClause {
                all_keys: true,
                keys: vec![range(if conflicting { !prefix } else { prefix })],
                attributes: Vec::new(),
                projection_key: None,
            }],
            ..Default::default()
        })?;
        let projected = filter.project(record.clone())?;
        let Some(changes) = &changes else {
            let expected =
                (!keys.is_empty() && keys.iter().all(|key| keep(key))).then(|| record.clone());
            if projected != expected {
                return Err(StreamError::InvalidResponse);
            }
            continue;
        };
        let mut expected = changes.clone();
        expected.entries.retain(|entry| keep(&entry.key));
        if expected.entries.is_empty() {
            if projected.is_some() {
                return Err(StreamError::InvalidResponse);
            }
            continue;
        }
        let projected = projected.ok_or(StreamError::InvalidResponse)?;
        if crate::changes::decode(&projected.value, &projected.headers)? != expected
            || projected.sequence != record.sequence
            || projected.commit_id != record.commit_id
            || projected.committed_at_micros != record.committed_at_micros
            || projected.origin != record.origin
            || projected.headers.attributes() != record.headers.attributes()
            || projected
                .headers
                .keys()
                .iter()
                .map(|key| key.as_ref())
                .ne(expected.entries.iter().map(|entry| entry.key.as_ref()))
            || filter.project(projected.clone())? != Some(projected)
        {
            return Err(StreamError::InvalidResponse);
        }
    }
    Ok(())
}
