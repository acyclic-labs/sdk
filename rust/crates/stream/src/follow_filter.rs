use crate::{RecordHeaders, StreamError, wire};
use prost::Message;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Canonical requested filter intersected with every capability filter.
/// Requested ranges select any key; each authority ceiling covers every key.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FollowFilter(Option<Arc<Vec<Clause>>>);

#[derive(Clone, Debug, PartialEq)]
struct Clause {
    all_keys: bool,
    filter: wire::SessionFilter,
}

impl FollowFilter {
    fn clauses(&self) -> &[Clause] {
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
    /// Validates and canonicalizes a requested subscription filter.
    pub fn new(requested: wire::SessionFilter) -> Result<Self, StreamError> {
        Ok(Self::from_clauses(Self::requested_clauses(requested)?))
    }

    fn from_clauses(clauses: Vec<Clause>) -> Self {
        Self((!clauses.is_empty()).then(|| Arc::new(clauses)))
    }

    fn requested_clauses(mut requested: wire::SessionFilter) -> Result<Vec<Clause>, StreamError> {
        if requested.encoded_len() > crate::MAX_COMMAND_BYTES
            || requested.all_of.len() > crate::MAX_ITEMS
        {
            return Err(StreamError::LimitExceeded);
        }
        let conjunction = std::mem::take(&mut requested.all_of);
        let filter = canonical(requested)?;
        let mut clauses = Vec::new();
        if !filter.keys.is_empty() || !filter.attributes.is_empty() {
            clauses.push(Clause {
                all_keys: false,
                filter,
            });
        }
        for clause in conjunction {
            let filter = canonical(wire::SessionFilter {
                keys: clause.keys,
                attributes: clause.attributes,
                all_of: vec![],
            })?;
            if !filter.keys.is_empty() || !filter.attributes.is_empty() {
                clauses.push(Clause {
                    all_keys: clause.all_keys,
                    filter,
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

    fn matches_entry(&self, key: &[u8], headers: &RecordHeaders) -> bool {
        self.clauses().iter().all(|clause| {
            (clause.filter.keys.is_empty() || clause.contains(key))
                && clause.matches_attributes(headers)
        })
    }

    /// Validates a record and checks whether projection would preserve it whole.
    /// Typed changes require every entry to pass, while [`Self::matches`] only
    /// requires an entry that can survive projection. No record owner is cloned
    /// and no projected value is constructed.
    pub fn preserves(&self, record: &crate::Record) -> Result<bool, StreamError> {
        let format = crate::changes::validated_format(&record.value, &record.headers)?;
        Ok(if format.is_some() {
            record
                .headers
                .keys()
                .iter()
                .all(|key| self.matches_entry(key, &record.headers))
        } else {
            self.matches_format(&record.headers, format)
        })
    }

    /// Projects typed provider changes before delivery. Opaque values retain
    /// the stronger rule that every key must satisfy every authority ceiling.
    pub fn project(&self, mut record: crate::Record) -> Result<Option<crate::Record>, StreamError> {
        let format = crate::changes::validated_format(&record.value, &record.headers)?;
        if self.clauses().is_empty() {
            return Ok(Some(record));
        }
        if !self.matches_format(&record.headers, format) {
            return Ok(None);
        }
        if format.is_some() {
            let body = crate::AppendRecord {
                value: record.value,
                headers: record.headers,
            }
            .retain_changes(|key, headers| self.matches_entry(key, headers))?
            .ok_or(StreamError::InvalidArgument)?;
            record.value = body.value;
            record.headers = body.headers;
        }
        Ok(Some(record))
    }

    #[cfg(any(
        test,
        all(feature = "session", not(target_arch = "wasm32")),
        all(feature = "wasm", target_arch = "wasm32")
    ))]
    pub(crate) fn transport_filter(&self) -> Result<wire::SessionFilter, StreamError> {
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

    pub(crate) fn ranges(&self) -> Option<&[wire::KeyRange]> {
        self.clauses()
            .iter()
            .find(|clause| !clause.filter.keys.is_empty())
            .map(|clause| clause.filter.keys.as_slice())
    }

    pub(crate) fn attribute(&self) -> Option<&wire::AttributeMatch> {
        self.clauses()
            .iter()
            .find_map(|clause| clause.filter.attributes.first())
    }
}

impl Clause {
    fn contains(&self, key: &[u8]) -> bool {
        let index = self
            .filter
            .keys
            .partition_point(|range| range.start.as_ref() <= key);
        index.checked_sub(1).is_some_and(|index| {
            self.filter
                .keys
                .get(index)
                .is_some_and(|range| range.end.is_empty() || key < range.end.as_ref())
        })
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

fn canonical(mut filter: wire::SessionFilter) -> Result<wire::SessionFilter, StreamError> {
    if !filter.all_of.is_empty() {
        return Err(StreamError::InvalidArgument);
    }
    if filter.keys.len() > crate::MAX_ITEMS
        || filter.attributes.len() > crate::MAX_RECORD_ATTRIBUTES
        || filter.encoded_len() > crate::MAX_COMMAND_BYTES
    {
        return Err(StreamError::LimitExceeded);
    }
    for range in &filter.keys {
        if range.start.len() > crate::MAX_RECORD_KEY_BYTES
            || range.end.len() > crate::MAX_RECORD_KEY_BYTES
        {
            return Err(StreamError::LimitExceeded);
        }
        if !range.end.is_empty() && range.start >= range.end {
            return Err(StreamError::InvalidArgument);
        }
    }
    filter.keys.sort_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then_with(|| left.end.cmp(&right.end))
    });
    let mut ranges: Vec<wire::KeyRange> = Vec::new();
    for range in filter.keys {
        if let Some(last) = ranges.last_mut()
            && (last.end.is_empty() || range.start <= last.end)
        {
            if !last.end.is_empty() && (range.end.is_empty() || range.end > last.end) {
                last.end = range.end;
            }
        } else {
            ranges.push(range);
        }
    }
    filter.keys = ranges;
    filter
        .attributes
        .sort_by(|left, right| left.name.cmp(&right.name));
    if filter
        .attributes
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if left.name == right.name))
    {
        return Err(StreamError::InvalidArgument);
    }
    for attribute in &mut filter.attributes {
        if attribute.name.is_empty()
            || attribute.name.chars().any(char::is_control)
            || attribute.any_of.is_empty()
        {
            return Err(StreamError::InvalidArgument);
        }
        if attribute.any_of.len() > crate::MAX_ITEMS {
            return Err(StreamError::LimitExceeded);
        }
        attribute.any_of.sort();
        attribute.any_of.dedup();
    }
    Ok(filter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn projection_reuses_validation_and_skips_unrestricted_decoding()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::changes::{take_marker_probes, take_parsed_bytes, validate};

        fn measure<T>(
            run: impl FnOnce() -> T,
        ) -> (T, acyclic_test_support::AllocationInfo, usize, usize) {
            take_marker_probes();
            take_parsed_bytes();
            let mut result = None;
            let allocations = acyclic_test_support::measure(|| result = Some(run()));
            (
                result.unwrap(),
                allocations,
                take_marker_probes(),
                take_parsed_bytes(),
            )
        }

        let followed = crate::StreamPath::new("followed")?;
        let golden = acyclic_proto_build::golden::frame("stream.changes-v1")?;
        let retained =
            crate::wire_codec::record(wire::Record::decode(golden.as_slice())?, Some(&followed))?;
        let current = crate::AppendRecord::changes(
            b"publication",
            &["b", "z"].map(|key| wire::ChangeEntry {
                key: key.into(),
                operation: "write".into(),
                metadata: key.into(),
            }),
            vec![],
        )?;
        let filters = [
            FollowFilter::default(),
            FollowFilter::new(wire::SessionFilter {
                keys: vec![range(b"b", b"c")],
                ..Default::default()
            })?,
            FollowFilter::new(wire::SessionFilter {
                all_of: vec![wire::SessionFilterClause {
                    all_keys: true,
                    keys: vec![range(b"b", b"c")],
                    attributes: vec![],
                }],
                ..Default::default()
            })?,
            FollowFilter::new(wire::SessionFilter {
                keys: vec![range(b"a", b"b")],
                ..Default::default()
            })?,
        ];
        for marker in [
            None,
            Some(b"changes/v1".as_slice()),
            Some(crate::changes::WRITER_FORMAT),
            Some(b"changes/v3".as_slice()),
        ] {
            for (padding, prefix) in [(0, "z"), (15, "z"), (15, "a")] {
                let mut attributes = (0..padding)
                    .map(|index| crate::Attribute {
                        name: format!("{prefix}{index:02}"),
                        value: "other".into(),
                    })
                    .collect::<Vec<_>>();
                if let Some(value) = marker {
                    attributes.push(crate::Attribute {
                        name: "acyclic.record".into(),
                        value: value.into(),
                    });
                }
                attributes.sort_by(|left, right| left.name.cmp(&right.name));
                for fault in 0..3 {
                    let mut record = retained.clone();
                    if marker == Some(crate::changes::WRITER_FORMAT) {
                        record.value = current.value.clone();
                    }
                    if fault == 1 {
                        record.value = bytes::Bytes::new().into();
                    }
                    record.headers = RecordHeaders::new(
                        if fault == 2 {
                            vec!["a".into(), "z".into()]
                        } else {
                            retained.headers.keys().to_vec()
                        },
                        attributes.clone(),
                    )?;
                    take_marker_probes();
                    let format = crate::changes::reader_format(&record.headers);
                    let probes = take_marker_probes();
                    let valid = validate(&record.value, &record.headers).is_ok();
                    for (index, filter) in filters.iter().enumerate() {
                        let (matched, allocations, marker_probes, parsed_bytes) =
                            measure(|| filter.matches(&record.headers));
                        assert_eq!(allocations, acyclic_test_support::AllocationInfo::default());
                        assert_eq!(marker_probes, probes, "classify the marker once");
                        assert_eq!(parsed_bytes, 0, "header matching must not decode the body");
                        let (preservation, allocations, marker_probes, parsed_bytes) =
                            measure(|| filter.preserves(&record));
                        assert_eq!(allocations, acyclic_test_support::AllocationInfo::default());
                        assert_eq!(marker_probes, probes, "classify the marker once");
                        let validation_bytes =
                            if format.as_ref().is_ok_and(|format| format.is_some()) {
                                record.value.len()
                            } else {
                                0
                            };
                        assert_eq!(
                            parsed_bytes, validation_bytes,
                            "validate the original body once"
                        );
                        let (projected, allocations, marker_probes, parsed_bytes) =
                            measure(|| filter.project(record.clone()));
                        if !valid {
                            assert_eq!(preservation, Err(StreamError::InvalidArgument));
                            assert_eq!(projected, Err(StreamError::InvalidArgument));
                        } else {
                            let projected = projected.as_ref().unwrap().as_ref();
                            assert_eq!(matched, projected.is_some());
                            assert_eq!(preservation, Ok(projected == Some(&record)));
                            if let Some(projected) = projected {
                                assert_eq!(filter.preserves(projected), Ok(true));
                                assert_eq!(projected.sequence, record.sequence);
                                assert_eq!(projected.commit_id, record.commit_id);
                                assert_eq!(
                                    projected.committed_at_micros,
                                    record.committed_at_micros
                                );
                                assert_eq!(projected.origin, record.origin);
                                assert_eq!(
                                    projected.headers.attributes(),
                                    record.headers.attributes()
                                );
                            }
                        }
                        if !valid || index == 0 || format == Ok(None) || !matched {
                            assert_eq!(
                                allocations,
                                acyclic_test_support::AllocationInfo::default(),
                                "unrestricted projection must transfer the original owners"
                            );
                            assert_eq!(marker_probes, probes, "classify the marker once");
                            assert_eq!(
                                parsed_bytes, validation_bytes,
                                "validate the original body once"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn range(start: &'static [u8], end: &'static [u8]) -> wire::KeyRange {
        wire::KeyRange {
            start: start.into(),
            end: end.into(),
        }
    }
    fn headers(keys: &[&'static str], owner: &'static str) -> RecordHeaders {
        RecordHeaders::new(
            keys.iter().map(|key| (*key).into()).collect(),
            vec![crate::Attribute {
                name: "owner".into(),
                value: owner.into(),
            }],
        )
        .unwrap()
    }
    #[test]
    fn any_requested_key_and_every_attribute_must_match() {
        let filter = FollowFilter::new(wire::SessionFilter {
            all_of: vec![],
            keys: vec![range(b"a", b"c")],
            attributes: vec![wire::AttributeMatch {
                name: "owner".into(),
                any_of: vec![b"alice".as_slice().into()],
            }],
        })
        .unwrap();
        assert!(filter.matches(&headers(&["b", "z"], "alice")));
        assert!(!filter.matches(&headers(&["c"], "alice")));
        assert!(!filter.matches(&headers(&["b"], "bob")));
        assert!(!filter.matches(&headers(&[], "alice")));
    }
    #[test]
    fn opaque_authority_never_discloses_a_record_with_an_outside_key() {
        let requested = Clause {
            all_keys: false,
            filter: canonical(wire::SessionFilter {
                all_of: vec![],
                keys: vec![range(b"a", b"c")],
                attributes: vec![],
            })
            .unwrap(),
        };
        let ceiling = Clause {
            all_keys: true,
            filter: requested.filter.clone(),
        };
        let filter = FollowFilter(Some(Arc::new(vec![requested.clone(), ceiling])));
        assert!(filter.matches(&headers(&["a", "b"], "alice")));
        assert!(!filter.matches(&headers(&["b", "z"], "alice")));
        assert!(
            FollowFilter(Some(Arc::new(vec![requested]))).matches(&headers(&["b", "z"], "alice"))
        );
    }
    #[tokio::test]
    async fn provider_hops_preserve_requested_any_keys_and_all_authority_ceilings() {
        use acyclic_auth::{contract as auth, testing as signed};
        use futures::StreamExt as _;

        let make = |all_keys, start: &[u8], end: &[u8]| Clause {
            all_keys,
            filter: canonical(wire::SessionFilter {
                keys: vec![wire::KeyRange {
                    start: start.to_vec().into(),
                    end: end.to_vec().into(),
                }],
                attributes: vec![],
                all_of: vec![],
            })
            .unwrap(),
        };
        let token = signed::root(|block| {
            for (start, end) in [(b"a", b"m"), (b"b", b"z")] {
                block.caveats.push(auth::Caveat {
                    kind: auth::CaveatKind::Filter(auth::Filter {
                        keys: vec![auth::KeyRange {
                            start: start.to_vec(),
                            end: end.to_vec(),
                        }],
                        attributes: vec![],
                    }),
                });
            }
            block.caveats.push(auth::Caveat {
                kind: auth::CaveatKind::Pin(auth::Pin {
                    name: "owner".into(),
                    value: auth::PinValue::Literal(b"alice".to_vec()),
                }),
            });
        });
        let capability =
            acyclic_auth::verify::verify(&token, &signed::Snapshot::default(), signed::NOW)
                .unwrap();
        let requested = wire::SessionFilter {
            keys: vec![range(b"a", b"c")],
            attributes: vec![wire::AttributeMatch {
                name: "owner".into(),
                any_of: vec![b"bob".as_slice().into(), b"alice".as_slice().into()],
            }],
            all_of: vec![wire::SessionFilterClause {
                keys: vec![range(b"a", b"z")],
                attributes: vec![],
                all_keys: false,
            }],
        };
        let old_request = requested.clone();
        let requested_intersection = FollowFilter::new(requested.clone()).unwrap();
        let mut original = None;
        let mut transferred = None;
        let build_original = || {
            // Original ownership boundary, with independently specified authority clauses.
            let mut clauses = FollowFilter::new(old_request).unwrap().clauses().to_vec();
            clauses.push(make(true, b"a", b"m"));
            clauses.push(make(true, b"b", b"z"));
            clauses.push(Clause {
                all_keys: true,
                filter: canonical(wire::SessionFilter {
                    keys: vec![],
                    attributes: vec![wire::AttributeMatch {
                        name: "owner".into(),
                        any_of: vec![bytes::Bytes::copy_from_slice(b"alice")],
                    }],
                    all_of: vec![],
                })
                .unwrap(),
            });
            original = Some(FollowFilter(Some(Arc::new(clauses))));
        };
        #[cfg(not(target_arch = "wasm32"))]
        let before = acyclic_test_support::measure(build_original);
        #[cfg(target_arch = "wasm32")]
        build_original();
        let authorize = || {
            transferred = Some(FollowFilter::authorized(requested, &capability).unwrap());
        };
        #[cfg(not(target_arch = "wasm32"))]
        let after = acyclic_test_support::measure(authorize);
        #[cfg(target_arch = "wasm32")]
        authorize();
        let original = original.unwrap();
        let transferred = transferred.unwrap();
        assert_eq!(original, transferred);
        #[cfg(not(target_arch = "wasm32"))]
        {
            assert!(after.count_total < before.count_total);
            assert!(after.bytes_total < before.bytes_total);
            eprintln!(
                "authorized clauses: {} -> {} allocations, {} -> {} bytes",
                before.count_total, after.count_total, before.bytes_total, after.bytes_total
            );
        }
        let reversed = FollowFilter(Some(Arc::new(
            original.clauses().iter().rev().cloned().collect(),
        )));
        let single = FollowFilter(Some(Arc::new(vec![original.clauses()[0].clone()])));
        for filter in [&original, &reversed, &single, &FollowFilter::default()] {
            let (mut expected, mut actual) = (None, None);
            let mut original_digest = || {
                // Original digest framing and protobuf encoding, one owner per clause.
                let mut hash = Sha256::new();
                hash.update(b"acyclic.stream.follow-filter.v1\0");
                hash.update((filter.clauses().len() as u64).to_le_bytes());
                for clause in filter.clauses() {
                    hash.update([u8::from(clause.all_keys)]);
                    let bytes = clause.filter.encode_to_vec();
                    hash.update((bytes.len() as u64).to_le_bytes());
                    hash.update(bytes);
                }
                expected = Some(<[u8; 32]>::from(hash.finalize()));
            };
            #[cfg(not(target_arch = "wasm32"))]
            let before = acyclic_test_support::measure(&mut original_digest);
            #[cfg(target_arch = "wasm32")]
            original_digest();
            let mut digest = || actual = Some(filter.digest());
            #[cfg(not(target_arch = "wasm32"))]
            let after = acyclic_test_support::measure(&mut digest);
            #[cfg(target_arch = "wasm32")]
            digest();
            assert_eq!(actual, expected);
            #[cfg(not(target_arch = "wasm32"))]
            {
                assert!(after.count_total <= before.count_total);
                assert!(after.bytes_total <= before.bytes_total);
                assert!(after.bytes_max <= before.bytes_max);
                if filter.clauses().len() > 1 {
                    assert!(after.count_total < before.count_total);
                    assert!(after.bytes_total < before.bytes_total);
                }
                eprintln!(
                    "digest {} clauses: {} -> {} allocations, {} -> {} bytes, peak {} -> {}",
                    filter.clauses().len(),
                    before.count_total,
                    after.count_total,
                    before.bytes_total,
                    after.bytes_total,
                    before.bytes_max,
                    after.bytes_max
                );
            }
        }
        let forwarded = FollowFilter::new(original.transport_filter().unwrap()).unwrap();
        let ceiling = FollowFilter::authorized(Default::default(), &capability).unwrap();
        let intersection = requested_intersection.intersection(&ceiling);
        assert_eq!(intersection, original);
        assert_eq!(
            intersection.intersection(&FollowFilter::default()),
            original
        );
        assert_eq!(
            FollowFilter::default().intersection(&intersection),
            original
        );
        assert_eq!(original.digest(), forwarded.digest());
        assert!(forwarded.matches(&headers(&["b", "j"], "alice")));
        assert!(!forwarded.matches(&headers(&["b", "z"], "alice")));
        assert!(!forwarded.matches(&headers(&["a", "b"], "alice")));
        assert!(!forwarded.matches(&headers(&["j"], "alice")));
        assert!(!forwarded.matches(&headers(&["b"], "bob")));
        let dropped_ceiling = FollowFilter(Some(Arc::new(vec![make(false, b"a", b"c")])));
        assert!(
            dropped_ceiling.matches(&headers(&["b", "z"], "alice")),
            "negative control needs the authority ceiling"
        );

        // Capture opaque and typed records from the real provider, then compare
        // projection and refusal across the ownership boundary and transport hop.
        let provider = crate::MemoryStream::default();
        let path = crate::StreamPath::new("filter-projection").unwrap();
        crate::StreamProviderExt::append(
            &provider,
            crate::AppendRequest {
                path: path.clone(),
                records: vec![
                    crate::AppendRecord {
                        value: "opaque".into(),
                        headers: headers(&["b", "j"], "alice"),
                    },
                    crate::AppendRecord {
                        value: "denied".into(),
                        headers: headers(&["b"], "bob"),
                    },
                    crate::AppendRecord::changes(
                        b"filter-control",
                        &["b", "z"].map(|key| wire::ChangeEntry {
                            key: key.into(),
                            operation: "write".into(),
                            metadata: bytes::Bytes::new(),
                        }),
                        vec![crate::Attribute {
                            name: "owner".into(),
                            value: "alice".into(),
                        }],
                    )
                    .unwrap(),
                ],
                if_tail: Some(0),
                idempotency_key: crate::IdempotencyKey::unique(),
            },
        )
        .await
        .unwrap();
        let mut records = crate::StreamProvider::read(
            &provider,
            crate::ReadRequest {
                key: None,
                path,
                from: 0,
                limit: 3,
                at_least: Default::default(),
            },
        )
        .await
        .unwrap();
        let mut seen = 0;
        while let Some(record) = records.next().await {
            let record = record.unwrap();
            let expected = original.project(record.clone()).unwrap();
            assert_eq!(transferred.project(record.clone()).unwrap(), expected);
            assert_eq!(forwarded.project(record.clone()).unwrap(), expected);
            if crate::changes::reader_format(&record.headers)
                .unwrap()
                .is_some()
            {
                let projected = expected.unwrap();
                assert_eq!(projected.headers.keys().len(), 1);
                assert_eq!(projected.headers.keys()[0].as_ref(), b"b");
                let mut malformed = record;
                malformed.value = bytes::Bytes::new().into();
                assert_eq!(
                    original.project(malformed.clone()),
                    Err(StreamError::InvalidArgument)
                );
                assert_eq!(
                    transferred.project(malformed.clone()),
                    original.project(malformed)
                );
            }
            seen += 1;
        }
        assert_eq!(seen, 3);
        let malformed = wire::SessionFilter {
            keys: vec![range(b"c", b"a")],
            ..Default::default()
        };
        assert_eq!(
            FollowFilter::authorized(malformed.clone(), &capability),
            FollowFilter::new(malformed)
        );
        let token = signed::root(|_| {});
        let unrestricted =
            acyclic_auth::verify::verify(&token, &signed::Snapshot::default(), signed::NOW)
                .unwrap();
        let mut empty = None;
        let mut authorize = || {
            empty = Some(FollowFilter::authorized(Default::default(), &unrestricted).unwrap());
        };
        #[cfg(not(target_arch = "wasm32"))]
        let work = acyclic_test_support::measure(&mut authorize);
        #[cfg(target_arch = "wasm32")]
        authorize();
        assert!(empty.unwrap().matches(&headers(&["z"], "bob")));
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(
            work.count_total, 0,
            "unrestricted filters must stay allocation-free"
        );
    }

    #[test]
    fn canonical_ranges_and_values_have_one_digest() {
        let left = FollowFilter::new(wire::SessionFilter {
            all_of: vec![],
            keys: vec![range(b"b", b"d"), range(b"a", b"c")],
            attributes: vec![],
        })
        .unwrap();
        let right = FollowFilter::new(wire::SessionFilter {
            all_of: vec![],
            keys: vec![range(b"a", b"d")],
            attributes: vec![],
        })
        .unwrap();
        assert_eq!(left.digest(), right.digest());
        assert_ne!(left.digest(), FollowFilter::default().digest());
    }
    #[test]
    fn malformed_filters_fail_closed() {
        for keys in [vec![range(b"b", b"a")], vec![range(b"a", b"a")]] {
            assert!(
                FollowFilter::new(wire::SessionFilter {
                    all_of: vec![],
                    keys,
                    attributes: vec![]
                })
                .is_err()
            );
        }
        assert!(
            FollowFilter::new(wire::SessionFilter {
                all_of: vec![],
                keys: vec![],
                attributes: vec![wire::AttributeMatch {
                    name: "owner".into(),
                    any_of: vec![]
                }]
            })
            .is_err()
        );
    }
}
