use crate::{RecordHeaders, StreamError, wire};
use prost::Message;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Canonical requested filter intersected with every capability filter.
/// Requested ranges select any key; each authority ceiling covers every key.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FollowFilter(Arc<Vec<Clause>>);

#[derive(Clone, Debug, PartialEq)]
struct Clause {
    all_keys: bool,
    filter: wire::SessionFilter,
}

impl FollowFilter {
    /// Validates and canonicalizes a requested subscription filter.
    pub fn new(requested: wire::SessionFilter) -> Result<Self, StreamError> {
        let filter = canonical(requested)?;
        if filter.keys.is_empty() && filter.attributes.is_empty() {
            return Ok(Self::default());
        }
        Ok(Self(Arc::new(vec![Clause {
            all_keys: false,
            filter,
        }])))
    }

    /// Intersects a request with authenticated capability clauses. Subject
    /// references bind at their declaring block, before any child lineage.
    pub fn authorized(
        requested: wire::SessionFilter,
        capability: &acyclic_auth::verify::VerifiedCapability,
    ) -> Result<Self, StreamError> {
        use acyclic_auth::contract::SubjectReferenceKind;
        let mut clauses = vec![Clause {
            all_keys: false,
            filter: canonical(requested)?,
        }];
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
            clauses.push(Clause {
                all_keys: true,
                filter: canonical(wire::SessionFilter {
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
        Ok(Self(Arc::new(clauses)))
    }

    /// Returns whether opaque record headers satisfy the full intersection.
    pub fn matches(&self, headers: &RecordHeaders) -> bool {
        self.0.iter().all(|clause| clause.matches(headers))
    }

    /// Stable identity of the canonical effective conjunction.
    pub fn digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"acyclic.stream.follow-filter.v1\0");
        hash.update((self.0.len() as u64).to_le_bytes());
        for clause in self.0.iter() {
            hash.update([u8::from(clause.all_keys)]);
            let bytes = clause.filter.encode_to_vec();
            hash.update((bytes.len() as u64).to_le_bytes());
            hash.update(bytes);
        }
        hash.finalize().into()
    }

    pub(crate) fn ranges(&self) -> Option<&[wire::KeyRange]> {
        self.0
            .iter()
            .find(|clause| !clause.filter.keys.is_empty())
            .map(|clause| clause.filter.keys.as_slice())
    }

    pub(crate) fn attribute(&self) -> Option<&wire::AttributeMatch> {
        self.0
            .iter()
            .find_map(|clause| clause.filter.attributes.first())
    }
}

impl Clause {
    fn matches(&self, headers: &RecordHeaders) -> bool {
        let in_range = |key: &crate::OwnedBytes| {
            let index = self
                .filter
                .keys
                .partition_point(|range| range.start.as_ref() <= key.as_ref());
            index.checked_sub(1).is_some_and(|index| {
                let end = &self.filter.keys[index].end;
                end.is_empty() || key.as_ref() < end.as_ref()
            })
        };
        (self.filter.keys.is_empty()
            || !headers.keys().is_empty()
                && if self.all_keys {
                    headers.keys().iter().all(in_range)
                } else {
                    headers.keys().iter().any(in_range)
                })
            && self.filter.attributes.iter().all(|clause| {
                headers
                    .attributes()
                    .binary_search_by(|attribute| attribute.name.cmp(&clause.name))
                    .ok()
                    .is_some_and(|index| {
                        clause
                            .any_of
                            .binary_search_by(|value| {
                                value
                                    .as_ref()
                                    .cmp(headers.attributes()[index].value.as_ref())
                            })
                            .is_ok()
                    })
            })
    }
}

fn canonical(mut filter: wire::SessionFilter) -> Result<wire::SessionFilter, StreamError> {
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
        .any(|pair| pair[0].name == pair[1].name)
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
                keys: vec![range(b"a", b"c")],
                attributes: vec![],
            })
            .unwrap(),
        };
        let ceiling = Clause {
            all_keys: true,
            filter: requested.filter.clone(),
        };
        let filter = FollowFilter(Arc::new(vec![requested.clone(), ceiling]));
        assert!(filter.matches(&headers(&["a", "b"], "alice")));
        assert!(!filter.matches(&headers(&["b", "z"], "alice")));
        assert!(FollowFilter(Arc::new(vec![requested])).matches(&headers(&["b", "z"], "alice")));
    }
    #[test]
    fn canonical_ranges_and_values_have_one_digest() {
        let left = FollowFilter::new(wire::SessionFilter {
            keys: vec![range(b"b", b"d"), range(b"a", b"c")],
            attributes: vec![],
        })
        .unwrap();
        let right = FollowFilter::new(wire::SessionFilter {
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
                    keys,
                    attributes: vec![]
                })
                .is_err()
            );
        }
        assert!(
            FollowFilter::new(wire::SessionFilter {
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
