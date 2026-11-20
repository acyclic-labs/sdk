pub use acyclic_stream_contract::follow_filter::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use prost::Message as _;
    use sha2::Digest as _;
    use sha2::Sha256;

    #[test]
    fn exact_selection_excludes_prefix_extensions_at_the_key_boundary() -> Result<(), StreamError> {
        // Exercise the real canonical filter/header matcher over every possible
        // last byte, including carry, empty keys and the greatest legal key.
        for size in [
            0,
            1,
            acyclic_stream_wire::headers::MAX_RECORD_KEY_BYTES - 1,
            acyclic_stream_wire::headers::MAX_RECORD_KEY_BYTES,
        ] {
            for byte in 0..=u8::MAX {
                let key = bytes::Bytes::from(vec![byte; size]);
                let filter = FollowFilter::exact_key(key.clone())?;
                let mut candidates = vec![key.to_vec(), vec![], vec![0], vec![u8::MAX]];
                let mut extended = key.to_vec();
                extended.push(0);
                if extended.len() <= acyclic_stream_wire::headers::MAX_RECORD_KEY_BYTES {
                    candidates.push(extended);
                }
                if !key.is_empty() {
                    candidates.push(key[..key.len() - 1].to_vec());
                    let mut adjacent = key.to_vec();
                    adjacent[0] = adjacent[0].wrapping_add(1);
                    candidates.push(adjacent);
                }
                for candidate in candidates {
                    let headers = RecordHeaders::new(
                        vec![bytes::Bytes::from(candidate.clone()).into()],
                        vec![],
                    )?;
                    assert_eq!(
                        filter.matches(&headers),
                        candidate.as_slice() == key.as_ref()
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn projection_candidate_keeps_full_caller_capacity_through_transport() {
        for include_root in [false, true] {
            let keys = (0..crate::MAX_ITEMS)
                .map(|index| {
                    let start = if include_root && index == 0 {
                        Vec::new()
                    } else {
                        format!("path-{index:08}").into_bytes()
                    };
                    let mut end = start.clone();
                    end.push(0);
                    wire::KeyRange {
                        start: start.into(),
                        end: end.into(),
                    }
                })
                .collect();
            let original = FollowFilter::new(wire::SessionFilter {
                keys,
                ..Default::default()
            })
            .unwrap();
            let candidate = original.including_projection_key(b"").unwrap();
            let transported = FollowFilter::new(candidate.transport_filter().unwrap()).unwrap();
            assert_eq!(candidate, transported);
            let headers =
                crate::RecordHeaders::new(vec![bytes::Bytes::new().into()], vec![]).unwrap();
            assert!(transported.matches_entry(b"", &headers));
            assert!(!transported.matches_entry(b"outside", &headers));
            assert_eq!(original.matches_entry(b"", &headers), include_root);
        }
    }

    #[test]
    fn projection_candidates_preserve_attributes_and_every_other_key_ceiling() {
        let requested = FollowFilter::new(wire::SessionFilter {
            keys: vec![wire::KeyRange {
                start: bytes::Bytes::from_static(b"watched"),
                end: bytes::Bytes::from_static(b"watchee"),
            }],
            attributes: vec![wire::AttributeMatch {
                name: "owner".into(),
                any_of: vec![bytes::Bytes::from_static(b"caller")],
            }],
            all_of: vec![wire::SessionFilterClause {
                all_keys: true,
                keys: vec![wire::KeyRange {
                    start: bytes::Bytes::from_static(b"watched/nested"),
                    end: bytes::Bytes::from_static(b"watched/nestee"),
                }],
                attributes: vec![],
                projection_key: None,
            }],
        })
        .unwrap();
        let candidates = requested.including_projection_key(&[]).unwrap();
        let headers = |key: &'static [u8], owner: &'static [u8]| {
            crate::AppendRecord::changes(
                b"",
                &[wire::ChangeEntry {
                    key: bytes::Bytes::from_static(key),
                    operation: "delta".into(),
                    metadata: bytes::Bytes::new(),
                }],
                vec![crate::Attribute {
                    name: "owner".into(),
                    value: bytes::Bytes::from_static(owner).into(),
                }],
            )
            .unwrap()
            .headers
        };
        assert!(!requested.matches(&headers(b"", b"caller")));
        assert!(candidates.matches(&headers(b"", b"caller")));
        assert!(!candidates.matches(&headers(b"", b"foreign")));
        assert!(!candidates.matches(&headers(b"watched/other", b"caller")));
        assert!(candidates.matches(&headers(b"watched/nested/file", b"caller")));
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn measure<T>(
        run: impl FnOnce() -> T,
    ) -> Result<(T, acyclic_test_support::AllocationInfo, usize, usize), Box<dyn std::error::Error>>
    {
        use crate::changes::{take_marker_probes, take_parsed_bytes};
        take_marker_probes();
        take_parsed_bytes();
        let mut result = None;
        let allocations = acyclic_test_support::measure(|| result = Some(run()));
        Ok((
            result.ok_or("measure did not run")?,
            allocations,
            take_marker_probes(),
            take_parsed_bytes(),
        ))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn check_filters(
        filters: &[FollowFilter],
        record: &crate::Record,
        valid: bool,
        format: &Result<Option<&[u8]>, StreamError>,
        probes: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (index, filter) in filters.iter().enumerate() {
            let (matched, allocations, marker_probes, parsed_bytes) =
                measure(|| filter.matches(&record.headers))?;
            assert_eq!(allocations, acyclic_test_support::AllocationInfo::default());
            assert_eq!(marker_probes, probes, "classify the marker once");
            assert_eq!(parsed_bytes, 0, "header matching must not decode the body");
            let (preservation, allocations, marker_probes, parsed_bytes) =
                measure(|| filter.preserves(record))?;
            if valid {
                assert_eq!(allocations, acyclic_test_support::AllocationInfo::default());
            }
            assert_eq!(marker_probes, probes, "classify the marker once");
            let validation_bytes = if format.as_ref().is_ok_and(|format| format.is_some()) {
                record.value.len()
            } else {
                0
            };
            assert_eq!(
                parsed_bytes, validation_bytes,
                "validate the original body once"
            );
            let (projected, allocations, marker_probes, parsed_bytes) =
                measure(|| filter.project(record.clone()))?;
            if valid {
                let projected = projected.as_ref().map_err(|error| error.clone())?;
                let projected = projected.as_ref();
                assert_eq!(matched, projected.is_some());
                assert_eq!(
                    preservation
                        .as_ref()
                        .map(|value| *value)
                        .map_err(|error| error.category()),
                    Ok(projected == Some(record))
                );
                if let Some(projected) = projected {
                    assert_eq!(
                        filter
                            .preserves(projected)
                            .map_err(|error| error.category()),
                        Ok(true)
                    );
                    assert_eq!(projected.sequence, record.sequence);
                    assert_eq!(projected.commit_id, record.commit_id);
                    assert_eq!(projected.committed_at_micros, record.committed_at_micros);
                    assert_eq!(projected.origin, record.origin);
                    assert_eq!(projected.headers.attributes(), record.headers.attributes());
                }
            } else {
                assert_eq!(
                    preservation.as_ref().map_err(|error| error.category()),
                    Err(crate::StreamCategory::InvalidArgument)
                );
                assert_eq!(
                    projected.as_ref().map_err(|error| error.category()),
                    Err(crate::StreamCategory::InvalidArgument)
                );
            }
            if !valid
                || index == 0
                || preservation.as_ref().is_ok_and(|value| *value)
                || format.as_ref().is_ok_and(Option::is_none)
                || !matched
            {
                if valid {
                    assert_eq!(
                        allocations,
                        acyclic_test_support::AllocationInfo::default(),
                        "unrestricted projection must transfer the original owners"
                    );
                }
                assert_eq!(marker_probes, probes, "classify the marker once");
                assert_eq!(
                    parsed_bytes, validation_bytes,
                    "validate the original body once"
                );
            } else {
                let output = projected
                    .as_ref()
                    .map_err(|error| error.clone())?
                    .as_ref()
                    .ok_or("partial projection was not retained")?;
                assert_eq!(marker_probes, 2 * probes);
                assert_eq!(
                    parsed_bytes,
                    2 * record.value.len() + output.value.len(),
                    "partial projection must reuse input admission"
                );
            }
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn projection_reuses_validation_and_skips_unrestricted_decoding()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::changes::{take_marker_probes, validate};

        let followed = crate::StreamPath::new("followed")?;
        let golden = acyclic_compatibility_golden::frame("stream.changes-v1")?;
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
        #[cfg(feature = "test-support")]
        for (name, bytes) in futures::executor::block_on(acyclic_stream::codec_fuzz::seeds())? {
            assert!(
                acyclic_stream::codec_fuzz::check(&bytes),
                "projection fuzz oracle: {name}"
            );
        }
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
                    projection_key: None,
                }],
                ..Default::default()
            })?,
            FollowFilter::new(wire::SessionFilter {
                keys: vec![range(b"a", b"b")],
                ..Default::default()
            })?,
            FollowFilter::new(wire::SessionFilter {
                keys: vec![range(b"a", b"zz")],
                all_of: vec![wire::SessionFilterClause {
                    all_keys: true,
                    keys: vec![range(b"b", b"zz")],
                    attributes: vec![],
                    projection_key: None,
                }],
                ..Default::default()
            })?,
        ];
        for marker in [
            None,
            Some(b"changes/v1".as_slice()),
            Some(acyclic_stream_wire::changes::WRITER_FORMAT),
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
                    if marker == Some(acyclic_stream_wire::changes::WRITER_FORMAT) {
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
                    if valid {
                        projection_contract(&record)?;
                    }
                    check_filters(&filters, &record, valid, &format, probes)?;
                }
            }
        }
        // Grow only the discarded entry through the actual current writer.
        // Projected content and physical heap work must remain identical.
        let mut baseline = None;
        for discarded in [1, 4096] {
            let body = crate::AppendRecord::changes(
                b"publication",
                &[("b", b'b', 1), ("z", b'z', discarded)].map(|(key, byte, length)| {
                    wire::ChangeEntry {
                        key: key.into(),
                        operation: "write".into(),
                        metadata: vec![byte; length].into(),
                    }
                }),
                vec![],
            )?;
            let mut expected = crate::changes::decode_changes(&body.value)?;
            expected.entries.truncate(1);
            let expected =
                crate::AppendRecord::changes(&expected.context, &expected.entries, vec![])?;
            let mut record = retained.clone();
            record.value = body.value;
            record.headers = body.headers;
            let (projected, allocations, _, parsed_bytes) =
                measure(|| filters[1].project(record.clone()))?;
            let projected = projected?.unwrap();
            assert_eq!(projected.value, expected.value);
            assert_eq!(projected.headers, expected.headers);
            assert_eq!(
                parsed_bytes,
                2 * record.value.len() + projected.value.len(),
                "partial projection must reuse input admission"
            );
            let observed = (projected, allocations);
            if let Some(baseline) = &baseline {
                assert_eq!(
                    &observed, baseline,
                    "discarded change payload must not be copied"
                );
            }
            baseline = Some(observed);
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
    #[tokio::test]
    async fn provider_hops_preserve_requested_any_keys_and_all_authority_ceilings() {
        use acyclic_auth::{contract as auth, testing as signed};
        use futures::StreamExt as _;

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
                projection_key: None,
            }],
        };
        let requested_intersection = FollowFilter::new(requested.clone()).unwrap();
        let authorized = FollowFilter::authorized(requested, &capability).unwrap();
        let reversed = FollowFilter(Some(Arc::new(
            authorized.clauses().iter().rev().cloned().collect(),
        )));
        let single = FollowFilter(Some(Arc::new(vec![authorized.clauses()[0].clone()])));
        let projected = authorized.including_projection_key(b"a").unwrap();
        let other_projection = authorized.including_projection_key(b"b").unwrap();
        for filter in [
            &authorized,
            &reversed,
            &single,
            &projected,
            &other_projection,
            &FollowFilter::default(),
        ] {
            // Independent v1 framing oracle: preserve the public digest format.
            let mut hash = Sha256::new();
            hash.update(b"acyclic.stream.follow-filter.v1\0");
            hash.update((filter.clauses().len() as u64).to_le_bytes());
            for clause in filter.clauses() {
                hash.update([u8::from(clause.all_keys)]);
                hash.update([u8::from(clause.projection.is_some())]);
                if let Some(range) = &clause.projection {
                    hash.update((range.start.len() as u64).to_le_bytes());
                    hash.update(&range.start);
                }
                let bytes = clause.filter.encode_to_vec();
                hash.update((bytes.len() as u64).to_le_bytes());
                hash.update(bytes);
            }
            assert_eq!(filter.digest(), <[u8; 32]>::from(hash.finalize()));
        }
        assert_ne!(authorized.digest(), projected.digest());
        assert_ne!(projected.digest(), other_projection.digest());
        assert_eq!(
            projected.digest(),
            FollowFilter::new(projected.transport_filter().unwrap())
                .unwrap()
                .digest()
        );
        #[cfg(not(target_arch = "wasm32"))]
        {
            let empty = acyclic_test_support::measure(|| {
                std::hint::black_box(FollowFilter::default().digest());
            });
            assert_eq!(
                empty.count_total, 0,
                "empty digest allocated scratch storage"
            );
            let baseline = acyclic_test_support::measure(|| {
                std::hint::black_box(single.digest());
            });
            assert!(
                baseline.count_total <= 1
                    && baseline.bytes_total <= single.clauses()[0].filter.encoded_len() as u64,
                "single-clause digest allocated more than its encoded scratch storage"
            );
            for count in [2, 4, 16] {
                let repeated =
                    FollowFilter(Some(Arc::new(vec![single.clauses()[0].clone(); count])));
                let work = acyclic_test_support::measure(|| {
                    std::hint::black_box(repeated.digest());
                });
                assert_eq!(
                    (work.count_total, work.bytes_total),
                    (baseline.count_total, baseline.bytes_total),
                    "digest scratch storage grew with repeated clauses"
                );
            }
        }
        let forwarded = FollowFilter::new(authorized.transport_filter().unwrap()).unwrap();
        let ceiling = FollowFilter::authorized(Default::default(), &capability).unwrap();
        let intersection = requested_intersection.intersection(&ceiling);
        assert_eq!(intersection, authorized);
        assert_eq!(
            intersection.intersection(&FollowFilter::default()),
            authorized
        );
        assert_eq!(
            FollowFilter::default().intersection(&intersection),
            authorized
        );
        assert_eq!(authorized.digest(), forwarded.digest());
        assert!(forwarded.matches(&headers(&["b", "j"], "alice")));
        assert!(!forwarded.matches(&headers(&["b", "z"], "alice")));
        assert!(!forwarded.matches(&headers(&["a", "b"], "alice")));
        assert!(!forwarded.matches(&headers(&["j"], "alice")));
        assert!(!forwarded.matches(&headers(&["b"], "bob")));
        assert!(
            requested_intersection.matches(&headers(&["b", "z"], "alice")),
            "negative control needs the authority ceiling"
        );

        // Capture opaque and typed records from the real provider, then compare
        // projection and refusal across the transport hop.
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
            let expected = authorized.project(record.clone()).unwrap();
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
                    authorized
                        .project(malformed.clone())
                        .map_err(|error| error.category()),
                    Err(crate::StreamCategory::InvalidArgument)
                );
                assert_eq!(
                    forwarded
                        .project(malformed.clone())
                        .map_err(|error| error.category()),
                    authorized
                        .project(malformed)
                        .map_err(|error| error.category())
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
            FollowFilter::authorized(malformed.clone(), &capability)
                .map_err(|error| error.category()),
            FollowFilter::new(malformed).map_err(|error| error.category())
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
