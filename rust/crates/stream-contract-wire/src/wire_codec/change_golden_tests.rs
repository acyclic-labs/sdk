//! The retained `stream.changes-v1` golden through the wire record codec.
use acyclic_stream_contract_core::changes::{
    decode, decode_changes, reader_format, take_marker_probes, take_parsed_bytes, validate,
};
use acyclic_stream_contract_core::{
    AppendRecord, Attribute, FollowFilter, RecordHeaders, StreamCategory,
};
use acyclic_stream_wire::changes::{RECORD_FORMAT_ATTRIBUTE as ATTRIBUTE, WRITER_FORMAT as FORMAT};
use acyclic_stream_wire::wire;
use prost::Message as _;

fn filter() -> FollowFilter {
    use acyclic_auth::{contract::*, testing as signed};
    let token = signed::root(|block| {
        block.caveats.push(Caveat {
            kind: CaveatKind::Filter(Filter {
                keys: vec![KeyRange {
                    start: b"b".to_vec(),
                    end: b"c".to_vec(),
                }],
                attributes: vec![],
            }),
        });
    });
    let capability =
        acyclic_auth::verify::verify(&token, &signed::Snapshot::default(), signed::NOW).unwrap();
    FollowFilter::authorized(Default::default(), &capability).unwrap()
}

fn changes() -> AppendRecord {
    AppendRecord::changes(
        b"publication-identity",
        &["b", "z"]
            .into_iter()
            .map(|key| wire::ChangeEntry {
                key: key.into(),
                operation: "write".into(),
                metadata: key.into(),
            })
            .collect::<Vec<_>>(),
        vec![],
    )
    .unwrap()
}

fn qualify_marker(
    headers: &RecordHeaders,
    filter: &FollowFilter,
    expected: &Result<bool, StreamCategory>,
    probes: usize,
    value: &[u8],
    decoded: &wire::ChangeRecord,
) {
    take_marker_probes();
    assert_eq!(
        reader_format(headers)
            .map(|format| format.is_some())
            .map_err(|error| error.category()),
        expected.clone()
    );
    let after = take_marker_probes();
    assert_eq!(
        after, probes,
        "reserved marker classification repeated an attribute probe"
    );
    assert_eq!(filter.matches(headers), *expected == Ok(true));
    assert_eq!(take_marker_probes(), after);
    assert_eq!(FollowFilter::default().matches(headers), expected.is_ok());
    assert_eq!(take_marker_probes(), after);
    assert_eq!(
        validate(value, headers).map_err(|error| error.category()),
        expected.map(|_| ())
    );
    assert_eq!(take_marker_probes(), after);
    assert_eq!(
        decode(value, headers).map_err(|error| error.category()),
        expected.map(|_| decoded.clone())
    );
    assert_eq!(take_marker_probes(), after);

    assert_eq!(
        decode(&[], headers).map_err(|error| error.category()),
        Err(StreamCategory::InvalidArgument)
    );
    assert_eq!(take_marker_probes(), after);
    assert_eq!(
        validate(&[], headers).map_err(|error| error.category()),
        if *expected == Ok(false) {
            Ok(())
        } else {
            Err(StreamCategory::InvalidArgument)
        }
    );
    assert_eq!(take_marker_probes(), after);

    // A valid body alone cannot establish its immutable index. Bypassing
    // validation would accept the same decoded value under a forged key.
    let forged = RecordHeaders::new(
        vec!["a".into(), "z".into()],
        headers
            .attributes()
            .iter()
            .map(acyclic_stream_contract_core::RecordAttribute::to_owned)
            .collect(),
    )
    .unwrap();
    assert_eq!(
        decode(value, &forged).map_err(|error| error.category()),
        if *expected == Ok(false) {
            Ok(decoded.clone())
        } else {
            Err(StreamCategory::InvalidArgument)
        }
    );
    assert_eq!(take_marker_probes(), after);
}

#[test]
fn retained_and_unknown_change_versions_keep_identity_and_one_marker_pass()
-> Result<(), Box<dyn std::error::Error>> {
    let golden = acyclic_compatibility_golden::frame("stream.changes-v1")?;
    let old = wire::Record::decode(golden.as_slice())?;
    let current = changes();
    let retained_value = wire::ChangeRecord::decode(old.value.clone())?;
    let current_value = decode_changes(&current.value)?;
    let followed = acyclic_stream_contract_core::StreamPath::new("followed")?;
    let filter = filter();
    let retained = super::record(old.clone(), Some(&followed))?;
    assert_eq!(
        super::record_wire(retained.clone(), Some(&followed)).encode_to_vec(),
        golden
    );
    assert_eq!(
        decode(&retained.value, &retained.headers)?,
        wire::ChangeRecord::decode(old.value.clone())?
    );
    let projected = filter
        .project(retained.clone())?
        .ok_or("missing retained projection")?;
    assert_eq!(projected.headers.keys().len(), 1);
    assert_eq!(projected.headers.keys()[0].as_ref(), b"b");
    assert_eq!(
        projected.headers.attributes(),
        retained.headers.attributes()
    );
    assert_eq!(projected.sequence, retained.sequence);
    assert_eq!(projected.commit_id, retained.commit_id);
    assert_eq!(projected.committed_at_micros, retained.committed_at_micros);
    assert_eq!(projected.origin, retained.origin);
    assert_eq!(
        wire::ChangeRecord::decode(projected.value.as_ref())?
            .entries
            .len(),
        1
    );
    assert_eq!(
        FollowFilter::default().project(retained.clone())?,
        Some(retained.clone())
    );
    assert_ne!(
        projected.value, retained.value,
        "omitting projection exposes the outside key"
    );

    for (value, expected) in [
        (None, Ok(false)),
        (Some(acyclic_stream_wire::changes::V1), Ok(true)),
        (Some(FORMAT), Ok(true)),
        (
            Some(b"changes/v0".as_slice()),
            Err(StreamCategory::InvalidArgument),
        ),
        (
            Some(b"changes/v3".as_slice()),
            Err(StreamCategory::InvalidArgument),
        ),
        (
            Some(b"unrecognized".as_slice()),
            Err(StreamCategory::InvalidArgument),
        ),
    ] {
        // The marker can be first or last, and absent headers require a full pass.
        for (padding, prefix) in [(0, "z"), (15, "z"), (15, "a")] {
            let mut attributes = (0..padding)
                .map(|index| Attribute {
                    name: format!("{prefix}{index:02}"),
                    value: "other".into(),
                })
                .collect::<Vec<_>>();
            if let Some(value) = value {
                attributes.push(Attribute {
                    name: ATTRIBUTE.into(),
                    value: value.into(),
                });
            }
            attributes.sort_by(|left, right| left.name.cmp(&right.name));
            let headers = RecordHeaders::new(retained.headers.keys().to_vec(), attributes)?;
            let (body, decoded) = if value == Some(FORMAT) {
                (current.value.as_ref(), &current_value)
            } else {
                (old.value.as_ref(), &retained_value)
            };
            let probes = if value.is_some() && prefix == "z" {
                1
            } else {
                headers.attributes().len()
            };
            qualify_marker(&headers, &filter, &expected, probes, body, decoded);
            #[cfg(not(target_arch = "wasm32"))]
            {
                let observed = std::sync::Arc::new(
                    acyclic_stream_contract_bytes::test_meter::Meter::default(),
                );
                let prepared =
                    acyclic_stream_contract_core::record_headers::PreparedHeaders::new_shape(
                        &observed.original(),
                        headers.payload_bytes(),
                        headers.keys().len(),
                        headers.attributes().len(),
                    )?
                    .finish(&headers)?;
                observed
                    .deny_allocation
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                observed
                    .deny_codec
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                qualify_marker(&prepared, &filter, &expected, probes, body, decoded);
                drop(prepared);
                observed.assert_refunded();
            }
        }
        if expected.is_err() {
            let mut refused = old.clone();
            refused
                .attributes
                .first_mut()
                .ok_or("missing format marker")?
                .value = value.ok_or("missing refusal marker")?.into();
            for value in [old.value.clone(), bytes::Bytes::new()] {
                refused.value = value;
                take_parsed_bytes();
                assert_eq!(
                    super::record(refused.clone(), Some(&followed))
                        .map_err(|error| error.category()),
                    Err(StreamCategory::InvalidArgument)
                );
                assert_eq!(
                    take_parsed_bytes(),
                    0,
                    "unknown markers must refuse before parsing"
                );
            }
        }
    }
    assert_eq!(
        super::append_record(super::append_record_wire(current.clone()))?,
        current
    );
    let mut malformed = retained;
    malformed.value = bytes::Bytes::new().into();
    assert_eq!(
        filter.project(malformed).map_err(|error| error.category()),
        Err(StreamCategory::InvalidArgument)
    );
    Ok(())
}
