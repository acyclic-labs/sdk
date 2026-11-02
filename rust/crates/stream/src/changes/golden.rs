use super::*;
use crate::{FollowFilter, wire_codec};
use prost::Message as _;

fn qualify_marker(
    headers: &RecordHeaders,
    filter: &FollowFilter,
    expected: Result<bool, StreamError>,
) {
    // Original two-pass classification, independent of the shared lookup.
    let mut before = 0;
    let unknown = headers
        .attributes()
        .iter()
        .inspect(|_| before += 1)
        .any(|attribute| {
            attribute.name == ATTRIBUTE && !READER_FORMATS.contains(&attribute.value.as_ref())
        });
    let original = if unknown {
        Err(StreamError::InvalidArgument)
    } else {
        Ok(headers
            .attributes()
            .iter()
            .inspect(|_| before += 1)
            .any(|attribute| {
                attribute.name == ATTRIBUTE && READER_FORMATS.contains(&attribute.value.as_ref())
            }))
    };
    assert_eq!(original, expected);
    MARKER_PROBES.with(|count| count.set(0));
    assert_eq!(marked(headers), original);
    let after = MARKER_PROBES.with(|count| count.replace(0));
    assert!(after <= before);
    if expected.is_ok() && !headers.attributes().is_empty() {
        assert!(
            after < before,
            "the original repeated lookup exceeds this bound"
        );
    }
    assert_eq!(filter.matches(headers), expected == Ok(true));
    assert_eq!(MARKER_PROBES.with(|count| count.replace(0)), after);
    assert_eq!(FollowFilter::default().matches(headers), expected.is_ok());
    eprintln!(
        "marker {:?}, {} attributes: {before} -> {after} name probes",
        expected,
        headers.attributes().len()
    );
}

#[test]
fn retained_and_unknown_change_versions_keep_identity_and_one_marker_pass()
-> Result<(), Box<dyn std::error::Error>> {
    let golden = acyclic_proto_build::golden::frame("stream.changes-v1")?;
    let old = wire::Record::decode(golden.as_slice())?;
    let followed = crate::StreamPath::new("followed")?;
    let filter = super::tests::filter();
    let retained = wire_codec::record(old.clone(), Some(&followed))?;
    assert_eq!(
        wire_codec::record_wire(retained.clone(), Some(&followed)).encode_to_vec(),
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
        (Some(V1), Ok(true)),
        (Some(FORMAT), Ok(true)),
        (
            Some(b"changes/v0".as_slice()),
            Err(StreamError::InvalidArgument),
        ),
        (
            Some(b"changes/v3".as_slice()),
            Err(StreamError::InvalidArgument),
        ),
        (
            Some(b"unrecognized".as_slice()),
            Err(StreamError::InvalidArgument),
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
            qualify_marker(&headers, &filter, expected.clone());
            #[cfg(not(target_arch = "wasm32"))]
            {
                let observed = std::sync::Arc::new(crate::test_meter::Meter::default());
                let prepared = crate::record_headers::PreparedHeaders::new(
                    &observed.original(),
                    headers.payload_bytes(),
                )?
                .finish(&headers)?;
                observed
                    .deny_allocation
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                observed
                    .deny_codec
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                qualify_marker(&prepared, &filter, expected.clone());
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
                    wire_codec::record(refused.clone(), Some(&followed)),
                    Err(StreamError::InvalidArgument)
                );
                assert_eq!(
                    take_parsed_bytes(),
                    0,
                    "unknown markers must refuse before parsing"
                );
            }
        }
    }
    let current = super::tests::changes();
    assert_eq!(
        wire_codec::append_record(wire_codec::append_record_wire(current.clone()))?,
        current
    );
    let mut malformed = retained;
    malformed.value = bytes::Bytes::new().into();
    assert_eq!(filter.project(malformed), Err(StreamError::InvalidArgument));
    Ok(())
}
