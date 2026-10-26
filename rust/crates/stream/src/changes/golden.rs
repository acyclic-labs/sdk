use super::*;
use crate::{FollowFilter, Record, wire_codec};
use prost::Message;

// Frozen v1 protobuf frame: the producer in 2344393e026e78807e85edff8a87c880d31d7f7c
// encoded ChangeRecord with prost, retained both indexed keys, and used the
// changes/v1 attribute. The test also checks that exact producer representation.
const V1_FRAME: &[u8] = b"\x08\x04\x12\x23\x0a\x03pub\x12\x0d\x0a\x01b\x12\x05write\x1a\x01B\x12\x0d\x0a\x01z\x12\x05write\x1a\x01Z\x1a\x20\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x20\x2a\x2a\x01b\x2a\x01z\x32\x1c\x0a\x0eacyclic.record\x12\x0achanges/v1";

#[test]
fn retained_v1_golden_frame_and_unknown_marker_control() {
    let wire = wire::Record::decode(V1_FRAME).unwrap();
    let old_value = wire::ChangeRecord {
        context: b"pub".as_slice().into(),
        entries: [(b"b", b"B"), (b"z", b"Z")]
            .into_iter()
            .map(|(key, metadata)| wire::ChangeEntry {
                key: key.as_slice().into(),
                operation: "write".into(),
                metadata: metadata.as_slice().into(),
            })
            .collect(),
    };
    assert_eq!(old_value.encode_to_vec(), wire.value);
    assert_eq!(wire.encode_to_vec(), V1_FRAME);
    let record = wire_codec::record(wire.clone()).unwrap();
    assert_eq!(decode(&record.value, &record.headers).unwrap(), old_value);
    assert_eq!(
        wire_codec::record_wire(record.clone()).encode_to_vec(),
        V1_FRAME
    );
    let body = AppendRecord {
        value: record.value.clone(),
        headers: record.headers.clone(),
    };
    let request = crate::AppendRequest {
        path: crate::StreamPath::new("retained").unwrap(),
        records: vec![body.clone()],
        if_tail: Some(4),
        idempotency_key: None,
    };
    let digest = crate::request::append_digest(&request).unwrap();
    let reopened = wire_codec::append_record(wire_codec::append_record_wire(body)).unwrap();
    assert_eq!(
        crate::request::append_digest(&crate::AppendRequest {
            records: vec![reopened],
            ..request
        })
        .unwrap(),
        digest
    );
    assert!(is_changes(&record.headers));
    assert_eq!(
        FollowFilter::default().project(record.clone()).unwrap(),
        Some(record.clone())
    );
    let filter = FollowFilter::new(wire::SessionFilter {
        keys: vec![wire::KeyRange {
            start: b"b".as_slice().into(),
            end: b"c".as_slice().into(),
        }],
        ..Default::default()
    })
    .unwrap();
    let projected = filter.project(record.clone()).unwrap().unwrap();
    assert_eq!(projected.commit_id, record.commit_id);
    assert_eq!(projected.sequence, record.sequence);
    assert_eq!(projected.committed_at_micros, record.committed_at_micros);
    assert_eq!(
        decode(&projected.value, &projected.headers)
            .unwrap()
            .entries,
        old_value.entries[..1]
    );
    assert_eq!(
        wire_codec::record_wire(projected).attributes,
        wire.attributes
    );
    // The signed authority ceiling also projects the prior typed format.
    let authorized = super::tests::filter()
        .project(record.clone())
        .unwrap()
        .unwrap();
    assert_eq!(
        decode(&authorized.value, &authorized.headers)
            .unwrap()
            .entries
            .len(),
        1
    );

    // A reserved unknown marker must never become an opaque, unprojected value,
    // even if an in-process caller constructs headers without wire validation.
    for marker in [b"changes/v0".as_slice(), b"changes/v3", b"unrecognized"] {
        let mut unknown = wire.clone();
        unknown.attributes.first_mut().unwrap().value = marker.to_vec().into();
        assert_eq!(
            wire_codec::record(unknown.clone()),
            Err(StreamError::InvalidArgument)
        );
        let unknown_record = Record {
            headers: RecordHeaders::new(
                record.headers.keys().to_vec(),
                vec![Attribute {
                    name: ATTRIBUTE.into(),
                    value: marker.to_vec().into(),
                }],
            )
            .unwrap(),
            ..record.clone()
        };
        assert!(!FollowFilter::default().matches(&unknown_record.headers));
        assert_eq!(
            FollowFilter::default().project(unknown_record),
            Err(StreamError::InvalidArgument)
        );
    }
    // Both formats remain typed; a mismatched v1 index cannot enter a provider.
    let mut forged = wire.clone();
    forged
        .keys
        .first_mut()
        .unwrap()
        .clone_from(&b"a".as_slice().into());
    assert_eq!(
        wire_codec::record(forged),
        Err(StreamError::InvalidArgument)
    );
    for value in [
        b"".as_slice(),
        b"\x0a\x00",
        b"\x0a\x83\x00pub",
        b"\x12\x03\x12\x01\n",
    ] {
        let mut malformed = wire.clone();
        malformed.value = value.to_vec().into();
        assert_eq!(
            wire_codec::record(malformed),
            Err(StreamError::InvalidArgument)
        );
    }
    let v2 = super::tests::changes();
    assert!(is_changes(&v2.headers));
    assert_eq!(
        wire_codec::append_record(wire_codec::append_record_wire(v2.clone())).unwrap(),
        v2
    );
    assert!(READER_FORMATS.contains(&V1));
    assert!(READER_FORMATS.contains(&WRITER_FORMAT));
    let old_voter_formats: &[&[u8]] = &[V1];
    assert!(
        ![READER_FORMATS, old_voter_formats]
            .iter()
            .all(|formats| formats.contains(&WRITER_FORMAT)),
        "one old applied voter must hold the new writer"
    );
}
