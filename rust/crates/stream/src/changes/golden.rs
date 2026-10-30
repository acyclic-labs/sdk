use super::*;
use crate::{FollowFilter, Record, wire_codec};
use prost::Message as _;

#[test]
fn old_and_unknown_change_versions_refuse_before_the_value()
-> Result<(), Box<dyn std::error::Error>> {
    let golden = acyclic_proto_build::golden::frame("stream.changes-v1")?;
    let old = wire::Record::decode(golden.as_slice())?;
    let followed = crate::StreamPath::new("followed")?;
    assert_eq!(old.encode_to_vec(), golden);
    for marker in [
        b"changes/v0".as_slice(),
        b"changes/v1",
        b"changes/v3",
        b"unrecognized",
    ] {
        let mut refused = old.clone();
        refused
            .attributes
            .first_mut()
            .ok_or("missing format marker")?
            .value = marker.to_vec().into();
        // Refuse before interpreting either an intact or missing body.
        for value in [old.value.clone(), bytes::Bytes::new()] {
            refused.value = value;
            assert_eq!(
                wire_codec::record(refused.clone(), Some(&followed)),
                Err(StreamError::Unsupported)
            );
        }
        let record = Record {
            sequence: old.sequence,
            commit_id: crate::CommitId::from_bytes([1; 32]),
            committed_at_micros: old.committed_at_micros,
            origin: followed.clone(),
            value: old.value.clone().into(),
            headers: RecordHeaders::new(
                vec![],
                vec![Attribute {
                    name: ATTRIBUTE.into(),
                    value: marker.to_vec().into(),
                }],
            )?,
        };
        assert!(!FollowFilter::default().matches(&record.headers));
        assert_eq!(
            FollowFilter::default().project(record),
            Err(StreamError::Unsupported)
        );
    }
    let current = super::tests::changes();
    assert_eq!(
        wire_codec::append_record(wire_codec::append_record_wire(current.clone()))?,
        current
    );
    assert!(!READER_FORMATS.contains(&b"changes/v1".as_slice()));
    Ok(())
}
