use super::*;
use acyclic_stream::{
    AppendRecord, AppendRequest, Attribute, IdempotencyKey, MemoryStream, OwnedBytes, ReadRequest,
    RecordHeaders, StreamProvider, StreamProviderExt as _, session::Frame, wire_codec,
};
use futures::StreamExt as _;
use std::cell::Cell;

thread_local! {
    static METADATA_VISITS: Cell<usize> = const { Cell::new(0) };
}

pub(super) fn visit_metadata() {
    METADATA_VISITS.set(METADATA_VISITS.get() + 1);
}

fn view(path: &StreamPath, bytes: usize) -> StreamView {
    StreamView::new(
        StreamViewPolicy::new(1, bytes).unwrap(),
        0,
        0,
        false,
        Some(path.clone()),
        BackingPool::default(),
    )
}

#[tokio::test]
async fn metadata_retention_counts_one_pass_and_refuses_uncharged_backing()
-> Result<(), Box<dyn std::error::Error>> {
    let memory = MemoryStream::default();
    let huge = OwnedBytes::from(vec![b'z'; 64 * 1024]);
    for count in [0, 1, 4, 16] {
        let path = StreamPath::new(format!("metadata/{count}"))?;
        memory
            .append(AppendRequest {
                path: path.clone(),
                records: vec![AppendRecord {
                    value: OwnedBytes::from(vec![b'v'; 32]),
                    headers: RecordHeaders::new(
                        (0..count)
                            .map(|index| {
                                if index == count - 1 {
                                    huge.slice(..3)
                                } else {
                                    OwnedBytes::from(format!("k{index:04}").into_bytes())
                                }
                            })
                            .collect(),
                        (0..count)
                            .map(|index| Attribute {
                                name: format!("a{index:04}"),
                                value: if index == count - 1 {
                                    huge.slice(..7)
                                } else {
                                    OwnedBytes::from(vec![b'a'; 8])
                                },
                            })
                            .collect(),
                    )?,
                }],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        let mut records = memory
            .read(ReadRequest {
                path: path.clone(),
                from: 0,
                limit: 1,
                key: None,
                at_least: Default::default(),
            })
            .await?;
        let original = records.next().await.ok_or(StreamError::unavailable())??;
        assert!(
            records.next().await.is_none(),
            "finite producer did not end"
        );
        let mut record = wire_codec::record_wire(original.clone(), Some(&path));
        let (native, _) = retain_metadata(&record, Some(&original), 64 * 1024)?;
        for (index, key) in native.keys.iter().enumerate() {
            assert_eq!(
                key.as_ptr() == record.keys[index].as_ptr(),
                original.headers.keys()[index].retained_capacity() == Some(key.len()),
                "native key sharing disagrees with its charged backing"
            );
            let value = &native.attributes[index].value;
            assert_eq!(
                value.as_ptr() == record.attributes[index].value.as_ptr(),
                original
                    .headers
                    .attributes()
                    .get(index)
                    .unwrap()
                    .value
                    .retained_capacity()
                    == Some(value.len()),
                "native attribute sharing disagrees with its charged backing"
            );
        }
        // The provider may coallocate headers. Rehouse the same admitted facts
        // through their canonical constructor to exercise exact owners too.
        let mut exact_owner = original.clone();
        exact_owner.headers = RecordHeaders::new(
            original
                .headers
                .keys()
                .iter()
                .map(|key| OwnedBytes::copy_from_slice(key.as_slice()))
                .collect(),
            original
                .headers
                .attributes()
                .iter()
                .map(|attribute| Attribute {
                    name: attribute.name.to_owned(),
                    value: OwnedBytes::copy_from_slice(attribute.value.as_slice()),
                })
                .collect(),
        )?;
        let exact_wire = wire_codec::record_wire(exact_owner.clone(), Some(&path));
        let (shared, _) = retain_metadata(&exact_wire, Some(&exact_owner), 64 * 1024)?;
        for (index, key) in shared.keys.iter().enumerate() {
            assert_eq!(
                key.as_ptr(),
                exact_wire.keys[index].as_ptr(),
                "full native key was copied"
            );
            assert_eq!(
                shared.attributes[index].value.as_ptr(),
                exact_wire.attributes[index].value.as_ptr(),
                "full native attribute was copied"
            );
        }
        // A transport may reserve far more metadata capacity than it sends.
        // Only the admitted output's allocations may enter its byte charge.
        record.keys.reserve(4096);
        record.attributes.reserve(4096);
        record.origin.reserve(64 * 1024);
        for attribute in &mut record.attributes {
            attribute.name.reserve(64 * 1024);
        }
        let frame: Frame = wire::SessionFrame {
            body: Some(wire::session_frame::Body::Data(wire::SessionData {
                item: Some(wire::session_data::Item::Record(record)),
                after: Some(wire::SessionCursor {
                    next: 1,
                    ..Default::default()
                }),
                accepted: 1,
                durable: 1,
            })),
            ..Default::default()
        }
        .into();
        let Some(wire::session_frame::Body::Data(data)) = &frame.wire().body else {
            unreachable!()
        };
        let Some(wire::session_data::Item::Record(record)) = &data.item else {
            unreachable!()
        };
        let mut retained = view(&path, 64 * 1024);
        METADATA_VISITS.set(0);
        retained.apply(&frame, 1)?;
        assert_eq!(
            METADATA_VISITS.get(),
            count * 2,
            "metadata visited more than once"
        );
        let output = retained.record(0).ok_or(StreamError::unavailable())?;
        assert_eq!(output, record);
        let expected = size_of::<RecordEntry>()
            + output.value.len()
            + output.commit_id.len()
            + output.origin.capacity()
            + output.keys.capacity() * size_of::<Bytes>()
            + output.attributes.capacity() * size_of::<wire::Attribute>()
            + output.keys.iter().map(Bytes::len).sum::<usize>()
            + output
                .attributes
                .iter()
                .map(|attribute| attribute.name.capacity() + attribute.value.len())
                .sum::<usize>();
        assert_eq!(
            retained.retained_bytes(),
            expected,
            "retained metadata escaped credit"
        );
        for (index, key) in output.keys.iter().enumerate() {
            assert_ne!(
                key.as_ptr(),
                record.keys[index].as_ptr(),
                "unknown key backing was retained"
            );
        }
        let mut exact = view(&path, expected);
        exact.apply(&frame, 1)?;
        let mut short = view(&path, expected - 1);
        assert!(matches!(
            short.apply(&frame, 1),
            Err(StreamError::LimitExceeded)
        ));
        assert_eq!(
            (
                short.next_sequence(),
                short.retained_bytes(),
                short.records().len()
            ),
            (0, 0, 0)
        );
        let mut corrupt = frame.into_wire();
        let Some(wire::session_frame::Body::Data(data)) = &mut corrupt.body else {
            unreachable!()
        };
        let Some(wire::session_data::Item::Record(record)) = &mut data.item else {
            unreachable!()
        };
        record.commit_id = Bytes::from_static(&[0; 31]);
        let mut refused = view(&path, expected);
        assert!(
            refused.apply(&corrupt.into(), 1).is_err(),
            "malicious external frame bypassed canonical validation"
        );
        assert_eq!((refused.next_sequence(), refused.retained_bytes()), (0, 0));
        eprintln!(
            "headers={} metadata_visits={} retained_bytes={expected}",
            count * 2,
            count * 2
        );
    }
    Ok(())
}
