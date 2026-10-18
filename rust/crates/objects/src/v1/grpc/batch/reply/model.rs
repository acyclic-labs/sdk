use super::*;
fn history(early: bool) {
    let counts = std::sync::Arc::new(crate::test_meter::CountingMeter::default());
    let meter: std::sync::Arc<dyn NativeReadMeter> = counts;
    let mut sizes = batch_vector(1, Some(&meter)).unwrap();
    sizes.value.push(7);
    let received = received(Some(&meter), sizes, Completion::new(Some(&meter)).unwrap()).unwrap();
    let writer = {
        let meter = meter.clone();
        let received = received.clone();
        loom::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                if early {
                    received.value.completed.sent();
                }
                let mut header = wire::PutObjectsHeader {
                    objects: vec![wire::PutObjectHeader {
                        metadata: Some(wire::ObjectMetadata {
                            user: [("key".into(), "value".into())].into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                };
                received
                    .value
                    .keep_metadata(&mut header, Some(&meter))
                    .unwrap();
                assert!(header.objects[0].metadata.is_none());
                received.value.completed.sent();
            })
            .unwrap()
    };
    let reader = {
        let received = received.clone();
        loom::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                if received.value.completed.validate().is_err() {
                    return;
                }
                {
                    let metadata = received.value.metadata.lock().unwrap();
                    assert_eq!(
                        metadata.value.len(),
                        1,
                        "completion published before metadata transfer"
                    );
                    assert_eq!(metadata.value[0].as_ref().unwrap().user["key"], "value");
                }
                let response = batch_vector(1, Some(&meter)).unwrap();
                let mut decoder = Decoder {
                    prepared: Some(Reply {
                        response: Guarded {
                            value: wire::PutObjectsResponse {
                                results: response.value,
                            },
                            _allocation: response._allocation,
                        },
                        failure: None,
                    }),
                    expected: 1,
                    meter: Some(meter),
                    received: received.clone(),
                };
                let reply = wire::PutObjectsResponse {
                    results: vec![wire::PutObjectResult {
                        result: Some(wire::put_object_result::Result::Object(wire::ObjectInfo {
                            etag: "receipt".into(),
                            size: 7,
                            last_modified: Some(prost_types::Timestamp::default()),
                            metadata: Some(wire::ObjectMetadata {
                                user: [("key".into(), "value".into())].into(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        })),
                    }],
                };
                let reply = decoder.bytes(&reply.encode_to_vec()).unwrap();
                decoder.accept(reply);
                assert!(received.value.metadata.lock().unwrap().value[0].is_none());
                drop(decoder);
                let settled = received.value.reply.lock().unwrap().take().unwrap();
                assert!(settled.failure.is_none());
                assert_eq!(settled.response.value.results.len(), 1);
                assert!(received.value.reply.lock().unwrap().take().is_none());
            })
            .unwrap()
    };
    writer.join().unwrap();
    reader.join().unwrap();
}
#[test]
fn batch_exchange_models_metadata_transfer_decode_settlement_and_drop() {
    loom::model(|| history(false));
}
#[test]
fn batch_exchange_rejects_publication_before_metadata_transfer() {
    assert!(std::panic::catch_unwind(|| loom::model(|| history(true))).is_err());
}
