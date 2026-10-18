#![allow(
    clippy::indexing_slicing,
    clippy::unwrap_used,
    reason = "Test helpers index known fixtures and fail loudly on fixture errors."
)]
use super::*;
use proptest::prelude::*;

fn decoder(expected: usize) -> Decoder {
    metered_decoder(expected, None)
}

fn metered_decoder(expected: usize, meter: Option<std::sync::Arc<dyn NativeReadMeter>>) -> Decoder {
    let response = batch_vector(expected, meter.as_ref()).unwrap();
    let mut sizes = batch_vector(expected, meter.as_ref()).unwrap();
    sizes.value.resize(expected, 0);
    let received = received(meter.as_ref(), sizes, Arc::new(Completion::default())).unwrap();
    if let Some(meter) = meter.as_ref() {
        received
            .value
            .keep_metadata(
                &mut wire::PutObjectsHeader {
                    objects: vec![wire::PutObjectHeader::default(); expected],
                },
                Some(meter),
            )
            .unwrap();
    }
    Decoder {
        prepared: Some(Reply {
            response: Guarded {
                value: wire::PutObjectsResponse {
                    results: response.value,
                },
                _allocation: response._allocation,
            },
            failure: None,
        }),
        expected,
        received,
        meter,
    }
}

fn object(size: u64) -> wire::PutObjectResult {
    wire::PutObjectResult {
        result: Some(wire::put_object_result::Result::Object(wire::ObjectInfo {
            etag: "receipt".into(),
            size,
            last_modified: Some(prost_types::Timestamp::default()),
            ..Default::default()
        })),
    }
}

fn transfer_metadata(decoder: &mut Decoder, response: &wire::PutObjectsResponse) {
    let mut header = wire::PutObjectsHeader {
        objects: response
            .results
            .iter()
            .map(|result| {
                let metadata = match &result.result {
                    Some(wire::put_object_result::Result::Object(info)) => info.metadata.clone(),
                    _ => None,
                };
                wire::PutObjectHeader {
                    metadata,
                    ..Default::default()
                }
            })
            .collect(),
    };
    decoder
        .received
        .value
        .metadata
        .lock()
        .unwrap()
        .value
        .clear();
    decoder
        .received
        .value
        .keep_metadata(&mut header, decoder.meter.as_ref())
        .unwrap();
}

#[cfg(feature = "local")]
#[test]
fn batch_reply_reuses_maps_with_exact_allocator_and_meter_counts() {
    use std::sync::atomic::Ordering;
    for entries in [1, 11, 12, 24, 128] {
        let mut item = object(7);
        if let Some(wire::put_object_result::Result::Object(info)) = &mut item.result {
            info.metadata = Some(wire::ObjectMetadata {
                user: (0..entries)
                    .map(|index| (format!("key-{index}"), "value".into()))
                    .collect(),
                ..Default::default()
            });
        }
        let response = wire::PutObjectsResponse {
            results: vec![item],
        };
        let bytes = response.encode_to_vec();
        for control in [false, true] {
            let counts = std::sync::Arc::new(crate::physical::read_tests::CountingMeter::default());
            let mut decoder = metered_decoder(1, Some(counts.clone()));
            transfer_metadata(&mut decoder, &response);
            std::sync::Arc::get_mut(&mut decoder.received)
                .unwrap()
                .value
                .sizes
                .value[0] = 7;
            decoder.received.value.completed.sent();
            let pointer = decoder.received.value.metadata.lock().unwrap().value[0]
                .as_ref()
                .unwrap()
                .user
                .first_key_value()
                .unwrap()
                .0
                .as_ptr();
            let before = counts.allocations.load(Ordering::SeqCst);
            counts.allocation_limit.store(before + 1, Ordering::SeqCst);
            if control {
                decoder.meter = None;
            }
            let physical = acyclic_test_support::measure(|| {
                let reply = decoder.bytes(&bytes).unwrap();
                decoder.accept(reply);
            });
            let reply = decoder.received.value.reply.lock().unwrap().take().unwrap();
            assert!(reply.failure.is_none());
            assert_eq!(reply.response.value, response);
            let metered = counts.allocations.load(Ordering::SeqCst) - before;
            if control {
                assert_eq!(metered, 0);
                assert_eq!(counts.written.load(Ordering::SeqCst), 0);
                assert!(physical.count_total > 1);
            } else {
                assert_eq!(physical.count_total, 1);
                assert_eq!(physical.bytes_total, 7);
                assert_eq!(metered, physical.count_total);
                assert_eq!(counts.written.load(Ordering::SeqCst), 7);
                let Some(wire::put_object_result::Result::Object(info)) =
                    &reply.response.value.results[0].result
                else {
                    panic!("receipt")
                };
                assert_eq!(
                    info.metadata
                        .as_ref()
                        .unwrap()
                        .user
                        .first_key_value()
                        .unwrap()
                        .0
                        .as_ptr(),
                    pointer
                );
            }
            drop(reply);
            drop(decoder);
            assert_eq!(counts.retained(), 0);
        }
    }
}

fn append_text(tag: u32, value: &str, output: &mut Vec<u8>) {
    append_message(tag, value.as_bytes(), output);
}

fn append_message(tag: u32, value: &[u8], output: &mut Vec<u8>) {
    prost::encoding::encode_key(tag, WireType::LengthDelimited, output);
    prost::encoding::encode_varint(value.len() as u64, output);
    output.extend_from_slice(value);
}

#[test]
fn batch_reply_merges_repeated_oneof_messages_and_replaces_changed_variants() {
    use std::sync::atomic::Ordering;
    let head = wire::ObjectInfo {
        etag: "receipt".into(),
        metadata: Some(wire::ObjectMetadata {
            user: [("first".into(), "one".into())].into(),
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec();
    let tail = wire::ObjectInfo {
        size: 7,
        last_modified: Some(prost_types::Timestamp::default()),
        metadata: Some(wire::ObjectMetadata {
            user: [("second".into(), "two".into())].into(),
            ..Default::default()
        }),
        ..Default::default()
    }
    .encode_to_vec();
    let denied = wire::ErrorDetail {
        code: wire::ErrorCode::AccessDenied as i32,
        ..Default::default()
    }
    .encode_to_vec();
    let request = wire::ErrorDetail {
        request_id: "final".into(),
        ..Default::default()
    }
    .encode_to_vec();
    for parts in [
        vec![(1, &head), (1, &tail)],
        vec![(2, &denied), (2, &request)],
        vec![(1, &head), (1, &tail), (2, &denied), (2, &request)],
        vec![(2, &denied), (2, &request), (1, &head), (1, &tail)],
        vec![(1, &tail), (2, &denied), (1, &head), (1, &tail)],
    ] {
        let mut result = Vec::new();
        for (tag, part) in parts {
            append_message(tag, part, &mut result);
        }
        let mut bytes = Vec::new();
        append_message(1, &result, &mut bytes);
        let expected = wire::PutObjectsResponse::decode(bytes.as_slice()).unwrap();
        for native in [false, true] {
            let counts = std::sync::Arc::new(crate::test_meter::CountingMeter::default());
            let mut decoder = metered_decoder(
                1,
                native.then(|| counts.clone() as std::sync::Arc<dyn NativeReadMeter>),
            );
            if native {
                transfer_metadata(&mut decoder, &expected);
            }
            std::sync::Arc::get_mut(&mut decoder.received)
                .unwrap()
                .value
                .sizes
                .value[0] = 7;
            decoder.received.value.completed.sent();
            let mut decoded = None;
            let physical =
                acyclic_test_support::measure(|| decoded = Some(decoder.bytes(&bytes).unwrap()));
            let reply = decoded.unwrap();
            assert!(reply.failure.is_none());
            assert_eq!(reply.response.value, expected);
            let object = matches!(
                expected.results[0].result,
                Some(wire::put_object_result::Result::Object(_))
            );
            if native {
                assert_eq!(physical.count_total, 1);
                assert_eq!(physical.bytes_total, if object { 7 } else { 5 });
            }
            decoder.accept(reply);
            assert_eq!(
                counts.written.load(Ordering::SeqCst),
                if native && object { 7 } else { 0 }
            );
            drop(decoder);
            assert_eq!(counts.retained(), 0);
        }
    }
    // A later variant cannot hide malformed earlier message contents.
    let mut result = Vec::new();
    append_message(1, &[10, 1, 255], &mut result);
    append_message(2, &denied, &mut result);
    let mut bytes = Vec::new();
    append_message(1, &result, &mut bytes);
    assert!(wire::PutObjectsResponse::decode(bytes.as_slice()).is_err());
    assert!(decoder(1).bytes(&bytes).unwrap().failure.is_some());
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn batch_reply_copies_only_final_string_values(
        previous in "[a-z]{1,8192}",
        values in prop::collection::vec("[a-z]{1,24}", 6),
        cancel in any::<bool>(),
        error in any::<bool>(),
    ) {
        use std::sync::atomic::Ordering;
        let item = if error {
            let mut item = wire::ErrorDetail { code: wire::ErrorCode::AccessDenied as i32, request_id: previous }.encode_to_vec();
            append_text(2, &values[0], &mut item);
            item
        } else {
            let Some(wire::put_object_result::Result::Object(mut info)) = object(0).result else { unreachable!() };
            info.etag = previous.clone();
            let mut item = info.encode_to_vec();
            append_text(1, &values[0], &mut item);
            // Repeated message fields merge; scalar strings use the final value.
            for overwrite in [true, false] {
                let mut metadata = Vec::new();
                for (tag, value) in [1, 3, 4, 5, 6].into_iter().zip(&values[1..]) {
                    append_text(tag, if overwrite { &previous } else { value }, &mut metadata);
                }
                append_message(3, &metadata, &mut item);
            }
            item
        };
        let mut result = Vec::new();
        append_message(if error { 2 } else { 1 }, &item, &mut result);
        let mut bytes = Vec::new();
        append_message(1, &result, &mut bytes);
        let expected = wire::PutObjectsResponse::decode(bytes.as_slice()).unwrap();
        let counts = std::sync::Arc::new(crate::test_meter::CountingMeter::default());
        {
            let mut native = metered_decoder(1, Some(counts.clone()));
            counts.cancelled.store(cancel, Ordering::SeqCst);
            let baseline = counts.retained();
            let mut reply = None;
            let physical = acyclic_test_support::measure(|| reply = Some(native.bytes(&bytes).unwrap()));
            let reply = reply.unwrap();
            prop_assert!(reply.failure.is_none());
            prop_assert_eq!(&reply.response.value, &expected);
            let strings = if error { &values[..1] } else { &values[..] };
            let exact: u64 = strings.iter().map(|text| text.len() as u64).sum();
            prop_assert_eq!(physical.count_total, strings.len() as u64);
            prop_assert_eq!(physical.bytes_total, exact);
            prop_assert_eq!(counts.retained() - baseline, exact);
            prop_assert_eq!(counts.codec_copied.load(Ordering::SeqCst), exact);
        }
        prop_assert_eq!(counts.retained(), 0);
    }
}

#[test]
fn batch_reply_ordinary_exchange_has_no_native_storage() {
    for count in [0, 1, 1000] {
        let mut sizes = batch_vector(count, None).unwrap();
        sizes.value.resize(count, 0);
        let mut exchange = None;
        let allocations = acyclic_test_support::measure(|| {
            exchange = Some(received(None, sizes, Arc::new(Completion::default())).unwrap());
        });
        let exchange = exchange.unwrap();
        assert_eq!(
            exchange
                .value
                .resources
                .lock()
                .unwrap()
                .texts
                .value
                .capacity(),
            0
        );
        assert_eq!(exchange.value.metadata.lock().unwrap().value.capacity(), 0);
        // Only the completion and exchange Arcs, independent of batch length.
        assert_eq!(allocations.count_total, 2);
    }
}

#[test]
fn batch_reply_finalizes_valid_receipts_after_every_validation_failure() {
    for completion in [Ok(()), Err(protocol())] {
        for invalid_index in 0..3 {
            let mut response = wire::PutObjectsResponse {
                results: vec![object(2), object(3), object(5)],
            };
            if let Some(wire::put_object_result::Result::Object(info)) =
                &mut response.results[invalid_index].result
            {
                info.metadata = Some(wire::ObjectMetadata {
                    content_type: "invalid\r\nmetadata".into(),
                    ..Default::default()
                });
            }
            let mut reply = decoder(3).bytes(&response.encode_to_vec()).unwrap();
            reply.validate(&[2, 3, 5], completion);
            assert!(reply.failure.is_some());
            for (index, result) in reply.response.value.results.iter().enumerate() {
                assert_eq!(result.result.is_some(), index != invalid_index);
            }
            // The old all-or-nothing validation loses these accepted peers.
            let control = response
                .results
                .iter()
                .try_for_each(|result| match &result.result {
                    Some(wire::put_object_result::Result::Object(object)) => info(object),
                    _ => Err(protocol()),
                });
            assert!(control.is_err());
            assert_eq!(
                reply
                    .response
                    .value
                    .results
                    .iter()
                    .filter(|result| result.result.is_some())
                    .count(),
                2
            );
        }
    }
}

#[cfg(feature = "local")]
#[test]
fn batch_reply_records_accepted_bytes_despite_validation_or_transport_failure() {
    use std::sync::atomic::Ordering;
    let counts = crate::physical::read_tests::CountingMeter::default();
    let response = wire::PutObjectsResponse {
        results: vec![object(7)],
    };
    let mut reply = decoder(2).bytes(&response.encode_to_vec()).unwrap();
    reply.validate(&[7, 9], Err(protocol()));
    reply.record_write(&counts);
    assert_eq!(counts.written.load(Ordering::SeqCst), 7);
    assert!(reply.failure.is_some());
    assert!(reply.response.value.results[0].result.is_some());
}

#[cfg(feature = "local")]
#[test]
fn batch_reply_accounts_before_cancellation_can_drop_the_exchange() {
    use std::sync::atomic::Ordering;
    let counts = std::sync::Arc::new(crate::physical::read_tests::CountingMeter::default());
    let meter: std::sync::Arc<dyn NativeReadMeter> = counts.clone();
    let mut decoder = metered_decoder(2, Some(meter.clone()));
    let mut sizes = batch_vector(2, Some(&meter)).unwrap();
    sizes.value.extend([7, 9]);
    let completed = Completion::new(Some(&meter)).unwrap();
    completed.sent();
    decoder.received = received(Some(&meter), sizes, completed).unwrap();
    decoder
        .received
        .value
        .keep_metadata(
            &mut wire::PutObjectsHeader {
                objects: vec![wire::PutObjectHeader::default(); 2],
            },
            Some(&meter),
        )
        .unwrap();
    let response = wire::PutObjectsResponse {
        results: vec![object(7)],
    };
    let reply = decoder.bytes(&response.encode_to_vec()).unwrap();
    decoder.accept(reply);
    assert_eq!(counts.written.load(Ordering::SeqCst), 7);
    let pending = decoder.received.clone();
    drop(decoder);
    assert!(counts.retained() > 0);
    drop(pending);
    assert_eq!(counts.written.load(Ordering::SeqCst), 7);
    assert_eq!(counts.retained(), 0);

    // Delaying the same production accounting until after the await loses it on drop.
    let mut decoder = metered_decoder(2, Some(meter));
    let reply = decoder.bytes(&response.encode_to_vec()).unwrap();
    drop(reply);
    drop(decoder);
    assert_ne!(counts.written.load(Ordering::SeqCst), 14);
}

#[test]
fn batch_reply_preserves_prefix_through_malformed_tail_and_rejects_overflow() {
    let response = wire::PutObjectsResponse {
        results: vec![object(7)],
    };
    let mut bytes = response.encode_to_vec();
    bytes.extend_from_slice(&[10, 127]);
    let reply = decoder(2).bytes(&bytes).unwrap();
    assert!(reply.failure.is_some());
    assert_eq!(reply.response.value.results, response.results);
    let reply = decoder(0).bytes(&response.encode_to_vec()).unwrap();
    assert!(reply.failure.is_some());
    assert!(reply.response.value.results.is_empty());
}

#[cfg(feature = "local")]
#[test]
fn batch_reply_preserves_peers_after_a_malformed_or_changed_map() {
    use std::sync::atomic::Ordering;
    for invalid_index in 0..3 {
        for malformed in [false, true] {
            let counts = std::sync::Arc::new(crate::physical::read_tests::CountingMeter::default());
            let mut decoder = metered_decoder(3, Some(counts.clone()));
            let response = wire::PutObjectsResponse {
                results: vec![object(7); 3],
            };
            transfer_metadata(&mut decoder, &response);
            std::sync::Arc::get_mut(&mut decoder.received)
                .unwrap()
                .value
                .sizes
                .value
                .fill(7);
            decoder.received.value.completed.sent();
            let mut bytes = Vec::new();
            for (index, result) in response.results.iter().enumerate() {
                let item = if index == invalid_index {
                    if malformed {
                        vec![10, 1, 255]
                    } else {
                        let mut changed = result.clone();
                        if let Some(wire::put_object_result::Result::Object(info)) =
                            &mut changed.result
                        {
                            info.metadata = Some(wire::ObjectMetadata {
                                user: [("changed".into(), "value".into())].into(),
                                ..Default::default()
                            });
                        }
                        changed.encode_to_vec()
                    }
                } else {
                    result.encode_to_vec()
                };
                prost::encoding::encode_key(1, WireType::LengthDelimited, &mut bytes);
                prost::encoding::encode_varint(item.len() as u64, &mut bytes);
                bytes.extend(item);
            }
            let reply = decoder.bytes(&bytes).unwrap();
            decoder.accept(reply);
            let reply = decoder.received.value.reply.lock().unwrap().take().unwrap();
            assert!(reply.failure.is_some());
            assert_eq!(counts.written.load(Ordering::SeqCst), 14);
            for (index, item) in reply.response.value.results.iter().enumerate() {
                assert_eq!(item.result.is_some(), index != invalid_index);
            }
            drop(reply);
            drop(decoder);
            assert_eq!(counts.retained(), 0);
        }
    }
}

#[test]
fn batch_reply_checked_in_seed_corpus() {
    for bytes in [
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-0.pb").as_slice(),
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-1.pb").as_slice(),
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-2.pb").as_slice(),
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-3.pb").as_slice(),
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-4.pb").as_slice(),
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-5.pb").as_slice(),
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-6.pb").as_slice(),
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-7.pb").as_slice(),
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-8.pb").as_slice(),
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-9.pb").as_slice(),
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-10.pb").as_slice(),
    ] {
        for expected in 0..3 {
            let reply = decoder(expected).bytes(bytes).unwrap();
            assert!(reply.response.value.results.len() <= expected);
            if reply.failure.is_none() {
                assert_eq!(
                    reply.response.value,
                    wire::PutObjectsResponse::decode(bytes).unwrap()
                );
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn batch_reply_round_trips_protobuf(
        sizes in prop::collection::vec(any::<u64>(), 0..12),
        text in "[a-zA-Z0-9]{0,32}",
        failures in any::<bool>(),
        entries in prop::collection::vec(("[a-z]{1,8}", "[a-z]{0,16}"), 0..30),
    ) {
        let expected = wire::PutObjectsResponse { results: sizes.iter().map(|size| {
            if failures {
                wire::PutObjectResult { result: Some(wire::put_object_result::Result::Error(
                    wire::ErrorDetail { code: wire::ErrorCode::AccessDenied as i32, request_id: text.clone() }
                )) }
            } else {
                let mut result = object(*size);
                if let Some(wire::put_object_result::Result::Object(info)) = &mut result.result {
                    info.metadata = Some(wire::ObjectMetadata {
                        content_type: text.clone(), user: entries.iter().cloned().collect(), ..Default::default()
                    });
                }
                result
            }
        }).collect() };
        let reply = decoder(sizes.len()).bytes(&expected.encode_to_vec()).unwrap();
        prop_assert!(reply.failure.is_none());
        prop_assert_eq!(&reply.response.value, &expected);
        #[cfg(feature = "local")]
        if !failures {
            let counts = std::sync::Arc::new(crate::physical::read_tests::CountingMeter::default());
            {
                let mut native = metered_decoder(sizes.len(), Some(counts.clone()));
                transfer_metadata(&mut native, &expected);
                let reply = native.bytes(&expected.encode_to_vec()).unwrap();
                prop_assert!(reply.failure.is_none());
                prop_assert_eq!(&reply.response.value, &expected);
            }
            prop_assert_eq!(counts.retained(), 0);
        }
    }

    #[test]
    fn batch_reply_arbitrary_bytes_never_exceed_prepared_capacity(
        bytes in prop::collection::vec(any::<u8>(), 0..512),
        expected in 0usize..12,
    ) {
        let mut decoder = decoder(expected);
        let capacity = decoder.prepared.as_ref().unwrap().response.value.results.capacity();
        let reply = decoder.bytes(&bytes).unwrap();
        prop_assert!(reply.response.value.results.len() <= expected);
        prop_assert_eq!(reply.response.value.results.capacity(), capacity);
        if reply.failure.is_none() {
            prop_assert_eq!(wire::PutObjectsResponse::decode(bytes.as_slice()).unwrap(), reply.response.value);
        }
        prop_assert!(decoder.bytes(&bytes).is_err());
    }
}

#[cfg(feature = "local")]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn batch_reply_drop_retires_all_decoder_custody(
        entries in prop::collection::vec(("[a-z]{1,8}", "[a-z]{0,16}"), 0..30),
        cut in any::<usize>(),
    ) {
        let mut result = object(7);
        if let Some(wire::put_object_result::Result::Object(info)) = &mut result.result {
            info.metadata = Some(wire::ObjectMetadata { user: entries.into_iter().collect(), ..Default::default() });
        }
        let response = wire::PutObjectsResponse { results: vec![result] };
        let bytes = response.encode_to_vec();
        let counts = std::sync::Arc::new(crate::physical::read_tests::CountingMeter::default());
        {
            let mut decoder = metered_decoder(1, Some(counts.clone()));
            transfer_metadata(&mut decoder, &response);
            let reply = decoder.bytes(&bytes[..cut % (bytes.len() + 1)]).unwrap();
            drop(reply);
            drop(decoder);
        }
        prop_assert_eq!(counts.retained(), 0);
    }
}

#[cfg(feature = "local")]
#[test]
fn batch_reply_completion_survives_a_genuinely_cancelled_meter() {
    use std::sync::atomic::Ordering;
    let counts = std::sync::Arc::new(crate::physical::read_tests::CountingMeter::default());
    let mut decoder = metered_decoder(1, Some(counts.clone()));
    std::sync::Arc::get_mut(&mut decoder.received)
        .unwrap()
        .value
        .sizes
        .value[0] = 7;
    decoder.received.value.completed.sent();
    let bytes = wire::PutObjectsResponse {
        results: vec![object(7)],
    }
    .encode_to_vec();
    counts.cancelled.store(true, Ordering::SeqCst);
    assert!(counts.check().is_err());
    // The former post-dispatch preflight rejects every acknowledgement here.
    assert!(counts.admit_codec(bytes.len() as u64, 0, 0).is_err());
    assert!(
        ReadAllocation::new(&(counts.clone() as std::sync::Arc<dyn NativeReadMeter>), 7).is_err()
    );
    let reply = decoder.bytes(&bytes).unwrap();
    decoder.accept(reply);
    let reply = decoder.received.value.reply.lock().unwrap().take().unwrap();
    assert!(reply.failure.is_none());
    assert_eq!(counts.written.load(Ordering::SeqCst), 7);
    assert!(reply.response.value.results[0].result.is_some());
    drop(reply);
    drop(decoder);
    assert_eq!(counts.retained(), 0);
}

#[cfg(feature = "local")]
#[test]
fn batch_reply_rejected_item_retires_capacity_before_the_next_item() {
    use std::sync::atomic::Ordering;
    for control in [false, true] {
        let counts = std::sync::Arc::new(crate::physical::read_tests::CountingMeter::default());
        let mut decoder = metered_decoder(2, Some(counts.clone()));
        std::sync::Arc::get_mut(&mut decoder.received)
            .unwrap()
            .value
            .sizes
            .value
            .fill(7);
        decoder.received.value.completed.sent();
        let base = counts.retained();
        counts
            .byte_limit
            .store(base + TEXT_BYTES as u64, Ordering::SeqCst);
        // The wire is valid, so the borrowed pass succeeds before the etag allocation.
        let mut malformed = object(7);
        if let Some(wire::put_object_result::Result::Object(object)) = &mut malformed.result {
            object.last_modified.as_mut().unwrap().nanos = 1_000_000_000;
        }
        let bytes = wire::PutObjectsResponse {
            results: vec![malformed, object(7)],
        }
        .encode_to_vec();
        // Mutant: keep the freed first item's charge on the shared reply owner.
        let retained = control.then(|| {
            ReadAllocation::adopt(
                &(counts.clone() as std::sync::Arc<dyn NativeReadMeter>),
                TEXT_BYTES,
            )
            .unwrap()
        });
        let reply = decoder.bytes(&bytes).unwrap();
        decoder.accept(reply);
        let reply = decoder.received.value.reply.lock().unwrap().take().unwrap();
        assert!(reply.failure.is_some());
        assert!(reply.response.value.results[0].result.is_none());
        assert_eq!(reply.response.value.results[1].result.is_some(), !control);
        assert_eq!(
            counts.written.load(Ordering::SeqCst),
            if control { 0 } else { 7 }
        );
        drop(retained);
        drop(reply);
        drop(decoder);
        assert_eq!(counts.retained(), 0);
    }
}
#[cfg(feature = "local")]
#[test]
fn batch_reply_receipt_strings_retain_only_their_decoded_length() {
    let counts = std::sync::Arc::new(crate::physical::read_tests::CountingMeter::default());
    let mut decoder = metered_decoder(1, Some(counts.clone()));
    std::sync::Arc::get_mut(&mut decoder.received)
        .unwrap()
        .value
        .sizes
        .value[0] = 7;
    decoder.received.value.completed.sent();
    let response = wire::PutObjectsResponse {
        results: vec![object(7)],
    };
    let reply = decoder.bytes(&response.encode_to_vec()).unwrap();
    assert!(reply.failure.is_none());
    assert_eq!(reply.response.value, response);
    let Some(wire::put_object_result::Result::Object(info)) =
        &reply.response.value.results[0].result
    else {
        panic!("receipt")
    };
    // A fixed per-string reservation capacity would retain 8 KiB per receipt.
    assert_eq!(info.etag.capacity(), "receipt".len());
    assert_eq!(
        reply.response._allocation.as_ref().unwrap().bytes(),
        (info_capacity(info).unwrap()
            + reply.response.value.results.capacity()
                * std::mem::size_of::<wire::PutObjectResult>()) as u64
    );
    drop(reply);
    drop(decoder);
    assert_eq!(counts.retained(), 0);
}

#[cfg(feature = "test-support")]
#[test]
fn batch_reply_fuzz_probe_checks_cancellation_and_custody_on_shared_seeds() {
    for bytes in [
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-1.pb").as_slice(),
        include_bytes!("../../../../../fuzz/corpus/batch_reply/seed-2.pb").as_slice(),
    ] {
        for expected in 0..=3 {
            for cancel in [false, true] {
                assert!(
                    fuzz_reply(
                        bytes,
                        expected,
                        std::sync::Arc::new(crate::test_meter::CountingMeter::default()),
                        cancel,
                        None,
                    )
                    .is_ok()
                );
            }
        }
    }
}
