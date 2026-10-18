use super::*;

#[doc(hidden)]
#[allow(
    clippy::too_many_lines,
    clippy::needless_pass_by_value,
    reason = "The fuzz entry point keeps its owned-meter signature and checks every invariant in one place."
)]
pub fn fuzz_reply(
    bytes: &[u8],
    expected: usize,
    counts: std::sync::Arc<crate::test_meter::CountingMeter>,
    cancel: bool,
    allocation_budget: Option<u64>,
) -> Result<(), NativeObjectsError> {
    use std::sync::atomic::Ordering;
    assert!(expected <= 8);
    let meter: std::sync::Arc<dyn NativeReadMeter> = counts.clone();
    {
        let reference = wire::PutObjectsResponse::decode(bytes).ok();
        let mut sizes = batch_vector(expected, Some(&meter))?;
        let mut header = wire::PutObjectsHeader::default();
        for index in 0..expected {
            let object = reference
                .as_ref()
                .and_then(|reply| reply.results.get(index))
                .and_then(|item| match &item.result {
                    Some(wire::put_object_result::Result::Object(object)) => Some(object),
                    _ => None,
                });
            sizes
                .value
                .push(object.map_or(0, |object| object.size.min(1 << 40)));
            header.objects.push(wire::PutObjectHeader {
                metadata: object.and_then(|object| object.metadata.clone()),
                ..Default::default()
            });
        }
        let completed = Completion::new(Some(&meter))?;
        completed.sent();
        let received = received(Some(&meter), sizes, completed)?;
        received.value.keep_metadata(&mut header, Some(&meter))?;
        let response = batch_vector(expected, Some(&meter))?;
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
            expected,
            meter: Some(meter),
            received,
        };
        if let Some(budget) = allocation_budget {
            counts.allocation_limit.store(
                counts.allocations.load(Ordering::SeqCst) + budget,
                Ordering::SeqCst,
            );
        }
        counts.cancelled.store(cancel, Ordering::SeqCst);
        let reply = decoder.bytes(bytes).map_err(|_| invalid())?;
        assert!(reply.response.value.results.len() <= expected);
        let retained = reply
            .response
            .value
            .results
            .iter()
            .try_fold(0usize, |total, item| {
                Ok::<_, NativeObjectsError>(
                    total
                        + match &item.result {
                            Some(wire::put_object_result::Result::Object(object)) => {
                                info_capacity(object)?
                            }
                            Some(wire::put_object_result::Result::Error(error)) => {
                                error.request_id.capacity()
                            }
                            None => 0,
                        },
                )
            })?
            + reply.response.value.results.capacity()
                * std::mem::size_of::<wire::PutObjectResult>();
        assert_eq!(
            reply
                .response
                ._allocation
                .as_ref()
                .ok_or_else(invalid)?
                .bytes(),
            retained as u64
        );
        if reply.failure.is_none() {
            assert_eq!(Some(&reply.response.value), reference.as_ref());
        }
        decoder.accept(reply);
        let reply = decoder
            .received
            .value
            .reply
            .lock()
            .map_err(|_| invalid())?
            .take()
            .ok_or_else(invalid)?;
        let accepted: u64 = reply
            .response
            .value
            .results
            .iter()
            .filter_map(|item| match &item.result {
                Some(wire::put_object_result::Result::Object(object)) => Some(object.size),
                _ => None,
            })
            .sum();
        assert_eq!(counts.written.load(Ordering::SeqCst), accepted);
        assert!(decoder.bytes(bytes).is_err());
    }
    assert_eq!(counts.retained(), 0);
    Ok(())
}
