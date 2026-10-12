use super::*;
use acyclic_native_runtime::NativeReadMeter;
use std::sync::{Arc, atomic::Ordering::SeqCst};
use std::task::{Context, Poll};
use tonic::codec::Codec;

fn encoded_body<S>(source: S) -> tonic::body::Body
where
    S: futures::Stream<
            Item = Result<response::RequestInput<wire::GetObjectRequest>, tonic::Status>,
        > + Send
        + 'static,
{
    let encoder =
        response::CustodyProstCodec::<wire::GetObjectRequest, wire::GetObjectResponse>::new()
            .encoder();
    tonic::body::Body::new(tonic::codec::EncodeBody::new_client(
        encoder,
        source,
        None,
        Some(MESSAGE_BYTES),
    ))
}

#[test]
fn actual_ready_codec_frame_keeps_original_claim_to_last_wire_alias()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let observed = Arc::new(crate::v1::tests::NativeTransportMeter::new());
    let meter: Arc<dyn NativeReadMeter> = observed.clone();
    let errors = crate::v1::NativeTransportErrors::new(&meter)?;
    errors.prepare_codec_failure()?;
    let controls = errors;
    let packet =
        crate::v1::ObjectsNativeRequest::new(wire::GetObjectRequest::default(), (), &meter)?;
    let input =
        response::RequestInput::native(packet, &meter)?.with_controls(Some(controls.clone()))?;
    let ready = PreparedWireBody::new(controls.clone(), input.value().encoded_len() + 5, 1)?;
    let mut body = ready.initialize(encoded_body(stream::iter([Ok(input)])))?;
    let mut context = Context::from_waker(futures::task::noop_waker_ref());
    let frame = match http_body::Body::poll_frame(std::pin::Pin::new(&mut body), &mut context) {
        Poll::Ready(Some(Ok(frame))) => frame,
        _ => panic!("the actual CPU-only native query encoder must produce a ready frame"),
    };
    let wire = frame
        .into_data()
        .expect("the actual codec frame owns encoded bytes");
    let alias = wire.slice(1..);
    drop(body);
    drop(controls);
    drop(meter);
    let retained = observed.memory.load(SeqCst);
    assert!(retained > 0);
    observed.cancelled.store(true, SeqCst);
    observed.limit.store(0, SeqCst);
    let last = alias.clone();
    drop(wire);
    drop(alias);
    assert!(!last.is_empty());
    assert_eq!(observed.memory.load(SeqCst), retained);
    drop(last);
    assert_eq!(observed.memory.load(SeqCst), 0);
    Ok(())
}
