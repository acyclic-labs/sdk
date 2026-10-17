use acyclic_harness::wire;
use prost::Message;

pub fn check(data: &[u8]) {
    check_with_dropped_generation(data, false);
}

pub fn check_with_dropped_generation(data: &[u8], broken: bool) {
    if let Ok(frame) = wire::ClientFrame::decode(data) {
        assert_eq!(
            wire::ClientFrame::decode(frame.encode_to_vec().as_slice()).unwrap(),
            frame
        );
    }
    if let Ok(frame) = wire::ServerFrame::decode(data) {
        assert_eq!(
            wire::ServerFrame::decode(frame.encode_to_vec().as_slice()).unwrap(),
            frame
        );
    }
    // Every input also exercises valid messages, arbitrary UTF-8 escaping,
    // unknown enum values and the full u64 range.
    let text = String::from_utf8_lossy(data).into_owned();
    let mut number = [0; 8];
    let copied = data.len().min(number.len());
    number[..copied].copy_from_slice(&data[..copied]);
    let frame = wire::ClientFrame {
        frame: Some(wire::client_frame::Frame::Acknowledge(wire::Acknowledge {
            authority: None,
            generation: text.clone(),
            through_revision: u64::from_le_bytes(number),
        })),
    };
    let mut decoded = wire::ClientFrame::decode(frame.encode_to_vec().as_slice()).unwrap();
    if broken {
        let Some(wire::client_frame::Frame::Acknowledge(ack)) = &mut decoded.frame else {
            unreachable!();
        };
        ack.generation.clear();
    }
    assert_eq!(
        decoded, frame,
        "client protobuf roundtrip changed the generation"
    );
    let frame = wire::ServerFrame {
        frame: Some(wire::server_frame::Frame::Admission(wire::Admission {
            operation: Some(wire::OperationIdentity {
                operation_id: text.clone(),
                idempotency_key: text,
            }),
            state: i32::from_le_bytes(number[..4].try_into().unwrap()),
            error: None,
        })),
    };
    assert_eq!(
        wire::ServerFrame::decode(frame.encode_to_vec().as_slice()).unwrap(),
        frame
    );
}
