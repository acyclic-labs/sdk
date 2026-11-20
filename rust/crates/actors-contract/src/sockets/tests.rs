use super::*;

#[test]
fn frames_and_opens_round_trip_and_reject_noncanonical_bytes() -> Result<(), GuestError> {
    let open = SocketOpen {
        actor: ActorReference::new(b"account", b"targets", b"laptop").map_err(|_| invalid())?,
        subscription: "sockets".into(),
        app_channel: Some("presence".into()),
        subprotocols: vec!["v1".into()],
        headers: vec![("x-device".into(), "windows".into())],
        resume_after: 7,
        reconnected: true,
    };
    assert_eq!(SocketOpen::decode(&open.encode())?, open);
    assert_eq!(open.resource(), "actors:/targets");
    for frame in [
        SocketFrame::Message(SocketPayload::Text {
            text: "ping".into(),
        }),
        SocketFrame::Message(SocketPayload::Binary {
            binary: Bytes::from_static(b"\0\x01"),
        }),
        SocketFrame::Close {
            code: 1000,
            reason: "bye".into(),
            initiator: SocketCloseInitiator::Runtime,
        },
        SocketFrame::Accepted { subprotocol: None },
        SocketFrame::Accepted {
            subprotocol: Some("v1".into()),
        },
        SocketFrame::Rejected {
            status: 403,
            reason: "no".into(),
        },
    ] {
        let original = match &frame {
            SocketFrame::Message(SocketPayload::Text { text }) => Some(text.as_ptr()),
            SocketFrame::Message(SocketPayload::Binary { binary }) => Some(binary.as_ptr()),
            SocketFrame::Close { reason, .. } | SocketFrame::Rejected { reason, .. } => {
                Some(reason.as_ptr())
            }
            SocketFrame::Accepted { subprotocol } => {
                subprotocol.as_ref().map(|value| value.as_ptr())
            }
        };
        let expected = frame.clone();
        let wire = frame.wire();
        let retained = match wire.kind.as_ref().ok_or_else(invalid)? {
            FrameKind::Text(text) => Some(text.as_ptr()),
            FrameKind::Binary(binary) => Some(binary.as_ptr()),
            FrameKind::Close(close) => Some(close.reason.as_ptr()),
            FrameKind::Accepted(accepted) => {
                accepted.subprotocol.as_ref().map(|value| value.as_ptr())
            }
            FrameKind::Rejected(rejected) => Some(rejected.reason.as_ptr()),
        };
        assert_eq!(
            original, retained,
            "wire conversion copied the payload owner"
        );
        let encoded = wire.encode_to_vec();
        assert_eq!(expected.clone().encode().as_ref(), encoded.as_slice());
        assert_eq!(SocketFrame::decode(&encoded)?, expected);
    }
    // Negative controls: an empty frame, an out-of-range close code and
    // trailing bytes all fail instead of decoding to a default.
    assert!(SocketFrame::decode(&[]).is_err());
    let wide = FrameWire {
        kind: Some(FrameKind::Close(CloseWire {
            code: 70_000,
            reason: String::new(),
            initiator: 1,
        })),
    };
    assert!(SocketFrame::decode(&wide.encode_to_vec()).is_err());
    let mut trailing = open.encode().to_vec();
    trailing.extend_from_slice(&open.encode());
    assert!(SocketOpen::decode(&trailing).is_err());
    Ok(())
}
