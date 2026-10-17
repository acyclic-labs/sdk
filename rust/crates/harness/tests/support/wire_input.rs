//! Wire frame decoding, shared by the `wire_frames` fuzz target and the
//! decoder properties: the fixture records [`Seen`] events and only
//! [`INVARIANTS`] judge them. Tests inject defects through `tamper`.
use acyclic_harness::wire;
use prost::Message;

/// One protobuf round trip.
#[derive(Debug)]
pub struct Seen {
    pub site: &'static str,
    pub equal: bool,
}

pub type Check = fn(&Seen) -> Result<(), String>;

pub const INVARIANTS: [(&str, Check); 1] = [("roundtrip", |seen| {
    if seen.equal {
        Ok(())
    } else {
        Err(format!(
            "{} protobuf roundtrip changed the frame",
            seen.site
        ))
    }
})];

/// Fuzz entry: every invariant, as panics.
pub fn check(data: &[u8]) {
    let judged = observe(data, |_| {}, &mut |seen| {
        for (name, check) in INVARIANTS {
            if let Err(detail) = check(&seen) {
                panic!("{name}: {detail}");
            }
        }
        Ok::<(), std::convert::Infallible>(())
    });
    if let Err(never) = judged {
        match never {}
    }
}

/// Records every round trip of `data`; `tamper` sees the decoded generated
/// client frame before it is compared.
pub fn observe<S>(
    data: &[u8],
    tamper: fn(&mut wire::ClientFrame),
    see: &mut impl FnMut(Seen) -> Result<(), S>,
) -> Result<(), S> {
    if let Ok(frame) = wire::ClientFrame::decode(data) {
        see(Seen {
            site: "decoded client",
            equal: wire::ClientFrame::decode(frame.encode_to_vec().as_slice()).ok() == Some(frame),
        })?;
    }
    if let Ok(frame) = wire::ServerFrame::decode(data) {
        see(Seen {
            site: "decoded server",
            equal: wire::ServerFrame::decode(frame.encode_to_vec().as_slice()).ok() == Some(frame),
        })?;
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
    let decoded = wire::ClientFrame::decode(frame.encode_to_vec().as_slice()).map(|mut decoded| {
        tamper(&mut decoded);
        decoded
    });
    see(Seen {
        site: "generated client",
        equal: decoded.ok() == Some(frame),
    })?;
    let [a, b, c, d, ..] = number;
    let frame = wire::ServerFrame {
        frame: Some(wire::server_frame::Frame::Admission(wire::Admission {
            operation: Some(wire::OperationIdentity {
                operation_id: text.clone(),
                idempotency_key: text,
            }),
            state: i32::from_le_bytes([a, b, c, d]),
            error: None,
        })),
    };
    see(Seen {
        site: "generated server",
        equal: wire::ServerFrame::decode(frame.encode_to_vec().as_slice()).ok() == Some(frame),
    })
}
