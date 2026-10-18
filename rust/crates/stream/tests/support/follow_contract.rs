// One independent oracle drives the property suite and the actual fuzz target.
use acyclic_stream::{
    CommitId, StreamError, StreamWatermarks, wire,
    wire_codec::{
        session::{confirm_follow, preceding_follow, record_anchor, record_cursor},
        watermarks_from_wire,
    },
};
use prost::Message;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fault {
    None,
    AcceptShortGuard,
    DropChecked,
    DropSnapshotAnchor,
    AcceptAnyConfirmation,
    ReverseEncode,
    ReverseDecode,
    ReverseBoth,
    InferDurability,
}

fn require(valid: bool, invariant: &'static str) -> Result<(), &'static str> {
    if valid { Ok(()) } else { Err(invariant) }
}
fn round_trip<M: Message + Default + PartialEq>(value: &M) {
    crate::fuzz_support::round_trip(value, M::decode(value.encode_to_vec().as_slice()));
}

fn guard(
    follow: &wire::SessionFollow,
    resume: &wire::SessionCursor,
    fault: Fault,
) -> Result<(), &'static str> {
    let expected = if follow.preceding_commit_id.is_empty() {
        Some(None)
    } else if follow.delivery == wire::SessionDelivery::All as i32
        && resume.next > 0
        && resume.opaque.is_empty()
        && follow.preceding_commit_id.len() == 32
    {
        follow
            .preceding_commit_id
            .as_ref()
            .first_chunk::<32>()
            .copied()
            .map(Some)
    } else {
        None
    };
    let mut actual = preceding_follow(follow, resume).map(|anchor| anchor.map(|id| *id.as_bytes()));
    if fault == Fault::AcceptShortGuard && follow.preceding_commit_id.len() == 31 {
        // Deliberately broken codec pads an invalid guard and accepts it.
        let mut padded = [0; 32];
        padded[..31].copy_from_slice(&follow.preceding_commit_id);
        actual = Ok(Some(padded));
    }
    if fault == Fault::DropChecked {
        actual = actual.map(|_| None);
    }
    require(
        actual == expected.ok_or(StreamError::InvalidArgument),
        "checked guard",
    )?;
    if let Some(Some(anchor)) = expected {
        let mut mismatched = anchor;
        mismatched[0] ^= 1;
        for echo in [anchor.to_vec(), Vec::new(), mismatched.to_vec()] {
            let opened = wire::SessionOpened {
                resume: Some(resume.clone()),
                preceding_commit_id: echo.clone().into(),
            };
            let mut result = confirm_follow(Some(CommitId::from_bytes(anchor)), &opened);
            if fault == Fault::AcceptAnyConfirmation && echo.len() == 32 {
                result = Ok(());
            }
            let expected = if echo.as_slice() == anchor {
                Ok(())
            } else {
                Err(StreamError::Unsupported)
            };
            require(result == expected, "confirmation anchor")?;
        }
    }
    Ok(())
}

fn snapshot(cursor: &wire::SessionCursor, fault: Fault) -> Result<(), &'static str> {
    let anchor: Option<[u8; 32]> = (cursor.opaque.len() == 32).then(|| {
        cursor
            .opaque
            .as_ref()
            .try_into()
            .expect("checked anchor size")
    });
    let expected = match (cursor.next, anchor) {
        (0, _) if cursor.opaque.is_empty() => Ok(None),
        (1.., Some(anchor)) => Ok(Some(anchor)),
        _ => Err(StreamError::InvalidArgument),
    };
    let mut actual = record_anchor(cursor).map(|anchor| anchor.map(|id| *id.as_bytes()));
    if matches!(fault, Fault::ReverseDecode | Fault::ReverseBoth) {
        actual = actual.map(|anchor| {
            anchor.map(|mut bytes| {
                bytes.reverse();
                bytes
            })
        });
    }
    if fault == Fault::DropSnapshotAnchor {
        actual = actual.map(|_| None);
    }
    // Compare directly to the independently supplied wire bytes, never to an encoder.
    require(actual == expected, "snapshot decode bytes")?;
    if let Some(anchor) = anchor {
        let encoded = record_cursor(cursor.next, CommitId::from_bytes(anchor)).map(|mut cursor| {
            if matches!(fault, Fault::ReverseEncode | Fault::ReverseBoth) {
                cursor.opaque = cursor
                    .opaque
                    .iter()
                    .rev()
                    .copied()
                    .collect::<Vec<_>>()
                    .into();
            }
            cursor
        });
        let expected = if cursor.next == 0 {
            Err(StreamError::InvalidArgument)
        } else {
            Ok(wire::SessionCursor {
                next: cursor.next,
                opaque: anchor.to_vec().into(),
            })
        };
        require(encoded == expected, "snapshot encode bytes")?;
    }
    Ok(())
}

pub(super) fn check(
    fuzz_input: crate::fuzz_support::FuzzInput<'_>,
    fault: Fault,
) -> Result<(), &'static str> {
    let Some((&kind, input)) = fuzz_input.bytes().split_first() else {
        return Ok(());
    };
    if input.len() > 4096 {
        return Ok(());
    }
    let fuzz_input = fuzz_input
        .view(input)
        .unwrap_or_else(|| unreachable!("tagged raw input slice"));
    match kind % 3 {
        0 => {
            let Some(subscription) =
                fuzz_support::FuzzInput::parse(fuzz_input, wire::SessionSubscription::decode)
            else {
                return Ok(());
            };
            round_trip(&subscription);
            if let (Some(wire::session_subscription::Kind::Follow(follow)), Some(resume)) =
                (&subscription.kind, &subscription.resume)
            {
                guard(follow, resume, fault)?;
            }
        }
        1 => {
            let Some(cursor) =
                fuzz_support::FuzzInput::parse(fuzz_input, wire::SessionCursor::decode)
            else {
                return Ok(());
            };
            round_trip(&cursor);
            snapshot(&cursor, fault)?;
        }
        _ => {
            let Some(response) =
                fuzz_support::FuzzInput::parse(fuzz_input, wire::TailResponse::decode)
            else {
                return Ok(());
            };
            round_trip(&response);
            let expected = match response.durable {
                None => Err(StreamError::Unsupported),
                Some(durable) if durable > response.tail => Err(StreamError::Unavailable),
                Some(durable) => Ok(StreamWatermarks {
                    accepted: response.tail,
                    durable,
                }),
            };
            let accepted = response.tail;
            let actual = watermarks_from_wire(response);
            let actual = if fault == Fault::InferDurability {
                Ok(StreamWatermarks {
                    accepted,
                    durable: accepted,
                })
            } else {
                actual
            };
            require(actual == expected, "durability boundary")?;
        }
    }
    Ok(())
}
