#![no_main]

pub use acyclic_fuzz_support as fuzz_support;
use acyclic_stream::{
    wire,
    wire_codec::session::{confirm_follow, preceding_follow},
};
use prost::Message;

fuzz_support::fuzz_target!(|input: &[u8], fuzz_input| {
    if input.len() > 4096 {
        return;
    }
    let Some(frame) = fuzz_support::FuzzInput::parse(fuzz_input, wire::SessionFrame::decode) else {
        return;
    };
    let _ = fuzz_support::round_trip(
        &frame,
        wire::SessionFrame::decode(frame.encode_to_vec().as_slice()),
    );
    if let Some(wire::session_frame::Body::Opened(opened)) = &frame.body {
        let anchor = acyclic_stream::CommitId::from_bytes([7; 32]);
        assert_eq!(
            confirm_follow(Some(anchor), opened).is_ok(),
            opened.preceding_commit_id.as_ref() == anchor.as_bytes()
        );
    }
    if let Some(wire::session_frame::Body::Interest(interest)) = frame.body {
        for subscription in interest.upsert {
            if let (Some(wire::session_subscription::Kind::Follow(follow)), Some(resume)) =
                (subscription.kind, subscription.resume)
            {
                let expected_anchor = (follow.preceding_commit_id.len() == 32
                    && follow.delivery == wire::SessionDelivery::All as i32
                    && resume.next > 0
                    && resume.opaque.is_empty())
                .then(|| {
                    acyclic_stream::CommitId::from_bytes(
                        follow
                            .preceding_commit_id
                            .as_ref()
                            .try_into()
                            .expect("checked anchor size"),
                    )
                });
                let expected = if follow.preceding_commit_id.is_empty() {
                    Ok(None)
                } else {
                    expected_anchor
                        .map(Some)
                        .ok_or(acyclic_stream::StreamError::InvalidArgument)
                };
                let _ = fuzz_support::same_result(&expected, preceding_follow(&follow, &resume));
                let Some(anchor) = expected_anchor else {
                    continue;
                };
                assert_eq!(follow.preceding_commit_id.as_ref(), anchor.as_bytes());
                assert!(confirm_follow(Some(anchor), &wire::SessionOpened::default()).is_err());
                let opened = wire::SessionOpened {
                    resume: Some(resume),
                    preceding_commit_id: follow.preceding_commit_id,
                };
                assert!(confirm_follow(Some(anchor), &opened).is_ok());
            }
        }
    }
});
