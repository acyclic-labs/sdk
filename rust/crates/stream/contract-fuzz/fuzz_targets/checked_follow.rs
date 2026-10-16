#![no_main]
use acyclic_stream::{
    wire,
    wire_codec::session::{confirm_follow, preceding_follow},
};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|input: &[u8]| {
    if input.len() > 4096 {
        return;
    }
    let Ok(frame) = wire::SessionFrame::decode(input) else {
        return;
    };
    assert_eq!(
        wire::SessionFrame::decode(frame.encode_to_vec().as_slice())
            .ok()
            .as_ref(),
        Some(&frame)
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
                let decoded = preceding_follow(&follow, &resume);
                if !follow.preceding_commit_id.is_empty()
                    && (follow.preceding_commit_id.len() != 32
                        || follow.delivery != wire::SessionDelivery::All as i32
                        || resume.next == 0
                        || !resume.opaque.is_empty())
                {
                    assert_eq!(decoded, Err(acyclic_stream::StreamError::InvalidArgument));
                }
                let Ok(Some(anchor)) = decoded else {
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
