use super::*;
use acyclic_auth::{contract::*, testing as signed};
use wasm_bindgen_test::wasm_bindgen_test;

fn public_trust(revoked: bool) -> String {
    let snapshot = signed::Snapshot::default();
    serde_json::json!({
        "environment": "test", "environment_keys": snapshot.environments,
        "account": "account", "issuer_keys": snapshot.issuers,
        "minimum_key_epoch": 1,
        "revoked_key_ids": if revoked { vec!["key"] } else { vec![] },
        "revoked_block_ids": [],
    })
    .to_string()
}
fn credential(expired: bool) -> Vec<u8> {
    let now = (Date::now() / 1000.0) as u64;
    signed::root(|block| {
        let key = &mut block
            .certificates
            .as_mut()
            .expect("fixture certificates")
            .keys[0]
            .key;
        key.validity = Validity {
            not_before_unix_seconds: now - 60,
            expires_at_unix_seconds: if expired { now - 1 } else { now + 60 },
        };
        key.grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: "streams:/events".into(),
                        prefix: false,
                    },
                    actions: vec!["streams/follow".into()],
                }),
            }],
        };
    })
}
fn subscription(path: &str) -> wire::SessionSubscription {
    wire::SessionSubscription {
        credential: 2,
        kind: Some(wire::session_subscription::Kind::Follow(
            wire::SessionFollow {
                resource: crate::wire_codec::session::stream_resource(path),
                delivery: wire::SessionDelivery::All as i32,
                ..Default::default()
            },
        )),
        resume: Some(wire::SessionCursor::default()),
        ..Default::default()
    }
}

#[wasm_bindgen_test]
async fn local_session_requires_explicit_trust_and_rejects_forgery_expiry_and_widened_paths() {
    let mut stream = WasmStream::new();
    let request = subscription("events").encode_to_vec();
    assert!(stream.open_data(&request).await.is_err());
    let authority = WasmSessionAuthority::new(
        &public_trust(false),
        "workers:/test".into(),
        Some("account".into()),
    )
    .expect("authority");
    stream.bind_session(&authority).expect("bind");
    assert!(stream.bind_session(&authority).is_err());
    for invalid in [vec![1, 2, 3], credential(true)] {
        stream.register_credential(2, &invalid).expect("register");
        assert!(stream.open_data(&request).await.is_err());
    }
    stream
        .register_credential(2, &credential(false))
        .expect("register");
    assert!(
        stream
            .open_data(&subscription("neighbor").encode_to_vec())
            .await
            .is_err()
    );
}

#[wasm_bindgen_test]
async fn local_session_closes_an_idle_follow_on_revocation_and_rejects_trust_rollback() {
    let mut stream = WasmStream::new();
    let authority = WasmSessionAuthority::new(
        &public_trust(false),
        "workers:/test".into(),
        Some("account".into()),
    )
    .expect("authority");
    stream.bind_session(&authority).expect("bind");
    stream
        .register_credential(2, &credential(false))
        .expect("register");
    stream
        .provider
        .append(crate::AppendRequest {
            path: crate::StreamPath::new("events").expect("path"),
            records: vec![crate::OwnedBytes::from_static(b"first").into()],
            if_tail: None,
            idempotency_key: None,
        })
        .await
        .expect("append");
    let follow = stream
        .open_data(&subscription("events").encode_to_vec())
        .await
        .expect("open");
    let first = follow.next().await.expect("initial data");
    let frame = wire::SessionFrame::decode(Uint8Array::new(&first).to_vec().as_slice())
        .expect("data frame");
    assert!(matches!(
        frame.body,
        Some(wire::session_frame::Body::Data(_))
    ));
    authority.update(&public_trust(true)).expect("revoke");
    assert!(authority.update(&public_trust(false)).is_err());
    assert!(follow.next().await.is_err());
    follow.close();
}
