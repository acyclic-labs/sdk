use super::*;
use acyclic_auth::contract::*;
use acyclic_auth::testing as signed;
use std::sync::{
    RwLock,
    atomic::{AtomicU64, Ordering},
};

struct State {
    trust: RwLock<Arc<signed::Snapshot>>,
    clock: AtomicU64,
    revision: tokio::sync::watch::Sender<u64>,
}
impl State {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            trust: RwLock::new(Arc::new(signed::Snapshot::default())),
            clock: AtomicU64::new(signed::NOW),
            revision: tokio::sync::watch::channel(0).0,
        })
    }
    fn publish(&self, trust: signed::Snapshot, now: u64) {
        *self.trust.write().unwrap() = Arc::new(trust);
        self.clock.store(now, Ordering::SeqCst);
        self.revision.send_modify(|revision| *revision += 1);
    }
}
impl AuthorityState for State {
    fn snapshot(&self) -> Result<TrustSnapshot, VerifyError> {
        Ok(TrustSnapshot {
            trust: self.trust.read().unwrap().clone(),
            now_unix_seconds: self.clock.load(Ordering::SeqCst),
            revision: *self.revision.borrow(),
        })
    }
    fn changed(&self, revision: u64) -> crate::BoxProviderFuture<'_, ()> {
        let mut changed = self.revision.subscribe();
        Box::pin(async move {
            while *changed.borrow_and_update() == revision {
                if changed.changed().await.is_err() {
                    break;
                }
            }
        })
    }
    fn expires_at(&self, deadline: u64) -> crate::BoxProviderFuture<'_, ()> {
        let mut changed = self.revision.subscribe();
        Box::pin(async move {
            while self.clock.load(Ordering::SeqCst) < deadline {
                let _ = changed.borrow_and_update();
                if changed.changed().await.is_err() {
                    break;
                }
            }
        })
    }
}
fn authority(state: Arc<State>) -> Authority {
    Authority::new(state, "workers:/test".into(), Some("account".into())).unwrap()
}
fn token(path: &str, change: impl Fn(&mut Block)) -> Vec<u8> {
    signed::root(|block| {
        block.certificates.as_mut().unwrap().keys[0].key.grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: format!("streams:/{path}"),
                        prefix: true,
                    },
                    actions: vec!["streams/follow".into()],
                }),
            }],
        };
        change(block);
    })
}
fn bind(
    authority: &Authority,
    slots: &Slots,
    path: &str,
) -> Result<(Binding, crate::FollowFilter), Rejection> {
    authority.bind(
        slots,
        0,
        &StreamPath::new(path).unwrap(),
        wire::SessionFilter::default(),
        1,
    )
}
#[test]
fn slots_reject_forgery_expiry_wrong_audience_holder_and_widening() {
    let state = State::new();
    let auth = authority(state.clone());
    let mut slots = Slots::default();
    assert!(matches!(
        bind(&auth, &slots, "allowed"),
        Err(Rejection::Forbidden)
    ));
    let bytes = token("allowed", |_| {});
    auth.register(&mut slots, 0, &bytes, 2).unwrap();
    assert!(bind(&auth, &slots, "allowed/child").is_ok());
    assert!(matches!(
        bind(&auth, &slots, "allowed-other"),
        Err(Rejection::Forbidden)
    ));
    assert!(matches!(
        bind(&auth, &slots, "other"),
        Err(Rejection::Forbidden)
    ));
    assert_eq!(
        auth.register(&mut slots, 2, &bytes, 2),
        Err(StreamError::Capacity)
    );
    let mut forged = bytes.clone();
    forged[20] ^= 1;
    auth.register(&mut slots, 0, &forged, 2).unwrap();
    assert!(matches!(
        bind(&auth, &slots, "allowed"),
        Err(Rejection::Forbidden)
    ));
    for caveat in [
        CaveatKind::ExpiresAtUnixSeconds(signed::NOW),
        CaveatKind::Audience(ResourcePattern {
            name: "workers:/other".into(),
            prefix: false,
        }),
        CaveatKind::Holder(Holder {
            account: Some("other".into()),
        }),
    ] {
        auth.register(
            &mut slots,
            0,
            &token("allowed", |block| {
                block.caveats = vec![Caveat {
                    kind: caveat.clone(),
                }]
            }),
            2,
        )
        .unwrap();
        assert!(bind(&auth, &slots, "allowed").is_err());
    }
    auth.register(&mut slots, 0, &bytes, 2).unwrap();
    let (mut binding, _) = bind(&auth, &slots, "allowed").unwrap();
    state.publish(signed::Snapshot::default(), 1000);
    assert_eq!(
        auth.validate(&slots, &mut binding, 1),
        Err(Rejection::Expired)
    );
}
#[tokio::test]
async fn revocation_and_drop_invalidate_idle_bindings_without_record_credit() {
    let state = State::new();
    let auth = authority(state.clone());
    let mut slots = Slots::default();
    let bytes = token("allowed", |_| {});
    auth.register(&mut slots, 0, &bytes, 2).unwrap();
    let (mut binding, _) = bind(&auth, &slots, "allowed").unwrap();
    let revision = auth.snapshot().unwrap().revision;
    let changed = auth.changed(revision, auth.deadline(&mut slots));
    futures::pin_mut!(changed);
    assert!(futures::poll!(changed.as_mut()).is_pending());
    let cap = verify::verify(&bytes, &signed::Snapshot::default(), signed::NOW).unwrap();
    state.publish(
        signed::Snapshot {
            revoked_blocks: vec![cap.block_ids()[0]],
            ..Default::default()
        },
        signed::NOW,
    );
    changed.await;
    assert_eq!(
        auth.validate(&slots, &mut binding, 1),
        Err(Rejection::Revoked)
    );
    state.publish(signed::Snapshot::default(), signed::NOW);
    auth.register(&mut slots, 0, &[], 2).unwrap();
    assert_eq!(
        auth.validate(&slots, &mut binding, 1),
        Err(Rejection::Forbidden)
    );
}
#[test]
fn rotation_cannot_drop_pins_filters_or_rebind_the_declaring_subject() {
    let state = State::new();
    let auth = authority(state);
    let mut slots = Slots::default();
    let bytes = token("allowed", |block| {
        block.caveats = vec![Caveat {
            kind: CaveatKind::Pin(Pin {
                name: "author".into(),
                value: PinValue::SubjectId(SubjectId {}),
            }),
        }]
    });
    auth.register(&mut slots, 0, &bytes, 2).unwrap();
    let (mut binding, filter) = bind(&auth, &slots, "allowed").unwrap();
    let headers = |author: &'static str| {
        crate::RecordHeaders::new(
            vec![],
            vec![crate::Attribute {
                name: "author".into(),
                value: author.into(),
            }],
        )
        .unwrap()
    };
    assert!(filter.matches(&headers("issuer")));
    assert!(!filter.matches(&headers("child")));
    assert!(
        crate::FollowFilter::default().matches(&headers("child")),
        "omitted predicate control"
    );
    let cap = verify::verify(&bytes, &signed::Snapshot::default(), signed::NOW).unwrap();
    let child = cap
        .attenuate(
            vec![Caveat {
                kind: CaveatKind::LineageAppend(LineageAppend {
                    ids: vec!["child".into()],
                }),
            }],
            &[9; 32],
        )
        .unwrap();
    let child_filter = crate::FollowFilter::authorized(Default::default(), &child).unwrap();
    assert!(child_filter.matches(&headers("issuer")));
    assert!(!child_filter.matches(&headers("child")));
    let plain = token("allowed", |_| {});
    auth.register(&mut slots, 0, &plain, 2).unwrap();
    assert_eq!(
        auth.validate(&slots, &mut binding, 1),
        Err(Rejection::Forbidden)
    );
    let attributes = RecordAttributes::default();
    let usage = signed::limits(None);
    assert!(
        cap.authorize_follow(
            &signed::Snapshot::default(),
            &signed::request("fs:/allowed", &attributes, &usage),
            PolicyDecision::Allow
        )
        .is_err(),
        "record deferral cannot authorize another action"
    );
}
