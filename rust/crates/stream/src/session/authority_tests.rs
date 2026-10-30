use super::*;
use acyclic_auth::contract::*;
use acyclic_auth::testing as signed;
use acyclic_auth::verify;
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
            trust: self.trust.read().map_err(|_| VerifyError::Trust)?.clone(),
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
        for certificate in block
            .certificates
            .iter_mut()
            .flat_map(|chain| chain.keys.iter_mut())
            .take(1)
        {
            certificate.key.grants = GrantSet {
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
        }
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
        &StreamPath::new(path).map_err(|_| Rejection::Forbidden)?,
        wire::SessionFilter::default(),
        1,
    )
}

#[tokio::test]
async fn revision_only_bindings_recheck_credential_context_without_resource_access() {
    let state = State::new();
    let auth = Arc::new(authority(state.clone()));
    let mut slots = Slots::default();
    let bytes = token("allowed", |block| {
        block.caveats.push(Caveat {
            kind: CaveatKind::Limits(acyclic_auth::contract::Limits {
                rps: None,
                burst: None,
                bytes_per_s: None,
                channels: Some(1),
                concurrency: None,
                bytes: None,
                spend_micros: None,
                tokens: None,
            }),
        });
        block.caveats.push(Caveat {
            kind: CaveatKind::Audience(ResourcePattern {
                name: "workers:/test".into(),
                prefix: false,
            }),
        });
        block.caveats.push(Caveat {
            kind: CaveatKind::Holder(Holder {
                account: Some("account".into()),
            }),
        });
    });
    auth.register(&mut slots, 0, &bytes, 2).await.unwrap();
    let request = crate::HeadsRequest {
        retained: Default::default(),
        durable_through: None,
        projection: Default::default(),
        paths: Default::default(),
        children: None,
        limit: 1,
        at_least: Default::default(),
    };
    let (mut binding, _, _) = auth.bind_heads(&slots, 0, &request, 1).unwrap();
    // A context observation conveys neither root access nor record access.
    let named = crate::HeadsRequest {
        paths: [StreamPath::new("allowed").unwrap()].into(),
        ..request.clone()
    };
    assert!(auth.bind_heads(&slots, 0, &named, 1).is_err());
    assert!(auth.bind_heads(&slots, 0, &request, 2).is_err());
    let other_holder = Arc::new(
        Authority::new(state.clone(), "workers:/test".into(), Some("other".into())).unwrap(),
    );
    assert!(other_holder.bind_heads(&slots, 0, &request, 1).is_err());
    let other_audience = Arc::new(
        Authority::new(
            state.clone(),
            "workers:/other".into(),
            Some("account".into()),
        )
        .unwrap(),
    );
    assert!(other_audience.bind_heads(&slots, 0, &request, 1).is_err());
    let verified = verify::verify(&bytes, &signed::Snapshot::default(), signed::NOW).unwrap();
    state.publish(
        signed::Snapshot {
            revoked_blocks: vec![*verified.block_ids().first().unwrap()],
            ..Default::default()
        },
        signed::NOW,
    );
    assert_eq!(
        auth.validate(&slots, &mut binding, 1),
        Err(Rejection::Revoked)
    );
    state.publish(signed::Snapshot::default(), 1000);
    assert_eq!(
        auth.validate(&slots, &mut binding, 1),
        Err(Rejection::Expired)
    );
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
    futures::executor::block_on(auth.register(&mut slots, 0, &bytes, 2)).unwrap();
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
        futures::executor::block_on(auth.register(&mut slots, 2, &bytes, 2)),
        Err(StreamError::Capacity)
    );
    let mut forged = bytes.clone();
    if let Some(byte) = forged.get_mut(20) {
        *byte ^= 1;
    }
    futures::executor::block_on(auth.register(&mut slots, 0, &forged, 2)).unwrap();
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
        futures::executor::block_on(auth.register(
            &mut slots,
            0,
            &token("allowed", |block| {
                block.caveats = vec![Caveat {
                    kind: caveat.clone(),
                }];
            }),
            2,
        ))
        .unwrap();
        assert!(bind(&auth, &slots, "allowed").is_err());
    }
    futures::executor::block_on(auth.register(&mut slots, 0, &bytes, 2)).unwrap();
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
    futures::executor::block_on(auth.register(&mut slots, 0, &bytes, 2)).unwrap();
    let (mut binding, _) = bind(&auth, &slots, "allowed").unwrap();
    let revision = auth.snapshot().unwrap().revision;
    let changed = auth.changed(revision, auth.deadline(&mut slots));
    futures::pin_mut!(changed);
    assert!(futures::poll!(changed.as_mut()).is_pending());
    let cap = verify::verify(&bytes, &signed::Snapshot::default(), signed::NOW).unwrap();
    state.publish(
        signed::Snapshot {
            revoked_blocks: vec![*cap.block_ids().first().unwrap()],
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
    futures::executor::block_on(auth.register(&mut slots, 0, &[], 2)).unwrap();
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
        }];
    });
    futures::executor::block_on(auth.register(&mut slots, 0, &bytes, 2)).unwrap();
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
    futures::executor::block_on(auth.register(&mut slots, 0, &plain, 2)).unwrap();
    assert_eq!(
        auth.validate(&slots, &mut binding, 1),
        Err(Rejection::Forbidden)
    );
    let attributes = RecordAttributes::default();
    let usage = signed::limits(None);
    assert!(
        cap.authorize(
            &signed::Snapshot::default(),
            &signed::request("fs:/allowed", &attributes, &usage),
            PolicyDecision::Allow,
            acyclic_auth::authority::Scope::Follow
        )
        .is_err(),
        "record deferral cannot authorize another action"
    );
}

#[tokio::test]
async fn live_sessions_close_idle_and_queued_follows_before_disclosure() {
    use crate::{AppendRequest, MemoryStream};
    for queued in [false, true] {
        for reason in ["revoked", "credential_expired", "rotation", "drop"] {
            let state = State::new();
            let provider = Arc::new(MemoryStream::default());
            let path = StreamPath::new("allowed/events").unwrap();
            let append = |value: &'static [u8]| {
                let provider = provider.clone();
                let path = path.clone();
                async move {
                    provider
                        .append(AppendRequest {
                            path,
                            records: vec![Bytes::from_static(value).into()],
                            if_tail: None,
                            idempotency_key: crate::IdempotencyKey::unique(),
                        })
                        .await
                        .unwrap();
                }
            };
            append(b"seed").await;
            let bytes = token("allowed", |_| {});
            let (sender, mut output) =
                live_session(provider.clone(), state.clone(), bytes.clone(), &path).unwrap();
            let mut opened = false;
            for _ in 0..3 {
                let message =
                    tokio::time::timeout(std::time::Duration::from_secs(1), output.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                opened |= matches!(message.body, Some(Body::Opened(_)));
            }
            assert!(opened);
            sender
                .unbounded_send(Ok(frame(
                    1,
                    1,
                    Body::Window(wire::SessionWindow {
                        bytes: 1_000_000,
                        frames: 1,
                    }),
                )))
                .unwrap();
            append(b"positive control").await;
            let permitted = tokio::time::timeout(std::time::Duration::from_secs(1), output.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(
                matches!(permitted.body, Some(Body::Data(_))),
                "unrevoked follow must deliver"
            );
            if queued {
                append(b"must remain private").await;
            }
            assert!(futures::poll!(output.next()).is_pending());
            invalidate_session(reason, &state, &bytes, &sender);
            let closed = tokio::time::timeout(std::time::Duration::from_secs(1), output.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(closed.channel_id, 1);
            let Some(Body::Closed(closed)) = closed.body else {
                panic!("disclosed data after {reason}, queued={queued}");
            };
            assert_eq!(
                closed.code,
                if matches!(reason, "rotation" | "drop") {
                    "forbidden"
                } else {
                    reason
                }
            );
            sender
                .unbounded_send(Ok(frame(
                    1,
                    1,
                    Body::Window(wire::SessionWindow {
                        bytes: 1_000_000,
                        frames: 1,
                    }),
                )))
                .unwrap();
            // A closed generation cannot be revived by delayed credit.
            let result = futures::poll!(output.next());
            assert!(!matches!(
                result,
                std::task::Poll::Ready(Some(Ok(wire::SessionFrame {
                    body: Some(Body::Data(_)),
                    ..
                })))
            ));
        }
    }
}

type LiveSender = futures::channel::mpsc::UnboundedSender<Result<wire::SessionFrame, StreamError>>;
fn invalidate_session(reason: &str, state: &State, bytes: &[u8], sender: &LiveSender) {
    match reason {
        "revoked" => {
            let capability =
                verify::verify(bytes, &signed::Snapshot::default(), signed::NOW).unwrap();
            state.publish(
                signed::Snapshot {
                    revoked_blocks: vec![*capability.block_ids().first().unwrap()],
                    ..Default::default()
                },
                signed::NOW,
            );
        }
        "credential_expired" => state.publish(signed::Snapshot::default(), 1000),
        _ => {
            let replacement = if reason == "drop" {
                vec![]
            } else {
                token("other", |_| {})
            };
            sender
                .unbounded_send(Ok(frame(
                    0,
                    0,
                    Body::Credential(wire::SessionCredential {
                        slot: 0,
                        credential: replacement.into(),
                    }),
                )))
                .unwrap();
        }
    }
}

fn live_session(
    provider: Arc<crate::MemoryStream>,
    state: Arc<State>,
    bytes: Vec<u8>,
    path: &StreamPath,
) -> Result<(LiveSender, transport::Responses), Box<dyn std::error::Error>> {
    use futures::channel::mpsc;
    let (sender, receiver) = mpsc::unbounded();
    sender.unbounded_send(Ok(frame(
        0,
        0,
        Body::Hello(wire::SessionHello {
            version: 1,
            client_id: Bytes::from_static(b"authority-control"),
            destination_node: "node".into(),
            receive_bytes: 1_000_000,
            receive_frames: 100,
            credentials: vec![bytes.into()],
        }),
    )))?;
    sender.unbounded_send(Ok(frame(
        0,
        0,
        Body::Interest(wire::SessionInterest {
            revision: 1,
            upsert: vec![wire::SessionSubscription {
                channel: Some(wire::SessionChannel {
                    channel_id: 1,
                    generation: 1,
                }),
                resume: Some(cursor(1)),
                credential: 0,
                filter: None,
                kind: Some(Kind::Follow(wire::SessionFollow {
                    resource: crate::wire_codec::session::stream_resource(path.as_str()),
                    delivery: wire::SessionDelivery::All as i32,
                    ..Default::default()
                })),
            }],
            remove: vec![],
        }),
    )))?;
    let output = serve(
        provider,
        Arc::new(authority(state)),
        "node".into(),
        crate::session::Limits::default(),
        receiver.boxed(),
    )?;
    Ok((sender, output))
}

fn fs_token(resource: &str, change: impl Fn(&mut Block)) -> Vec<u8> {
    signed::root(|block| {
        for certificate in block
            .certificates
            .iter_mut()
            .flat_map(|chain| chain.keys.iter_mut())
            .take(1)
        {
            certificate.key.grants = GrantSet {
                grants: vec![Grant {
                    kind: GrantKind::Resource(ResourceGrant {
                        resource: ResourcePattern {
                            name: resource.into(),
                            prefix: true,
                        },
                        actions: vec!["fs/list".into()],
                    }),
                }],
            };
        }
        change(block);
    })
}
#[test]
fn filesystem_discovery_defers_only_list_and_checks_full_paths_and_deny_wins() {
    let auth = Arc::new(authority(State::new()));
    let mut slots = Slots::default();
    let bytes = fs_token("fs:/volume/public", |_| {});
    futures::executor::block_on(auth.register(&mut slots, 0, &bytes, 2)).unwrap();
    let (_, filter, context) = auth
        .bind_resource(&slots, 0, "fs:/volume".into(), Default::default(), 1)
        .unwrap();
    let attributes = RecordAttributes::try_new(vec![]).unwrap();
    assert!(
        context
            .authorize(
                "fs:/volume/public/file",
                &[],
                &attributes,
                PolicyDecision::Allow
            )
            .is_ok()
    );
    for name in [
        "fs:/volume/private",
        "fs:/volume/public-other",
        "fs:/other/public",
    ] {
        assert!(
            context
                .authorize(name, &[], &attributes, PolicyDecision::Allow)
                .is_err()
        );
    }
    assert!(
        context
            .authorize("fs:/volume/public", &[], &attributes, PolicyDecision::Deny)
            .is_err()
    );
    let indexed = context.filter_paths("fs:/volume", &filter).unwrap();
    for key in [b"public".as_slice(), b"public/file"] {
        assert!(
            indexed.matches(&crate::RecordHeaders::new(vec![key.to_vec().into()], vec![]).unwrap())
        );
    }
    for key in [b"private".as_slice(), b"public-other", b"public0"] {
        assert!(
            !indexed
                .matches(&crate::RecordHeaders::new(vec![key.to_vec().into()], vec![]).unwrap())
        );
    }
    let cap = verify::verify(&bytes, &signed::Snapshot::default(), signed::NOW).unwrap();
    let usage = signed::limits(None);
    assert!(
        cap.authorize(
            &signed::Snapshot::default(),
            &signed::request("fs:/volume/public", &attributes, &usage),
            PolicyDecision::Allow,
            acyclic_auth::authority::Scope::ResourceFollow
        )
        .is_err(),
        "fs/read cannot use discovery deferral"
    );
}
#[test]
fn filesystem_prefix_grant_trailing_slash_indexes_the_same_subtree() {
    let auth = Arc::new(authority(State::new()));
    let mut slots = Slots::default();
    futures::executor::block_on(auth.register(
        &mut slots,
        0,
        &fs_token("fs:/volume/public/", |_| {}),
        2,
    ))
    .unwrap();
    let (_, filter, context) = auth
        .bind_resource(&slots, 0, "fs:/volume".into(), Default::default(), 1)
        .unwrap();
    let indexed = context.filter_paths("fs:/volume", &filter).unwrap();
    let matches = |key: &[u8]| {
        indexed.matches(&crate::RecordHeaders::new(vec![key.to_vec().into()], vec![]).unwrap())
    };
    assert!(matches(b"public/file"));
    assert!(!matches(b"public-other"));
}
#[test]
fn filesystem_context_borrows_its_resource_and_rechecks_current_authority() {
    for revoke in [false, true] {
        let state = State::new();
        let auth = Arc::new(authority(state.clone()));
        let mut slots = Slots::default();
        let bytes = fs_token("fs:/volume/public", |_| {});
        futures::executor::block_on(auth.register(&mut slots, 0, &bytes, 2)).unwrap();
        let (_, _, context) = auth
            .bind_resource(&slots, 0, "fs:/volume".into(), Default::default(), 1)
            .unwrap();
        let attributes = RecordAttributes::try_new(vec![]).unwrap();
        for checks in [1, 100, 1000] {
            let measured = acyclic_test_support::measure(|| {
                for _ in 0..checks {
                    assert_eq!(
                        context.authorize(
                            "fs:/volume/public",
                            &[],
                            &attributes,
                            PolicyDecision::Allow
                        ),
                        Ok(())
                    );
                }
            });
            assert_eq!(measured.count_total, 0);
            // The former per-check reconstruction must fail the same oracle.
            let copied = acyclic_test_support::measure(|| {
                for _ in 0..checks {
                    std::hint::black_box(ResourcePattern {
                        name: context.resource.name.clone(),
                        prefix: true,
                    });
                }
            });
            assert_eq!(copied.count_total, checks);
        }
        let cap = verify::verify(&bytes, &signed::Snapshot::default(), signed::NOW).unwrap();
        state.publish(
            signed::Snapshot {
                revoked_blocks: if revoke {
                    vec![*cap.block_ids().first().unwrap()]
                } else {
                    vec![]
                },
                ..Default::default()
            },
            if revoke { signed::NOW } else { 1000 },
        );
        assert!(
            context
                .authorize("fs:/volume/public", &[], &attributes, PolicyDecision::Allow)
                .is_err()
        );
    }
    let auth = Arc::new(authority(State::new()));
    let mut slots = Slots::default();
    futures::executor::block_on(auth.register(&mut slots, 0, &fs_token("fs:/volume", |_| {}), 2))
        .unwrap();
    let (mut binding, _, _) = auth
        .bind_resource(&slots, 0, "fs:/volume".into(), Default::default(), 1)
        .unwrap();
    futures::executor::block_on(auth.register(
        &mut slots,
        0,
        &fs_token("fs:/volume/public", |_| {}),
        2,
    ))
    .unwrap();
    assert!(
        matches!(
            auth.validate(&slots, &mut binding, 1),
            Err(Rejection::Forbidden)
        ),
        "queued entries must not retain the former grant ceiling"
    );
}
#[test]
fn filesystem_context_rejects_forged_expired_and_wrong_audience_credentials() {
    let auth = Arc::new(authority(State::new()));
    let mut slots = Slots::default();
    let valid = fs_token("fs:/volume", |_| {});
    let mut forged = valid;
    if let Some(byte) = forged.get_mut(20) {
        *byte ^= 1;
    }
    for bytes in [
        forged,
        fs_token("fs:/volume", |block| {
            block.caveats.push(Caveat {
                kind: CaveatKind::ExpiresAtUnixSeconds(signed::NOW),
            });
        }),
        fs_token("fs:/volume", |block| {
            block.caveats.push(Caveat {
                kind: CaveatKind::Audience(ResourcePattern {
                    name: "workers:/other".into(),
                    prefix: false,
                }),
            });
        }),
    ] {
        futures::executor::block_on(auth.register(&mut slots, 0, &bytes, 2)).unwrap();
        assert!(
            auth.bind_resource(&slots, 0, "fs:/volume".into(), Default::default(), 1)
                .is_err()
        );
    }
}

/// A gateway's own credential format, `gateway:<principal>:<actor>`: not
/// this SDK's capability envelope. Like a keyring- or database-backed
/// verifier, it awaits before it decides, and `gateway-down:` stands for its
/// dependency being unreachable.
struct Gateway {
    verified: AtomicU64,
}

/// What the gateway admits: invoking one actor, until its expiry.
struct GatewayAdmission {
    principal: Principal,
    grants: GrantSet,
    actor: String,
    expires: u64,
}

impl CredentialVerifier for Gateway {
    fn verify<'a>(
        &'a self,
        credential: &'a [u8],
        _: &'a TrustSnapshot,
    ) -> futures::future::BoxFuture<'a, Result<Arc<dyn Admission>, VerifyError>> {
        Box::pin(self.admit(credential))
    }
}

impl Gateway {
    async fn admit(&self, credential: &[u8]) -> Result<Arc<dyn Admission>, VerifyError> {
        self.verified.fetch_add(1, Ordering::SeqCst);
        // The verdict waits on a dependency: the session is suspended here,
        // holding no slot.
        tokio::task::yield_now().await;
        let text = std::str::from_utf8(credential).map_err(|_| VerifyError::Malformed)?;
        if text.starts_with("gateway-down:") {
            return Err(VerifyError::Unavailable);
        }
        let mut parts = text.splitn(3, ':');
        let (Some("gateway"), Some(id), Some(actor)) = (parts.next(), parts.next(), parts.next())
        else {
            return Err(VerifyError::Malformed);
        };
        Ok(Arc::new(GatewayAdmission {
            principal: Principal {
                kind: PrincipalKind::User,
                id: id.into(),
                lineage: vec![],
                groups: vec![],
                roles: vec![],
                attributes: vec![],
            },
            grants: GrantSet {
                grants: vec![Grant {
                    kind: GrantKind::Resource(ResourceGrant {
                        resource: ResourcePattern {
                            name: actor.into(),
                            prefix: false,
                        },
                        actions: vec!["actors/invoke".into()],
                    }),
                }],
            },
            actor: actor.into(),
            expires: signed::NOW + 60,
        }))
    }
}

impl Admission for GatewayAdmission {
    fn principal(&self) -> &Principal {
        &self.principal
    }

    fn expires_at_unix_seconds(&self) -> u64 {
        self.expires
    }

    fn grants(&self) -> Box<dyn Iterator<Item = &GrantSet> + '_> {
        Box::new(std::iter::once(&self.grants))
    }

    fn authorize(
        &self,
        _: &(dyn acyclic_auth::verify::Trust + Send + Sync),
        request: &Request<'_>,
        policy: PolicyDecision,
        scope: acyclic_auth::authority::Scope,
    ) -> Result<(), VerifyError> {
        let admitted = matches!(
            (scope, &request.target),
            (acyclic_auth::authority::Scope::Exact, Target::Resource { action: "actors/invoke", resource })
                if *resource == self.actor
        );
        if request.now_unix_seconds >= self.expires {
            return Err(VerifyError::Time);
        }
        if admitted && policy == PolicyDecision::Allow {
            Ok(())
        } else {
            Err(VerifyError::Malformed)
        }
    }

    fn follow_filter(
        &self,
        requested: wire::SessionFilter,
    ) -> Result<crate::FollowFilter, StreamError> {
        crate::FollowFilter::new(requested)
    }
}

#[tokio::test]
async fn a_hosts_own_verifier_is_the_only_one_and_its_admission_is_call_authority() {
    let state = State::new();
    let gateway = Arc::new(Gateway {
        verified: AtomicU64::new(0),
    });
    let auth = Arc::new(authority(state.clone()).verified_by(gateway.clone()));
    let mut slots = Slots::default();
    // The stock envelope is not this session's format: the host's verifier
    // alone admits credentials.
    auth.register(&mut slots, 1, &token("agents", |_| {}), 4)
        .await
        .unwrap();
    assert!(
        auth.bind_call(&slots, 1, "actors:/agents/server".into(), 1)
            .is_err()
    );
    auth.register(&mut slots, 0, b"gateway:alice:actors:/agents/server", 4)
        .await
        .unwrap();
    assert_eq!(gateway.verified.load(Ordering::SeqCst), 2);
    // An unreachable dependency admits nothing, and refuses retryably: not
    // as a verdict on the credential.
    auth.register(
        &mut slots,
        2,
        b"gateway-down:alice:actors:/agents/server",
        4,
    )
    .await
    .unwrap();
    let refused = auth
        .bind_call(&slots, 2, "actors:/agents/server".into(), 1)
        .err();
    assert_eq!(refused, Some(Rejection::Unavailable));
    assert_eq!(
        StreamError::from_session_close(Rejection::Unavailable.code()),
        StreamError::Unavailable
    );
    let (_, call) = auth
        .bind_call(&slots, 0, "actors:/agents/server".into(), 1)
        .unwrap();
    assert_eq!(call.principal().unwrap().id, "alice");
    let attributes = RecordAttributes::try_new(vec![]).unwrap();
    assert!(
        call.authorize(
            "actors:/agents/server",
            &[],
            &attributes,
            PolicyDecision::Allow
        )
        .is_ok()
    );
    assert!(
        call.authorize(
            "actors:/agents/server",
            &[],
            &attributes,
            PolicyDecision::Deny
        )
        .is_err()
    );
    // It names one actor and admits no follow.
    assert!(
        auth.bind_call(&slots, 0, "actors:/agents/other".into(), 1)
            .is_err()
    );
    assert!(
        auth.bind(
            &slots,
            0,
            &StreamPath::new("agents/server").unwrap(),
            Default::default(),
            1
        )
        .is_err()
    );
    // Its expiry closes the call like any credential's.
    state.publish(signed::Snapshot::default(), signed::NOW + 60);
    assert!(
        call.authorize(
            "actors:/agents/server",
            &[],
            &attributes,
            PolicyDecision::Allow
        )
        .is_err()
    );
}
