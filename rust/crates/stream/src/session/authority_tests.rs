use super::*;
use acyclic_auth::contract::*;
use acyclic_auth::testing as signed;
use acyclic_auth::verify;
use std::sync::{
    RwLock,
    atomic::{AtomicU64, Ordering},
};

/// Published trust and clock: trust is swapped before each publication or
/// the owner's loss resolves a registered change.
pub(in crate::session) struct State {
    trust: RwLock<Arc<dyn verify::Trust + Send + Sync>>,
    clock: AtomicU64,
    published: tokio::sync::watch::Sender<Result<(), SourceClosed>>,
    clock_events: tokio::sync::watch::Sender<Result<(), SourceClosed>>,
}
impl State {
    pub(in crate::session) fn new() -> Arc<Self> {
        Self::with_trust(Arc::new(signed::Snapshot::default()))
    }
    pub(in crate::session) fn with_trust(trust: Arc<dyn verify::Trust + Send + Sync>) -> Arc<Self> {
        Arc::new(Self {
            trust: RwLock::new(trust),
            clock: AtomicU64::new(signed::NOW),
            published: tokio::sync::watch::channel(Ok(())).0,
            clock_events: tokio::sync::watch::channel(Ok(())).0,
        })
    }
    pub(in crate::session) fn publish(&self, trust: signed::Snapshot, now: u64) {
        *self.trust.write().unwrap() = Arc::new(trust);
        self.clock.store(now, Ordering::SeqCst);
        let _ = self.published.send_replace(Ok(()));
        let _ = self.clock_events.send_replace(Ok(()));
    }
    /// The trust owner is lost.
    fn lose(&self) {
        let _ = self.published.send_replace(Err(SourceClosed));
    }
    fn lose_clock(&self) {
        let _ = self.clock_events.send_replace(Err(SourceClosed));
    }
}
impl ChangeSource for State {
    fn check_open(&self) -> Result<(), SourceClosed> {
        *self.published.borrow()
    }
    fn changed(&self) -> Changed {
        let mut published = self.published.subscribe();
        Box::pin(async move {
            published.changed().await.map_err(|_| SourceClosed)?;
            *published.borrow_and_update()
        })
    }
}

#[tokio::test]
async fn alarm_producer_loss_terminates_the_original_authorized_session() {
    use crate::{AppendRequest, MemoryStream};
    let state = State::new();
    let auth = authority(state.clone());
    let mut registration = auth.register_change();
    state.lose_clock();
    assert_eq!(
        auth.changed(&mut registration, Some(signed::NOW + 1))
            .now_or_never(),
        Some(AuthorityEvent::ClockLost),
        "a lost expiry producer is not an elapsed deadline",
    );
    for early in [false, true] {
        let state = State::new();
        let provider = Arc::new(MemoryStream::default());
        let path = StreamPath::new("allowed/events").unwrap();
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![Bytes::from_static(b"seed").into()],
                if_tail: None,
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await
            .unwrap();
        let (_sender, mut output) =
            live_session(provider, state.clone(), token("allowed", |_| {}), &path).unwrap();
        for _ in 0..3 {
            output.next().await.unwrap().unwrap();
        }
        if early {
            state.lose_clock();
        }
        let next = output.next();
        futures::pin_mut!(next);
        if !early {
            assert!(futures::poll!(next.as_mut()).is_pending());
            state.lose_clock();
        }
        assert!(
            matches!(
                futures::poll!(next.as_mut()),
                std::task::Poll::Ready(Some(Err(StreamError::Unavailable))),
            ),
            "clock loss must terminate, rather than expire, re-arm or park"
        );
        assert!(
            state.check_open().is_ok(),
            "the original trust feed remains alive"
        );
    }
    let state = State::new();
    let auth = authority(state.clone());
    let mut registration = auth.register_change();
    state.publish(signed::Snapshot::default(), signed::NOW + 1);
    // Consume the independent trust publication before observing real expiry.
    assert_eq!(
        auth.changed(&mut registration, None).await,
        AuthorityEvent::Trust(Ok(()))
    );
    let mut registration = auth.register_change();
    assert_eq!(
        auth.changed(&mut registration, Some(signed::NOW + 1)).await,
        AuthorityEvent::Expired
    );
}
impl AuthorityState for State {
    fn snapshot(&self) -> Result<TrustSnapshot, VerifyError> {
        Ok(TrustSnapshot {
            trust: self.trust.read().map_err(|_| VerifyError::Trust)?.clone(),
            now_unix_seconds: self.clock.load(Ordering::SeqCst),
        })
    }
    fn changes(&self) -> &dyn ChangeSource {
        self
    }
    fn expires_at(&self, deadline: u64) -> crate::BoxProviderFuture<'_, Result<(), SourceClosed>> {
        let mut published = self.clock_events.subscribe();
        Box::pin(async move {
            loop {
                let lost = published.borrow_and_update().is_err();
                if lost {
                    return Err(SourceClosed);
                }
                if self.clock.load(Ordering::SeqCst) >= deadline {
                    return Ok(());
                }
                if published.changed().await.is_err() {
                    return Err(SourceClosed);
                }
            }
        })
    }
}
pub(in crate::session) fn authority(state: Arc<State>) -> Authority {
    Authority::new(state, "workers:/test".into(), Some("account".into())).unwrap()
}

#[tokio::test]
async fn call_contributions_pin_admission_and_end_on_original_producer_loss() {
    fn credential(change: impl Fn(&mut Block)) -> Vec<u8> {
        signed::root(|block| {
            block.certificates.as_mut().unwrap().keys[0].key.grants = GrantSet {
                grants: vec![Grant {
                    kind: GrantKind::Resource(ResourceGrant {
                        resource: ResourcePattern {
                            name: "actors:/selected".into(),
                            prefix: false,
                        },
                        actions: vec!["actors/invoke".into()],
                    }),
                }],
            };
            change(block);
        })
    }
    for (case, registered) in [
        "slot",
        "session",
        "replacement",
        "principal",
        "revocation",
        "trust",
        "clock",
        "expiry",
    ]
    .into_iter()
    .flat_map(|case| [false, true].map(|registered| (case, registered)))
    {
        let state = State::new();
        let auth = Arc::new(authority(state.clone()));
        let mut slots = Slots::default();
        auth.register(&mut slots, 0, &credential(|_| {}), 1)
            .unwrap();
        let (binding, call) = auth
            .bind_call(&slots, 0, "actors:/selected".into(), 1)
            .unwrap();
        let expires = call.2.effective().expires_at_unix_seconds();
        let contribution = call.contribution().unwrap();
        auth.register(&mut slots, 0, &credential(|_| {}), 1)
            .unwrap();
        contribution.check().unwrap();
        drop(binding);
        drop(call);
        contribution.check().unwrap();
        let ended = contribution.terminated();
        futures::pin_mut!(ended);
        if registered {
            assert!(futures::poll!(ended.as_mut()).is_pending());
        }
        match case {
            "slot" => auth.register(&mut slots, 0, &[], 1).unwrap(),
            "session" => drop(slots),
            "replacement" => auth
                .register(
                    &mut slots,
                    0,
                    &credential(|block| {
                        block.certificates.as_mut().unwrap().keys[0]
                            .key
                            .grants
                            .grants
                            .push(Grant {
                                kind: GrantKind::Resource(ResourceGrant {
                                    resource: ResourcePattern {
                                        name: "streams:/extra".into(),
                                        prefix: false,
                                    },
                                    actions: vec!["streams/follow".into()],
                                }),
                            });
                    }),
                    1,
                )
                .unwrap(),
            "principal" => auth
                .register(
                    &mut slots,
                    0,
                    &credential(|block| {
                        block.certificates.as_mut().unwrap().keys[0]
                            .key
                            .principal
                            .id = "other".into();
                    }),
                    1,
                )
                .unwrap(),
            "revocation" => state.publish(
                signed::Snapshot {
                    revoked_keys: vec!["key".into()],
                    ..Default::default()
                },
                signed::NOW,
            ),
            "trust" => state.lose(),
            "clock" => state.lose_clock(),
            "expiry" => state.publish(signed::Snapshot::default(), expires),
            _ => unreachable!(),
        }
        let expected = if matches!(case, "slot" | "session" | "trust" | "clock") {
            StreamError::Unavailable
        } else {
            StreamError::AccessDenied
        };
        assert_eq!(
            futures::poll!(ended.as_mut()),
            std::task::Poll::Ready(expected),
            "{case}, registered={registered}"
        );
    }
    let auth = Arc::new(authority(State::new()));
    let mut slots = Slots::default();
    auth.register(&mut slots, 0, &credential(|_| {}), 1)
        .unwrap();
    let (_, call) = auth
        .bind_call(&slots, 0, "actors:/selected".into(), 1)
        .unwrap();
    auth.register(
        &mut slots,
        0,
        &credential(|block| {
            block.certificates.as_mut().unwrap().keys[0]
                .key
                .principal
                .id = "replacement".into();
        }),
        1,
    )
    .unwrap();
    assert!(
        matches!(call.contribution(), Err(StreamError::AccessDenied)),
        "selection must retain admission before a replacement, never capture the replacement as its origin"
    );
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

#[test]
fn credential_replacement_retires_the_previous_expiry_without_retaining_history() {
    let state = State::new();
    let auth = authority(state.clone());
    let mut slots = Slots::default();
    for extension in 1..=32 {
        let expiry = signed::NOW + extension;
        let bytes = token("allowed", |block| {
            block.caveats.push(Caveat {
                kind: CaveatKind::ExpiresAtUnixSeconds(expiry),
            })
        });
        auth.register(&mut slots, 0, &bytes, 2).unwrap();
        assert_eq!(
            auth.deadline(&mut slots),
            Some(expiry),
            "a replaced expiry must neither wake nor fence the new credential"
        );
        assert_eq!(
            slots.expiries.len(),
            1,
            "cost control: no retained replacement history"
        );
    }
    let earlier = signed::NOW + 1;
    let bytes = token("allowed", |block| {
        block.caveats.push(Caveat {
            kind: CaveatKind::ExpiresAtUnixSeconds(earlier),
        })
    });
    auth.register(&mut slots, 1, &bytes, 2).unwrap();
    assert_eq!(auth.deadline(&mut slots), Some(earlier));
    auth.register(&mut slots, 1, &[], 2).unwrap();
    assert_eq!(auth.deadline(&mut slots), Some(signed::NOW + 32));
    assert_eq!(slots.expiries.len(), 1);
    state.publish(signed::Snapshot::default(), signed::NOW + 32);
    assert_eq!(auth.deadline(&mut slots), None);
    assert!(
        bind(&auth, &slots, "allowed").is_err(),
        "expired authority cannot be revived by retiring its alarm entry"
    );
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

#[test]
fn revision_only_bindings_recheck_credential_context_without_resource_access() {
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
    auth.register(&mut slots, 0, &bytes, 2).unwrap();
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
    if let Some(byte) = forged.get_mut(20) {
        *byte ^= 1;
    }
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
                }];
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
    let mut registration = auth.register_change();
    let deadline = auth.deadline(&mut slots);
    let changed = auth.changed(&mut registration, deadline);
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
    assert_eq!(changed.await, AuthorityEvent::Trust(Ok(())));
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
        }];
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
        for reason in ["revoked", "credential_expired", "rotation", "drop", "lost"] {
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
                let message = output.next().await.unwrap().unwrap();
                opened |= matches!(message.wire().body, Some(Body::Opened(_)));
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
            let permitted = output.next().await.unwrap().unwrap();
            assert!(
                matches!(permitted.wire().body, Some(Body::Data(_))),
                "unrevoked follow must deliver"
            );
            if queued {
                append(b"must remain private").await;
            }
            if !queued {
                assert!(futures::poll!(output.next()).is_pending());
            }
            invalidate_session(reason, &state, &bytes, &sender);
            let closed = output.next().await.unwrap().unwrap();
            assert_eq!(closed.wire().channel_id, 1);
            let Some(Body::Closed(closed)) = closed.into_wire().body else {
                panic!("disclosed data after {reason}, queued={queued}");
            };
            // A lost trust owner fences every channel: no trust can be read.
            let expected = match reason {
                "rotation" | "drop" => "forbidden",
                "lost" => "unavailable",
                reason => reason,
            };
            assert_eq!(closed.code, expected);
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
                result.map(|item| item.map(|item| item.map(Frame::into_wire))),
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
        "lost" => state.lose(),
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
            interest: None,
            version: crate::session::VERSION,
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
    auth.register(&mut slots, 0, &bytes, 2).unwrap();
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
    auth.register(&mut slots, 0, &fs_token("fs:/volume/public/", |_| {}), 2)
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
        auth.register(&mut slots, 0, &bytes, 2).unwrap();
        let (_, _, context) = auth
            .bind_resource(&slots, 0, "fs:/volume".into(), Default::default(), 1)
            .unwrap();
        let attributes = RecordAttributes::try_new(vec![]).unwrap();
        for checks in [1, 100, 1000] {
            let authorize = || {
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
            };
            #[cfg(target_arch = "wasm32")]
            authorize();
            #[cfg(not(target_arch = "wasm32"))]
            {
                let measured = acyclic_test_support::measure(authorize);
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
    auth.register(&mut slots, 0, &fs_token("fs:/volume", |_| {}), 2)
        .unwrap();
    let (mut binding, _, _) = auth
        .bind_resource(&slots, 0, "fs:/volume".into(), Default::default(), 1)
        .unwrap();
    auth.register(&mut slots, 0, &fs_token("fs:/volume/public", |_| {}), 2)
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
        auth.register(&mut slots, 0, &bytes, 2).unwrap();
        assert!(
            auth.bind_resource(&slots, 0, "fs:/volume".into(), Default::default(), 1)
                .is_err()
        );
    }
}
