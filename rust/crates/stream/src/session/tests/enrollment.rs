use super::*;
use acyclic_auth::contract::{Grant, GrantKind, GrantSet, ResourceGrant, ResourcePattern};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Custody(tokio::sync::watch::Receiver<()>);

impl CredentialCustody for Custody {
    fn check(&self) -> Result<(), StreamError> {
        self.0
            .has_changed()
            .map(|_| ())
            .map_err(|_| StreamError::Unavailable)
    }

    fn terminated(&self) -> crate::BoxProviderFuture<'static, StreamError> {
        let mut owner = self.0.clone();
        Box::pin(async move {
            // This producer publishes only its loss, never a readiness probe.
            match owner.changed().await {
                Ok(()) => StreamError::AccessDenied,
                Err(_) => StreamError::Unavailable,
            }
        })
    }
}

struct Enrollment {
    token: Bytes,
    state: Arc<dyn AuthorityState>,
    seal: bool,
    custody: Arc<Custody>,
    calls: Arc<AtomicUsize>,
}

impl CallProvider for Enrollment {
    fn resource(&self, _: &[u8]) -> Result<String, StreamError> {
        Ok("actors:/selected".into())
    }

    fn call(
        &self,
        _: Bytes,
        authorization: CallAuthorization,
    ) -> crate::BoxProviderFuture<'static, Result<CallReply, StreamError>> {
        let token = self.token.clone();
        let state = self.state.clone();
        let seal = self.seal;
        let custody = self.custody.clone();
        let calls = self.calls.clone();
        Box::pin(async move {
            assert!(authorization.enrolling());
            authorization.authorize(
                "actors:/selected",
                &[],
                &Default::default(),
                acyclic_auth::authority::PolicyDecision::Allow,
            )?;
            let snapshot = state.snapshot().map_err(|_| StreamError::AccessDenied)?;
            let capability = acyclic_auth::verify::verify(
                &token,
                snapshot.trust.as_ref(),
                snapshot.now_unix_seconds,
            )
            .map_err(|_| StreamError::AccessDenied)?;
            let capability = if seal {
                capability.seal().map_err(|_| StreamError::AccessDenied)?
            } else {
                capability
            };
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(CallReply::enrolled(
                Bytes::from_static(b"selected"),
                CredentialEnrollment::new(capability, custody),
            ))
        })
    }
}

struct SuspendedIssuance {
    started: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}

impl CallProvider for SuspendedIssuance {
    fn resource(&self, _: &[u8]) -> Result<String, StreamError> {
        Ok("actors:/selected".into())
    }

    fn call(
        &self,
        _: Bytes,
        authorization: CallAuthorization,
    ) -> crate::BoxProviderFuture<'static, Result<CallReply, StreamError>> {
        let started = self.started.clone();
        let dropped = self.dropped.clone();
        Box::pin(async move {
            struct Held(Arc<AtomicUsize>);
            impl Drop for Held {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::SeqCst);
                }
            }
            let _held = Held(dropped);
            let _original = authorization.contribution()?;
            started.fetch_add(1, Ordering::SeqCst);
            future::pending().await
        })
    }
}

fn token(other_principal: bool) -> Bytes {
    acyclic_auth::testing::root(|block| {
        let key = &mut block.certificates.as_mut().unwrap().keys[0].key;
        if other_principal {
            key.principal.id = "another-caller".into();
        }
        key.grants = GrantSet {
            grants: [
                ("actors:/selected", false, "actors/invoke"),
                ("streams:/selected", false, "streams/follow"),
            ]
            .into_iter()
            .map(|(name, prefix, action)| Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: name.into(),
                        prefix,
                    },
                    actions: vec![action.into()],
                }),
            })
            .collect(),
        };
    })
    .into()
}

fn request(id: u64, slot: u32) -> wire::SessionSubscription {
    wire::SessionSubscription {
        channel: Some(wire::SessionChannel {
            channel_id: id,
            generation: 1,
        }),
        resume: Some(cursor(0)),
        credential: 0,
        filter: None,
        kind: Some(Kind::ActorEnrollment(wire::SessionActorEnrollment {
            invocation: Bytes::from_static(b"select"),
            credential_slot: slot,
        })),
    }
}

fn fixture(
    other_principal: bool,
) -> (
    Session<MemoryStream>,
    tokio::sync::watch::Sender<()>,
    Arc<AtomicUsize>,
) {
    fixture_with_proof(other_principal, true)
}

fn fixture_with_proof(
    other_principal: bool,
    seal: bool,
) -> (
    Session<MemoryStream>,
    tokio::sync::watch::Sender<()>,
    Arc<AtomicUsize>,
) {
    let mut session = properties::empty_session(false);
    session.channels.clear();
    session.incoming = stream::pending().boxed();
    session
        .authority
        .register(
            &mut session.slots,
            0,
            &token(false),
            session.limits.channels,
        )
        .unwrap();
    let (owner, changed) = tokio::sync::watch::channel(());
    let calls = Arc::new(AtomicUsize::new(0));
    session.calls = Some(Arc::new(Enrollment {
        token: token(other_principal),
        state: session.authority.state.clone(),
        seal,
        custody: Arc::new(Custody(changed)),
        calls: calls.clone(),
    }));
    (session, owner, calls)
}

async fn enroll(session: &mut Session<MemoryStream>) -> TestResult {
    session.interest(wire::SessionInterest {
        revision: 2,
        upsert: vec![request(1, 1)],
        remove: vec![],
    })?;
    session.credit = wire::SessionWindow {
        bytes: 1_000_000,
        frames: 100,
    };
    session.channels.get_mut(&1).unwrap().credit = session.credit;
    assert!(matches!(
        session.next_frame().await?.unwrap().wire().body,
        Some(Body::InterestApplied(_))
    ));
    assert!(matches!(
        session.next_frame().await?.unwrap().wire().body,
        Some(Body::Opened(_))
    ));
    let reply = session.next_frame().await?.unwrap();
    assert!(
        matches!(&reply.wire().body, Some(Body::Data(wire::SessionData { item: Some(Item::ActorCall(bytes)), .. })) if bytes.as_ref() == b"selected")
    );
    // The first reply fragment is already sufficient to use the SAME slot.
    session
        .authority
        .bind(
            &session.slots,
            1,
            &StreamPath::new("selected")?,
            Default::default(),
            1,
        )
        .unwrap();
    Ok(())
}

#[tokio::test]
async fn caller_loss_fences_suspended_issuance_before_enrollment_without_credit() -> TestResult {
    for dispatch in [false, true] {
        for omit_terminal in [false, true] {
            let (mut session, _owner, _) = fixture(false);
            let started = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicUsize::new(0));
            session.calls = Some(Arc::new(SuspendedIssuance {
                started: started.clone(),
                dropped: dropped.clone(),
            }));
            session.interest(wire::SessionInterest {
                revision: 2,
                upsert: vec![request(1, 1)],
                remove: vec![],
            })?;
            assert!(matches!(
                session.next_frame().await?.unwrap().wire().body,
                Some(Body::InterestApplied(_))
            ));
            assert!(matches!(
                session.next_frame().await?.unwrap().wire().body,
                Some(Body::Opened(_))
            ));
            if dispatch {
                session.credit = wire::SessionWindow {
                    bytes: 100,
                    frames: 1,
                };
                session.channels.get_mut(&1).unwrap().credit = session.credit;
                session.refresh_source(1);
                assert!(futures::poll!(std::pin::pin!(session.next_frame())).is_pending());
            }
            assert_eq!(started.load(Ordering::SeqCst), usize::from(dispatch));
            session.credit = Default::default();
            session.channels.get_mut(&1).unwrap().credit = Default::default();
            session.refresh_source(1);
            if omit_terminal {
                // Negative control: remove only the original caller's wait.
                session.terminals.remove(1);
            }
            // A new, equivalent slot cannot revive the dropped original owner.
            // Bypass incoming-frame revalidation to isolate that owner's event.
            session
                .authority
                .register(&mut session.slots, 0, &[], session.limits.channels)?;
            session.authority.register(
                &mut session.slots,
                0,
                &token(false),
                session.limits.channels,
            )?;
            let closed = futures::poll!(std::pin::pin!(session.next_frame()));
            if omit_terminal {
                assert!(closed.is_pending());
                assert_eq!(dropped.load(Ordering::SeqCst), 0);
                assert_eq!(
                    session.slots.can_reserve(1, session.limits.channels),
                    Err(StreamError::AlreadyExists)
                );
                session.remove(1)?;
            } else {
                let std::task::Poll::Ready(closed) = closed else {
                    panic!("original caller loss must end suspended issuance without credit");
                };
                let closed = closed?.ok_or("caller loss ended the physical session")?;
                assert_eq!(closed.wire().channel_id, 1);
                assert!(
                    matches!(&closed.wire().body, Some(Body::Closed(value)) if value.code == "unavailable")
                );
                session.slots.can_reserve(1, session.limits.channels)?;
            }
            assert_eq!(started.load(Ordering::SeqCst), usize::from(dispatch));
            assert_eq!(dropped.load(Ordering::SeqCst), usize::from(dispatch));
        }
    }
    Ok(())
}

#[tokio::test]
async fn terminal_loss_discards_ended_reply_without_credit_and_ignores_stale_generation()
-> TestResult {
    let (mut session, _owner, _) = fixture(false);
    enroll(&mut session).await?;
    session.credit = Default::default();
    session.channels.get_mut(&1).unwrap().credit = Default::default();
    let identity = wire::SessionChannel {
        channel_id: 1,
        generation: 1,
    };
    session.record(
        identity,
        Ok(Event::CallChunk {
            index: 1,
            total: 8,
            bytes: Bytes::from_static(b"withheld"),
            item: Item::ActorCall,
        }),
    )?;
    session.record(identity, Ok(Event::End))?;
    assert_eq!(session.pending_frames, 1);
    assert!(session.channels[&1].ending.is_some());
    session.record(
        wire::SessionChannel {
            generation: 0,
            ..identity
        },
        Ok(Event::Terminated(StreamError::Unavailable)),
    )?;
    assert_eq!(session.pending_frames, 1);
    // The finite data source is already gone; its original terminal still owns closure.
    session.record(identity, Ok(Event::Terminated(StreamError::Unavailable)))?;
    let closed = session
        .next_frame()
        .await?
        .ok_or("terminal ended the physical session")?;
    assert!(
        matches!(&closed.wire().body, Some(Body::Closed(value)) if value.code == "unavailable")
    );
    assert_eq!(session.pending_frames, 0);
    assert_eq!(session.pending_bytes, 0);
    Ok(())
}

#[tokio::test]
async fn enrollment_precedes_disclosure_and_both_original_losses_close_without_credit() -> TestResult
{
    for caller_lost in [false, true] {
        let (mut session, owner, calls) = fixture(false);
        append(
            &session.provider,
            "selected",
            Bytes::from_static(b"private"),
        )
        .await?;
        enroll(&mut session).await?;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        session.remove(1)?;
        session.credit = Default::default();
        let mut follow = subscribe(3, "selected", 0, false);
        follow.credential = 1;
        session.interest(wire::SessionInterest {
            revision: 3,
            upsert: vec![follow],
            remove: vec![],
        })?;
        assert!(matches!(
            session.next_frame().await?.unwrap().wire().body,
            Some(Body::InterestApplied(_))
        ));
        assert!(matches!(
            session.next_frame().await?.unwrap().wire().body,
            Some(Body::Opened(_))
        ));
        assert!(futures::poll!(std::pin::pin!(session.next_frame())).is_pending());
        assert_eq!(
            session.channels[&3].pending.len(),
            1,
            "negative control: withheld data is actually queued"
        );
        if caller_lost {
            // Bypass the normal incoming-frame revalidation to prove that the
            // original slot producer, rather than that incidental wake, ends it.
            session
                .authority
                .register(&mut session.slots, 0, &[], session.limits.channels)?;
        } else {
            drop(owner);
        }
        let std::task::Poll::Ready(closed) = futures::poll!(std::pin::pin!(session.next_frame()))
        else {
            panic!("original producer loss must complete without data credit");
        };
        let closed = closed?.ok_or("original producer loss ended the session")?;
        assert_eq!(closed.wire().channel_id, 3);
        assert!(
            matches!(&closed.wire().body, Some(Body::Closed(value)) if value.code == "unavailable")
        );
        assert_eq!(session.pending_frames, 0);
        assert!(
            session
                .authority
                .bind(
                    &session.slots,
                    1,
                    &StreamPath::new("selected")?,
                    Default::default(),
                    1
                )
                .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
async fn enrollment_requires_sealed_original_identity_and_an_exclusive_slot() -> TestResult {
    for (other_principal, seal, lose_owner, code) in [
        (true, true, false, "access_denied"),
        (false, false, false, "access_denied"),
        (false, true, true, "unavailable"),
    ] {
        let (mut session, owner, calls) = fixture_with_proof(other_principal, seal);
        let mut owner = Some(owner);
        if lose_owner {
            drop(owner.take());
        }
        session.interest(wire::SessionInterest {
            revision: 2,
            upsert: vec![request(1, 1)],
            remove: vec![],
        })?;
        session.credit = wire::SessionWindow {
            bytes: 1_000_000,
            frames: 100,
        };
        session.channels.get_mut(&1).unwrap().credit = session.credit;
        session.next_frame().await?;
        session.next_frame().await?;
        let closed = session.next_frame().await?.unwrap();
        assert!(matches!(&closed.wire().body, Some(Body::Closed(value)) if value.code == code));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        session.slots.can_reserve(1, session.limits.channels)?;
    }

    let (mut session, _owner, calls) = fixture(false);
    assert_eq!(
        session.interest(wire::SessionInterest {
            revision: 2,
            upsert: vec![request(1, 1), request(3, 1)],
            remove: vec![]
        }),
        Err(StreamError::InvalidArgument)
    );
    assert!(session.channels.is_empty());
    session.slots.can_reserve(1, session.limits.channels)?;
    session.interest(wire::SessionInterest {
        revision: 2,
        upsert: vec![request(1, 1)],
        remove: vec![],
    })?;
    assert_eq!(
        session.slots.can_reserve(1, session.limits.channels),
        Err(StreamError::AlreadyExists)
    );
    assert_eq!(
        session.authority.register(
            &mut session.slots,
            1,
            &token(false),
            session.limits.channels
        ),
        Err(StreamError::AlreadyExists)
    );
    session.remove(1)?;
    session.slots.can_reserve(1, session.limits.channels)?;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "cancelled admission cannot dispatch"
    );
    Ok(())
}

#[tokio::test]
async fn completed_call_does_not_own_enrollment_and_new_session_cannot_replay_it() -> TestResult {
    let (mut session, _owner, _) = fixture(false);
    enroll(&mut session).await?;
    session.remove(1)?;
    session
        .authority
        .bind(
            &session.slots,
            1,
            &StreamPath::new("selected")?,
            Default::default(),
            1,
        )
        .unwrap();
    assert_eq!(
        session.authority.register(
            &mut session.slots,
            1,
            &token(false),
            session.limits.channels
        ),
        Err(StreamError::AlreadyExists)
    );
    let (replacement, _replacement_owner, _) = fixture(false);
    assert!(
        replacement
            .authority
            .bind(
                &replacement.slots,
                1,
                &StreamPath::new("selected")?,
                Default::default(),
                1
            )
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn client_enrollment_uses_the_original_pool_and_target_slot() -> TestResult {
    let (session, _owner, calls) = fixture(false);
    append(
        &session.provider,
        "selected",
        Bytes::from_static(b"private"),
    )
    .await?;
    let client = Client::in_process_with_products(
        session.provider,
        session.authority,
        None,
        session.calls.ok_or("missing original call provider")?,
        Arc::new(crate::runtime::PlatformExecutor),
        None,
    );
    client.register_credential(0, token(false))?;
    assert_eq!(
        client
            .actor_call_enrolling(&Bytes::from_static(b"select"), 0, 1, 32)
            .await?
            .as_ref(),
        b"selected"
    );
    let mut follow = subscribe(3, "selected", 0, false);
    follow.channel = None;
    follow.credential = 1;
    let mut records = client.subscribe(follow).await?;
    let record = records
        .next()
        .await
        .ok_or("selected follow ended")??
        .into_data()?;
    assert!(
        matches!(record.item, Some(Item::Record(record)) if record.value.as_ref() == b"private")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}
