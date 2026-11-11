//! Peer membership uses the same atomic root creation and archive publications.
use super::*;
use acyclic_harness_core::Capabilities;
use proptest::prelude::*;

#[tokio::test]
async fn archive_closes_a_root_without_peer_membership() -> Result {
    let provider = MemoryStream::default();
    let mut root = create_root(&intent("pre-peer-root"), adopted(id(42)))?;
    let membership = root.child.peer_membership()?;
    // Publish the original root shape, without the optional peer admission.
    for effect in &mut root.request.effects {
        effect
            .conditions
            .retain(|condition| condition.path() != &membership);
        effect
            .mutations
            .retain(|mutation| mutation.path() != &membership);
    }
    submit(&provider, root.request).await?;
    let request = archive(&root.child, &user(), &intent("archive"))?;
    assert!(matches!(
        submit(&provider, request.clone()).await?,
        Submitted::Committed(_)
    ));
    submit(&provider, request).await?;
    assert_eq!(provider.tail(root.child.inbox()?).await?, 2);
    assert_eq!(provider.tail(root.child.closed()?).await?, 1);
    assert!(matches!(
        provider.tail(membership).await,
        Err(StreamError::NotFound)
    ));
    Ok(())
}

#[tokio::test]
async fn either_opt_out_fences_prepared_peer_delivery_and_replay() -> Result {
    for delivery in [MessageDelivery::Steer, MessageDelivery::FollowUp] {
        for opt_out_sender in [false, true] {
            let provider = MemoryStream::default();
            let sender = create(&provider, id(40)).await?;
            let recipient = create(&provider, id(41)).await?;
            let grants = Capabilities::new([SEND_PEER]);
            let admission = session_peer_admission(&provider, &grants, &sender, &recipient)
                .await?
                .ok_or("enabled roots not admitted")?;
            let state = AgentState::adopted(sender.clone(), &adopted(sender.id()))?;
            let effect = |peer, name| TurnEffect::Send {
                peer: Some(peer),
                delivery,
                to: recipient.clone(),
                id: intent(name),
                re: None,
                body: Box::new(super::driver::file("peer.md").expect("valid file")),
            };
            let step = Step {
                outs: effect_outs(&state, vec![effect(admission, "before-opt-out")]),
                state: state.clone(),
                status: None,
                alarm: Alarm::Clear,
            };
            let request = lower(&step, 1, acyclic_stream::IdempotencyKey::unique())?;
            let changed = if opt_out_sender { &sender } else { &recipient };
            submit(
                &provider,
                set_peer_membership(changed, &user(), &intent("off"), false)?,
            )
            .await?;
            assert!(matches!(
                submit(&provider, request.clone()).await?,
                Submitted::Committed(_)
            ));
            assert_eq!(
                provider.tail(recipient.inbox()?).await?,
                1,
                "no input after opt-out"
            );
            assert_eq!(
                sent(&provider, &sender, &intent("before-opt-out"))
                    .await?
                    .ok_or("missing rejection receipt")?
                    .delivery,
                Delivered::Closed
            );
            submit(
                &provider,
                set_peer_membership(changed, &user(), &intent("on"), true)?,
            )
            .await?;
            submit(&provider, request).await?;
            assert_eq!(
                provider.tail(recipient.inbox()?).await?,
                1,
                "replay cannot undo refusal"
            );
            let admission = session_peer_admission(&provider, &grants, &sender, &recipient)
                .await?
                .ok_or("re-enabled roots not admitted")?;
            let step = Step {
                outs: effect_outs(&state, vec![effect(admission, "after-opt-in")]),
                state,
                status: None,
                alarm: Alarm::Clear,
            };
            submit(
                &provider,
                lower(&step, 1, acyclic_stream::IdempotencyKey::unique())?,
            )
            .await?;
            assert_eq!(provider.tail(recipient.inbox()?).await?, 2);
        }
    }
    Ok(())
}

proptest! {
    #[test]
    fn peer_send_requires_label_and_both_member_roots(
        granted in any::<bool>(),
        sender_member in any::<bool>(),
        recipient_member in any::<bool>(),
        sender_child in any::<bool>(),
        recipient_child in any::<bool>(),
        self_send in any::<bool>(),
    ) {
        let observed = futures::executor::block_on(async {
            let provider = MemoryStream::default();
            let sender = create(&provider, id(20)).await?;
            let recipient = create(&provider, id(21)).await?;
            let recipient = if self_send { sender.clone() } else { recipient };
            for (root, enabled) in [(&sender, sender_member), (&recipient, recipient_member)] {
                if !enabled {
                    assert!(matches!(submit(&provider, set_peer_membership(
                        root, &user(), &intent("opt-out"), false,
                    )?).await?, Submitted::Committed(_)));
                }
            }
            let sender = if sender_child { sender.child(&id(22))? } else { sender };
            let recipient = if recipient_child { recipient.child(&id(23))? } else { recipient };
            let grants = Capabilities::new(granted.then_some(SEND_PEER));
            Ok::<_, Box<dyn std::error::Error>>(session_peer_allowed(
                &provider, &grants, &sender, &recipient,
            ).await?)
        }).expect("membership publication");
        prop_assert_eq!(observed,
            granted && sender_member && recipient_member && !sender_child && !recipient_child && !self_send);
    }
}

#[tokio::test]
async fn peer_opt_out_is_symmetric_user_only_and_archive_fences_rejoin() -> Result {
    let provider = MemoryStream::default();
    let a = create(&provider, id(30)).await?;
    let b = create(&provider, id(31)).await?;
    let grants = Capabilities::new([SEND_PEER]);
    assert!(session_peer_allowed(&provider, &grants, &a, &b).await?);
    let mut launch = adopted(id(32));
    launch.profile = Some(acyclic_harness_core::templates::root()?);
    let born_off = create_root_with_peer(&intent("create-off"), launch, false)?;
    let opted_out = born_off.child.clone();
    assert!(matches!(
        submit(&provider, born_off.request).await?,
        Submitted::Committed(_)
    ));
    assert!(!session_peer_allowed(&provider, &grants, &opted_out, &a).await?);
    assert!(!session_peer_allowed(&provider, &grants, &a, &opted_out).await?);
    let records = values(&provider, opted_out.inbox()?).await?;
    let InboxRecord::ForkAdopted(adopted) = InboxRecord::decode(&records[0])? else {
        return Err("root adoption missing".into());
    };
    assert!(
        adopted
            .profile
            .as_ref()
            .is_some_and(|profile| profile.peers.contains(SEND_PEER))
    );
    for by in [
        Sender::Agent { path: a.clone() },
        Sender::Host {
            host: "model".into(),
        },
    ] {
        assert!(set_peer_membership(&a, &by, &intent("forbidden"), false).is_err());
    }
    let off = set_peer_membership(&a, &user(), &intent("off"), false)?;
    submit(&provider, off.clone()).await?;
    submit(&provider, off).await?;
    assert_eq!(provider.tail(a.peer_membership()?).await?, 2);
    assert!(!session_peer_allowed(&provider, &grants, &a, &b).await?);
    assert!(!session_peer_allowed(&provider, &grants, &b, &a).await?);
    submit(
        &provider,
        set_peer_membership(&a, &user(), &intent("on"), true)?,
    )
    .await?;
    assert!(session_peer_allowed(&provider, &grants, &a, &b).await?);
    let rejoin = set_peer_membership(&a, &user(), &intent("stale-on"), true)?;
    submit(&provider, archive(&a, &user(), &intent("archive"))?).await?;
    assert!(!session_peer_allowed(&provider, &grants, &a, &b).await?);
    assert!(!session_peer_allowed(&provider, &grants, &b, &a).await?);
    assert!(matches!(
        submit(&provider, rejoin).await?,
        Submitted::RecipientClosed(_)
    ));
    Ok(())
}
