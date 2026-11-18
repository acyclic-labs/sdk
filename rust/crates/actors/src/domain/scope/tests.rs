use super::*;

#[test]
fn scoped_operations_preserve_addresses_and_copy_only_owned_fields()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::addressing::DEFAULT_ACTOR_NAMESPACE;

    let actor = ActorId::new("original".into())?;
    assert!(RemoveSubscriptionRequest::new(actor.clone(), String::new(), "remove".into()).is_err());
    assert!(ResumeSubscriptionRequest::new(actor, "events".into(), String::new()).is_err());

    for account in [vec![0xff, 0, 1], vec![0xfe; 65_536]] {
        let address = ActorReference::new(&account, b"Class_One", b"Bound_ID")?;
        let subscription = wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: "events/source".into(),
            start: Some(wire::SubscriptionStart {
                start: Some(wire::subscription_start::Start::CurrentHead(true)),
            }),
            placement_anchor: false,
        };
        macro_rules! check {
            ($request:ident, $($field:ident: $value:expr),* $(,)?) => {{
                let raw = wire::$request {
                    actor_id: "original".into(),
                    $($field: $value,)*
                    ..Default::default()
                };
                let request = $request::try_from(raw.clone())?;
                let inherited = request.reference(b"fallback")?;
                assert_eq!(inherited.account().as_bytes(), b"fallback");
                assert_eq!(inherited.namespace().as_str(), DEFAULT_ACTOR_NAMESPACE);
                assert_eq!(request.reference(&[]), Err(IdentityError::EmptyAccount));

                #[cfg(not(target_arch = "wasm32"))]
                let bound = {
                    let allocations = acyclic_test_support::measure(|| request.with_reference(&address));
                    assert_eq!(allocations.count_total, 3);
                    allocations.output
                };
                #[cfg(target_arch = "wasm32")]
                let bound = request.with_reference(&address);
                assert_eq!(bound.reference(b"wrong-account")?, address);
                assert_eq!(bound.namespace(), "Class_One");
                let mut expected = raw;
                expected.actor_id = address.id().to_owned();
                expected.namespace = Some(address.namespace().as_str().to_owned());
                expected.account_id = account.clone();
                let actual: wire::$request = bound.into();
                assert_eq!(actual, expected);

                // Negative control: invalid wire data still requires admission.
                expected.namespace = Some("invalid/namespace".into());
                assert!($request::try_from(expected).is_err());
            }};
        }
        check!(InspectActorRequest,);
        check!(AddSubscriptionRequest,
            subscription: Some(subscription), idempotency_key: "add".into());
        check!(RemoveSubscriptionRequest,
            subscription_id: "events".into(), idempotency_key: "remove".into());
        check!(ResumeSubscriptionRequest,
            subscription_id: "events".into(), idempotency_key: "resume".into());
        check!(CheckpointActorRequest,);
        check!(DeleteAllRequest, idempotency_key: "delete".into());
        check!(InvokeActorRequest, method: "POST".into(), url: "/call".into(), body: vec![0, 0xff]);
        check!(ActorObservation, code_sha256: vec![1; 32], state: 1);

        let raw = wire::ForkActorRequest {
            actor_id: "parent".into(),
            child_actor_id: "child".into(),
            namespace: Some("Class_One".into()),
            account_id: account,
            idempotency_key: "fork".into(),
            ..Default::default()
        };
        let mut invalid_child = raw.clone();
        invalid_child.child_actor_id = "invalid/child".into();
        assert!(crate::contract::validate_fork(&invalid_child).is_err());
        let mut same_actor = raw.clone();
        same_actor.child_actor_id.clone_from(&raw.actor_id);
        assert_eq!(
            crate::contract::validate_fork(&same_actor),
            Err(crate::ContractError::InvalidArgument)
        );
        assert!(ForkActorRequest::try_from(same_actor).is_err());
        let fork = ForkActorRequest::try_from(raw)?;
        let parent = fork.reference(b"wrong-account")?;
        let child = fork.child_reference(b"wrong-account")?;
        assert_eq!(parent.account(), child.account());
        assert_eq!(parent.namespace(), child.namespace());
        assert_eq!(parent.id(), "parent");
        assert_eq!(child.id(), "child");
        assert_ne!(parent, child);

        #[cfg(not(target_arch = "wasm32"))]
        {
            let name = address.namespace().as_str().to_owned();
            let original = name.as_ptr();
            let allocations = acyclic_test_support::measure(|| ActorNamespace::new(name));
            assert_eq!(allocations.count_total, 0);
            let namespace = allocations.output?;
            assert_eq!(namespace.as_str().as_ptr(), original);
        }
    }
    Ok(())
}
