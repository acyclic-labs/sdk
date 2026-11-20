#![doc = include_str!("../README.md")]
#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

pub use acyclic_actors_call::{call, escrow};
/// Shared canonical account-relative resource addressing.
pub use acyclic_auth::addressing;

pub use acyclic_actors_contract::{
    AUTHORIZATION, ActorsMethod, ContractError, FILE_DESCRIPTOR_SET, authorization, contract,
    domain, guest, invocation, live_update, sockets, validate_add_subscription, validate_delete,
    validate_deployment, validate_fork, validate_remove_subscription, validate_resume_subscription,
    validate_update, wire, workers_authorization,
};

use acyclic_actors_client::identity;
/// Authenticated operations over the platform's Rust-owned transport.
pub use acyclic_actors_client::{client, namespace};
#[cfg(not(target_arch = "wasm32"))]
pub use acyclic_actors_client::{grpc, http};

/// One conformance suite for local and hosted Actors runtimes.
#[cfg(any(test, feature = "conformance"))]
pub mod conformance;
/// Event-owned hosting of an arbitrary Handler over injected storage.
pub mod hosting;
/// Provider-backed actor storage and the single-node `local::Runtime`.
pub mod local;
/// Reusable fenced reservations over the actor's original checkpoint/alarm.
pub mod reservation;

pub use acyclic_actors_inventory::HTTP_ROUTES;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interrupt_filters_round_trip_and_refuse_empty_or_undeclared_prefixes()
    -> Result<(), Box<dyn std::error::Error>> {
        use bytes::Bytes;
        use guest::Handler as _;
        use guest::wire::{decode_manifest, encode_manifest};

        let package = conformance::Package::new()?;
        let mut manifest = decode_manifest(&encode_manifest(package.manifest()))?;
        let encoded = encode_manifest(&manifest);
        assert_eq!(encode_manifest(&decode_manifest(&encoded)?), encoded);
        let interrupt = manifest.interrupts.first_mut().ok_or("missing interrupt")?;
        interrupt.value_prefix = Bytes::new();
        assert!(decode_manifest(&encode_manifest(&manifest)).is_err());
        let interrupt = manifest.interrupts.first_mut().ok_or("missing interrupt")?;
        interrupt.value_prefix = Bytes::from_static(b"stop");
        interrupt.subscription = "another-subscription".into();
        assert!(decode_manifest(&encode_manifest(&manifest)).is_err());
        Ok(())
    }

    #[test]
    fn subscription_start_is_exactly_one_choice() -> Result<(), ContractError> {
        let mut create = wire::DeploymentRequest {
            publication: Vec::new(),
            entry_source: None,
            secrets: Vec::new(),
            fetch: None,
            worker_limits: None,
            name: "example".into(),
            account_id: Vec::new(),
            jurisdiction: String::new(),
            expected_revision: 0,
            code_sha256: vec![1; 32],

            bindings: vec![],
            limits: Some(wire::ActorLimits {
                handler_timeout_millis: Some(1_000),
                memory_bytes: Some(1024),
                checkpoint_bytes: Some(1024),
            }),
            subscriptions: vec![wire::SubscriptionSpec {
                subscription_id: "events".into(),
                stream_path: "agents/a/events".into(),
                start: Some(wire::SubscriptionStart {
                    start: Some(wire::subscription_start::Start::Cursor(0)),
                }),
                placement_anchor: true,
            }],
            idempotency_key: "create-a".into(),
        };
        assert_eq!(validate_deployment(&create), Ok(()));
        let start = create
            .subscriptions
            .first_mut()
            .ok_or(ContractError::InvalidArgument)?
            .start
            .take();
        assert_eq!(
            validate_deployment(&create),
            Err(ContractError::InvalidArgument)
        );
        create
            .subscriptions
            .first_mut()
            .ok_or(ContractError::InvalidArgument)?
            .start = start;
        let duplicate = create.subscriptions.clone();
        create.subscriptions.extend(duplicate);
        assert_eq!(
            validate_deployment(&create),
            Err(ContractError::DuplicateName)
        );
        Ok(())
    }
    #[test]
    fn delete_requires_actor_and_bounded_idempotency_key() {
        let valid = wire::DeleteAllRequest {
            namespace: None,
            account_id: Vec::new(),
            actor_id: "actor-a".into(),
            idempotency_key: "delete-a".into(),
        };
        assert_eq!(validate_delete(&valid), Ok(()));
        #[cfg(not(target_arch = "wasm32"))]
        {
            let scoped = wire::DeleteAllRequest {
                account_id: vec![1; 65_536],
                ..valid.clone()
            };
            let cost = acyclic_test_support::measure(|| validate_delete(&scoped).unwrap());
            assert_eq!(cost.count_total, 0);
        }
        let semantic = domain::DeleteAllRequest::try_from(valid.clone()).expect("valid delete");
        assert_eq!(wire::DeleteAllRequest::from(semantic), valid);
        for invalid in [
            wire::DeleteAllRequest {
                actor_id: String::new(),
                ..valid.clone()
            },
            wire::DeleteAllRequest {
                idempotency_key: String::new(),
                ..valid.clone()
            },
            wire::DeleteAllRequest {
                idempotency_key: "k".repeat(257),
                ..valid.clone()
            },
        ] {
            assert_eq!(
                validate_delete(&invalid),
                Err(ContractError::InvalidArgument)
            );
            assert!(domain::DeleteAllRequest::try_from(invalid).is_err());
        }
        let boundary = wire::DeleteAllRequest {
            idempotency_key: "k".repeat(256),
            ..valid
        };
        assert_eq!(validate_delete(&boundary), Ok(()));
    }
}
