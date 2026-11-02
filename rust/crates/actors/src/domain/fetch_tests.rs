use super::*;
use prost::Message;

#[test]
fn untrusted_fetch_policy_rejects_destination_widening_and_environment_collisions()
-> Result<(), Box<dyn std::error::Error>> {
    let fetch = FetchBinding::new(
        "UPSTREAM".into(),
        vec![EgressRule::new("api.example.com".into(), 443)?],
    )?;
    #[cfg(not(target_arch = "wasm32"))]
    {
        let rule = fetch.egress().first().ok_or("rule")?;
        let mut admitted = None;
        let borrowed = acyclic_test_support::measure(|| {
            admitted = Some(EgressRule::validate_authority(rule.host(), rule.port()));
        });
        admitted.ok_or("authority admission did not complete")??;
        assert_eq!(borrowed.count_total, 0);
        assert_eq!(borrowed.bytes_total, 0);
        // Reconstructing the retained rule restores the removed host copy.
        let copied = acyclic_test_support::measure(|| {
            std::hint::black_box(EgressRule::new(rule.host().to_owned(), rule.port()))
                .expect("same admitted authority");
        });
        assert_eq!(copied.count_total, 1);
        assert_eq!(copied.bytes_total, rule.host().len() as u64);
    }
    let deployment = DeploymentRequest::worker(
        ActorNamespace::new("gateway".into())?,
        CodeSha256::new(vec![1; 32])?,
        vec![],
        WorkerLimits::new(1, 1024, 10)?,
        "deploy".into(),
    )?
    .with_fetch(Some(fetch.clone()))?;
    let wire: wire::DeploymentRequest = deployment.clone().into();
    let decoded = wire::DeploymentRequest::decode(wire.encode_to_vec().as_slice())?;
    assert_eq!(DeploymentRequest::try_from(decoded)?, deployment);
    // The public wire path must enforce policy even when no manifest is used.
    for host in [
        "127.0.0.1",
        "127.1",
        "2130706433",
        "0x7f000001",
        "api.0x7f",
        "API.example.com",
        "api.local",
        "api.localhost",
        "*.example.com",
        "api.example.com.",
    ] {
        assert!(EgressRule::validate_authority(host, 443).is_err(), "{host}");
        let mut candidate = wire.clone();
        candidate
            .fetch
            .as_mut()
            .ok_or("fetch")?
            .egress
            .first_mut()
            .ok_or("rule")?
            .host = host.into();
        assert!(DeploymentRequest::try_from(candidate).is_err(), "{host}");
    }
    for port in [0, 65536, u32::MAX] {
        assert!(EgressRule::validate_authority("api.example.com", port).is_err());
        let mut candidate = wire.clone();
        candidate
            .fetch
            .as_mut()
            .ok_or("fetch")?
            .egress
            .first_mut()
            .ok_or("rule")?
            .port = port;
        assert!(DeploymentRequest::try_from(candidate).is_err());
    }
    let mut duplicate = wire.clone();
    let policy = duplicate.fetch.as_mut().ok_or("fetch")?;
    policy
        .egress
        .push(policy.egress.first().ok_or("rule")?.clone());
    assert!(DeploymentRequest::try_from(duplicate).is_err());
    let mut empty = wire;
    empty.fetch.as_mut().ok_or("fetch")?.egress.clear();
    assert!(DeploymentRequest::try_from(empty).is_err());
    assert!(
        deployment
            .clone()
            .with_secrets(vec![SecretSelection::new(
                "UPSTREAM".into(),
                acyclic_auth::secrets::SecretRef::new("app".into(), "key".into(), 1)?
            )?])
            .is_err()
    );
    assert!(deployment.with_fetch(None)?.fetch().is_none());

    let update = UpdateNamespaceRequest::new(
        ActorNamespace::new("agent".into())?,
        CodeSha256::new(vec![1; 32])?,
        vec![],
        ActorLimits::new(1, 1024, 1024)?,
        2,
        "update".into(),
    )?
    .with_fetch(Some(fetch))?;
    let wire: wire::UpdateNamespaceRequest = update.clone().into();
    assert_eq!(
        UpdateNamespaceRequest::try_from(wire::UpdateNamespaceRequest::decode(
            wire.encode_to_vec().as_slice()
        )?)?,
        update
    );
    let mut collision = wire;
    collision
        .bindings
        .push(Binding::new("UPSTREAM".into(), "grant".into(), "streams:/inbox".into())?.into());
    assert!(UpdateNamespaceRequest::try_from(collision).is_err());
    assert!(update.with_fetch(None)?.fetch().is_none());
    Ok(())
}

#[test]
fn fetch_wire_keeps_predecessor_fields_and_defaults_to_no_authority()
-> Result<(), Box<dyn std::error::Error>> {
    use prost_reflect::{DescriptorPool, DynamicMessage};
    let pool = DescriptorPool::decode(
        include_bytes!("../../tests/fixtures/actors/v1/actors-pre-secrets.bin").as_slice(),
    )?;
    let fetch = FetchBinding::new(
        "UPSTREAM".into(),
        vec![EgressRule::new("api.example.com".into(), 443)?],
    )?;
    let deployment = DeploymentRequest::worker(
        ActorNamespace::new("gateway".into())?,
        CodeSha256::new(vec![1; 32])?,
        vec![],
        WorkerLimits::new(1, 1024, 10)?,
        "deploy".into(),
    )?;
    let update = UpdateNamespaceRequest::new(
        ActorNamespace::new("agent".into())?,
        CodeSha256::new(vec![1; 32])?,
        vec![],
        ActorLimits::new(1, 1024, 1024)?,
        2,
        "update".into(),
    )?;
    let old_deploy: wire::DeploymentRequest = deployment.clone().into();
    let new_deploy: wire::DeploymentRequest = deployment.with_fetch(Some(fetch.clone()))?.into();
    let old_update: wire::UpdateNamespaceRequest = update.clone().into();
    let new_update: wire::UpdateNamespaceRequest = update.with_fetch(Some(fetch))?.into();
    for (name, old, new, field) in [
        (
            "DeploymentRequest",
            old_deploy.encode_to_vec(),
            new_deploy.encode_to_vec(),
            13,
        ),
        (
            "UpdateNamespaceRequest",
            old_update.encode_to_vec(),
            new_update.encode_to_vec(),
            8,
        ),
    ] {
        let descriptor = pool
            .get_message_by_name(&format!("acyclic.actors.v1.{name}"))
            .ok_or("predecessor descriptor")?;
        let old = DynamicMessage::decode(descriptor.clone(), old.as_slice())?;
        let mut new = DynamicMessage::decode(descriptor, new.as_slice())?;
        assert_eq!(
            new.unknown_fields()
                .map(|value| value.number())
                .collect::<Vec<_>>(),
            vec![field]
        );
        new.take_unknown_fields().for_each(drop);
        assert_eq!(new.encode_to_vec(), old.encode_to_vec());
    }
    assert!(DeploymentRequest::try_from(old_deploy)?.fetch().is_none());
    assert!(
        UpdateNamespaceRequest::try_from(old_update)?
            .fetch()
            .is_none()
    );
    Ok(())
}
