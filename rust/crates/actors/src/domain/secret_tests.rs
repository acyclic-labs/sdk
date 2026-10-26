use super::*;
use prost::Message;

fn deployment() -> Result<DeploymentRequest, DomainError> {
    DeploymentRequest::worker(
        ActorNamespace::new("worker".to_owned())?,
        CodeSha256::new(vec![1; 32])?,
        vec![],
        WorkerLimits::new(1, 1024, 10)?,
        "deploy".to_owned(),
    )
}

#[test]
fn secret_selections_round_trip_and_replace_without_inheritance()
-> Result<(), Box<dyn std::error::Error>> {
    let secret =
        SecretSelection::new("MODEL_KEY".to_owned(), "secrets:/models/primary".to_owned())?;
    let request = deployment()?.with_secrets(vec![secret.clone()])?;
    let wire: wire::DeploymentRequest = request.clone().into();
    let decoded = wire::DeploymentRequest::decode(wire.encode_to_vec().as_slice())?;
    assert_eq!(DeploymentRequest::try_from(decoded)?, request);
    assert_eq!(request.secrets(), std::slice::from_ref(&secret));
    assert!(request.with_secrets(vec![])?.secrets().is_empty());

    let update = UpdateNamespaceRequest::new(
        ActorNamespace::new("namespace".to_owned())?,
        CodeSha256::new(vec![1; 32])?,
        vec![],
        ActorLimits::new(1, 1024, 1024)?,
        2,
        "update".to_owned(),
    )?
    .with_secrets(vec![secret])?;
    let wire: wire::UpdateNamespaceRequest = update.clone().into();
    assert_eq!(
        UpdateNamespaceRequest::try_from(wire::UpdateNamespaceRequest::decode(
            wire.encode_to_vec().as_slice()
        )?)?,
        update
    );
    assert!(update.with_secrets(vec![])?.secrets().is_empty());
    Ok(())
}

#[test]
fn secret_admission_rejects_values_duplicates_and_binding_collisions()
-> Result<(), Box<dyn std::error::Error>> {
    for reference in [
        "plaintext",
        "https://vault/key",
        "secrets:/",
        "secrets:/key?version=1",
        "secrets:/key#unwrap",
        "secrets:/a//b",
    ] {
        assert!(
            SecretSelection::new("KEY".to_owned(), reference.to_owned()).is_err(),
            "{reference}"
        );
    }
    let secret = SecretSelection::new("KEY".to_owned(), "secrets:/key".to_owned())?;
    assert!(
        deployment()?
            .with_secrets(vec![secret.clone(), secret.clone()])
            .is_err()
    );
    let mut wire: wire::DeploymentRequest =
        deployment()?.with_secrets(vec![secret.clone()])?.into();
    wire.bindings.push(
        Binding::new(
            "KEY".to_owned(),
            "grant".to_owned(),
            "streams:/inbox".to_owned(),
        )?
        .into(),
    );
    assert!(DeploymentRequest::try_from(wire).is_err());
    let update = UpdateNamespaceRequest::new(
        ActorNamespace::new("namespace".to_owned())?,
        CodeSha256::new(vec![1; 32])?,
        vec![],
        ActorLimits::new(1, 1024, 1024)?,
        2,
        "update".to_owned(),
    )?;
    assert!(
        update
            .clone()
            .with_secrets(vec![secret.clone(), secret.clone()])
            .is_err()
    );
    let mut wire: wire::UpdateNamespaceRequest = update.with_secrets(vec![secret])?.into();
    wire.bindings.push(
        Binding::new(
            "KEY".to_owned(),
            "grant".to_owned(),
            "streams:/inbox".to_owned(),
        )?
        .into(),
    );
    assert!(UpdateNamespaceRequest::try_from(wire).is_err());
    Ok(())
}

#[test]
fn secret_fields_are_additive_for_the_actual_predecessor_reader()
-> Result<(), Box<dyn std::error::Error>> {
    use prost_reflect::{DescriptorPool, DynamicMessage};
    // Captured verbatim from main b7333953cb, before adding selection fields.
    let pool = DescriptorPool::decode(
        include_bytes!("../../tests/fixtures/actors/v1/actors-pre-secrets.bin").as_slice(),
    )?;
    let selected =
        SecretSelection::new("MODEL_KEY".to_owned(), "secrets:/models/primary".to_owned())?;
    let base: wire::DeploymentRequest = deployment()?.into();
    assert!(
        wire::DeploymentRequest::decode(base.encode_to_vec().as_slice())?
            .secrets
            .is_empty()
    );
    let new: wire::DeploymentRequest = deployment()?.with_secrets(vec![selected.clone()])?.into();
    let descriptor = pool
        .get_message_by_name("acyclic.actors.v1.DeploymentRequest")
        .ok_or("predecessor deployment descriptor")?;
    let old_base = DynamicMessage::decode(descriptor.clone(), base.encode_to_vec().as_slice())?;
    let mut old_new = DynamicMessage::decode(descriptor, new.encode_to_vec().as_slice())?;
    assert_eq!(
        old_new
            .unknown_fields()
            .map(|field| field.number())
            .collect::<Vec<_>>(),
        vec![12]
    );
    old_new.take_unknown_fields().for_each(drop);
    assert_eq!(
        old_new.encode_to_vec(),
        old_base.encode_to_vec(),
        "new writer changed a predecessor field"
    );

    let update = UpdateNamespaceRequest::new(
        ActorNamespace::new("namespace".to_owned())?,
        CodeSha256::new(vec![1; 32])?,
        vec![],
        ActorLimits::new(1, 1024, 1024)?,
        2,
        "update".to_owned(),
    )?;
    let base: wire::UpdateNamespaceRequest = update.clone().into();
    let new: wire::UpdateNamespaceRequest = update.with_secrets(vec![selected])?.into();
    let descriptor = pool
        .get_message_by_name("acyclic.actors.v1.UpdateNamespaceRequest")
        .ok_or("predecessor update descriptor")?;
    let old_base = DynamicMessage::decode(descriptor.clone(), base.encode_to_vec().as_slice())?;
    let mut old_new = DynamicMessage::decode(descriptor, new.encode_to_vec().as_slice())?;
    assert_eq!(
        old_new
            .unknown_fields()
            .map(|field| field.number())
            .collect::<Vec<_>>(),
        vec![7]
    );
    old_new.take_unknown_fields().for_each(drop);
    assert_eq!(
        old_new.encode_to_vec(),
        old_base.encode_to_vec(),
        "update writer changed a predecessor field"
    );
    assert!(
        wire::UpdateNamespaceRequest::decode(base.encode_to_vec().as_slice())?
            .secrets
            .is_empty()
    );
    Ok(())
}
