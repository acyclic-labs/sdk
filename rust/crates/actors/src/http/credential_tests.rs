use super::*;
use proptest::prelude::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test]
async fn deployment_kind_mismatch_is_rejected_before_credentials_or_io()
-> Result<(), Box<dyn std::error::Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let client = Client::with_credential_provider(
        "https://fixture.invalid",
        CredentialProvider::new(move || {
            seen.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Err(acyclic_auth::credential::CredentialError::Invalid))
        }),
        1024,
    )?;
    let worker = wire::DeploymentRequest {
        name: "worker".into(),
        code_sha256: vec![1; 32],
        idempotency_key: "deployment".into(),
        worker_limits: Some(wire::WorkerLimits {
            cpu_millis: 1,
            memory_bytes: 1,
            timeout_millis: 1,
        }),
        ..Default::default()
    };
    // Negative control: generic envelope admission accepts this valid Worker.
    crate::domain::DeploymentRequest::try_from(worker.clone())?;
    assert!(matches!(
        client.deploy_namespace(&worker).await,
        Err(Error::InvalidArgument)
    ));
    let namespace = wire::DeploymentRequest {
        worker_limits: None,
        limits: Some(wire::ActorLimits {
            handler_timeout_millis: 1,
            memory_bytes: 1,
            checkpoint_bytes: 1,
        }),
        ..worker.clone()
    };
    crate::domain::DeploymentRequest::try_from(namespace.clone())?;
    assert!(matches!(
        client.deploy_worker(&namespace).await,
        Err(Error::InvalidArgument)
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    // Correct operation alternatives proceed as far as credential admission.
    assert!(matches!(
        client.deploy_worker(&worker).await,
        Err(Error::Credential(_))
    ));
    assert!(matches!(
        client.deploy_namespace(&namespace).await,
        Err(Error::Credential(_))
    ));
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    Ok(())
}

#[test]
fn credential_callbacks_preserve_current_headers_and_fail_before_transport() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(32))
        .run(
            &prop::collection::vec("[a-zA-Z0-9_-]{1,64}", 2..8),
            |values| {
                runtime.block_on(async {
                    let expected = values.clone();
                    let index = Arc::new(AtomicUsize::new(0));
                    let calls = index.clone();
                    let client = Client::with_credential_provider(
                        "https://fixture.invalid",
                        CredentialProvider::new(move || {
                            std::future::ready(
                                values
                                    .get(calls.fetch_add(1, Ordering::Relaxed))
                                    .cloned()
                                    .ok_or(acyclic_auth::credential::CredentialError::Invalid),
                            )
                        }),
                        1024,
                    )
                    .unwrap();
                    for value in &expected {
                        let header = client.authorization().await.unwrap();
                        prop_assert_eq!(header.to_str().unwrap(), format!("Bearer {value}"));
                        prop_assert!(header.is_sensitive());
                    }
                    prop_assert_eq!(index.load(Ordering::Relaxed), expected.len());
                    let result = client
                        .inspect_actor(&wire::InspectActorRequest {
                            namespace: None,
                            account_id: Vec::new(),
                            actor_id: "actor-a".into(),
                        })
                        .await;
                    prop_assert!(matches!(result, Err(Error::Credential(_))));
                    Ok(())
                })
            },
        )
        .unwrap();
}
