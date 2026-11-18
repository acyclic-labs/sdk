use crate::domain::{
    ActorLimits, ActorNamespace, CodeSha256, DeploymentRequest, WorkerLimits,
};
use crate::{ContractError, validate_deployment, wire};

#[test]
fn deployment_limit_alternatives_are_exclusive_and_operation_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let worker = DeploymentRequest::worker(
        ActorNamespace::new("worker".into())?,
        CodeSha256::new(vec![1; 32])?,
        vec![],
        WorkerLimits::new(2, 3, 4)?,
        "deploy".into(),
    )?;
    let raw: wire::DeploymentRequest = worker.clone().into();
    assert!(crate::client::validate_operation("DeployWorker", &raw).is_ok());
    #[cfg(not(target_arch = "wasm32"))]
    {
        let allocation = acyclic_test_support::measure(|| {
            assert!(crate::client::validate_operation("DeployWorker", &raw).is_ok());
        });
        assert_eq!(
            allocation.count_total, 0,
            "typed admission must borrow its input"
        );
        assert_eq!(allocation.bytes_total, 0);
    }
    assert!(crate::client::validate_operation("DeployNamespace", &raw).is_err());
    let mut neither = raw.clone();
    neither.worker_limits = None;
    assert!(DeploymentRequest::try_from(neither).is_err());
    let mut both = raw.clone();
    both.limits = Some(ActorLimits::new(1, 2, 3)?.into());
    assert!(DeploymentRequest::try_from(both).is_err());
    let mut namespace = raw;
    namespace.worker_limits = None;
    namespace.limits = Some(ActorLimits::new(1, 2, 3)?.into());
    let actor = DeploymentRequest::try_from(namespace.clone())?;
    for (jurisdiction, actor_allowed, worker_allowed) in [
        ("", true, true),
        ("eu", true, false),
        ("us", true, false),
        ("fedramp", true, false),
        ("invalid", false, false),
    ] {
        for (request, allowed) in [(&actor, actor_allowed), (&worker, worker_allowed)] {
            assert_eq!(
                request
                    .clone()
                    .with_jurisdiction(jurisdiction.into())
                    .is_ok(),
                allowed
            );
            assert_eq!(
                request
                    .clone()
                    .with_placement(b"account".to_vec(), jurisdiction.into(), 0)
                    .is_ok(),
                allowed
            );
        }
    }
    assert!(crate::client::validate_operation("DeployNamespace", &namespace).is_ok());
    assert!(crate::client::validate_operation("DeployWorker", &namespace).is_err());
    for name in ["worker", "worker-1", "a"] {
        let mut deployment: wire::DeploymentRequest = namespace.clone();
        deployment.name = name.into();
        deployment.limits = None;
        deployment.worker_limits = Some(WorkerLimits::new(2, 3, 4)?.into());
        assert!(crate::validate_deployment(&deployment).is_ok());
        assert!(DeploymentRequest::try_from(deployment).is_ok());
    }
    for name in ["My_Worker", "a.b", "svc_1", "-worker", "worker-"] {
        let mut deployment = namespace.clone();
        deployment.name = name.into();
        // Namespace identifiers retain their broader public grammar.
        assert!(crate::validate_deployment(&deployment).is_ok());
        deployment.limits = None;
        deployment.worker_limits = Some(WorkerLimits::new(2, 3, 4)?.into());
        assert_eq!(
            crate::validate_deployment(&deployment),
            Err(crate::ContractError::InvalidArgument)
        );
        assert!(DeploymentRequest::try_from(deployment).is_err());
        assert!(
            DeploymentRequest::worker(
                ActorNamespace::new(name.into())?,
                CodeSha256::new(vec![1; 32])?,
                vec![],
                WorkerLimits::new(2, 3, 4)?,
                "deploy".into(),
            )
            .is_err()
        );
    }
    Ok(())
}
