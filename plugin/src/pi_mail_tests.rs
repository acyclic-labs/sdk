use super::*;
use crate::{pi_fork::tests::work, pi_root::PiRootIntent};

#[test]
fn bounded_envelope_rejects_unknown_fields_and_oversized_bodies() {
    let native = work();
    assert!(
        decode::<PiMailEnvelope>(
            br#"{"kind":"steer","message":"x","sender":"root"}"#,
            &native
        )
        .is_err()
    );
    assert!(
        decode::<PiMailEnvelope>(&vec![b' '; MAXIMUM_PI_MAIL_BYTES as usize + 1], &native).is_err()
    );
    assert!(
        validate_envelope(&PiMailEnvelope {
            kind: PiMailKind::Message,
            message: "x".repeat(MAXIMUM_PI_MAIL_BYTES as usize)
        })
        .is_err()
    );
}

#[tokio::test]
async fn passive_mailbox_preserves_task_identity_and_denies_bounds_or_cancellation() {
    let fixture = crate::pi_root_provider::tests::Fixture::new(PiRootIntent::ScratchOnly {}).await;
    let runtime = &fixture.runtime.runtime;
    let binding = &fixture.request.binding;
    let native = work();
    assert!(
        receive(runtime, binding, 0, 1, &native)
            .await
            .unwrap()
            .0
            .is_none()
    );
    let observed = wait(
        runtime,
        binding,
        binding.task_id,
        0,
        |caller, selected| {
            assert_eq!((caller, selected), (binding.task_id, binding.task_id));
            Ok(())
        },
        &native,
    )
    .await
    .unwrap();
    assert_eq!(observed.0.task, binding.task_id);
    assert_eq!(observed.0.state, "pending");
    assert!(observed.0.outcome.is_none() && observed.0.mail.is_none());
    native.cancellation().cancel();
    assert!(matches!(
        receive(runtime, binding, 0, 1, &native).await,
        Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::Cancelled,
            ..
        })
    ));
    assert!(matches!(
        receive(runtime, binding, 0, 2, &native).await,
        Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            ..
        })
    ));
}
