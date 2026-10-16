//! Public-client conformance for lost Create replies and stable recovery identity.

#![allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "conformance assertions fail the test on an unexpected provider outcome"
)]

use acyclic_machines::*;
use async_trait::async_trait;
use std::num::NonZeroU32;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

struct LostCreateReply {
    inner: SimulatedMachines,
    admit_before_disconnect: bool,
    disconnected: AtomicBool,
    requests: Mutex<Vec<CreateMachine>>,
}

impl LostCreateReply {
    fn new(admit_before_disconnect: bool) -> Self {
        Self {
            inner: SimulatedMachines::default(),
            admit_before_disconnect,
            disconnected: AtomicBool::new(false),
            requests: Mutex::new(Vec::new()),
        }
    }
}

macro_rules! provider {
    ($( $method:ident($($arg:ident: $input:ty),*) -> $output:ty; )*) => {
        #[async_trait]
        impl MachinesProvider for LostCreateReply {
            fn assurance(&self) -> ProviderAssurance {
                self.inner.assurance()
            }

            async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
                self.requests.lock().unwrap().push(request.clone());
                let disconnect = !self.disconnected.swap(true, Ordering::SeqCst);
                if disconnect && !self.admit_before_disconnect {
                    return Err(ProviderError::Indeterminate(request.idempotency_key));
                }
                let key = request.idempotency_key;
                let outcome = self.inner.create(request).await?;
                if disconnect {
                    Err(ProviderError::Indeterminate(key))
                } else {
                    Ok(outcome)
                }
            }

            $(async fn $method(&self, $($arg: $input),*) -> Result<$output, ProviderError> {
                self.inner.$method($($arg),*).await
            })*
        }
    };
}

provider! {
    exec(machine: MachineId, command: ExecCommand) -> ExecStream;
    read_output(machine: MachineId, offset: u64, limit: u32) -> MachineOutput;
    qualify_image(image: Image) -> ImageQualification;
    inspect_machine(machine: MachineId) -> MachineObservation;
    list_machines(after: Option<MachineId>, limit: u32) -> MachinePage;
    checkpoint(machine: MachineId, key: IdempotencyKey) -> MutationOutcome;
    inspect_checkpoint(checkpoint: CheckpointId) -> CheckpointObservation;
    fork(checkpoint: CheckpointId, count: NonZeroU32, key: IdempotencyKey) -> MutationOutcome;
    fork_machine(machine: MachineId, count: NonZeroU32, key: IdempotencyKey) -> MutationOutcome;
    suspend(machine: MachineId, key: IdempotencyKey) -> MutationOutcome;
    wake(machine: MachineId, key: IdempotencyKey) -> MutationOutcome;
    set_suspension_policy(machine: MachineId, policy: SuspensionPolicy, key: IdempotencyKey) -> MutationOutcome;
    destroy_machine(machine: MachineId, key: IdempotencyKey) -> MutationOutcome;
    destroy_checkpoint(checkpoint: CheckpointId, key: IdempotencyKey) -> MutationOutcome;
    events(machine: MachineId, after_sequence: Option<u64>, limit: u32) -> EventPage;
    usage(machine: MachineId, start_unix_ms: u64, end_unix_ms: u64) -> UsageReceipt;
    recover(key: IdempotencyKey) -> MutationOutcome;
    recover_operation(key: IdempotencyKey) -> OperationId;
    inspect_operation(operation: OperationId) -> OperationObservation;
    cancel(operation: OperationId) -> OperationObservation;
    watch_operation(operation: OperationId) -> OperationStream;
}

fn request() -> CreateMachine {
    CreateMachine::new(
        IdempotencyKey::parse("00000000-0000-4000-8000-000000000012").unwrap(),
        Image::Custom(ImageDigest::new([7; 32]).unwrap()),
        [8; 32],
    )
}

#[tokio::test]
async fn uncertain_create_retries_and_recovers_only_the_original_identity() {
    for admitted in [false, true] {
        let provider = Arc::new(LostCreateReply::new(admitted));
        let client = Machines::new(provider.clone());
        let request = request();
        let key = request.idempotency_key;
        match client.create(request.clone()).await {
            Err(error) => assert_eq!(error, ProviderError::Indeterminate(key)),
            Ok(_) => panic!("a lost admission reply cannot produce a successful handle"),
        }
        assert_eq!(
            provider.requests.lock().unwrap().as_slice(),
            std::slice::from_ref(&request)
        );
        assert_eq!(
            provider
                .list_machines(None, 10)
                .await
                .unwrap()
                .machines
                .len(),
            usize::from(admitted)
        );
        let original_operation = provider.recover_operation(key).await;
        let recovered = provider.recover(key).await;
        if admitted {
            assert!(matches!(recovered, Ok(MutationOutcome::Created(_))));
            assert_eq!(
                provider
                    .inspect_operation(original_operation.clone().unwrap())
                    .await
                    .unwrap()
                    .phase,
                OperationPhase::Succeeded
            );
        } else {
            assert!(matches!(
                original_operation,
                Err(ProviderError::NotFound(_))
            ));
            assert!(matches!(recovered, Err(ProviderError::NotFound(_))));
        }

        let retried = client.create(request.clone()).await.unwrap();
        let operation = provider.recover_operation(key).await.unwrap();
        if let Ok(original) = original_operation {
            assert_eq!(operation, original);
        }
        let MutationOutcome::Created(recovered) = provider.recover(key).await.unwrap() else {
            panic!("recovery substituted another operation kind");
        };
        assert_eq!(recovered.id, retried.id());
        assert_eq!(
            client.create(request.clone()).await.unwrap().id(),
            retried.id()
        );
        assert_eq!(provider.recover_operation(key).await.unwrap(), operation);
        let machines = provider.list_machines(None, 10).await.unwrap();
        assert_eq!(machines.machines.len(), 1);
        assert_eq!(machines.machines.first().unwrap().id, retried.id());
        assert_eq!(
            provider.requests.lock().unwrap().as_slice(),
            &[request.clone(), request.clone(), request]
        );
    }
}

#[tokio::test]
async fn uncertain_create_cannot_rebind_the_key_or_recover_under_another_key() {
    let provider = Arc::new(LostCreateReply::new(true));
    let client = Machines::new(provider.clone());
    let request = request();
    assert!(
        matches!(client.create(request.clone()).await, Err(ProviderError::Indeterminate(key)) if key == request.idempotency_key)
    );
    let original_operation = provider
        .recover_operation(request.idempotency_key)
        .await
        .unwrap();
    let original = provider.recover(request.idempotency_key).await.unwrap();
    let mut changed = request.clone();
    changed.network_policy_digest = [9; 32];
    assert!(matches!(
        client.create(changed).await,
        Err(ProviderError::Conflict(_))
    ));
    let other_key = IdempotencyKey::parse("00000000-0000-4000-8000-000000000013").unwrap();
    assert!(matches!(
        provider.recover(other_key).await,
        Err(ProviderError::NotFound(_))
    ));
    assert!(matches!(
        provider.recover_operation(other_key).await,
        Err(ProviderError::NotFound(_))
    ));
    assert_eq!(
        provider.recover(request.idempotency_key).await.unwrap(),
        original
    );
    assert_eq!(
        provider
            .recover_operation(request.idempotency_key)
            .await
            .unwrap(),
        original_operation
    );
    assert_eq!(
        provider
            .list_machines(None, 10)
            .await
            .unwrap()
            .machines
            .len(),
        1
    );
}
