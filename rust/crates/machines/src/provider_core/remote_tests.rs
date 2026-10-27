//! Shared admission checks; this driver has no provider protocol or fixtures.

use super::*;
use crate::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Default)]
struct NoDispatch(Arc<AtomicUsize>);

impl NoDispatch {
    fn dispatch<T>(&self) -> Result<T, ProviderError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(ProviderError::Unavailable)
    }
}

#[async_trait]
impl SandboxDriver for NoDispatch {
    fn name(&self) -> &'static str {
        "admission-test"
    }

    fn supports_suspend(&self) -> bool {
        false
    }

    async fn create(&self, _: &SandboxSpec) -> Result<SandboxInstance, ProviderError> {
        self.dispatch()
    }

    async fn state(&self, _: &SandboxInstance) -> Result<MachineState, ProviderError> {
        self.dispatch()
    }

    async fn destroy(&self, _: &SandboxInstance) -> Result<(), ProviderError> {
        self.dispatch()
    }

    async fn exec(
        &self,
        _: &SandboxInstance,
        _: Vec<String>,
        _: u32,
        _: Option<Vec<u8>>,
    ) -> Result<ExecStream, ProviderError> {
        self.dispatch()
    }

    async fn read_file(&self, _: &SandboxInstance, _: &str) -> Result<Vec<u8>, ProviderError> {
        self.dispatch()
    }

    async fn write_file(
        &self,
        _: &SandboxInstance,
        _: &str,
        _: &[u8],
    ) -> Result<(), ProviderError> {
        self.dispatch()
    }
}

#[tokio::test]
async fn missing_atomic_snapshot_boundary_refuses_every_entry_before_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    let driver = NoDispatch::default();
    let calls = Arc::clone(&driver.0);
    let provider = RemoteMachines::new(
        driver,
        RemoteMachinesConfig {
            images: [([7; 32], "registered-image".into())].into(),
            ..RemoteMachinesConfig::default()
        },
    );
    let image = Image::custom([7; 32])?;
    assert!(
        provider
            .qualify_image(image.clone())
            .await?
            .capabilities
            .is_empty()
    );
    let machine = MachineId::new();
    let checkpoint = CheckpointId::new();
    let checkpoint_key = IdempotencyKey::new();
    for _ in 0..2 {
        assert!(matches!(
            provider.checkpoint(machine, checkpoint_key).await,
            Err(ProviderError::Unsupported(_))
        ));
    }
    assert!(matches!(
        provider.inspect_checkpoint(checkpoint).await,
        Err(ProviderError::Unsupported(_))
    ));
    assert!(matches!(
        provider
            .fork(checkpoint, NonZeroU32::MIN, IdempotencyKey::new())
            .await,
        Err(ProviderError::Unsupported(_))
    ));
    assert!(matches!(
        provider
            .fork_machine(machine, NonZeroU32::MIN, IdempotencyKey::new())
            .await,
        Err(ProviderError::Unsupported(_))
    ));
    assert!(matches!(
        provider
            .destroy_checkpoint(checkpoint, IdempotencyKey::new())
            .await,
        Err(ProviderError::Unsupported(_))
    ));
    assert!(matches!(
        provider.qualify_image(Image::Checkpoint(checkpoint)).await,
        Err(ProviderError::Unsupported(_))
    ));
    assert!(matches!(
        provider
            .create(CreateMachine::new(
                IdempotencyKey::new(),
                Image::Checkpoint(checkpoint),
                [8; 32],
            ))
            .await,
        Err(ProviderError::Unsupported(_))
    ));
    let mut request = CreateMachine::new(IdempotencyKey::new(), image, [8; 32]);
    request.compatibility = CompatibilityPolicy::Require([Capability::DiskFork].into());
    assert!(matches!(
        provider.create(request).await,
        Err(ProviderError::Unsupported(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(provider.list_machines(None, 1).await?.machines.is_empty());
    Ok(())
}

#[tokio::test]
async fn native_policy_and_stdin_admission_precede_transport()
-> Result<(), Box<dyn std::error::Error>> {
    let driver = NoDispatch::default();
    let calls = Arc::clone(&driver.0);
    let provider = RemoteMachines::new(
        driver.clone(),
        RemoteMachinesConfig {
            images: [([7; 32], "registered-image".into())].into(),
            networks: [(
                [8; 32],
                NetworkPolicy::AllowDomains(vec!["example.com".into()]),
            )]
            .into(),
            ..RemoteMachinesConfig::default()
        },
    );
    assert!(matches!(
        provider
            .create(CreateMachine::new(
                IdempotencyKey::new(),
                Image::custom([7; 32])?,
                [8; 32]
            ))
            .await,
        Err(ProviderError::Unsupported(_))
    ));
    let instance = SandboxInstance {
        id: "internal-admission-vector".into(),
        endpoint: "".into(),
        allocation: Allocation::default(),
    };
    let command = ExecCommand::new(vec!["cat".into()], 1000)?;
    assert!(matches!(
        driver.start(&instance, &command, Some(vec![0, 255])),
        Err(ProviderError::Unsupported(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    Ok(())
}
