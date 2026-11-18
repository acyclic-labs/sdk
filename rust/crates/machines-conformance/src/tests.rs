use super::*;
use acyclic_machines::{SIMULATED_PLATFORM, SimulatedMachines};
use acyclic_stream::testing::RequestClass;

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl AttachedBackend for Fixture<SimulatedMachines> {
    async fn attach(&self) -> Result<MachineObservation, String> {
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000a01")
            .map_err(|error| error.to_string())?;
        self.provider
            .inner()
            .attach(key, SIMULATED_PLATFORM)
            .await
            .map_err(|error| error.to_string())
    }
    async fn set_online(&self, machine: MachineId, online: bool) -> Result<(), String> {
        self.provider
            .inner()
            .set_online(machine, online)
            .await
            .map_err(|error| error.to_string())
    }
}

async fn simulated() -> Result<Fixture<SimulatedMachines>, String> {
    Ok(Fixture::new(
        SimulatedMachines::default(),
        Image::custom([17; 32]).map_err(|error| error.to_string())?,
        [19; 32],
    ))
}
suite!(public_simulated_suite, simulated);

#[tokio::test]
async fn reference_provider_profiles() -> Result<(), Box<dyn std::error::Error>> {
    verify_attached(&simulated().await?).await?;
    #[cfg(not(target_arch = "wasm32"))]
    verify_local_files().await?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn verify_local_files() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_machines::{DeviceKind, local};
    use acyclic_stream::{MemoryStream, StreamPath};
    use std::sync::Arc;

    let image = Image::custom([17; 32])?;
    let mut request = CreateMachine::new(IdempotencyKey::new(), Some(image), [19; 32]);
    request.suspension = SuspensionPolicy::Manual;
    let MutationOutcome::Created(mut observation) =
        SimulatedMachines::with_capabilities(std::collections::BTreeSet::new())
            .create(request)
            .await?
    else {
        return Err("expected a Machine enrollment".into());
    };
    observation.contract.platform =
        Platform::host(DeviceKind::Desktop).ok_or("unsupported host platform")?;
    observation.endpoints.clear();

    let workspace = tempfile::tempdir()?;
    let path = workspace.path().join("binary");
    let machine = observation.id;
    let provider = local::provider(
        observation,
        local::LocalOptions {
            cwd: workspace.path().canonicalize()?,
            environment: Default::default(),
            process_custody: None,
        },
        Arc::new(MemoryStream::default()),
        StreamPath::new(format!("machine-tests/{machine}"))?,
    )
    .await?;
    verify_files(
        provider.as_ref(),
        machine,
        path.to_str().ok_or("invalid test path")?,
        4_102_444_800_000,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn live_fork_fidelities() -> Result<(), String> {
    for capabilities in [
        std::collections::BTreeSet::from([Capability::DiskFork, Capability::SuspendResume]),
        std::collections::BTreeSet::from([Capability::SuspendResume]),
    ] {
        verify(&Fixture::new(
            SimulatedMachines::with_capabilities(capabilities),
            Image::custom([17; 32]).map_err(|error| error.to_string())?,
            [19; 32],
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn benchmark_and_failure_counts() -> Result<(), String> {
    let fixture = simulated().await?;
    let measured = bench(&fixture).await?;
    assert_eq!(measured.reads, 256);
    assert_eq!(measured.requests.data(), 256);
    assert_eq!(measured.requests.control(), 0);
    let before = fixture.requests().requests();
    let missing = MachineId::from_digest([0x55; 32]);
    assert!(matches!(
        fixture
            .provider()
            .inspect_machine(missing)
            .await
            .as_ref()
            .map_err(ProviderError::classification),
        Err(ProviderError::NotFound(_))
    ));
    let failed = fixture.requests().requests().since(before);
    assert_eq!(failed.class(RequestClass::Metadata), 1);
    assert_eq!(failed.data(), 0);
    Ok(())
}
