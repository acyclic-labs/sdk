//! Shared qualification entry point; the canonical suite has no provider branches.
use acyclic_machines::provider_core::{
    NetworkPolicy, RemoteMachines, RemoteMachinesConfig, SandboxDriver,
};
use acyclic_machines::{IdempotencyKey, MachineState, MachinesProvider};

pub fn required(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|_| format!("conformance configuration {name} missing; recovery: configure {name} using the provider's Live steps in docs/backends and rerun its conformance example").into())
}

pub async fn run<D: SandboxDriver>(
    driver: D,
    image: String,
    workspace: String,
    recovery: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| format!("Conformance logging initialization failed: {error}; recovery: run the conformance example in a fresh process"))?;
    acyclic_machines::provider_core::validate_workspace(&workspace)?;
    let name = driver.name();
    let config = RemoteMachinesConfig {
        images: [([7; 32], image)].into(),
        networks: [([8; 32], NetworkPolicy::BlockAll)].into(),
        ..RemoteMachinesConfig::default()
    };
    println!(
        "{name} conformance resource namespace: {}",
        config.namespace
    );
    let provider = std::sync::Arc::new(RemoteMachines::new(driver, config));
    let result = async {
        acyclic_conformance::machines(provider.as_ref()).await?;
        acyclic_conformance::machines_shell::verify(provider.clone(), &workspace).await?;
        Ok::<(), String>(())
    }
    .await;
    // A failed vector still owns every machine it successfully observed. Clean
    // those resources before returning the original failure to the coordinator.
    let mut cursor = None;
    let mut cleanup_failed = false;
    loop {
        let page = provider.list_machines(cursor, 256).await?;
        for machine in page.machines {
            if machine.state != MachineState::Destroyed {
                let key = IdempotencyKey::new();
                if let Err(error) = provider.destroy_machine(machine.id, key).await {
                    cleanup_failed = true;
                    eprintln!(
                        "{name} conformance cleanup failed for {} ({}); recovery: {recovery}; inspect provider resources in the printed namespace before rerunning",
                        machine.id,
                        error.kind()
                    );
                }
            }
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    result.map_err(|error| {
        format!("{name} canonical Machines conformance failed: {error}; recovery: {recovery}")
    })?;
    if cleanup_failed {
        return Err(format!("{name} conformance cleanup failed; recovery: {recovery}; inspect provider resources in the printed namespace before rerunning").into());
    }
    println!("{name} machines conformance passed (canonical suite, unchanged)");
    Ok(())
}
