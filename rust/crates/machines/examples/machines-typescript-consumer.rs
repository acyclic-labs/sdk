#![allow(missing_docs, reason = "executable documentation scenario")]

use acyclic_machines::{
    CreateMachine, IdempotencyKey, Image, MachinesProvider, ProviderError, SimulatedMachines,
    MAX_PAGE_SIZE,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let request = CreateMachine::new(
        IdempotencyKey::parse("11111111-1111-4111-8111-111111111111")?,
        Image::managed([0x11; 32])?,
        [0x22; 32],
    );
    let provider = SimulatedMachines::default();
    let (outcome, page) = futures::executor::block_on(async {
        let outcome = provider.create(request.clone()).await?;
        let page = provider.list_machines(None, MAX_PAGE_SIZE).await?;
        Ok::<_, ProviderError>((outcome, page))
    })?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "request": request,
            "page_size": MAX_PAGE_SIZE,
            "outcome": outcome,
            "page": page,
        }))?
    );
    Ok(())
}
