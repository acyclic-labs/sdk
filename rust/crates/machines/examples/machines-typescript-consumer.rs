#![allow(missing_docs)]

//! Emits one deterministic Rust-owned Machines scenario for binding consumers.
//!
//! The request, policy variants, opaque identities, and readonly page payload
//! are produced by the executable Rust contract. The TypeScript declaration
//! projection is generated separately by the existing `tsify`/wasm-bindgen
//! build; this example is the source-owned behavior receipt for that projection.

use acyclic_machines::{
    CreateMachine, IdempotencyKey, Image, MAX_PAGE_SIZE, MachinesProvider, ProviderError,
    SimulatedMachines,
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
            "outcome": outcome,
            "page": page,
        }))?
    );
    Ok(())
}
