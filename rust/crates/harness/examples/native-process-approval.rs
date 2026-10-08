//! Public-contract consumer for comparing native and WASM approval identities.
//! Input is JSON text so filesystem u64 allowances never pass through JS numbers.

use acyclic_harness::{OperationId, TaskId, filesystem::NativeProcessRequest};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    task: String,
    command: String,
    request: NativeProcessRequest,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input: Input = serde_json::from_reader(std::io::stdin().lock())?;
    let digest = input.request.approval_digest(
        TaskId::parse(&input.task)?,
        OperationId::parse(&input.command)?,
    )?;
    serde_json::to_writer(std::io::stdout().lock(), &digest)?;
    Ok(())
}
