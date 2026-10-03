//! Compile-only capability discovery example.
//!
//! Running this example requires a real authenticated customer endpoint and a
//! trusted PEM CA. Package qualification therefore checks the source and does
//! not invoke the binary or imply hosted availability.

use std::{env, error::Error, fs};

use acyclic_inference::Inference;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = env::var("INFERENCE_ENDPOINT")?;
    let api_key = env::var("INFERENCE_API_KEY")?;
    let ca_pem = fs::read(env::var("INFERENCE_CA_PEM")?)?;
    let client = Inference::connect(&endpoint, &api_key, &ca_pem).await?;

    // The response is service evidence for this authenticated endpoint. It is
    // not a static claim that a model or hosted service is available.
    for capability in client.models().await? {
        println!("{capability:?}");
    }
    Ok(())
}
