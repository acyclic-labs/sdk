//! Compile-only capability discovery example.
//!
//! `client::Client::connect` selects the best authenticated transport for the
//! current target. Native callers prefer gRPC and browser callers use HTTP.

use std::{env, error::Error};

use acyclic_inference::{client::Client, wire::ListModelsRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = env::var("INFERENCE_ENDPOINT")?;
    let api_key = env::var("INFERENCE_API_KEY")?;
    let client = Client::connect(&endpoint, &api_key).await?;

    let response = client.list(&ListModelsRequest::default()).await?;
    println!("{response:?}");
    Ok(())
}
