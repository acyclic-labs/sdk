use acyclic_stream::{Stream, http::HttpStream};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = HttpStream::new(
        &std::env::var("STREAM_ENDPOINT")?,
        &std::env::var("ACYCLIC_API_KEY")?,
        1024 * 1024,
    )?;
    let client = Stream::new(Arc::new(provider), std::env::var("STREAM_PATH")?)?;
    println!("{}", client.tail().await?);
    Ok(())
}
