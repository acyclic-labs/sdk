use acyclic_objects::{Blobs, ByteRange, HttpObjects};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let objects = HttpObjects::new(
        &std::env::var("OBJECTS_ENDPOINT")?,
        &std::env::var("ACYCLIC_API_KEY")?,
        1024 * 1024,
        None,
    )?;
    let put = objects.put(b"hello".to_vec().into(), Vec::new()).await?;
    let digest = put
        .verified()
        .ok_or("outcome unknown; retrying is safe")?
        .digest;
    let object = objects.get(digest, ByteRange::FULL, 1024).await?;
    println!("{digest}: {:?}", object.body());
    Ok(())
}
