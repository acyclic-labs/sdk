use acyclic_actors::{http::Client, wire};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let actors = Client::new(
        &std::env::var("ACTORS_ENDPOINT")?,
        &std::env::var("ACYCLIC_API_KEY")?,
        1024 * 1024,
    )?;
    let actor = actors
        .inspect_actor(&wire::InspectActorRequest {
            actor_id: std::env::var("ACTOR_ID")?,
            ..Default::default()
        })
        .await?;
    println!("{actor:?}");
    Ok(())
}
