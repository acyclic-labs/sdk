use acyclic_fs::model::{Lifecycle, VolumeConfig};
use acyclic_fs::{CancellationToken, Fs, LocalOptions, WorkBudget};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::var("FS_ROOT")?;
    let fs = Fs::local(LocalOptions::new(&root)).await?;
    let cancellation = CancellationToken::default();
    let _workspace = fs
        .create_volume(
            VolumeConfig::portable(Lifecycle::Durable),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    println!("Created a durable workspace in {root}");
    Ok(())
}
