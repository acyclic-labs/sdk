//! Additional composition evidence; the canonical filesystem smoke is unchanged.
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, CancellationToken, Fs, WorkBudget};
use std::{error::Error, time::Instant};

pub async fn verify<A, O>(
    fs: &Fs<A, O>,
    reopen: impl AsyncFnOnce() -> Result<Fs<A, O>, Box<dyn Error>>,
) -> Result<(), Box<dyn Error>>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let start = Instant::now();
    let cancellation = CancellationToken::new();
    let workspace = fs
        .create_workspace("conformance-chunks", WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value;
    // Exceeds the inline-file ceiling and forces ordinary content objects.
    let body = bytes::Bytes::from((0u8..=255).cycle().take(256 * 1024).collect::<Vec<_>>());
    workspace.write("/chunks", body.clone()).await?;
    drop(workspace);
    let reopened = reopen().await?;
    let workspace = reopened
        .open_workspace("conformance-chunks", WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value;
    let observed = workspace
        .read(
            "/chunks",
            body.len() as u64,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    assert_eq!(observed.as_ref(), body.as_ref());
    println!(
        "Filesystem chunked content after independent provider reopen: {:?}",
        start.elapsed()
    );
    Ok(())
}
