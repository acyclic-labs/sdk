//! Credential-free local storage baseline, including forced snapshot recovery.
//! bb2: cargo run --locked -p acyclic-stream --all-features
//! --example local-storage-cost -- 1000 128
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
mod benchmark {
    use acyclic_stream::{
        AppendOutcome, AppendRequest, LocalStream, LocalStreamLimits, OwnedBytes, StreamPath,
        StreamProvider,
    };
    use std::{error::Error, path::Path, time::Instant};

    type Result<T> = std::result::Result<T, Box<dyn Error>>;

    fn length(root: &Path, name: &str) -> Result<u64> {
        match std::fs::metadata(root.join(name)) {
            Ok(metadata) => Ok(metadata.len()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error.into()),
        }
    }

    #[cfg(target_os = "linux")]
    fn read_chars() -> Result<Option<u64>> {
        let io = std::fs::read_to_string("/proc/self/io")?;
        Ok(Some(
            io.lines()
                .find_map(|line| line.strip_prefix("rchar:"))
                .ok_or("missing process rchar counter")?
                .trim()
                .parse()?,
        ))
    }
    #[cfg(not(target_os = "linux"))]
    fn read_chars() -> Result<Option<u64>> {
        Ok(None)
    }

    pub(super) async fn run() -> Result<()> {
        let mut args = std::env::args().skip(1);
        let count = args.next().map_or(Ok(1000), |arg| arg.parse::<u64>())?;
        // Lower the stock recovery bound to exercise maintenance in a short run.
        // Memory and payload ceilings remain stock, and durability is unchanged.
        let journal_operations = args.next().map_or(Ok(128), |arg| arg.parse::<u64>())?;
        let limits = LocalStreamLimits {
            journal_operations,
            ..Default::default()
        };
        if count == 0 {
            return Err("record count must be positive".into());
        }
        let root = tempfile::tempdir()?;
        let path = StreamPath::new("storage-cost/records")?;
        let mut provider = LocalStream::open(root.path(), limits).await?;
        let mut oldest = None;
        let mut started = Instant::now();
        let mut window_start = 0;
        for step in 1..=count {
            let AppendOutcome::Committed(receipt) = provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![OwnedBytes::copy_from_slice(&[42; 32]).into()],
                    if_tail: Some(step - 1),
                    idempotency_key: acyclic_stream::IdempotencyKey::new(
                        format!("local-storage-cost-{step}").into_bytes(),
                    )?,
                })
                .await?
            else {
                return Err("unexpected append conflict".into());
            };
            let oldest = *oldest.get_or_insert(receipt.commit_id);
            if [1, 10, 100, 1000, 10_000, 100_000, 1_000_000].contains(&step) || step == count {
                let append_us = started.elapsed().as_secs_f64() * 1e6
                    / f64::from(u32::try_from(step - window_start)?);
                let journal = length(root.path(), "stream.journal")?;
                let snapshot = length(root.path(), "stream.snapshot")?;
                drop(provider);
                let before = read_chars()?;
                let reopen = Instant::now();
                provider = LocalStream::open(root.path(), limits).await?;
                let reopen_ms = reopen.elapsed().as_secs_f64() * 1000.0;
                let read = match read_chars()?.zip(before) {
                    Some((after, before)) => Some(
                        after
                            .checked_sub(before)
                            .ok_or("process rchar counter regressed")?,
                    ),
                    None => None,
                };
                // Preserve the public historical identity across every recovery cut.
                assert_eq!(provider.tail(path.clone()).await?, step);
                assert_eq!(provider.read_commit(oldest).await?.commit_id, oldest);
                println!(
                    "storage ending at record {step}: {append_us:.3} us/append; journal {journal} bytes; snapshot {snapshot} bytes; reopen {reopen_ms:.3} ms; process reopen rchar {read:?}"
                );
                window_start = step;
                started = Instant::now();
            }
        }
        Ok(())
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    benchmark::run().await
}

#[cfg(not(all(feature = "local", not(target_arch = "wasm32"))))]
fn main() {
    eprintln!("local-storage-cost requires the native local feature");
}
