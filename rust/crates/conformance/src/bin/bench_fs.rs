//! Compare identical public-API workloads under local durability policies.

use std::time::Instant;

use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_fs::{
    CancellationToken, CaptureOptions, Fs, IdempotencyKey, LocalObjectsDurability, LocalOptions,
    LocalStreamDurability, WorkBudget, capture_baseline, capture_root_identity,
};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut writes = 64_u32;
    let mut batch = false;
    let mut barrier = false;
    let mut capture = false;
    let mut file_bytes = 1024 * 1024_u64;
    for argument in std::env::args().skip(1) {
        if argument == "--batch" {
            batch = true;
        } else if argument == "--capture" {
            capture = true;
        } else if argument == "--barrier" {
            barrier = true;
        } else if let Some(count) = argument.strip_prefix("--writes=") {
            writes = count.parse()?;
        } else if let Some(count) = argument.strip_prefix("--file-bytes=") {
            file_bytes = count.parse()?;
        } else {
            return Err(format!("unknown argument: {argument}").into());
        }
    }
    if writes == 0 || writes > 1024 {
        return Err("writes must be between 1 and 1024".into());
    }
    if capture {
        if file_bytes == 0 || file_bytes > 1024 * 1024 * 1024 {
            return Err("file-bytes must be between 1 and 1073741824".into());
        }
        return benchmark_capture(writes, file_bytes).await;
    }
    let root = tempfile::tempdir()?;
    let mut options = LocalOptions::new(root.path());
    if barrier {
        options.stream.durability = LocalStreamDurability::Barrier;
        options.objects.durability = LocalObjectsDurability::Barrier;
    }
    let fs = Box::pin(Fs::local(options)).await?;
    let workspace = Box::pin(fs.create_workspace("benchmark")).await?;
    let started = Instant::now();
    if batch {
        let mut transaction = Box::pin(workspace.begin_transaction(IdempotencyKey::new())).await?;
        for index in 0..writes {
            Box::pin(transaction.write(
                &format!("/entry-{index:04}"),
                Bytes::from(format!("value-{index}")),
            ))
            .await?;
        }
        Box::pin(transaction.commit()).await?;
    } else {
        for index in 0..writes {
            Box::pin(workspace.write(
                &format!("/entry-{index:04}"),
                Bytes::from(format!("value-{index}")),
            ))
            .await?;
        }
    }
    let elapsed = started.elapsed();
    for index in 0..writes {
        let value = Box::pin(workspace.read(&format!("/entry-{index:04}"), 64)).await?;
        let expected = format!("value-{index}");
        if value.as_ref() != expected.as_bytes() {
            return Err(format!("entry {index} differs from the model").into());
        }
    }
    println!(
        "{}",
        serde_json::json!({
            "schema": 1,
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "durability": if barrier { "barrier" } else { "full-flush" },
            "mode": if batch { "batch" } else { "individual" },
            "writes": writes,
            "elapsed_ms": elapsed.as_millis(),
            "writes_per_second": f64::from(writes) / elapsed.as_secs_f64(),
        })
    );
    Ok(())
}

async fn benchmark_capture(paths: u32, file_bytes: u64) -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    let body = vec![0x5a; usize::try_from(file_bytes)?];
    for index in 0..paths {
        std::fs::write(source.path().join(format!("entry-{index:04}")), &body)?;
    }
    let storage = tempfile::tempdir()?;
    let fs = Box::pin(Fs::local(LocalOptions::new(storage.path()))).await?;
    let workspace = Box::pin(fs.create_workspace("capture-benchmark")).await?;
    let mut checkout = Box::pin(workspace.checkout(
        GenerationSelector::Head,
        CheckoutMode::tracking_transaction(),
    ))
    .await?;
    let options = CaptureOptions {
        source_root: source.path().to_path_buf(),
        expected_root_identity: capture_root_identity(source.path())?,
        maximum_paths: paths.saturating_add(1),
        maximum_extent_spans: 1024,
    };
    let started = Instant::now();
    let receipt = capture_baseline(
        &mut checkout,
        &options,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    let elapsed = started.elapsed();
    let work = receipt.work;
    let total_bytes = file_bytes
        .checked_mul(u64::from(paths))
        .ok_or("capture byte count overflow")?;
    let elapsed_ns = elapsed.as_nanos().max(1);
    let bytes_per_second = u128::from(total_bytes)
        .saturating_mul(1_000_000_000)
        .checked_div(elapsed_ns)
        .unwrap_or_default();
    println!(
        "{}",
        serde_json::json!({
            "schema": 1,
            "case": "filesystem/capture-baseline",
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "paths": paths,
            "file_bytes": file_bytes,
            "total_bytes": total_bytes,
            "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
            "bytes_per_second": bytes_per_second,
            "examined_paths": receipt.value.examined_paths,
            "changed_paths": receipt.value.changed_paths,
            "source_bytes_read": work.source_bytes_read,
            "items_examined": work.items_examined,
            "backend_read_operations": work.backend_read_operations,
            "backend_write_operations": work.backend_write_operations,
            "allocation_operations": work.allocation_operations,
            "peak_allocation_bytes": work.peak_allocation_bytes,
        })
    );
    Ok(())
}
