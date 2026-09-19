//! One bounded, machine-readable local qualification entry point.

#[path = "qualify/tools.rs"]
#[allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::panic,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    reason = "the imported qualification fixture tool retains its existing bounded diagnostic behavior"
)]
mod tools;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use acyclic_fs::demand::{DemandSource, native::NativeDemandSource};
use acyclic_fs::model::{FilesystemProfile, VolumeLimits};
use acyclic_fs::native_mount::{MountOptions, NativeMountKind, probe_native_mount};
use acyclic_fs::path::PortablePath;
use acyclic_fs::{CancellationToken, Fs, LocalOptions};
use acyclic_objects::{
    GetRequest, LocalObjects, LocalObjectsLimits, ObjectsError, ObjectsProvider, PutRequest,
    ReadTarget, wire,
};
use acyclic_stream::{AppendOutcome, LocalStream, LocalStreamLimits, StreamClient};
use bytes::Bytes;
use futures::StreamExt as _;
use serde::Serialize;

#[derive(Serialize)]
struct Case {
    name: &'static str,
    elapsed_ms: u128,
    passed: bool,
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    supported: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unsupported_reason: Option<String>,
}

#[derive(Serialize)]
struct Report {
    schema: u32,
    os: &'static str,
    arch: &'static str,
    seed: String,
    budget_seconds: u64,
    elapsed_ms: u128,
    passed: bool,
    cases: Vec<Case>,
}

async fn run_case<F>(name: &'static str, started: Instant, budget: Duration, future: F) -> Case
where
    F: Future<Output = Result<(), String>>,
{
    let case_started = Instant::now();
    let result = match budget.checked_sub(started.elapsed()) {
        Some(remaining) => tokio::time::timeout(remaining, future)
            .await
            .unwrap_or_else(|_| Err("qualification time budget exceeded".into())),
        None => Err("qualification time budget exceeded".into()),
    };
    Case {
        name,
        elapsed_ms: case_started.elapsed().as_millis(),
        passed: result.is_ok(),
        error: result.err(),
        supported: None,
        unsupported_reason: None,
    }
}

async fn qualify_stream_local(root: PathBuf) -> Result<(), String> {
    let provider = LocalStream::open(root, LocalStreamLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    acyclic_conformance::stream(&provider).await
}

async fn qualify_objects_local(root: PathBuf) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    acyclic_conformance::objects(&provider).await
}

async fn qualify_filesystem_local(root: PathBuf) -> Result<(), String> {
    let provider = Fs::local(LocalOptions::new(root))
        .await
        .map_err(|error| error.to_string())?;
    acyclic_conformance::filesystem_smoke(&provider).await
}

async fn qualify_stream_objects(
    root: &Path,
    started: Instant,
    budget: Duration,
    seed: u64,
) -> Vec<Case> {
    let mut cases = Vec::new();
    cases.push(
        run_case(
            "stream/local",
            started,
            budget,
            Box::pin(qualify_stream_local(root.join("stream"))),
        )
        .await,
    );
    cases.push(
        run_case(
            "objects/local",
            started,
            budget,
            Box::pin(qualify_objects_local(root.join("objects"))),
        )
        .await,
    );
    let sparse = root.join("objects-sparse");
    cases.push(
        run_case(
            "objects/sparse-first-put",
            started,
            budget,
            objects_sparse_first_put(&sparse),
        )
        .await,
    );
    if cases.last().is_some_and(|case| case.passed) {
        cases.push(
            run_case(
                "objects/sparse-reopen-gc",
                started,
                budget,
                objects_sparse_reopen_gc(&sparse),
            )
            .await,
        );
    }
    cases.push(
        run_case(
            "stream/model-recovery",
            started,
            budget,
            Box::pin(stream_history(&root.join("stream-history"), seed)),
        )
        .await,
    );
    cases.push(
        run_case(
            "objects/model-recovery",
            started,
            budget,
            Box::pin(objects_history(&root.join("objects-history"), seed)),
        )
        .await,
    );
    let chunks = root.join("objects-many-chunks");
    cases.push(
        run_case(
            "objects/reopen-many-chunks/setup",
            started,
            budget,
            objects_many_chunks_setup(&chunks),
        )
        .await,
    );
    if cases.last().is_some_and(|case| case.passed) {
        cases.push(
            run_case(
                "objects/reopen-many-chunks/startup",
                started,
                budget,
                objects_many_chunks_startup(&chunks),
            )
            .await,
        );
    }
    cases
}

async fn qualify_filesystem_cases(
    root: &Path,
    started: Instant,
    budget: Duration,
    seed: u64,
) -> Vec<Case> {
    vec![
        run_case(
            "filesystem/local-smoke",
            started,
            budget,
            Box::pin(qualify_filesystem_local(root.join("filesystem"))),
        )
        .await,
        run_case(
            "filesystem/model-recovery",
            started,
            budget,
            Box::pin(filesystem_history(root.join("filesystem-history"), seed)),
        )
        .await,
        run_case(
            "filesystem/demand-local",
            started,
            budget,
            Box::pin(filesystem_demand(root.join("filesystem-demand"))),
        )
        .await,
    ]
}

async fn qualify_native_case(root: &Path, started: Instant, budget: Duration) -> Case {
    let capability = probe_native_mount();
    if capability.available
        && capability.writable
        && capability.kind != Some(NativeMountKind::MacOsNfs)
    {
        let mut case = run_case(
            "filesystem/sqlite-wal-mount",
            started,
            budget,
            filesystem_sqlite_wal(root.join("sqlite-wal")),
        )
        .await;
        case.supported = Some(true);
        case
    } else {
        Case {
            name: "filesystem/sqlite-wal-mount",
            elapsed_ms: 0,
            passed: true,
            error: None,
            supported: Some(false),
            unsupported_reason: Some(if capability.kind == Some(NativeMountKind::MacOsNfs) {
                "SQLite WAL declined on the macOS loopback NFS mount; the shared-memory WAL consumer is unsupported".into()
            } else {
                capability.unavailable_reason.unwrap_or_else(|| {
                    "native mount is unavailable or read-only on this host".into()
                })
            }),
        }
    }
}

async fn stream_history(root: &Path, seed: u64) -> Result<(), String> {
    let provider = LocalStream::open(root, LocalStreamLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    let client = StreamClient::new(Arc::new(provider));
    let stream = client
        .stream("consumer/history")
        .map_err(|error| error.to_string())?;
    let mut rng = seed | 1;
    let mut expected = Vec::<Bytes>::new();
    for step in 0..256 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let payload = Bytes::copy_from_slice(&rng.to_le_bytes());
        let tail = u64::try_from(expected.len()).map_err(|error| error.to_string())?;
        if step % 4 == 1 {
            let outcome = stream
                .append_at(payload, tail + 1)
                .await
                .map_err(|error| error.to_string())?;
            if outcome != (AppendOutcome::TailConflict { actual_tail: tail }) {
                return Err(format!("step {step}: stale append changed the tail"));
            }
        } else if step % 4 != 2 {
            let outcome = stream
                .append_at(payload.clone(), tail)
                .await
                .map_err(|error| error.to_string())?;
            let AppendOutcome::Committed(receipt) = outcome else {
                return Err(format!("step {step}: exact-tail append conflicted"));
            };
            if receipt.start != tail || receipt.end != tail + 1 {
                return Err(format!("step {step}: append receipt has wrong range"));
            }
            expected.push(payload);
        }
        let tail = u64::try_from(expected.len()).map_err(|error| error.to_string())?;
        if stream.tail().await.map_err(|error| error.to_string())? != tail {
            return Err(format!("step {step}: tail differs from model"));
        }
        let from = if expected.is_empty() {
            0
        } else {
            rng % (tail + 1)
        };
        let observed = stream
            .read(from, 8)
            .await
            .map_err(|error| error.to_string())?
            .collect::<Vec<_>>()
            .await;
        for (index, record) in observed.iter().enumerate() {
            let record = record.as_ref().map_err(|error| error.to_string())?;
            let offset = usize::try_from(from).map_err(|error| error.to_string())? + index;
            if record.sequence != from + index as u64 || expected.get(offset) != Some(&record.value)
            {
                return Err(format!("step {step}: read differs from model at {offset}"));
            }
        }
        let remaining =
            expected.len() - usize::try_from(from).map_err(|error| error.to_string())?;
        if observed.len() != remaining.min(8) {
            return Err(format!("step {step}: read length differs from model"));
        }
    }
    drop(stream);
    drop(client);
    let reopened = LocalStream::open(root, LocalStreamLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    let client = StreamClient::new(Arc::new(reopened));
    let stream = client
        .stream("consumer/history")
        .map_err(|error| error.to_string())?;
    let recovered = stream
        .read(
            0,
            u32::try_from(expected.len()).map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?
        .collect::<Vec<_>>()
        .await;
    if recovered.len() != expected.len()
        || recovered.iter().zip(&expected).any(|(record, value)| {
            !record
                .as_ref()
                .is_ok_and(|record| record.value.as_ref() == value.as_ref())
        })
    {
        return Err("reopened stream differs from model".into());
    }
    Ok(())
}

async fn objects_history(root: &Path, seed: u64) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    let bucket = provider
        .create_bucket("consumer-history".into(), Some("create-history".into()))
        .await
        .map_err(|error| error.to_string())?
        .bucket
        .ok_or("bucket creation returned no identity")?;
    let mut expected = std::collections::BTreeMap::<String, Bytes>::new();
    let mut rng = seed | 1;
    for step in 0..48_u64 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let key = format!("entry-{:02}", rng % 8);
        if step % 5 == 2 {
            let idempotency_key = Some(format!("delete-{step}"));
            let result = provider
                .delete(
                    bucket.clone(),
                    key.clone(),
                    None,
                    None,
                    idempotency_key.clone(),
                )
                .await
                .map_err(|error| error.to_string())?;
            let replay = provider
                .delete(bucket.clone(), key.clone(), None, None, idempotency_key)
                .await
                .map_err(|error| error.to_string())?;
            if result != replay {
                return Err(format!("step {step}: delete retry changed its outcome"));
            }
            expected.remove(&key);
        } else {
            let body = Bytes::copy_from_slice(&rng.to_le_bytes());
            let request = PutRequest {
                bucket: bucket.clone(),
                object_key: key.clone(),
                body: body.clone(),
                metadata: wire::ObjectMetadata::default(),
                condition: None,
                idempotency_key: Some(format!("put-{step}")),
            };
            let version = provider
                .put(request.clone())
                .await
                .map_err(|error| error.to_string())?;
            let replay = provider
                .put(request)
                .await
                .map_err(|error| error.to_string())?;
            if version != replay || version.size != body.len() as u64 {
                return Err(format!("step {step}: put retry or size differs"));
            }
            expected.insert(key.clone(), body);
        }
        verify_objects_current(&provider, &bucket, &expected, &key, step).await?;
    }
    drop(provider);
    let reopened = LocalObjects::open(root, LocalObjectsLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    reopened
        .head_bucket(&bucket)
        .await
        .map_err(|error| error.to_string())?;
    for (key, body) in expected {
        let observed = reopened
            .get(GetRequest {
                target: ReadTarget::Bucket(bucket.clone()),
                object_key: key.clone(),
                version_id: None,
                range: None,
                if_match: None,
                if_none_match: None,
                maximum_bytes: 8,
            })
            .await
            .map_err(|error| error.to_string())?;
        if observed.body != body {
            return Err(format!("reopened object {key} differs from model"));
        }
    }
    Ok(())
}

async fn objects_many_chunks_setup(root: &Path) -> Result<(), String> {
    const CHUNK_BYTES: usize = 1_024 * 1_024;
    const CHUNKS: usize = 48;
    let provider = LocalObjects::open(root, LocalObjectsLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    let bucket = provider
        .create_bucket("many-chunks".into(), Some("create-many-chunks".into()))
        .await
        .map_err(|error| error.to_string())?
        .bucket
        .ok_or("bucket creation returned no identity")?;
    let mut body = vec![0_u8; CHUNK_BYTES * CHUNKS];
    for (index, chunk) in body.chunks_mut(CHUNK_BYTES).enumerate() {
        chunk.fill(u8::try_from(index).map_err(|_| "chunk index exceeds byte range")?);
    }
    provider
        .put(PutRequest {
            bucket,
            object_key: "many-chunks".into(),
            body: Bytes::from(body),
            metadata: wire::ObjectMetadata::default(),
            condition: None,
            idempotency_key: Some("put-many-chunks".into()),
        })
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn objects_sparse_first_put(root: &Path) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    let bucket = provider
        .create_bucket("sparse".into(), None)
        .await
        .map_err(|error| error.to_string())?
        .bucket
        .ok_or("sparse bucket creation returned no identity")?;
    provider
        .put(PutRequest {
            bucket,
            object_key: "only-object".into(),
            body: Bytes::from_static(b"sparse body"),
            metadata: wire::ObjectMetadata::default(),
            condition: None,
            idempotency_key: None,
        })
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn objects_sparse_reopen_gc(root: &Path) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    let bucket = provider
        .bucket_named("sparse")
        .await
        .map_err(|error| error.to_string())?
        .ok_or("sparse bucket is missing after reopen")?;
    provider
        .collect_garbage(100)
        .await
        .map_err(|error| error.to_string())?;
    let object = provider
        .get(GetRequest {
            target: ReadTarget::Bucket(bucket),
            object_key: "only-object".into(),
            version_id: None,
            range: None,
            if_match: None,
            if_none_match: None,
            maximum_bytes: 11,
        })
        .await
        .map_err(|error| error.to_string())?;
    if object.body != Bytes::from_static(b"sparse body") {
        return Err("sparse body changed after reopen and collection".into());
    }
    Ok(())
}

async fn objects_many_chunks_startup(root: &Path) -> Result<(), String> {
    let provider = LocalObjects::open(root, LocalObjectsLimits::default())
        .await
        .map_err(|error| error.to_string())?;
    let bucket = provider
        .bucket_named("many-chunks")
        .await
        .map_err(|error| error.to_string())?
        .ok_or("reopened bucket is missing")?;
    provider
        .head_bucket(&bucket)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn verify_objects_current(
    provider: &impl ObjectsProvider,
    bucket: &wire::BucketRef,
    expected: &std::collections::BTreeMap<String, Bytes>,
    selected_key: &str,
    step: u64,
) -> Result<(), String> {
    let mut observed = std::collections::BTreeSet::new();
    let mut continuation = None;
    for page_number in 0..4 {
        let page = provider
            .list(
                ReadTarget::Bucket(bucket.clone()),
                String::new(),
                None,
                false,
                3,
                continuation,
            )
            .await
            .map_err(|error| error.to_string())?;
        for entry in page.entries {
            if entry
                .version
                .as_ref()
                .is_none_or(|version| version.delete_marker)
                || !observed.insert(entry.object_key)
            {
                return Err(format!(
                    "step {step}: listing returned a duplicate or marker"
                ));
            }
        }
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
        if page_number == 3 {
            return Err(format!("step {step}: listing continuation did not finish"));
        }
    }
    if observed != expected.keys().cloned().collect() {
        return Err(format!("step {step}: listing differs from model"));
    }
    let request = GetRequest {
        target: ReadTarget::Bucket(bucket.clone()),
        object_key: selected_key.to_owned(),
        version_id: None,
        range: Some((1, Some(3))),
        if_match: None,
        if_none_match: None,
        maximum_bytes: 3,
    };
    match (expected.get(selected_key), provider.get(request).await) {
        (Some(body), Ok(result)) if body.get(1..4) == Some(result.body.as_ref()) => Ok(()),
        (None, Err(ObjectsError::NotFound)) => Ok(()),
        _ => Err(format!("step {step}: range read differs from model")),
    }
}

async fn filesystem_history(root: std::path::PathBuf, seed: u64) -> Result<(), String> {
    let fs = Box::pin(Fs::local(LocalOptions::new(&root)))
        .await
        .map_err(|error| error.to_string())?;
    let workspace = Box::pin(fs.create_workspace("consumer-history"))
        .await
        .map_err(|error| error.to_string())?;
    let mut rng = seed | 1;
    let mut expected = std::collections::BTreeMap::<String, Bytes>::new();
    for step in 0..64_u64 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let path = format!("/history-{}.txt", rng % 8);
        if step % 5 == 0 && expected.remove(&path).is_some() {
            Box::pin(workspace.remove(&path))
                .await
                .map_err(|error| error.to_string())?;
            if Box::pin(workspace.read(&path, 64)).await.is_ok() {
                return Err(format!("step {step}: removed path remains readable"));
            }
        } else {
            let value = Bytes::from(format!("{step}:{rng}"));
            Box::pin(workspace.write(&path, value.clone()))
                .await
                .map_err(|error| error.to_string())?;
            expected.insert(path.clone(), value.clone());
            if Box::pin(workspace.read(&path, 64))
                .await
                .map_err(|error| error.to_string())?
                != value
            {
                return Err(format!("step {step}: written value differs from model"));
            }
        }
    }
    Box::pin(workspace.write("/unicode-雪.txt", Bytes::from_static(b"utf8 path")))
        .await
        .map_err(|error| error.to_string())?;
    expected.insert("/unicode-雪.txt".into(), Bytes::from_static(b"utf8 path"));
    drop(workspace);
    drop(fs);
    let reopened = Box::pin(Fs::local(LocalOptions::new(&root)))
        .await
        .map_err(|error| error.to_string())?;
    let workspace = Box::pin(reopened.open_workspace("consumer-history"))
        .await
        .map_err(|error| error.to_string())?;
    for (path, value) in expected {
        let observed = Box::pin(workspace.read(&path, 64))
            .await
            .map_err(|error| error.to_string())?;
        if observed != value {
            return Err(format!("reopened path {path} differs from model"));
        }
    }
    Ok(())
}

async fn filesystem_demand(root: std::path::PathBuf) -> Result<(), String> {
    std::fs::create_dir_all(root.join("unrelated")).map_err(|error| error.to_string())?;
    for index in 0..512 {
        std::fs::write(
            root.join("unrelated").join(format!("{index:04}")),
            b"ignored",
        )
        .map_err(|error| error.to_string())?;
    }
    std::fs::write(root.join("wanted"), b"0123456789").map_err(|error| error.to_string())?;
    let source =
        NativeDemandSource::open(&root, FilesystemProfile::Portable, VolumeLimits::default())
            .await
            .map_err(|error| error.to_string())?;
    let wanted = PortablePath::parse("/wanted", VolumeLimits::default())
        .map_err(|error| error.to_string())?;
    let wanted = acyclic_fs::kernel::NamespacePath::from_portable(&wanted, VolumeLimits::default())
        .map_err(|error| error.to_string())?;
    let cancellation = CancellationToken::new();
    let provider: &dyn DemandSource = &source;
    let looked_up = provider
        .lookup(provider.reference(), &wanted, &cancellation)
        .await
        .map_err(|error| error.to_string())?;
    if looked_up.work.source_path_components != 1
        || looked_up.work.source_entries_visited != 0
        || looked_up.work.source_bytes_read != 0
    {
        return Err("exact lookup traversed unrelated source content".into());
    }
    let version = looked_up.value.ok_or("wanted path is absent")?.version;
    let range = provider
        .read_range(provider.reference(), &wanted, version, 3, 4, &cancellation)
        .await
        .map_err(|error| error.to_string())?;
    if range.value.as_ref() != b"3456"
        || range.work.source_entries_visited != 0
        || range.work.source_bytes_read != 4
    {
        return Err("range read was incorrect or traversed unrelated source content".into());
    }
    Ok(())
}

fn sqlite_wal_child(path: &Path) -> Result<(), String> {
    use rusqlite::Connection;

    let writer = Connection::open(path).map_err(|error| error.to_string())?;
    writer
        .busy_timeout(Duration::from_secs(2))
        .map_err(|error| error.to_string())?;
    let mode: String = writer
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    if mode != "wal" {
        return Err(format!("SQLite declined WAL mode: {mode}"));
    }
    writer
        .execute_batch(
            "PRAGMA synchronous=FULL; CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT NOT NULL); \
             BEGIN IMMEDIATE; INSERT INTO entries VALUES (1, 'first'); COMMIT;",
        )
        .map_err(|error| error.to_string())?;
    let reader = Connection::open(path).map_err(|error| error.to_string())?;
    reader
        .execute_batch("BEGIN")
        .map_err(|error| error.to_string())?;
    let read_count = |connection: &Connection| -> Result<i64, String> {
        connection
            .query_row("SELECT count(*) FROM entries", [], |row| row.get(0))
            .map_err(|error| error.to_string())
    };
    if read_count(&reader)? != 1 {
        return Err("SQLite reader missed the first commit".into());
    }
    writer
        .execute("INSERT INTO entries VALUES (2, 'second')", [])
        .map_err(|error| error.to_string())?;
    if read_count(&reader)? != 1 {
        return Err("SQLite WAL reader lost snapshot isolation".into());
    }
    reader
        .execute_batch("COMMIT")
        .map_err(|error| error.to_string())?;
    if read_count(&reader)? != 2 {
        return Err("SQLite reader missed the second commit".into());
    }
    let mut wal_name = path.as_os_str().to_os_string();
    wal_name.push("-wal");
    if std::fs::metadata(std::path::PathBuf::from(wal_name))
        .map_err(|error| error.to_string())?
        .len()
        == 0
    {
        return Err("SQLite WAL contains no durable records".into());
    }
    // Exit without SQLite destructors: the mount must publish and recover a
    // committed WAL without a clean-close checkpoint.
    std::process::exit(0)
}

fn sqlite_verify_child(path: &Path) -> Result<(), String> {
    let connection = rusqlite::Connection::open(path).map_err(|error| error.to_string())?;
    let count: i64 = connection
        .query_row("SELECT count(*) FROM entries", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    let integrity: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    if count != 2 || integrity != "ok" {
        return Err(format!(
            "SQLite remount differs from committed state: rows={count}, integrity={integrity}"
        ));
    }
    let mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    if mode != "wal" {
        return Err(format!("SQLite remount lost WAL mode: {mode}"));
    }
    let (busy, _, _): (i64, i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(|error| error.to_string())?;
    if busy != 0 {
        return Err("SQLite recovery checkpoint remained busy".into());
    }
    Ok(())
}

async fn run_sqlite_child(mode: &str, path: &Path) -> Result<(), String> {
    let output =
        tokio::process::Command::new(std::env::current_exe().map_err(|error| error.to_string())?)
            .arg(mode)
            .arg(path)
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "SQLite child {mode} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

async fn filesystem_sqlite_wal(root: std::path::PathBuf) -> Result<(), String> {
    let store = root.join("store");
    let destination = root.join("mount");
    std::fs::create_dir_all(&destination).map_err(|error| error.to_string())?;
    let fs = Box::pin(Fs::local(LocalOptions::new(&store)))
        .await
        .map_err(|error| error.to_string())?;
    let workspace = Box::pin(fs.create_workspace("sqlite-wal"))
        .await
        .map_err(|error| error.to_string())?;
    let mount = workspace
        .mount(&destination, MountOptions::read_write())
        .await
        .map_err(|error| error.to_string())?;
    let child = run_sqlite_child("--sqlite-wal-child", &destination.join("wal.db")).await;
    let detached = mount.unmount().await.map_err(|error| error.to_string());
    child?;
    detached?;
    let body = Box::pin(workspace.read("/wal.db", 1024 * 1024))
        .await
        .map_err(|error| error.to_string())?;
    if body.is_empty() {
        return Err("mounted SQLite database was not published".into());
    }
    let wal = Box::pin(workspace.read("/wal.db-wal", 1024 * 1024))
        .await
        .map_err(|error| error.to_string())?;
    if wal.is_empty() {
        return Err("committed SQLite WAL was not published".into());
    }
    let remount_path = root.join("remount");
    std::fs::create_dir_all(&remount_path).map_err(|error| error.to_string())?;
    let remount = workspace
        .mount(&remount_path, MountOptions::read_write())
        .await
        .map_err(|error| error.to_string())?;
    let child = run_sqlite_child("--sqlite-verify-child", &remount_path.join("wal.db")).await;
    let detached = remount.unmount().await.map_err(|error| error.to_string());
    child?;
    detached?;
    Ok(())
}

fn main() {
    if let Some(argument) = std::env::args().nth(1) {
        if tools::is_command(&argument) {
            tools::run();
            return;
        }
        if !argument.starts_with("--") {
            eprintln!("unknown qualification command: {argument}");
            std::process::exit(2);
        }
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("cannot start qualification runtime: {error}");
            std::process::exit(2);
        }
    };
    qualify_main(&runtime);
}

#[allow(
    clippy::too_many_lines,
    reason = "one linear bounded qualification sequence"
)]
fn qualify_main(runtime: &tokio::runtime::Runtime) {
    let mut arguments = std::env::args_os();
    let child_mode = arguments.nth(1);
    if child_mode.as_deref() == Some(std::ffi::OsStr::new("--sqlite-wal-child"))
        || child_mode.as_deref() == Some(std::ffi::OsStr::new("--sqlite-verify-child"))
    {
        let result = arguments
            .next()
            .ok_or_else(|| "missing SQLite child database path".to_owned())
            .and_then(|path| {
                if child_mode.as_deref() == Some(std::ffi::OsStr::new("--sqlite-wal-child")) {
                    sqlite_wal_child(Path::new(&path))
                } else {
                    sqlite_verify_child(Path::new(&path))
                }
            });
        if let Err(error) = result {
            eprintln!("{error}");
            std::process::exit(1);
        }
        return;
    }
    let budget_seconds = std::env::args()
        .skip(1)
        .find_map(|argument| argument.strip_prefix("--max-seconds=").map(str::to_owned))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(300);
    let budget = Duration::from_secs(budget_seconds);
    let seed = std::env::var("ACYCLIC_QUAL_SEED").unwrap_or_else(|_| "0".into());
    let numeric_seed = seed.parse::<u64>().unwrap_or(0);
    let started = Instant::now();
    let mut cases = Vec::new();

    let root = match tempfile::tempdir() {
        Ok(root) => root,
        Err(error) => {
            eprintln!("cannot create qualification root: {error}");
            std::process::exit(2);
        }
    };
    cases.extend(runtime.block_on(qualify_stream_objects(
        root.path(),
        started,
        budget,
        numeric_seed,
    )));
    cases.extend(runtime.block_on(qualify_filesystem_cases(
        root.path(),
        started,
        budget,
        numeric_seed,
    )));
    cases.push(runtime.block_on(Box::pin(qualify_native_case(root.path(), started, budget))));
    let report = Report {
        schema: 1,
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        seed,
        budget_seconds,
        elapsed_ms: started.elapsed().as_millis(),
        passed: cases.iter().all(|case| case.passed),
        cases,
    };
    match serde_json::to_string(&report) {
        Ok(json) => println!("{json}"),
        Err(error) => {
            eprintln!("cannot encode qualification report: {error}");
            std::process::exit(2);
        }
    }
    if !report.passed {
        std::process::exit(1);
    }
}
