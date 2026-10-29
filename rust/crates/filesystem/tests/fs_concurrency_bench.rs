//! Real public commit-path benchmark. Run one named case per invocation with
//! `cargo test --release -p acyclic-fs --test fs_concurrency_bench CASE -- --exact --nocapture`.
//! The driver also builds unchanged on the pre-combiner revision.
use acyclic_fs::kernel::{
    FileMetadata, LogicalName, NameEncoding, NamespacePath, RebaseDecision, volume_authority_id,
};
use acyclic_fs::model::{
    AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
    VolumeConfig,
};
use acyclic_fs::{
    AuthoredMutation, ByteRange, CancellationToken, ContentChange, ContentTimes, Fs, FsError,
    LivePublicationObservation, OperationId, OperationReceipt, PublicationPermit, WorkBudget,
    WorkCounters,
};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{OwnedBytes, StreamProvider};
use futures::TryStreamExt;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
type Volume =
    acyclic_fs::Volume<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;
type Checkout =
    acyclic_fs::Checkout<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;
const B: WorkBudget = WorkBudget::UNBOUNDED;

fn mode() -> CheckoutMode {
    CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::Manual,
        mutations: MutationMode::PrivateOverlay,
    }
}
fn path(parts: &[String]) -> Result<NamespacePath> {
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    Ok(NamespacePath::new(
        parts
            .iter()
            .map(|part| {
                LogicalName::new(
                    NameEncoding::Utf8,
                    part.as_bytes().to_vec(),
                    config.limits.maximum_component_bytes,
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?,
        config.limits,
    )?)
}
async fn allow(_: WorkBudget) -> acyclic_fs::MeasuredResult<OperationReceipt<()>, FsError> {
    Ok(OperationReceipt {
        value: (),
        work: WorkCounters::UNCHARGED,
    })
}

#[derive(Default)]
struct Worker {
    committed: bool,
    rebase_calls: u64,
    rebase_items: u64,
    rebase_object_reads: u64,
}

async fn run(writers: usize, same_file: bool) -> Result {
    acyclic_native_runtime::initialize_native_io_services()?;
    let cancel = CancellationToken::new();
    let stream = Arc::new(acyclic_stream::MemoryStream::default());
    let objects = acyclic_objects::MemoryObjects::memory();
    let fs = Fs::from_memory_providers(Arc::clone(&stream), Arc::new(objects));
    let volume = fs
        .create_volume(VolumeConfig::portable(Lifecycle::Ephemeral), B, &cancel)
        .await?
        .value;
    let (paths, base) = seed_volume(&volume, same_file, &cancel).await?;
    let mut proposals = Vec::with_capacity(writers);
    for index in 0..writers {
        let mut checkout = volume
            .checkout(GenerationSelector::Head, mode(), B, &cancel)
            .await?
            .value;
        checkout
            .change_content(
                path(
                    paths
                        .get(if same_file { 0 } else { index })
                        .ok_or("missing writer path")?,
                )?,
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from(u16::try_from(index + 1)?.to_le_bytes().to_vec()),
                },
                ContentTimes::Preserve,
                B,
                &cancel,
            )
            .await?;
        proposals.push(checkout);
    }
    let began = Instant::now();
    let results = measure(&mut proposals, writers, &cancel).await?;
    let elapsed = began.elapsed();
    let committed = results.iter().filter(|worker| worker.committed).count();
    assert_eq!(committed, if same_file { 1 } else { writers });
    let records: Vec<_> = stream
        .read(acyclic_stream::ReadRequest {
            key: None,
            path: acyclic_stream::StreamPath::new(format!(
                "fs/authorities/{}/records",
                hex::encode(volume_authority_id(volume.id()).into_bytes())
            ))?,
            from: base.sequence.get() + 1,
            limit: u32::try_from(writers)?,
            at_least: Default::default(),
        })
        .await?
        .try_collect()
        .await?;
    assert_eq!(records.len(), committed);
    let appends = records
        .iter()
        .map(|record| record.commit_id)
        .collect::<BTreeSet<_>>()
        .len();
    verify_tree(&volume, &paths, writers, same_file, &cancel).await?;
    report(writers, same_file, &results, appends, elapsed)?;
    Ok(())
}

async fn seed_volume(
    volume: &Volume,
    same_file: bool,
    cancel: &CancellationToken,
) -> Result<(Vec<Vec<String>>, acyclic_fs::Head)> {
    let mut seed = volume
        .checkout(GenerationSelector::Head, mode(), B, cancel)
        .await?
        .value;
    // Keep the same 256-directory volume for every disjoint writer count.
    // Otherwise tree growth is confounded with concurrency.
    let paths: Vec<_> = (0..if same_file { 1 } else { 256 })
        .map(|index| vec![format!("directory-{index}"), "file".to_owned()])
        .collect();
    for parts in &paths {
        seed.create(
            AuthoredMutation::CreateDirectory {
                path: path(parts.get(..1).ok_or("missing directory")?)?,
                metadata: FileMetadata::default(),
            },
            B,
            cancel,
        )
        .await?;
        seed.create(
            AuthoredMutation::CreateFile {
                path: path(parts)?,
                bytes: OwnedBytes::from_static(&[0; 2]),
                metadata: FileMetadata::default(),
            },
            B,
            cancel,
        )
        .await?;
    }
    let base = match seed
        .commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            cancel,
            allow,
        )
        .await?
        .value
    {
        LivePublicationObservation::Committed { head, .. } => head,
        outcome => return Err(format!("seed did not commit: {outcome:?}").into()),
    };
    Ok((paths, base))
}

async fn measure(
    proposals: &mut [Checkout],
    writers: usize,
    cancel: &CancellationToken,
) -> Result<Vec<Worker>> {
    futures::future::join_all(proposals.iter_mut().map(|checkout| {
        async move {
            let operation = OperationId::new();
            let mut measured = Worker::default();
            // Every failed CAS must have advanced past another one of these
            // finite writers; no unrelated producer runs in this workload.
            for _ in 0..=writers {
                match checkout
                    .commit(
                        operation,
                        None,
                        None,
                        None,
                        PublicationPermit::Unrestricted,
                        B,
                        cancel,
                        allow,
                    )
                    .await?
                    .value
                {
                    LivePublicationObservation::Committed { .. } => {
                        measured.committed = true;
                        return Ok::<_, Box<dyn std::error::Error>>(measured);
                    }
                    LivePublicationObservation::Conflict { .. } => {
                        let rebased = checkout
                            .rebase_head(u32::try_from(writers)?, B, cancel)
                            .await?;
                        measured.rebase_calls += 1;
                        measured.rebase_items += rebased.work.items_examined;
                        measured.rebase_object_reads += rebased.work.object_probes;
                        if matches!(rebased.value, RebaseDecision::Conflicted { .. }) {
                            return Ok(measured);
                        }
                    }
                    outcome => return Err(format!("unexpected publication: {outcome:?}").into()),
                }
            }
            Err("finite writer history failed to settle".into())
        }
    }))
    .await
    .into_iter()
    .collect::<Result<Vec<_>>>()
}

async fn verify_tree(
    volume: &Volume,
    paths: &[Vec<String>],
    writers: usize,
    same_file: bool,
    cancel: &CancellationToken,
) -> Result {
    let mut final_tree = volume
        .checkout(GenerationSelector::Head, mode(), B, cancel)
        .await?
        .value;
    for (index, parts) in paths.iter().enumerate() {
        let value = final_tree
            .read_file_range(
                &path(parts)?,
                ByteRange {
                    offset: 0,
                    length: 2,
                },
                B,
                cancel,
            )
            .await?
            .value;
        let actual = u16::from_le_bytes(value.bytes.as_ref().try_into()?);
        if same_file {
            assert!((1..=u16::try_from(writers)?).contains(&actual));
        } else {
            assert_eq!(
                actual,
                if index < writers {
                    u16::try_from(index + 1)?
                } else {
                    0
                }
            );
        }
    }
    Ok(())
}

fn report(
    writers: usize,
    same_file: bool,
    results: &[Worker],
    appends: usize,
    elapsed: std::time::Duration,
) -> Result {
    let committed = results.iter().filter(|worker| worker.committed).count();
    let count = f64::from(u32::try_from(committed)?);
    println!(
        "FS_CONCURRENCY_BENCH {}",
        serde_json::json!({
            "writers": writers, "same_file": same_file, "committed": committed,
            "seconds": elapsed.as_secs_f64(), "commits_per_second": count / elapsed.as_secs_f64(),
            "stream_appends": appends, "appends_per_commit": f64::from(u32::try_from(appends)?) / count,
            "rebase_calls_per_commit": f64::from(u32::try_from(results.iter().map(|worker| worker.rebase_calls).sum::<u64>())?) / count,
            "rebase_items_per_commit": f64::from(u32::try_from(results.iter().map(|worker| worker.rebase_items).sum::<u64>())?) / count,
            "rebase_object_probes_per_commit": f64::from(u32::try_from(results.iter().map(|worker| worker.rebase_object_reads).sum::<u64>())?) / count,
        })
    );
    Ok(())
}

macro_rules! cases {
    ($(($name:ident, $writers:expr, $same:expr)),* $(,)?) => { $(
        #[test]
        fn $name() -> Result {
            tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(run($writers, $same))
        }
    )* };
}
cases! {
    (disjoint_1, 1, false), (disjoint_4, 4, false), (disjoint_16, 16, false), (disjoint_64, 64, false), (disjoint_256, 256, false),
    (same_file_1, 1, true), (same_file_4, 4, true), (same_file_16, 16, true), (same_file_64, 64, true), (same_file_256, 256, true),
}
