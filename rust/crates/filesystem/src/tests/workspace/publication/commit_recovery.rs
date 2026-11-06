//! Cold-process cuts through the original local transaction and its publication.
use super::*;
use acyclic_native_runtime::testing::{DurableFault, Schedule};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const CHILD: &str = "workspace::tests::publication::commit_recovery::commit_crash_child";
const KEY: IdempotencyKey = IdempotencyKey::from_bytes([0x73; 16]);

mod multi_root;

#[derive(Clone, Copy, Debug)]
enum PublicationKind {
    Transaction,
    MultiRoot,
}

impl PublicationKind {
    fn name(self) -> &'static str {
        match self {
            Self::Transaction => "transaction",
            Self::MultiRoot => "multi-root",
        }
    }
}

fn run_child(
    root: &std::path::Path,
    kind: PublicationKind,
    cut: Option<(usize, &str)>,
) -> TestResult<usize> {
    let mut command = acyclic_native_runtime::ProcessCommand::new(std::env::current_exe()?);
    command
        .args(["--exact", CHILD, "--nocapture"])
        .env("ACYCLIC_WORKSPACE_CRASH_ROOT", root)
        .env("ACYCLIC_WORKSPACE_CRASH_KIND", kind.name())
        .stdout(acyclic_native_runtime::ProcessStdio::piped())
        .stderr(acyclic_native_runtime::ProcessStdio::piped());
    if let Some((ordinal, fault)) = cut {
        command
            .env("ACYCLIC_WORKSPACE_CRASH_ORDINAL", ordinal.to_string())
            .env("ACYCLIC_WORKSPACE_CRASH_FAULT", fault);
    }
    let mut process = acyclic_native_runtime::ProcessTree::spawn(&mut command)?;
    let output = process
        .wait_with_output(
            std::time::Duration::from_secs(2),
            4096,
            None,
            || Ok(()),
            |_| Ok(acyclic_native_runtime::ProcessInput::default()),
            None,
            None,
            None,
        )
        .map_err(|failure| failure.error)?;
    assert_eq!(
        output.status.code(),
        Some(if cut.is_some() { 86 } else { 0 }),
        "workspace cut {cut:?}: {output:?}"
    );
    Ok(std::fs::read_to_string(root.join("physical-count"))?.parse()?)
}

async fn seed(root: &std::path::Path, kind: PublicationKind) -> TestResult {
    if matches!(kind, PublicationKind::MultiRoot) {
        return multi_root::seed(root).await;
    }
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let workspace = unbounded!(fs, create_workspace("repo"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    transaction.write_text("/first", "old").await?;
    transaction.write_text("/second", "old").await?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    drop((transaction, workspace));
    close_local(fs).await?;
    Ok(())
}

async fn assert_recovery(root: &std::path::Path, kind: PublicationKind) -> TestResult {
    if matches!(kind, PublicationKind::MultiRoot) {
        return multi_root::assert_recovery(root).await;
    }
    let fs = Fs::local(crate::LocalOptions::new(root)).await?;
    let workspace = unbounded!(fs, open_workspace("repo"))?;
    let first = unbounded!(workspace, read("/first", 16))?;
    let second = unbounded!(workspace, read("/second", 16))?;
    assert_eq!(
        first, second,
        "a recovered transaction exposed a partial publication"
    );
    assert!(matches!(first.as_ref(), b"old" | b"new"));
    let original = unbounded!(workspace, operation_generation(KEY))?;
    assert_eq!(original.is_some(), first.as_ref() == b"new");
    if let Some(original) = original {
        assert_eq!(original.id(), unbounded!(workspace, head())?.id());
    }
    // A different operation must progress immediately after the dead process,
    // including when the original frame was durable but never acknowledged.
    let mut successor = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    successor.write_text("/successor", "live").await?;
    assert!(matches!(
        commit(&mut successor).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/successor", 16))?.as_ref(),
        b"live"
    );
    assert_eq!(unbounded!(workspace, read("/first", 16))?, first);
    assert_eq!(unbounded!(workspace, read("/second", 16))?, second);
    drop((successor, workspace));
    close_local(fs).await?;
    Ok(())
}

#[tokio::test]
async fn every_local_commit_crash_reopens_atomically_and_allows_a_live_successor() -> TestResult {
    assert_publication_cuts(PublicationKind::Transaction).await
}

#[tokio::test]
async fn every_multi_root_crash_resumes_and_releases_the_dead_owners_reservations() -> TestResult {
    assert_publication_cuts(PublicationKind::MultiRoot).await
}

async fn assert_publication_cuts(kind: PublicationKind) -> TestResult {
    let directory = tempfile::tempdir()?;
    let baseline = directory.path().join("baseline");
    seed(&baseline, kind).await?;
    let count = run_child(&baseline, kind, None)?;
    assert!(
        count > 0,
        "the real transaction bypassed physical crash boundaries"
    );
    assert_recovery(&baseline, kind).await?;
    let jobs = (0..count)
        .flat_map(|ordinal| {
            ["before", "after", "torn"].map(|fault| {
                let root = directory.path().join(format!("{ordinal}-{fault}"));
                move || {
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(async {
                            seed(&root, kind).await?;
                            run_child(&root, kind, Some((ordinal, fault)))?;
                            assert_recovery(&root, kind).await
                        })
                        .unwrap();
                    ordinal
                }
            })
        })
        .collect();
    let completed = crate::test_support::finish_physical_cuts(jobs);
    assert_eq!(completed.len(), count * 3);
    eprintln!(
        "WORKSPACE_CRASH_CUTS kind={kind:?} physical={count} cases={}",
        completed.len()
    );
    Ok(())
}

#[test]
fn commit_crash_child() -> TestResult {
    let Some(root) = std::env::var_os("ACYCLIC_WORKSPACE_CRASH_ROOT") else {
        return Ok(());
    };
    crate::initialize_test_native_io();
    let root = std::path::PathBuf::from(root);
    let kind = match std::env::var("ACYCLIC_WORKSPACE_CRASH_KIND")?.as_str() {
        "transaction" => PublicationKind::Transaction,
        "multi-root" => PublicationKind::MultiRoot,
        _ => return Err("unknown workspace publication kind".into()),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let fs = Fs::local(crate::LocalOptions::new(&root)).await?;
        let workspace = unbounded!(fs, open_workspace("repo"))?;
        let coordinator = if matches!(kind, PublicationKind::MultiRoot) {
            Some(multi_root::coordinator(&root, fs.clone())?)
        } else {
            None
        };
        let (schedule, mut events) = Schedule::seeded(113);
        if let Ok(ordinal) = std::env::var("ACYCLIC_WORKSPACE_CRASH_ORDINAL") {
            let fault = match std::env::var("ACYCLIC_WORKSPACE_CRASH_FAULT")?.as_str() {
                "before" => DurableFault::Before,
                "after" => DurableFault::After,
                "torn" => DurableFault::Torn(1),
                _ => return Err("unknown workspace crash fault".into()),
            };
            schedule.cut_at(ordinal.parse()?, fault);
        }
        let observed = schedule.clone();
        let report = root.join("physical-count");
        let driver = tokio::spawn(async move {
            while let Some(boundary) = events.recv().await {
                if let Some(fault) = observed.storage_fault() {
                    std::fs::write(&report, (fault.ordinal + 1).to_string()).unwrap();
                    // Exit at the original worker's completion event, before
                    // releasing its acknowledgement or unwinding store owners.
                    std::process::exit(86);
                }
                boundary.release();
            }
        });
        let result = schedule
            .scope(async {
                if let Some(coordinator) = &coordinator {
                    assert!(matches!(
                        coordinator.publish(multi_root::candidate(&root)?).await?,
                        crate::Publication::Applied(_)
                    ));
                    return Ok(());
                }
                let mut transaction = unbounded!(workspace, begin_transaction(KEY))?;
                transaction.write_text("/first", "new").await?;
                transaction.write_text("/second", "new").await?;
                assert!(matches!(
                    commit(&mut transaction).await?,
                    TransactionCommit::Committed(_)
                ));
                Ok::<_, Box<dyn Error>>(())
            })
            .await;
        schedule.drained().await;
        driver.abort();
        let _ = driver.await;
        result?;
        std::fs::write(
            root.join("physical-count"),
            schedule.durable_write_count().to_string(),
        )?;
        // The acknowledged case also leaves all live store owners in place.
        std::process::exit(0);
    })
}
