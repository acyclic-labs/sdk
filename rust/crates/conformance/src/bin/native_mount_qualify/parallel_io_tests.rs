use super::supervision::CapturedChild;
use super::{Failure, parallel_io_child};
use std::path::Path;
use std::process::Command;

const CHILD_ROOT: &str = "ACYCLIC_PARALLEL_IO_CHILD_ROOT";

/// An actual process running the same publication and filesystem operations.
#[test]
fn parallel_io_process() {
    let Some(root) = std::env::var_os(CHILD_ROOT) else {
        return;
    };
    if std::env::var("ACYCLIC_PARALLEL_IO_EXIT").as_deref() == Ok("before-publication") {
        return;
    }
    let run = || -> Result<(), Failure> {
        let index = std::env::var("ACYCLIC_PARALLEL_IO_INDEX")?.parse()?;
        let count = std::env::var("ACYCLIC_PARALLEL_IO_COUNT")?.parse()?;
        parallel_io_child(Path::new(&root), index, count)
    };
    if let Err(error) = run() {
        if std::env::var("ACYCLIC_PARALLEL_IO_EXIT").as_deref() == Ok("record-input-close") {
            std::fs::write(Path::new(&root).join("orderly-close"), b"input closed")
                .expect("record the actual input-close path");
        }
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn child(root: &Path, index: usize, count: usize) -> Result<Command, Failure> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "parallel_io_tests::parallel_io_process",
            "--nocapture",
        ])
        .env(CHILD_ROOT, root)
        .env("ACYCLIC_PARALLEL_IO_INDEX", index.to_string())
        .env("ACYCLIC_PARALLEL_IO_COUNT", count.to_string());
    Ok(command)
}

#[tokio::test(flavor = "current_thread")]
async fn parallel_publication_barrier_uses_original_producers() -> Result<(), Failure> {
    let root = tempfile::tempdir()?;
    let mut first = CapturedChild::spawn(&mut child(root.path(), 0, 2)?)?;
    first.receipt(b"acyclic-parallel-published\n").await?;
    // A delivered receipt remains observable without another producer event.
    first.receipt(b"acyclic-parallel-published\n").await?;
    assert_eq!(
        std::fs::read_dir(root.path().join("parallel-clients"))?.count(),
        1
    );
    let mut second = CapturedChild::spawn(&mut child(root.path(), 1, 2)?)?;
    second.receipt(b"acyclic-parallel-published\n").await?;
    first.release(b"!")?;
    second.release(b"!")?;
    assert!(first.finish().await?.status.success());
    assert!(second.finish().await?.status.success());

    // A release before the other producer publishes is a terminal failure,
    // rather than a visibility retry that could hide a broken barrier.
    let root = tempfile::tempdir()?;
    let mut early = CapturedChild::spawn(&mut child(root.path(), 0, 2)?)?;
    early.receipt(b"acyclic-parallel-published\n").await?;
    early.release(b"!")?;
    let failed = early.finish().await?;
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("1 of 2 published peers"));

    // Producer exit settles a pending receipt with the original typed EOF.
    let root = tempfile::tempdir()?;
    let mut command = child(root.path(), 0, 1)?;
    command.env("ACYCLIC_PARALLEL_IO_EXIT", "before-publication");
    let mut exited = CapturedChild::spawn(&mut command)?;
    let error = exited
        .receipt(b"acyclic-parallel-published\n")
        .await
        .expect_err("missing producer receipt");
    assert_eq!(
        error
            .downcast_ref::<std::io::Error>()
            .map(std::io::Error::kind),
        Some(std::io::ErrorKind::UnexpectedEof),
    );
    assert!(exited.finish().await?.status.success());

    // The original release pipe closes when its owner is lost. The child
    // reports that EOF without listing or waiting for another parent.
    let root = tempfile::tempdir()?;
    let mut command = child(root.path(), 0, 1)?;
    command.env("ACYCLIC_PARALLEL_IO_EXIT", "record-input-close");
    let mut orphaned = CapturedChild::spawn(&mut command)?;
    orphaned.receipt(b"acyclic-parallel-published\n").await?;
    let failed = orphaned.finish().await?;
    assert!(!failed.status.success());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("publication release failed"));
    assert!(root.path().join("orderly-close").exists());

    // Forced crash joins physical termination while retaining input. The same
    // real child must not execute the orderly-close path proved above.
    let root = tempfile::tempdir()?;
    let mut command = child(root.path(), 0, 1)?;
    command.env("ACYCLIC_PARALLEL_IO_EXIT", "record-input-close");
    let mut crashed = CapturedChild::spawn(&mut command)?;
    crashed.receipt(b"acyclic-parallel-published\n").await?;
    crashed.stop().await?;
    assert!(!root.path().join("orderly-close").exists());
    Ok(())
}
