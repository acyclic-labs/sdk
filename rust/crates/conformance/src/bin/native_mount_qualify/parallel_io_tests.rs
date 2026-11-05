use super::supervision::CapturedChild;
use super::{Failure, parallel_io_child};
use acyclic_native_runtime::ProcessCommand;
use std::path::Path;

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

fn child(root: &Path, index: usize, count: usize) -> Result<ProcessCommand, Failure> {
    let mut command = ProcessCommand::new(std::env::current_exe()?);
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

/// Actual contained child process used by the exit-order and deadline controls.
#[test]
fn exit_wait_child() {
    match std::env::var("ACYCLIC_QUALIFIER_EXIT_CHILD").as_deref() {
        Ok("fail") => {
            println!("complete failed-child stdout");
            eprintln!("complete failed-child stderr");
            std::process::exit(7);
        }
        Ok("ready" | "bad-ready") => {
            let marker =
                std::path::PathBuf::from(std::env::var_os("ACYCLIC_QUALIFIER_READY").unwrap());
            let pending = marker.with_extension("ready-pending");
            std::fs::write(
                &pending,
                if std::env::var("ACYCLIC_QUALIFIER_EXIT_CHILD").unwrap() == "ready" {
                    b"ready".as_slice()
                } else {
                    b"partial".as_slice()
                },
            )
            .unwrap();
            std::fs::rename(pending, marker).unwrap();
            loop {
                std::thread::park();
            }
        }
        Ok("blocked") => loop {
            std::thread::park();
        },
        _ => {}
    }
}

fn exit_child(mode: &str) -> Result<Option<super::CapturedChild>, super::Failure> {
    Ok(Some(super::spawn_captured_child(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "parallel_io_tests::exit_wait_child",
                "--nocapture",
            ])
            .env("ACYCLIC_QUALIFIER_EXIT_CHILD", mode),
    )?))
}

#[tokio::test]
async fn exit_wait_settles_all_successes_and_failure_behind_a_blocked_child()
-> Result<(), super::Failure> {
    let mut children = vec![exit_child("success")?, exit_child("success")?];
    super::wait_captured_children(&mut children, Instant::now() + Duration::from_secs(2)).await?;
    assert!(children.iter().all(Option::is_none));

    // A prefix scheduler would wait for the blocked first child until deadline.
    // The actual later failure must win and retire every accepted sibling.
    let mut children = vec![exit_child("blocked")?, exit_child("fail")?];
    let error =
        super::wait_captured_children(&mut children, Instant::now() + Duration::from_secs(2))
            .await
            .expect_err("a failed actual child must refuse the qualification");
    assert!(
        error.to_string().contains("parallel I/O child 1 failed"),
        "{error}"
    );
    assert!(children.iter().all(Option::is_none));

    let mut children = vec![exit_child("blocked")?, exit_child("blocked")?];
    let error = super::wait_captured_children(&mut children, Instant::now())
        .await
        .expect_err("a deadline must retire blocked children, never invent success");
    assert!(
        error.to_string().contains("exceeded their deadline"),
        "{error}"
    );
    assert!(children.iter().all(Option::is_none));
    Ok(())
}

#[tokio::test]
async fn ready_marker_uses_native_events_and_settles_death_or_bad_publication()
-> Result<(), super::Failure> {
    let root = tempfile::tempdir()?;
    let marker = root.path().join("ready");
    // An early authoritative record remains visible even before subscription.
    std::fs::write(&marker, b"ready")?;
    let mut early = exit_child("blocked")?.ok_or("child")?;
    let result =
        super::captured_child_ready(&mut early, &marker, Instant::now() + Duration::from_secs(2))
            .await;
    early.process.terminate(None)?;
    result?;
    std::fs::remove_file(&marker)?;

    for mode in ["ready", "fail", "bad-ready"] {
        let mut child = super::spawn_captured_child(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "parallel_io_tests::exit_wait_child",
                    "--nocapture",
                ])
                .env("ACYCLIC_QUALIFIER_EXIT_CHILD", mode)
                .env("ACYCLIC_QUALIFIER_READY", &marker),
        )?;
        let result = super::captured_child_ready(
            &mut child,
            &marker,
            Instant::now() + Duration::from_secs(2),
        )
        .await;
        child.process.terminate(None)?;
        match mode {
            "ready" => result?,
            "fail" => assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("exited before readiness")
            ),
            "bad-ready" => assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("malformed ready marker")
            ),
            _ => unreachable!(),
        }
        if marker.try_exists()? {
            std::fs::remove_file(&marker)?;
        }
    }
    let mut child = exit_child("blocked")?.ok_or("child")?;
    let error = super::captured_child_ready(&mut child, &marker, Instant::now())
        .await
        .expect_err("an expired readiness observation must refuse");
    child.process.terminate(None)?;
    assert!(error.to_string().contains("before its deadline"), "{error}");
    Ok(())
}
