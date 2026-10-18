use super::tests::test_work;
use super::*;
use std::cell::RefCell;

thread_local! {
    static TRACE: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

pub(super) fn record(operation: &'static str) {
    TRACE.with_borrow_mut(|trace| trace.push(operation));
}

fn take_trace() -> Vec<&'static str> {
    TRACE.with_borrow_mut(std::mem::take)
}

// Each candidate/control has its own filesystem, checkout, source and journal.
// No native driver is needed to test the real command publication dispatch;
// destination retirement is proved separately against the actual fence packet.
async fn publish_command(
    duplicate_sync: bool,
    retire: bool,
) -> Result<(Vec<u8>, Vec<&'static str>), Box<dyn std::error::Error>> {
    crate::initialize_native_mount_services()?;
    let cancellation = crate::CancellationToken::new();
    let work = test_work(crate::WorkBudget::UNBOUNDED, &cancellation)?;
    let filesystem = crate::Fs::memory();
    let workspace = filesystem
        .create_workspace(
            "command-publication",
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)?;
    workspace.write_text("/result.txt", "original").await?;
    let original = workspace.head(&work, &cancellation).await?;
    let destination = tempfile::tempdir()?;
    let working = original
        .prepare_native_working_set(
            &MountOptions::read_write().publication(MountPublication::Manual),
            &crate::MaterializeOptions::native(destination.path()),
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let mount = Mount {
        source: working.source.clone(),
        session: Mutex::new(None),
        destination: destination.path().to_owned(),
        _destination_custody: work.path(destination.path(), None)?.1,
    };
    let path = {
        #[cfg(unix)]
        let name = b"result.txt".to_vec();
        #[cfg(windows)]
        let name = "result.txt"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        MountPath::root().child(name)
    };
    mount.source.open_file(&path)?.write_range_with_meter(
        0,
        crate::kernel::OwnedSourceChunk::claimed(
            acyclic_stream::OwnedBytes::from_static(b"updated!"),
            0,
        ),
        &work,
    )?;
    take_trace();
    if duplicate_sync {
        // Restore exactly the previous sync -> advance -> unmount path.
        mount.sync().await?;
        mount.advance_to_head().await?;
        mount.unmount().await?;
    } else {
        mount.complete_command(retire).await?;
    }
    let trace = take_trace();
    let reopened = workspace.head(&work, &cancellation).await?;
    assert_ne!(
        reopened.id(),
        original.id(),
        "command was acknowledged before publication"
    );
    let bytes = workspace
        .read("/result.txt", 64, &work, &cancellation)
        .await?;
    Ok((bytes.as_ref().to_vec(), trace))
}

#[tokio::test]
async fn command_completion_publishes_once_with_an_independent_duplicate_sync_control()
-> Result<(), Box<dyn std::error::Error>> {
    let (bytes, trace) = publish_command(false, true).await?;
    assert_eq!(bytes, b"updated!");
    assert_eq!(trace, ["sync", "advance", "detach"]);
    let (baseline_bytes, duplicate_trace) = publish_command(true, true).await?;
    assert_eq!(baseline_bytes, bytes);
    assert_eq!(duplicate_trace, ["sync", "advance", "sync", "detach"]);
    assert_ne!(
        duplicate_trace, trace,
        "duplicate sync escaped the production trace oracle"
    );
    let (retained_bytes, retained_trace) = publish_command(false, false).await?;
    assert_eq!(retained_bytes, bytes);
    assert_eq!(retained_trace, ["sync", "advance"]);
    Ok(())
}
