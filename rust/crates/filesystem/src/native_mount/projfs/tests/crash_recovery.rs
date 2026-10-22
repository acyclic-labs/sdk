use super::*;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use windows::Win32::Foundation::{HANDLE, WAIT_OBJECT_0};
use windows::Win32::System::Threading::{
    CreateEventW, EVENT_MODIFY_STATE, OpenEventW, SetEvent, WaitForSingleObject,
};

const ROLE: &str = "ACYCLIC_PROJFS_CRASH_FIXTURE";
const READY: &str = "ACYCLIC_PROJFS_CRASH_READY";
const TEST: &str = "native_mount::projfs::tests::crash_recovery::a_dead_provider_is_preserved_under_held_recovery_custody";

#[tokio::test]
async fn a_dead_provider_is_preserved_under_held_recovery_custody()
-> Result<(), Box<dyn std::error::Error>> {
    if let Some(root) = std::env::var_os(ROLE) {
        let source = windows_checkout_source().await?;
        let destination = std::path::PathBuf::from(root).join("projection");
        std::fs::write(
            destination.parent().unwrap().join("volume"),
            source.volume_id()?.into_bytes(),
        )?;
        let session = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: destination.clone(),
                writable: true,
            },
            source as std::sync::Arc<dyn crate::native_mount::MountFilesystem>,
        )?;
        let authored = std::fs::File::create(destination.join("authored.txt"))?;
        use std::io::Write as _;
        (&authored).write_all(b"authored before abrupt termination")?;
        authored.sync_all()?;
        let name = HSTRING::from(std::env::var(READY)?);
        // SAFETY: the event name is live and a successful call transfers its handle.
        let ready = unsafe {
            OwnedHandle::from_raw_handle(OpenEventW(EVENT_MODIFY_STATE, false, &name)?.0)
        };
        // SAFETY: ready owns this valid event handle through the call.
        unsafe { SetEvent(HANDLE(ready.as_raw_handle()))? };
        let _owned = (session, authored, ready);
        loop {
            std::thread::park();
        }
    }

    let root = tempfile::tempdir()?;
    let destination = root.path().join("projection");
    std::fs::create_dir(&destination)?;
    std::fs::create_dir(root.path().join("recovery"))?;
    let name = HSTRING::from(format!(
        "Local\\acyclic-projfs-crash-{}",
        uuid::Uuid::new_v4()
    ));
    // SAFETY: name is live and a successful call transfers its event handle.
    let ready = unsafe { OwnedHandle::from_raw_handle(CreateEventW(None, true, false, &name)?.0) };
    let mut command = std::process::Command::new(std::env::current_exe()?);
    command
        .args([TEST, "--exact", "--nocapture", "--test-threads=1"])
        .env(ROLE, root.path())
        .env(READY, name.to_string());
    let mut child = acyclic_native_runtime::ProcessTree::spawn(&mut command)?;
    // SAFETY: the owned event remains live through the bounded readiness wait.
    assert_eq!(
        unsafe { WaitForSingleObject(HANDLE(ready.as_raw_handle()), 3_000) },
        WAIT_OBJECT_0
    );
    let volume = crate::VolumeId::from_bytes(
        std::fs::read(root.path().join("volume"))?
            .try_into()
            .unwrap(),
    );
    let work = setup_work()?;
    let (parent, _) = crate::native_host::open_root_directory(root.path(), &mut &work)?;
    assert!(
        crate::NativeMountRecoveryOwner::acquire_in_directory(&destination, &parent, &work,)
            .is_err(),
        "a live provider keeps its actual destination fence"
    );
    assert!(
        child.try_wait(None)?.is_none(),
        "ready provider must still be alive"
    );
    // terminate() also consumes/reaps the child. Signal the owned Job first
    // so wait() can observe its actual unsuccessful exit, then retire custody.
    child.terminate_descendants()?;
    assert!(!child.wait(std::time::Duration::from_secs(1))?.success());
    child.terminate(None)?;
    assert!(child.is_reaped());
    super::reject_stale_projection(&destination).expect_err("cold startup must preserve this root");
    let (residue, _) =
        crate::native_host::open_root_directory(&root.path().join("recovery"), &mut &work)?;
    let mut owner =
        crate::NativeMountRecoveryOwner::acquire_in_directory(&destination, &parent, &work)?;
    assert!(owner.preserve_crashed_in_directory(
        &residue,
        std::path::Path::new("saved"),
        volume,
        &work
    )?);
    assert!(!destination.exists());
    assert!(root.path().join("recovery/saved").exists());
    assert_eq!(
        std::fs::read(root.path().join("recovery/saved/authored.txt"))?,
        b"authored before abrupt termination"
    );
    assert!(!owner.cleanup_state()?.destination_fence_retained);
    // Ordinary cold startup is now possible at the original name.
    std::fs::create_dir(&destination)?;
    super::reject_stale_projection(&destination)?;
    let source = windows_checkout_source().await?;
    let mut session = crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: destination.clone(),
            writable: true,
        },
        source as std::sync::Arc<dyn crate::native_mount::MountFilesystem>,
    )?;
    assert_eq!(
        std::fs::read(root.path().join("recovery/saved/authored.txt"))?,
        b"authored before abrupt termination"
    );
    session.stop()?;
    Ok(())
}

#[test]
fn recovery_refuses_a_replacement_leaf_and_retains_both_roots()
-> Result<(), Box<dyn std::error::Error>> {
    let volume = crate::VolumeId::new();
    let root = tempfile::tempdir()?;
    let destination = root.path().join("projection");
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("original.txt"), b"original")?;
    let work = setup_work()?;
    let (parent, _) = crate::native_host::open_root_directory(root.path(), &mut &work)?;
    let mut owner =
        crate::NativeMountRecoveryOwner::acquire_in_directory(&destination, &parent, &work)?;
    std::fs::rename(&destination, root.path().join("original"))?;
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("replacement.txt"), b"replacement")?;
    assert!(
        owner
            .preserve_crashed_in_directory(&parent, std::path::Path::new("saved"), volume, &work)
            .is_err()
    );
    assert_eq!(
        std::fs::read(root.path().join("original/original.txt"))?,
        b"original"
    );
    assert_eq!(
        std::fs::read(destination.join("replacement.txt"))?,
        b"replacement"
    );
    assert!(!root.path().join("saved").exists());
    Ok(())
}

#[test]
fn recovery_ownership_receipts_are_exact_and_durable() -> Result<(), Box<dyn std::error::Error>> {
    let volume = crate::VolumeId::new();
    let root = tempfile::tempdir()?;
    let destination = root.path().join("projection");
    std::fs::create_dir(&destination)?;
    let work = setup_work()?;
    let (parent, _) = crate::native_host::open_root_directory(root.path(), &mut &work)?;
    let mut guard = None;
    crate::native_mount::MountDestinationGuard::acquire_in_directory(
        &destination,
        parent.try_clone()?,
        &work,
        &mut guard,
        true,
    )?;
    let guard = guard.unwrap();
    assert_eq!(guard.owned_root(&work)?, None);
    let (leaf, identity) =
        super::open_recovery_leaf(&parent, std::path::Path::new("projection"), &work)?.unwrap();
    let before = work.receipt();
    guard.publish_root_ownership(identity, volume, &work)?;
    assert_eq!(
        work.receipt().durability_operations - before.durability_operations,
        2
    );
    assert_eq!(work.receipt().bytes_encoded - before.bytes_encoded, 124);
    assert_eq!(guard.owned_root(&work)?, Some(identity));
    let file = guard.file.as_ref().unwrap();
    use std::os::windows::fs::FileExt as _;
    assert_eq!(file.seek_write(b"X", 0)?, 1);
    assert_eq!(
        guard.owned_root(&work)?,
        None,
        "corrupt receipt must never authorize recovery"
    );
    guard.publish_root_ownership(identity, volume, &work)?;
    drop(leaf);
    std::fs::rename(&destination, root.path().join("original"))?;
    std::fs::create_dir(&destination)?;
    let (_, replacement) =
        super::open_recovery_leaf(&parent, std::path::Path::new("projection"), &work)?.unwrap();
    assert_ne!(identity, replacement);
    assert_eq!(
        guard.owned_root(&work)?,
        Some(identity),
        "copied ownership never changes to the replacement root"
    );
    drop(guard);
    let mut owner =
        crate::NativeMountRecoveryOwner::acquire_in_directory(&destination, &parent, &work)?;
    assert!(
        owner
            .preserve_crashed_in_directory(&parent, std::path::Path::new("saved"), volume, &work)
            .is_err()
    );
    assert!(destination.exists());
    assert!(root.path().join("original").exists());
    assert!(!root.path().join("saved").exists());
    Ok(())
}

#[test]
fn copied_or_malformed_receipts_cannot_substitute_physical_custody()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::windows::fs::FileExt as _;
    let volume = crate::VolumeId::new();
    let root = tempfile::tempdir()?;
    let work = setup_work()?;
    let (parent, _) = crate::native_host::open_root_directory(root.path(), &mut &work)?;
    let mut guards = Vec::new();
    for name in ["original", "copied"] {
        let destination = root.path().join(name);
        std::fs::create_dir(&destination)?;
        std::fs::write(destination.join("authored"), b"never remove")?;
        let mut guard = None;
        crate::native_mount::MountDestinationGuard::acquire_in_directory(
            &destination,
            parent.try_clone()?,
            &work,
            &mut guard,
            true,
        )?;
        guards.push(guard.unwrap());
    }
    let (_, identity) =
        super::open_recovery_leaf(&parent, std::path::Path::new("original"), &work)?.unwrap();
    guards[0].publish_root_ownership(identity, volume, &work)?;
    let original = guards[0].file.as_ref().unwrap();
    let copied = guards[1].file.as_ref().unwrap();
    let mut bytes = [0; 124];
    assert_eq!(original.seek_read(&mut bytes, 0)?, bytes.len());
    assert_eq!(copied.seek_write(&bytes, 0)?, bytes.len());
    copied.set_len(bytes.len() as u64)?;
    assert_eq!(
        guards[1].owned_root(&work)?,
        None,
        "a different fence is not the issuer"
    );
    for offset in [0, 16, 32, 48, 64, 80, 84, 92] {
        let mut forged = bytes;
        forged[offset] ^= 1;
        assert_eq!(original.seek_write(&forged, 0)?, forged.len());
        assert_ne!(guards[0].owned_root(&work)?, Some(identity));
    }
    assert_eq!(original.seek_write(&bytes, 0)?, bytes.len());
    original.set_len(125)?;
    assert_eq!(guards[0].owned_root(&work)?, None);
    original.set_len(123)?;
    assert_eq!(guards[0].owned_root(&work)?, None);
    for name in ["original", "copied"] {
        assert_eq!(
            std::fs::read(root.path().join(name).join("authored"))?,
            b"never remove"
        );
    }
    Ok(())
}

#[test]
fn ownership_publication_requires_encoding_and_both_durable_flushes()
-> Result<(), Box<dyn std::error::Error>> {
    let volume = crate::VolumeId::new();
    let root = tempfile::tempdir()?;
    let work = setup_work()?;
    let (parent, _) = crate::native_host::open_root_directory(root.path(), &mut &work)?;
    for (encoded, flushes) in [(0, 2), (124, 0), (124, 1), (124, 2)] {
        let name = format!("projection-{encoded}-{flushes}");
        let destination = root.path().join(&name);
        std::fs::create_dir(&destination)?;
        let mut guard = None;
        crate::native_mount::MountDestinationGuard::acquire_in_directory(
            &destination,
            parent.try_clone()?,
            &work,
            &mut guard,
            true,
        )?;
        let guard = guard.unwrap();
        let (_, identity) =
            super::open_recovery_leaf(&parent, std::path::Path::new(&name), &work)?.unwrap();
        let limited = crate::NativeMaterializationWork::new(
            WorkBudget {
                bytes_encoded: encoded,
                durability_operations: flushes,
                ..WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .map_err(|(error, _)| error)?;
        let before = limited.receipt();
        let result = guard.publish_root_ownership(identity, volume, &limited);
        assert_eq!(result.is_ok(), encoded == 124 && flushes == 2);
        assert_eq!(
            limited.receipt().durability_operations - before.durability_operations,
            if encoded == 0 { 0 } else { flushes }
        );
        if encoded == 0 {
            assert_eq!(
                limited.receipt().backend_write_operations,
                before.backend_write_operations
            );
            assert_eq!(guard.owned_root(&work)?, None);
        }
    }
    Ok(())
}

#[test]
fn ordinary_unowned_roots_are_rejected_without_moving_authored_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let volume = crate::VolumeId::new();
    let root = tempfile::tempdir()?;
    let destination = root.path().join("projection");
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("authored"), b"not SDK owned")?;
    let work = setup_work()?;
    let (parent, _) = crate::native_host::open_root_directory(root.path(), &mut &work)?;
    let mut owner =
        crate::NativeMountRecoveryOwner::acquire_in_directory(&destination, &parent, &work)?;
    assert!(
        owner
            .preserve_crashed_in_directory(&parent, std::path::Path::new("saved"), volume, &work)
            .is_err()
    );
    assert!(owner.cleanup_state()?.destination_fence_retained);
    assert_eq!(
        std::fs::read(destination.join("authored"))?,
        b"not SDK owned"
    );
    assert!(!root.path().join("saved").exists());
    Ok(())
}

#[test]
fn a_volume_mismatched_receipt_cannot_authorize_an_unchanged_root()
-> Result<(), Box<dyn std::error::Error>> {
    use std::os::windows::fs::FileExt as _;
    let volume = crate::VolumeId::new();
    let root = tempfile::tempdir()?;
    let destination = root.path().join("projection");
    std::fs::create_dir(&destination)?;
    std::fs::write(
        destination.join("authored"),
        b"retain across volume mismatch",
    )?;
    let work = setup_work()?;
    let (parent, _) = crate::native_host::open_root_directory(root.path(), &mut &work)?;
    let mut guard = None;
    crate::native_mount::MountDestinationGuard::acquire_in_directory(
        &destination,
        parent.try_clone()?,
        &work,
        &mut guard,
        true,
    )?;
    let guard = guard.unwrap();
    let (_, identity) =
        super::open_recovery_leaf(&parent, std::path::Path::new("projection"), &work)?.unwrap();
    guard.publish_root_ownership(identity, volume, &work)?;
    // Retain the object number and original fence/parent, but forge another
    // volume in the recorded physical root. Object numbers alone are unsafe.
    let mut different_volume = identity.to_bytes();
    different_volume[0] ^= 1;
    assert_eq!(
        guard
            .file
            .as_ref()
            .unwrap()
            .seek_write(&different_volume, 16)?,
        16
    );
    drop(guard);
    let mut owner =
        crate::NativeMountRecoveryOwner::acquire_in_directory(&destination, &parent, &work)?;
    assert!(matches!(
        owner.preserve_crashed_in_directory(&parent, std::path::Path::new("saved"), volume, &work),
        Err(NativeMountError::RecoveryOwnershipUnknown)
    ));
    assert_eq!(
        std::fs::read(destination.join("authored"))?,
        b"retain across volume mismatch"
    );
    assert!(!root.path().join("saved").exists());
    Ok(())
}

// A provider can relinquish its OS lock while its process still exists. The
// durable incarnation must independently deny recovery in that state.
#[test]
fn an_absent_pid_is_proven_dead_from_the_native_error_code()
-> Result<(), Box<dyn std::error::Error>> {
    let work = setup_work()?;
    // A nonzero PID that does not name a Windows process. Keep the actual
    // OpenProcess boundary: HRESULT-as-io conversion used to hide error 87.
    super::require_dead_process_owner(u32::MAX, 1, &work)?;
    Ok(())
}

#[test]
fn live_process_is_refused_even_without_a_provider_lock() -> Result<(), Box<dyn std::error::Error>>
{
    custody_control(false, false, false, false)
}

#[test]
fn reused_pid_does_not_keep_a_dead_incarnation_alive() -> Result<(), Box<dyn std::error::Error>> {
    custody_control(true, false, false, false)
}

#[test]
fn another_logical_volume_is_refused_before_owner_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    custody_control(true, true, false, false)
}

#[test]
fn a_failed_preservation_flush_retains_custody_and_retries()
-> Result<(), Box<dyn std::error::Error>> {
    custody_control(true, false, true, false)
}

#[test]
fn caller_drop_settles_preservation_before_releasing_the_fence()
-> Result<(), Box<dyn std::error::Error>> {
    custody_control(true, false, true, true)
}

fn custody_control(
    reused_pid: bool,
    different_volume: bool,
    fail_flush: bool,
    drop_owner: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::os::windows::fs::FileExt as _;
    let volume = crate::VolumeId::new();
    let root = tempfile::tempdir()?;
    let destination = root.path().join("projection");
    std::fs::create_dir(&destination)?;
    std::fs::write(destination.join("authored"), b"unpublished bytes")?;
    let work = setup_work()?;
    let (parent, _) = crate::native_host::open_root_directory(root.path(), &mut &work)?;
    let mut guard = None;
    crate::native_mount::MountDestinationGuard::acquire_in_directory(
        &destination,
        parent.try_clone()?,
        &work,
        &mut guard,
        true,
    )?;
    let guard = guard.unwrap();
    let (_, identity) =
        super::open_recovery_leaf(&parent, std::path::Path::new("projection"), &work)?.unwrap();
    guard.publish_root_ownership(identity, volume, &work)?;
    let receipt = guard.root_ownership(&work)?.unwrap();
    assert_eq!(receipt.pid, std::process::id());
    if reused_pid {
        // Model a previous process with this PID and a different creation time.
        let previous = receipt.started.checked_sub(1).unwrap();
        assert_eq!(
            guard
                .file
                .as_ref()
                .unwrap()
                .seek_write(&previous.to_le_bytes(), 84)?,
            8
        );
        let file = guard.file.as_ref().unwrap();
        let mut encoded = [0; 92];
        assert_eq!(file.seek_read(&mut encoded, 0)?, encoded.len());
        assert_eq!(file.seek_write(blake3::hash(&encoded).as_bytes(), 92)?, 32);
        file.sync_all()?;
    }
    drop(guard);
    let mut owner =
        crate::NativeMountRecoveryOwner::acquire_in_directory(&destination, &parent, &work)?;
    let recovery_path = root.path().join("recovery");
    std::fs::create_dir(&recovery_path)?;
    let (recovery, _) = work.open_root_directory(&recovery_path)?;
    let expected = if different_volume {
        crate::VolumeId::new()
    } else {
        volume
    };
    if fail_flush {
        let denied = crate::NativeMaterializationWork::new(
            WorkBudget {
                durability_operations: 0,
                ..WorkBudget::UNBOUNDED
            },
            &CancellationToken::new(),
        )
        .map_err(|(error, _)| error)?;
        assert!(
            owner
                .preserve_crashed_in_directory(
                    &recovery,
                    std::path::Path::new("saved"),
                    expected,
                    &denied
                )
                .is_err()
        );
        assert!(!destination.exists());
        assert_eq!(
            std::fs::read(recovery_path.join("saved/authored"))?,
            b"unpublished bytes"
        );
        assert!(owner.cleanup_state()?.destination_fence_retained);
    }
    let recovered_path = if fail_flush {
        assert!(owner.cleanup_state()?.controls_pending);
        assert!(matches!(
            owner.recover(),
            Err(NativeMountError::ControlPending)
        ));
        assert!(owner.cleanup_state()?.destination_fence_retained);
        // Substitute the displayed target after the failed flush. Retry must
        // retain the directory that actually received the authored root.
        let held_path = root.path().join("held-recovery");
        std::fs::rename(&recovery_path, &held_path)?;
        std::fs::create_dir(&recovery_path)?;
        held_path
    } else {
        recovery_path.clone()
    };
    if drop_owner {
        let before = work.receipt();
        let shared = crate::sync::native::Arc::clone(&owner.shared);
        drop(owner);
        crate::native_mount::recovery::wait_for_retirement(&shared);
        assert_eq!(
            work.receipt().durability_operations - before.durability_operations,
            2,
            "LastDrop flushes both held directories on the original acquisition ledger"
        );
        assert_eq!(
            std::fs::read(recovered_path.join("saved/authored"))?,
            b"unpublished bytes"
        );
        assert!(
            !recovery_path.join("saved").exists(),
            "replacement directory is never used"
        );
        let mut next =
            crate::NativeMountRecoveryOwner::acquire_in_directory(&destination, &parent, &work)?;
        assert!(!next.preserve_crashed_in_directory(
            &parent,
            std::path::Path::new("unused"),
            volume,
            &work
        )?);
        return Ok(());
    }
    let (retry_target, _) = work.open_root_directory(&recovery_path)?;
    let result = owner.preserve_crashed_in_directory(
        &retry_target,
        std::path::Path::new("saved"),
        expected,
        &work,
    );
    match (reused_pid, different_volume) {
        (_, true) => assert!(matches!(
            result,
            Err(NativeMountError::RecoveryVolumeMismatch)
        )),
        (false, false) => assert!(matches!(result, Err(NativeMountError::DestinationBusy))),
        (true, false) => {
            assert!(result?);
            assert!(!destination.exists());
            assert_eq!(
                std::fs::read(recovered_path.join("saved/authored"))?,
                b"unpublished bytes"
            );
            assert!(!owner.cleanup_state()?.destination_fence_retained);
            return Ok(());
        }
    }
    assert!(owner.cleanup_state()?.destination_fence_retained);
    assert_eq!(
        std::fs::read(destination.join("authored"))?,
        b"unpublished bytes"
    );
    assert!(!recovery_path.join("saved").exists());
    Ok(())
}
