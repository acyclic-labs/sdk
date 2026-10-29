use super::*;
use acyclic_native_runtime::{FileLock, FileLockMode};

#[tokio::test]
async fn blocked_physical_file_io_expires_without_attesting_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    let machine = MachineId::new();
    let backend = LocalBackend {
        machine: LocalMachine::new(
            machine,
            LocalOptions {
                cwd: std::fs::canonicalize(std::env::temp_dir())?,
                environment: BTreeMap::new(),
                inherit_environment: true,
                process_custody: None,
            },
        )?,
        generation: std::sync::Mutex::new(Generation::new()?),
    };
    let path = std::env::temp_dir().join(format!("acyclic-blocked-file-{machine}"));
    let held = FileLock::acquire(
        OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?,
        FileLockMode::Exclusive,
    )?;
    let deadline = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64
        + 100;
    let access = FileAccess::new(path.to_str().unwrap().into(), 0, deadline)?;
    let deadline = GuestDeadline::absolute(deadline)?;
    let (entered, entering) = tokio::sync::oneshot::channel();
    let mut operation = Box::pin(backend.file(access, false, &deadline, move |file| {
        let _ = entered.send(());
        let _lock = FileLock::acquire(file.try_clone()?, FileLockMode::Exclusive)?;
        Ok(())
    }));
    tokio::select! {
        result = &mut operation => panic!("blocked native transfer completed without its original lock release: {result:?}"),
        started = entering => started?,
    }
    let live = backend.generation.lock().unwrap().live.clone();
    // The original physical lease is held, unlike an observation-only guard.
    assert!(live.clone().try_write_owned().is_err());
    assert!(matches!(operation.await, Err(ProviderError::FileIndeterminate(id)) if id == machine));
    assert!(live.clone().try_write_owned().is_err());
    // This must fail promptly, rather than waiting forever or falsely joining.
    assert!(
        matches!(backend.stop().await, Err(ProviderError::CommandIndeterminate(id)) if id == machine)
    );
    held.unlock()?;
    let _physically_joined = live.write_owned().await;
    drop(held);
    std::fs::remove_file(path)?;
    Ok(())
}
