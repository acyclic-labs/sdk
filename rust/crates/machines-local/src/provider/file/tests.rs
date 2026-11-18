use super::*;
use acyclic_native_runtime::{FileLock, FileLockMode};

#[tokio::test]
async fn original_alarm_failure_distinguishes_unsubmitted_and_physical_file_work()
-> Result<(), Box<dyn std::error::Error>> {
    let machine = MachineId::new();
    let backend = LocalBackend {
        machine: LocalMachine::new(
            machine,
            LocalOptions {
                cwd: std::fs::canonicalize(std::env::temp_dir())?,
                environment: BTreeMap::new(),
                process_custody: None,
            },
        )?,
        generation: std::sync::Mutex::new(Generation::new()?),
    };
    let path = std::env::temp_dir().join(format!("acyclic-terminal-file-{machine}"));
    let access = FileAccess::new(path.to_str().unwrap().into(), 0, u64::MAX)?;
    let cause = Arc::new(std::io::Error::from_raw_os_error(5));

    // The known original terminal must refuse before opening or creating a file.
    let (deadline, finish) = GuestDeadline::controlled();
    finish(Err(cause.clone()));
    assert!(
        matches!(backend.file(access.clone(), true, &deadline, |_| Ok(())).await,
        Err(ProviderError::DeadlineSourceLost { machine: None, cause: actual })
            if Arc::ptr_eq(&actual.0, &cause))
    );
    assert!(!path.try_exists()?);
    let live = backend.generation.lock().unwrap().live.clone();
    assert!(live.clone().try_write_owned().is_ok());

    // After submission, loss ends observation but cannot retire physical custody.
    let (deadline, finish) = GuestDeadline::controlled();
    let (entered, entering) = tokio::sync::oneshot::channel();
    let (release, releasing) = std::sync::mpsc::channel();
    let mut operation = Box::pin(backend.file(access, true, &deadline, move |_| {
        let _ = entered.send(());
        releasing.recv().map_err(io::Error::other)?;
        Ok(())
    }));
    tokio::select! {
        result = &mut operation => panic!("physical work ended before release: {result:?}"),
        started = entering => started?,
    }
    finish(Err(cause.clone()));
    let result = operation
        .as_mut()
        .now_or_never()
        .expect("original source loss must settle observation");
    assert!(matches!(result,
        Err(ProviderError::DeadlineSourceLost { machine: Some(actual), cause: failure })
            if actual == machine && Arc::ptr_eq(&failure.0, &cause)));
    assert!(live.clone().try_write_owned().is_err());
    release.send(())?;
    let _retired = live.write_owned().await;
    std::fs::remove_file(path)?;
    Ok(())
}

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
