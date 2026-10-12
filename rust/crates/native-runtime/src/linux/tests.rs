use super::*;

#[test]
fn compio_poll_failure_retains_key_and_allows_unrelated_progress() -> io::Result<()> {
    let mut owner = Owner::new();
    if !owner.ensure_driver()? {
        return Ok(());
    }
    let temporary = tempfile::tempdir()?;
    let affected = Arc::new(NativeFilePayload {
        file: std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(temporary.path().join("affected"))?,
        _allocation: None,
    });
    affected.set_len(MAX_IO_BYTES as u64)?;
    let op = ReadAt::new(
        SharedFd::new(Arc::clone(&affected)),
        0,
        compio_buf::IoBuf::slice(Vec::with_capacity(MAX_IO_BYTES), ..MAX_IO_BYTES),
    );
    let key = match owner
        .driver
        .as_mut()
        .ok_or_else(|| io::Error::other("missing proactor"))?
        .push(op)
    {
        PushEntry::Pending(key) => key,
        PushEntry::Ready(_) => return Err(io::Error::other("read did not enter proactor")),
    };
    let error = owner.retire(
        RetiredKey::Read(key),
        io::Error::other("injected poll failure"),
    );
    assert!(is_uncertain_completion(&error));
    assert!(owner.retired.is_some());
    assert!(!owner.ensure_driver()?);

    let unrelated = Arc::new(NativeFilePayload {
        file: std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(temporary.path().join("unrelated"))?,
        _allocation: None,
    });
    owner.write_batch(
        &unrelated,
        vec![OwnedWrite {
            offset: 0,
            bytes: Bytes::from_static(b"progress"),
        }],
    )?;
    assert_eq!(
        owner.read_batch(
            &unrelated,
            &[OwnedRead {
                offset: 0,
                length: 8
            }]
        )?,
        vec![Bytes::from_static(b"progress")]
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while owner.retired.is_some() {
        owner.reap_retired();
        if std::time::Instant::now() >= deadline {
            return Err(io::Error::other(
                "retired proactor did not reach terminal completion",
            ));
        }
        std::thread::yield_now();
    }
    Ok(())
}

#[test]
fn retired_compio_proactor_cannot_drop_pending_key() -> io::Result<()> {
    use std::os::unix::process::ExitStatusExt;

    const CHILD: &str = "ACYCLIC_TEST_RETIRED_COMPIO_DROP";
    let mut driver = match Proactor::new() {
        Ok(driver) => driver,
        Err(error) if ring_unavailable(&error) => return Ok(()),
        Err(error) => return Err(error),
    };
    if std::env::var_os(CHILD).is_some() {
        let temporary = tempfile::tempdir()?;
        let file = Arc::new(NativeFilePayload {
            file: std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(temporary.path().join("pending"))?,
            _allocation: None,
        });
        file.set_len(MAX_IO_BYTES as u64)?;
        let op = ReadAt::new(
            SharedFd::new(file),
            0,
            compio_buf::IoBuf::slice(Vec::with_capacity(MAX_IO_BYTES), ..MAX_IO_BYTES),
        );
        let key = match driver.push(op) {
            PushEntry::Pending(key) => key,
            PushEntry::Ready(_) => return Err(io::Error::other("read did not enter proactor")),
        };
        drop(RetiredProactor {
            driver,
            key: Some(RetiredKey::Read(key)),
        });
        return Err(io::Error::other("pending proactor drop did not abort"));
    }
    drop(driver);
    let status = crate::process_status(
        std::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("linux::tests::retired_compio_proactor_cannot_drop_pending_key")
            .env(CHILD, "1"),
        std::time::Duration::from_secs(120),
    )?;
    assert_eq!(status.signal(), Some(libc::SIGABRT));
    Ok(())
}
