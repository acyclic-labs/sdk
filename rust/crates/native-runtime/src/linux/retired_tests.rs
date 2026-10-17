use super::*;

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
            .arg("linux::retired::tests::retired_compio_proactor_cannot_drop_pending_key")
            .env(CHILD, "1"),
        std::time::Duration::from_secs(120),
    )?;
    assert_eq!(status.signal(), Some(libc::SIGABRT));
    Ok(())
}
