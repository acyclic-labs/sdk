use super::*;
use acyclic_test_support::differential;

// Always release readonly files, including on an assertion or IO failure.
struct WritableOnDrop(Vec<PathBuf>, std::fs::Permissions);
impl Drop for WritableOnDrop {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::set_permissions(path, self.1.clone());
        }
    }
}

#[test]
fn restores_every_readonly_bit_without_reopening_writable_entries() -> io::Result<()> {
    for readonly in 0_u32..16 {
        let temporary = tempfile::tempdir()?;
        let source = temporary.path().join("source");
        let restored = temporary.path().join("restored");
        let relative = ["", "nested", "nested/a&%.txt", "b.txt"];
        let _writable = WritableOnDrop(
            [&source, &restored]
                .into_iter()
                .flat_map(|root| relative.map(|relative| root.join(relative)))
                .collect(),
            std::fs::metadata(temporary.path())?.permissions(),
        );
        std::fs::create_dir_all(source.join("nested"))?;
        std::fs::write(source.join(relative[2]), b"bytes\0\r\n")?;
        std::fs::write(source.join(relative[3]), [])?;
        for (index, relative) in relative.iter().enumerate() {
            let path = source.join(relative);
            let mut permissions = std::fs::metadata(&path)?.permissions();
            permissions.set_readonly(readonly & (1 << index) != 0);
            std::fs::set_permissions(path, permissions)?;
        }
        let snapshot = Snapshot::capture(&source)?;
        let (result, trace) =
            differential::observe::<_, _, PermissionWrite>(|| (), |()| snapshot.restore(&restored));
        result?;
        assert_eq!(Snapshot::capture(&restored)?, snapshot);
        assert_eq!(
            trace,
            (0..readonly.count_ones())
                .map(|_| PermissionWrite)
                .collect::<Vec<_>>(),
            "writable entries incurred redundant permission writes"
        );
    }
    Ok(())
}

// Keep the original path-query implementation as an independent physical-I/O
// control. Every file's bytes, names, directory kind and permissions still agree.
fn legacy_capture(root: &Path) -> io::Result<Snapshot> {
    let mut pending = vec![PathBuf::new()];
    let mut entries = Vec::new();
    while let Some(relative) = pending.pop() {
        let path = root.join(&relative);
        let metadata = std::fs::symlink_metadata(&path)?;
        assert!(!metadata.is_symlink());
        let (bytes, private) = if metadata.is_dir() {
            let mut children = std::fs::read_dir(&path)?
                .map(|entry| entry.map(|entry| relative.join(entry.file_name())))
                .collect::<std::io::Result<Vec<_>>>()?;
            children.sort();
            pending.extend(children.into_iter().rev());
            (None, directory_is_private(&path)?)
        } else {
            let (bytes, private) = capture_file(&path)?;
            (Some(bytes), private)
        };
        entries.push(Entry {
            relative,
            bytes,
            permissions: metadata.permissions(),
            private,
        });
    }
    Ok(Snapshot(entries))
}

#[allow(
    unsafe_code,
    reason = "query physical I/O counters for this isolated test's own process"
)]
fn other_operations() -> io::Result<u64> {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "GetCurrentProcess"]
        fn current_process() -> *mut std::ffi::c_void;
        #[link_name = "GetProcessIoCounters"]
        fn process_io_counters(process: *mut std::ffi::c_void, counters: *mut [u64; 6]) -> i32;
    }
    // IO_COUNTERS has six u64 fields; OtherOperationCount is the third.
    let mut counters = [0_u64; 6];
    // SAFETY: the current-process pseudo-handle is valid and the complete
    // IO_COUNTERS output remains writable for this synchronous query.
    if unsafe { process_io_counters(current_process(), &raw mut counters) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    let [_, _, other, ..] = counters;
    Ok(other)
}

#[test]
fn snapshot_enumeration_preserves_the_image_with_fewer_physical_queries() -> io::Result<()> {
    const CHILD: &str = "ACYCLIC_SNAPSHOT_IO_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // The counter belongs to this child alone, so parallel tests and their
        // background I/O cannot make either side of the differential pass.
        let current = std::thread::current();
        let name = current.name().expect("libtest names its test threads");
        let output = crate::process_output(
            std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", name, "--test-threads=1", "--nocapture"])
                .env(CHILD, "1"),
            std::time::Duration::from_secs(5),
            64 * 1024,
        )?;
        assert!(output.status.success(), "snapshot I/O child: {output:?}");
        return Ok(());
    }
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    for directory in ["empty", "nested", "nested/é&%"] {
        std::fs::create_dir(root.join(directory))?;
    }
    for index in 0_u8..32 {
        std::fs::write(root.join(format!("nested/é&%/{index}.bin")), [index; 32])?;
    }
    let readonly = root.join("nested/é&%/7.bin");
    let mut permissions = std::fs::metadata(&readonly)?.permissions();
    let _writable = WritableOnDrop(vec![readonly.clone()], permissions.clone());
    permissions.set_readonly(true);
    std::fs::set_permissions(readonly, permissions)?;
    let expected = legacy_capture(root)?;
    assert_eq!(Snapshot::capture(root)?, expected);
    let before = other_operations()?;
    let actual = Snapshot::capture(root)?;
    let actual_queries = other_operations()? - before;
    let before = other_operations()?;
    let control = legacy_capture(root)?;
    let control_queries = other_operations()? - before;
    assert_eq!(actual, expected);
    assert_eq!(control, expected);
    assert!(
        actual_queries < control_queries,
        "enumeration repeated path metadata queries: actual={actual_queries}, legacy={control_queries}"
    );
    Ok(())
}
