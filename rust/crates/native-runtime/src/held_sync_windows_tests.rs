use super::*;
use crate::{Durability, sync_data, sync_file, tests::ReadBudgetMeter};
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::os::windows::{fs::OpenOptionsExt as _, io::AsHandle as _};

fn full_flush_contract(flush: impl Fn(&File) -> io::Result<()>) -> io::Result<bool> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("record-é&%.bin");
    let mut file = File::create(&path)?;
    file.write_all(b"durable incarnation")?;
    flush(&file)?;
    let held = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(0x0200_0000)
        .open(directory.path())?;
    flush(&held)?;
    let readonly = File::open(&path)?;
    Ok(matches!(flush(&readonly), Err(error) if error.raw_os_error() == Some(5)))
}

#[test]
fn shared_full_flush_supports_files_directories_and_native_access_errors() {
    assert!(full_flush_contract(|file| sync_descriptor_once(file.as_handle())).unwrap());
    assert!(full_flush_contract(|file| sync_file(file, Durability::Full, None)).unwrap());
    assert!(full_flush_contract(|file| sync_data(file, Durability::Full)).unwrap());
    assert!(full_flush_contract(crate::flush_file_all).unwrap());
    assert!(full_flush_contract(crate::flush_file_data).unwrap());
    assert!(
        full_flush_contract(|file| {
            let meter = ReadBudgetMeter::new(1, 0);
            let result = sync_file(file, Durability::Full, Some(&meter));
            assert_eq!(
                meter
                    .used_operations
                    .load(std::sync::atomic::Ordering::Acquire),
                1
            );
            result
        })
        .unwrap()
    );
}

#[test]
fn negative_control_swallowing_windows_flush_errors_fails_the_contract() {
    assert!(!full_flush_contract(|_| Ok(())).unwrap());
}
