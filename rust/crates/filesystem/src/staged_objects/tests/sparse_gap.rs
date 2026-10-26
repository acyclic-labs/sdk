//! Synthetic spill gaps retain the platform's real sparse-file semantics.
use super::*;

pub(super) fn prepare(file: &std::fs::File) -> std::io::Result<()> {
    acyclic_native_runtime::device_control_in_place(
        file,
        windows::Win32::System::Ioctl::FSCTL_SET_SPARSE,
        &[],
        &mut [],
        None,
    )?;
    Ok(())
}

fn allocation(file: &std::fs::File) -> std::io::Result<u64> {
    let stat = crate::native_host::stat_file(file, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)?;
    u64::try_from(stat.raw.AllocationSize).map_err(std::io::Error::other)
}

#[test]
fn synthetic_spill_gaps_allocate_only_written_extents_with_dense_negative_control()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for gap in [4 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024] {
        let file = create_spill(directory.path())?;
        prepare(&file)?;
        acyclic_native_runtime::write_all_at(&file, gap, b"end", None)?;
        assert_eq!(file.metadata()?.len(), gap + 3);
        assert!(allocation(&file)? <= 1024 * 1024);
        let mut zero = [1];
        use std::os::windows::fs::FileExt as _;
        assert_eq!(file.seek_read(&mut zero, gap / 2)?, 1);
        assert_eq!(zero, [0]);
        let mut end = [0; 3];
        assert_eq!(file.seek_read(&mut end, gap)?, end.len());
        assert_eq!(&end, b"end");
    }
    // The former fixture writes at its advanced offset in an ordinary file,
    // forcing NTFS to allocate the intervening bytes. Keep the negative small;
    // the real drain regression still exercises the unchanged 1 GiB boundary.
    let dense = create_spill(directory.path())?;
    let gap = 4 * 1024 * 1024;
    acyclic_native_runtime::write_all_at(&dense, gap, b"end", None)?;
    assert!(allocation(&dense)? >= gap);
    Ok(())
}
