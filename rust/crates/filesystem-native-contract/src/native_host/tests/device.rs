use super::{join_device, split_device, widen};

#[test]
fn split_and_join_round_trip() -> Result<(), i32> {
    for (major, minor) in [(0, 0), (1, 3), (0xFF, 0xFF_FFFF)] {
        let device = join_device(major, minor)?;
        assert_eq!(split_device(widen(device)), (major, minor));
        #[cfg(target_os = "macos")]
        if device < 0 {
            // A checked narrowing loses valid device numbers after the
            // host's signed dev_t was widened by a metadata consumer.
            assert!(libc::dev_t::try_from(widen(device)).is_err());
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
#[test]
fn join_rejects_unrepresentable_components() {
    assert_eq!(join_device(0x100, 0), Err(libc::EOVERFLOW));
    assert_eq!(join_device(0, 0x100_0000), Err(libc::EOVERFLOW));
}
