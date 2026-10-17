use super::*;

fn directory() -> (tempfile::TempDir, OwnedFd) {
    let root = tempfile::tempdir().unwrap();
    let file = rustix::fs::open(
        root.path(),
        OFlags::PATH | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .unwrap();
    (root, file)
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn healthy_destinations_need_no_mount_identity(mount in proptest::prelude::any::<u64>()) {
        let (root, file) = directory();
        let mut base = rustix::fs::statx(
            &file, c"", AtFlags::EMPTY_PATH | AtFlags::STATX_DONT_SYNC,
            StatxFlags::TYPE | StatxFlags::MNT_ID,
        ).unwrap();
        base.stx_mask &= !StatxFlags::MNT_ID.bits();
        base.stx_mnt_id = mount;
        for flags in [AtFlags::STATX_DONT_SYNC, AtFlags::STATX_FORCE_SYNC] {
            for result in [
                Ok(base),
                Err(std::io::Error::from_raw_os_error(libc::EPERM)),
                Err(std::io::Error::from_raw_os_error(libc::ENOSYS)),
            ] {
                let actual = metadata(&file, flags, result, || {
                    rustix::fs::fstatfs(&file).map(|_| ()).map_err(Into::into)
                }).unwrap();
                proptest::prop_assert_eq!(actual, (FileType::Directory, None));
            }
        }
        let pinned = Pinned {
            parent: file.try_clone().unwrap(),
            file,
            path: root.path().to_path_buf(),
            mount: None,
        };
        proptest::prop_assert!(!recover(&pinned).unwrap());
        // The former eager requirement rejects this same healthy destination.
        proptest::prop_assert!(pinned.mount.ok_or("mount identity").is_err());
        proptest::prop_assert!(matches!(pinned.acquire(),
            Err(NativeMountError::CapabilityUnavailable("statx mount identity"))));
        proptest::prop_assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}

#[test]
fn blocked_statx_requires_a_fresh_probe_with_cached_metadata_negative_control() {
    let (_root, file) = directory();
    for blocked in [libc::EPERM, libc::ENOSYS] {
        let attempts = std::cell::Cell::new(0);
        let probe = || {
            attempts.set(attempts.get() + 1);
            Err(std::io::Error::from_raw_os_error(libc::ENOTCONN))
        };
        let actual = metadata(
            &file,
            AtFlags::STATX_FORCE_SYNC,
            Err(std::io::Error::from_raw_os_error(blocked)),
            probe,
        );
        assert_eq!(actual.unwrap_err().kind(), std::io::ErrorKind::NotConnected);
        assert_eq!(attempts.get(), 1);
        // A broken probe that trusts cached fstat attributes misses disconnect.
        let broken = metadata(
            &file,
            AtFlags::STATX_FORCE_SYNC,
            Err(std::io::Error::from_raw_os_error(blocked)),
            || rustix::fs::fstat(&file).map(|_| ()).map_err(Into::into),
        );
        assert!(broken.is_ok(), "negative control did not miss disconnect");
        metadata(
            &file,
            AtFlags::STATX_DONT_SYNC,
            Err(std::io::Error::from_raw_os_error(blocked)),
            || panic!("identity lookup must not contact a dead provider"),
        )
        .unwrap();
    }
    let error = metadata(
        &file,
        AtFlags::STATX_FORCE_SYNC,
        Err(std::io::Error::from_raw_os_error(libc::EACCES)),
        || panic!("an unrelated error must not fall back"),
    )
    .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EACCES));
}

#[test]
fn blocked_statx_keeps_the_no_follow_identity() {
    let (root, _file) = directory();
    let alias = root.path().join("alias");
    std::os::unix::fs::symlink(root.path(), &alias).unwrap();
    let file = rustix::fs::open(
        &alias,
        OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .unwrap();
    let actual = metadata(
        &file,
        AtFlags::STATX_DONT_SYNC,
        Err(std::io::Error::from_raw_os_error(libc::EPERM)),
        || panic!("identity lookup must not follow the alias"),
    )
    .unwrap();
    assert_eq!(actual, (FileType::Symlink, None));
    assert!(matches!(
        Pinned::open(&alias),
        Err(NativeMountError::InvalidDestination)
    ));
}
