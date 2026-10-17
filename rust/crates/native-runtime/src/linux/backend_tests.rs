use super::*;
use proptest::prelude::*;

const UNSUPPORTED: [i32; 4] = [libc::EPERM, libc::EACCES, libc::ENOSYS, libc::EOPNOTSUPP];

// Count probes at the actual backend-selection entry, with the original fault
// injected into its retained state. No substitute selection algorithm is used.
fn probe_count(code: i32, requests: usize, forget_selection: bool) -> io::Result<usize> {
    let mut owner = Owner::new();
    let mut probes = 0;
    for _ in 0..requests {
        assert_eq!(
            owner.select_with(|| {
                probes += 1;
                Err(io::Error::from_raw_os_error(code))
            })?,
            Selected::Positional
        );
        if forget_selection {
            owner.backend = Backend::Unprobed;
        }
    }
    Ok(probes)
}

#[test]
fn missing_retained_selection_negative_control_is_detected() -> io::Result<()> {
    for code in UNSUPPORTED {
        assert_eq!(probe_count(code, 3, false)?, 1);
        assert_ne!(probe_count(code, 3, true)?, 1);
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    // A definitive unsupported result is probed exactly once per owner, for
    // every supported denial code and any nonempty bounded request sequence.
    #[test]
    fn unsupported_selection_is_sticky(
        code in proptest::sample::select(UNSUPPORTED.to_vec()),
        requests in 1usize..16,
    ) {
        prop_assert_eq!(probe_count(code, requests, false).unwrap(), 1);
    }

    #[test]
    fn operational_probe_errors_keep_their_exact_cause(
        code in proptest::sample::select(vec![libc::EIO, libc::EMFILE, libc::ENOMEM, libc::EINVAL, libc::EBADF]),
    ) {
        let mut owner = Owner::new();
        let error = owner.select_with(|| Err(io::Error::from_raw_os_error(code))).unwrap_err();
        prop_assert_eq!(error.raw_os_error(), Some(code));
        prop_assert!(matches!(owner.backend, Backend::Unprobed));
    }

    // Exercise the production read/write dispatch after capability selection.
    // The independent expected file is derived only from generated input.
    #[test]
    fn unsupported_ring_uses_real_positional_io(
        code in proptest::sample::select(UNSUPPORTED.to_vec()),
        offset in 0usize..32,
        bytes in proptest::collection::vec(any::<u8>(), 1..128),
    ) {
        let directory = tempfile::tempdir().unwrap();
        let file = Arc::new(NativeFilePayload {
            file: std::fs::OpenOptions::new().read(true).write(true).create_new(true)
                .open(directory.path().join("fallback")).unwrap(),
            _allocation: None,
        });
        let mut owner = Owner::new();
        prop_assert_eq!(owner.select_with(|| Err(io::Error::from_raw_os_error(code))).unwrap(), Selected::Positional);
        owner.write_batch(&file, vec![OwnedWrite {
            offset: offset as u64, bytes: Bytes::copy_from_slice(&bytes),
        }]).unwrap();
        let mut expected = vec![0; offset];
        expected.extend_from_slice(&bytes);
        prop_assert_eq!(&std::fs::read(directory.path().join("fallback")).unwrap(), &expected);
        let actual = owner.read_batch(&file, &[OwnedRead { offset: 0, length: expected.len() + 8 }]).unwrap();
        prop_assert_eq!(actual, vec![Bytes::from(expected)]);
        prop_assert!(matches!(owner.backend, Backend::Positional));
    }
}

#[test]
fn compio_poll_failure_retains_key_and_allows_unrelated_progress() -> io::Result<()> {
    let mut owner = Owner::new();
    if owner.select()? == Selected::Positional {
        return Ok(());
    }
    // The retired read waits on an empty pipe, so it is still pending until
    // the test writes: unrelated progress and reaping are forced to happen
    // before its terminal completion instead of racing it.
    let (reader, mut writer) = std::io::pipe()?;
    let affected = Arc::new(NativeFilePayload {
        file: std::fs::File::from(std::os::fd::OwnedFd::from(reader)),
        _allocation: None,
    });
    let op = ReadAt::new(
        SharedFd::new(Arc::clone(&affected)),
        0,
        compio_buf::IoBuf::slice(Vec::with_capacity(8), ..8),
    );
    let Backend::Ring(driver) = &mut owner.backend else {
        return Err(io::Error::other("missing proactor"));
    };
    let key = match driver.push(op) {
        PushEntry::Pending(key) => key,
        PushEntry::Ready(_) => return Err(io::Error::other("read did not enter proactor")),
    };
    let error = owner.retire(
        RetiredKey::Read(key),
        io::Error::other("injected poll failure"),
    );
    assert!(is_uncertain_completion(&error));
    assert!(matches!(owner.backend, Backend::Retiring(_)));
    assert_eq!(owner.select()?, Selected::Positional);

    let temporary = tempfile::tempdir()?;
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
    owner.reap_retired();
    assert!(
        matches!(owner.backend, Backend::Retiring(_)),
        "a pending retired key was released"
    );

    std::io::Write::write_all(&mut writer, b"complete")?;
    let Backend::Retiring(retired) = &mut owner.backend else {
        return Err(io::Error::other("retired proactor was released early"));
    };
    retired.wait_terminal()?;
    owner.reap_retired();
    assert!(matches!(owner.backend, Backend::Unprobed));
    Ok(())
}
