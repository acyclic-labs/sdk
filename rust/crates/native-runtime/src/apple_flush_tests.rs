use super::*;
use std::cell::RefCell;
use std::collections::VecDeque;

#[derive(Default)]
struct Probe {
    calls: Vec<i32>,
    errors: VecDeque<i32>,
    weaken_full: bool,
}

thread_local! {
    static PROBE: RefCell<Option<Probe>> = const { RefCell::new(None) };
}

pub(super) fn intercept(command: i32) -> io::Result<i32> {
    PROBE.with_borrow_mut(|probe| {
        let Some(probe) = probe else {
            return Ok(command);
        };
        let command = if probe.weaken_full && command == libc::F_FULLFSYNC {
            0
        } else {
            command
        };
        probe.calls.push(command);
        if let Some(error) = probe.errors.pop_front() {
            return Err(io::Error::from_raw_os_error(error));
        }
        Ok(command)
    })
}

fn observe(
    errors: &[i32],
    weaken_full: bool,
    flush: impl FnOnce() -> io::Result<()>,
) -> (io::Result<()>, Vec<i32>) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            PROBE.set(None);
        }
    }
    PROBE.set(Some(Probe {
        errors: errors.iter().copied().collect(),
        weaken_full,
        ..Probe::default()
    }));
    let reset = Reset;
    let result = flush();
    let calls = PROBE.with_borrow_mut(|probe| probe.take().unwrap().calls);
    drop(reset);
    (result, calls)
}

#[test]
fn public_flush_paths_require_fullfsync_and_detect_fsync_negative_control() -> io::Result<()> {
    let file = tempfile::tempfile()?;
    let (result, calls) = observe(&[], false, || {
        crate::sync_file(&file, Durability::Full, None)
    });
    result?;
    assert_eq!(calls, [libc::F_FULLFSYNC]);
    let (result, calls) = observe(&[], true, || {
        crate::sync_file(&file, Durability::Full, None)
    });
    result?;
    assert_ne!(
        calls,
        [libc::F_FULLFSYNC],
        "fsync must fail the same Full pin"
    );
    let (result, calls) = observe(&[], false, || crate::sync_data(&file, Durability::Full));
    result?;
    assert_eq!(calls, [libc::F_FULLFSYNC]);
    let (result, calls) = observe(&[], false, || {
        crate::sync_file(&file, Durability::Barrier, None)
    });
    result?;
    assert_eq!(calls, [libc::F_BARRIERFSYNC]);
    Ok(())
}

#[test]
fn interrupted_flush_retries_once_and_admits_both_attempts() -> io::Result<()> {
    let file = tempfile::tempfile()?;
    for (durability, command) in [
        (Durability::Full, libc::F_FULLFSYNC),
        (Durability::Barrier, libc::F_BARRIERFSYNC),
    ] {
        let mut admissions = 0;
        let (result, calls) = observe(&[libc::EINTR], false, || {
            sync_once(&file, durability, || {
                admissions += 1;
                Ok(())
            })
        });
        result?;
        assert_eq!(calls, [command, command]);
        assert_eq!(admissions, 2);
    }
    Ok(())
}

#[test]
fn only_enotsup_falls_back_and_full_never_acknowledges_the_fallback() -> io::Result<()> {
    let file = tempfile::tempfile()?;
    for (errors, expected) in [
        (
            vec![libc::ENOTSUP],
            vec![libc::F_FULLFSYNC, libc::F_BARRIERFSYNC],
        ),
        (
            vec![libc::ENOTSUP, libc::ENOTSUP],
            vec![libc::F_FULLFSYNC, libc::F_BARRIERFSYNC, 0],
        ),
    ] {
        let mut admissions = 0;
        let (result, calls) = observe(&errors, false, || {
            sync_once(&file, Durability::Full, || {
                admissions += 1;
                Ok(())
            })
        });
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Unsupported);
        assert_eq!(calls, expected);
        assert_eq!(admissions, expected.len());
    }
    for error in [libc::EIO, libc::EINVAL, libc::EBADF, libc::EACCES] {
        let (result, calls) = observe(&[error], false, || {
            crate::sync_file(&file, Durability::Full, None)
        });
        assert_eq!(result.unwrap_err().raw_os_error(), Some(error));
        assert_eq!(calls, [libc::F_FULLFSYNC]);
    }
    let (result, calls) = observe(&[libc::ENOTSUP], false, || {
        crate::sync_file(&file, Durability::Barrier, None)
    });
    result?;
    assert_eq!(calls, [libc::F_BARRIERFSYNC, 0]);
    Ok(())
}
