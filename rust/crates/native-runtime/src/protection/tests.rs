use super::*;
use std::cell::Cell;

#[test]
fn private_payload_round_trips_through_the_callers_copy() -> io::Result<()> {
    let plain = b"durable owner authority";
    let protected = protect_with_copy(plain, 4096, |bytes| Ok::<_, io::Error>(bytes.to_vec()))?;
    #[cfg(windows)]
    assert_ne!(protected, plain);
    unprotect_with_copy(&protected, 4096, |bytes| {
        assert_eq!(bytes, plain);
        Ok::<_, io::Error>(())
    })
}

#[test]
fn invalid_input_never_reaches_the_copy_callback() {
    for (bytes, bound) in [
        (b"".as_slice(), 4096),
        (b"key".as_slice(), 0),
        (b"key".as_slice(), 2),
    ] {
        for protect in [false, true] {
            let copied = Cell::new(false);
            let result = transform(bytes, bound, protect, |_| {
                copied.set(true);
                Ok::<_, io::Error>(())
            });
            assert!(result.is_err());
            assert!(!copied.get());
        }
    }
}

#[test]
fn callback_failure_is_preserved() -> io::Result<()> {
    let result = protect_with_copy(b"owner", 4096, |_| {
        Err::<(), _>(io::Error::new(io::ErrorKind::OutOfMemory, "copy refused"))
    });
    assert_eq!(
        result
            .err()
            .ok_or_else(|| io::Error::other("callback unexpectedly succeeded"))?
            .kind(),
        io::ErrorKind::OutOfMemory
    );
    Ok(())
}

#[cfg(windows)]
#[test]
fn oversized_dpapi_output_never_reaches_the_copy_callback() {
    let copied = Cell::new(false);
    let result = protect_with_copy(b"x", 1, |_| {
        copied.set(true);
        Ok::<_, io::Error>(())
    });
    assert!(result.is_err());
    assert!(!copied.get());
}

#[cfg(windows)]
#[test]
fn corrupted_protection_cannot_be_opened() -> io::Result<()> {
    let mut protected =
        protect_with_copy(b"owner", 4096, |bytes| Ok::<_, io::Error>(bytes.to_vec()))?;
    protected[0] ^= 0xff;
    let copied = Cell::new(false);
    let result = unprotect_with_copy(&protected, 4096, |_| {
        copied.set(true);
        Ok::<_, io::Error>(())
    });
    assert!(result.is_err());
    assert!(!copied.get());
    Ok(())
}
