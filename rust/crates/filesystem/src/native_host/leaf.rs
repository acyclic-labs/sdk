//! A raw syscall leaf must be exactly one normal component. `Path::components`
//! normalizes trailing separators and `/.`; passing those raw names to the
//! kernel can follow a directory symlink despite `O_NOFOLLOW`.
use std::{
    io,
    path::{Component, Path},
};

pub(crate) fn require_leaf(path: &Path) -> io::Result<()> {
    let mut components = path.components();
    let Some(Component::Normal(name)) = components.next() else {
        return Err(io::ErrorKind::InvalidInput.into());
    };
    let exact = name == path.as_os_str();
    #[cfg(test)]
    let exact = exact || ACCEPT_NORMALIZED.get();
    if !exact || components.next().is_some() {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    Ok(())
}

#[cfg(test)]
thread_local! { static ACCEPT_NORMALIZED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

    fn correct(bytes: &[u8]) -> bool {
        let expected =
            !bytes.is_empty() && bytes != b"." && bytes != b".." && !bytes.contains(&b'/');
        require_leaf(Path::new(OsStr::from_bytes(bytes))).is_ok() == expected
    }

    proptest::proptest! {
        #[test]
        fn raw_leaves_round_trip_without_path_normalization(
            bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
        ) {
            proptest::prop_assert!(correct(&bytes));
        }
    }

    #[test]
    fn negative_control_a_normalized_leaf_accepts_an_alias() {
        ACCEPT_NORMALIZED.set(true);
        let caught = !correct(b"alias/.");
        ACCEPT_NORMALIZED.set(false);
        assert!(caught);
    }
}
