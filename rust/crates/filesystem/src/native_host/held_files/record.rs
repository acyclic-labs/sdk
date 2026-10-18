//! Exact-length native records, shared with the records fuzz target.
use std::io::{self, Read};

pub(super) fn read<const N: usize>(reader: &mut impl Read) -> io::Result<[u8; N]> {
    let mut bytes = [0; N];
    reader.read_exact(&mut bytes)?;
    #[cfg(test)]
    if ALLOW_TRAILING.get() {
        return Ok(bytes);
    }
    match reader.read_exact(&mut [0]) {
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(bytes),
        Err(error) => Err(error),
        Ok(()) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "published record is too long",
        )),
    }
}

#[cfg(test)]
thread_local! { static ALLOW_TRAILING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(test)]
mod tests {
    use super::*;

    fn exact(bytes: &[u8]) -> bool {
        read::<16>(&mut &*bytes).ok() == <[u8; 16]>::try_from(bytes).ok()
    }

    proptest::proptest! {
        #[test]
        fn records_round_trip_and_accept_exactly_their_length(
            record in proptest::prelude::any::<[u8; 16]>(),
            input in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
        ) {
            proptest::prop_assert!(exact(&record));
            proptest::prop_assert!(exact(&input));
        }
    }

    #[test]
    fn negative_control_omitting_the_eof_check_accepts_an_invalid_record() {
        ALLOW_TRAILING.set(true);
        let rejected = !exact(&[1; 17]);
        ALLOW_TRAILING.set(false);
        assert!(rejected);
    }
}
