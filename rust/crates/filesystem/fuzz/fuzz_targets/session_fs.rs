#![no_main]
use acyclic_fs::session::{Cursor, Publication, Subscription};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(value) = Subscription::decode(data) {
        assert_eq!(
            Subscription::decode(&value.encode().unwrap()).unwrap(),
            value
        );
    }
    if let Ok(value) = Publication::decode(data) {
        assert_eq!(
            Publication::decode(&value.encode().unwrap()).unwrap(),
            value
        );
    }
    if let Ok(value) = Cursor::decode(data) {
        assert_eq!(Cursor::decode(&value.encode().unwrap()).unwrap(), value);
    }
});
