#![no_main]

pub use acyclic_fuzz_support as fuzz_support;
use acyclic_stream::StreamPath;

fuzz_support::fuzz_target!(|data: &[u8], fuzz_input| {
    let Some(text) = fuzz_input.text() else {
        return;
    };
    if let Some(path) = fuzz_support::FuzzInput::parse_text(fuzz_input, StreamPath::new) {
        assert_eq!(path.as_str(), text);
        let _ = fuzz_support::round_trip(&path, StreamPath::new(path.as_str()));
        let mut current = path.clone();
        while let Some(parent) = current.parent() {
            let end = current.as_str().rfind('/').unwrap();
            assert_eq!(parent.as_str(), &current.as_str()[..end]);
            let _ = fuzz_support::round_trip(&parent, StreamPath::new(parent.as_str()));
            assert!(parent < current);
            current = parent;
        }
        assert!(!current.as_str().contains('/'));
    }
});
