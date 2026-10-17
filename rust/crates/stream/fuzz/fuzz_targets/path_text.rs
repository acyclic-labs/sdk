#![no_main]
use acyclic_stream::StreamPath;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    if let Ok(path) = StreamPath::new(text) {
        assert_eq!(path.as_str(), text);
        assert_eq!(StreamPath::new(path.as_str()).unwrap(), path);
        let mut current = path.clone();
        while let Some(parent) = current.parent() {
            let end = current.as_str().rfind('/').unwrap();
            assert_eq!(parent.as_str(), &current.as_str()[..end]);
            assert_eq!(StreamPath::new(parent.as_str()).unwrap(), parent);
            assert!(parent < current);
            current = parent;
        }
        assert!(!current.as_str().contains('/'));
    }
});
