//! Checks canonical stream paths and each parent through the production parser.
#![no_main]
use acyclic_stream::StreamPath;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    acyclic_stream::codec_fuzz::filesystem_paths(data).expect("filesystem resource index contract");
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    if let Ok(path) = StreamPath::new(text) {
        assert_eq!(path.as_str(), text);
        assert_eq!(StreamPath::new(path.as_str()).ok().as_ref(), Some(&path));
        let mut current = path;
        while let Some(parent) = current.parent() {
            assert_eq!(
                current.as_str().rsplit_once('/').map(|(prefix, _)| prefix),
                Some(parent.as_str())
            );
            assert_eq!(
                StreamPath::new(parent.as_str()).ok().as_ref(),
                Some(&parent)
            );
            assert!(parent < current);
            current = parent;
        }
        assert!(!current.as_str().contains('/'));
    }
});
