#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    if let Some(members) = fuzz_support::FuzzInput::parse(fuzz_input, |raw_input| {
        sdk_generation::historical::decode_archive_members(raw_input, "fixture", "0.1.0")
    }) {
        for path in members.keys() {
            assert!(!path.is_empty());
            assert!(!path.contains(['\\', ':']));
            assert!(!std::path::Path::new(path).has_root());
            assert!(
                !std::path::Path::new(path).components().any(|part| matches!(
                    part,
                    std::path::Component::ParentDir
                        | std::path::Component::RootDir
                        | std::path::Component::Prefix(_)
                ))
            );
        }
    }
});
