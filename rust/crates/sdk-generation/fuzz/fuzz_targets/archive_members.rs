#![no_main]

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if let Ok(members) =
        sdk_generation::historical::decode_archive_members(bytes, "fixture", "0.1.0")
    {
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
