libfuzzer_sys::fuzz_target!(|input: &[u8]| acyclic_fs::fuzzing::fuse_reply(input));
