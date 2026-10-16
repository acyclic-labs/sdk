#![no_main]
#[path = "../../tests/cli_regressions/package_manifest.rs"]
mod package_manifest;
#[path = "../../tests/cli_regressions/paths.rs"]
mod paths;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if let Ok(text) = std::str::from_utf8(bytes) {
        let mut fields = text.splitn(3, '\n');
        if let (Some(old), Some(new), Some(path)) = (fields.next(), fields.next(), fields.next()) {
            for windows in [false, true] {
                if let Some(rebased) = paths::rebase(path, old, new, windows) {
                    assert_eq!(paths::rebase(&rebased, new, new, windows), Some(rebased));
                }
            }
        }
    }
    let template = b"[package]\nname='demo'\nversion='1.0.0'\nbuild=false\n[lib]\nname='demo'\npath='src/lib.rs'\n";
    if let Some(result) = package_manifest::normalize(template, bytes) {
        assert_eq!(package_manifest::normalize(&result, bytes), Some(result));
    }
});
