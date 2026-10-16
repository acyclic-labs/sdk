#![no_main]
#[path = "../../tests/cli_regressions/package_manifest.rs"]
mod package_manifest;
#[path = "../../tests/cli_regressions/paths.rs"]
mod paths;
#[path = "../../src/historical/probe.rs"]
mod probe;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let _ = probe::graft_path(bytes);
    let _ = paths::fixed_revision(b"ref: refs/heads/sdk-fixture\n", bytes);
    let _ = paths::fixed_revision(bytes, b"0123456789012345678901234567890123456789");
    if let Ok(text) = std::str::from_utf8(bytes) {
        let mut fields = text.splitn(3, '\n');
        if let (Some(old), Some(new), Some(path)) = (fields.next(), fields.next(), fields.next()) {
            for windows in [false, true] {
                if let Some(rebased) = paths::rebase(path, old, new, windows) {
                    assert_eq!(paths::rebase(&rebased, new, new, windows), Some(rebased));
                    let before = paths::stable_filename(path, old);
                    let root_is_windows = old.as_bytes().get(1) == Some(&b':') || old.starts_with("\\\\") || old.starts_with("//");
                    let new_is_windows = new.as_bytes().get(1) == Some(&b':') || new.starts_with("\\\\") || new.starts_with("//");
                    if before.starts_with("<root>/") && windows == root_is_windows && windows == new_is_windows {
                        assert_eq!(
                            paths::stable_filename(
                                &paths::rebase(path, old, new, windows).unwrap(),
                                new
                            ),
                            before
                        );
                    }
                }
            }
        }
    }
    let template = b"[package]\nname='demo'\nversion='1.0.0'\nbuild=false\n[lib]\nname='demo'\npath='src/lib.rs'\n";
    if let Some(result) = package_manifest::normalize(template, bytes) {
        assert_eq!(package_manifest::normalize(&result, bytes), Some(result));
    }
});
