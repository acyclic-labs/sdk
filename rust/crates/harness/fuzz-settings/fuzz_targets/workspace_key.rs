#![no_main]

#[path = "../../../harness-core/src/native_settings/workspace.rs"]
mod workspace;

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &str| {
    if input.len() > 4096 {
        return;
    }
    let mut paths = input.trim_end_matches('\n').splitn(3, '\n');
    let left = paths.next().unwrap();
    let right = paths.next().unwrap_or(left);
    let third = paths.next().unwrap_or(right);
    // Call the production matcher as well as both platform-specific pure paths.
    assert!(workspace::matches(
        left,
        std::path::Path::new(left),
        std::path::Path::new(left)
    ));
    for windows in [false, true] {
        assert!(workspace::equivalent(left, left, windows));
        assert_eq!(
            workspace::equivalent(left, right, windows),
            workspace::equivalent(right, left, windows)
        );
        if workspace::equivalent(left, right, windows)
            && workspace::equivalent(right, third, windows)
        {
            assert!(workspace::equivalent(left, third, windows));
        }
    }
});
