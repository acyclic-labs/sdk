#![no_main]

use libfuzzer_sys::fuzz_target;
use std::{ffi::OsString, path::Path};

#[path = "../../harness-core/src/mcp/install/executable.rs"]
mod executable;

fuzz_target!(|input: &str| {
    if input.len() > 4096 {
        return;
    }
    let (program, paths) = input.split_once('\n').unwrap_or((input, ""));
    let cwd = std::env::current_dir().unwrap();
    if let Ok(resolved) = executable::resolve(program, Some(OsString::from(paths)), &cwd) {
        assert!(resolved.is_absolute());
        #[cfg(unix)]
        if Path::new(program).is_absolute() {
            assert_eq!(resolved.as_os_str(), program);
        }
        if let Ok(repeated) =
            executable::resolve(resolved.to_str().unwrap_or(""), None, Path::new(&cwd))
        {
            assert_eq!(resolved, repeated);
        }
    }
});
