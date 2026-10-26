#![no_main]

use libfuzzer_sys::fuzz_target;
use std::{ffi::OsString, path::Path};

use acyclic_native_runtime::ExecutableSearch;

fuzz_target!(|input: &str| {
    if input.len() > 4096 {
        return;
    }
    let (program, paths) = input.split_once('\n').unwrap_or((input, ""));
    let cwd = std::env::current_dir().unwrap();
    if let Ok(resolved) =
        ExecutableSearch::new(&cwd, Some(OsString::from(paths)), None).resolve(program)
    {
        assert!(resolved.is_absolute());
        #[cfg(unix)]
        if Path::new(program).is_absolute() {
            assert_eq!(resolved.as_os_str(), program);
        }
        if let Ok(repeated) = ExecutableSearch::new(&cwd, None, None).resolve(&resolved) {
            assert_eq!(resolved, repeated);
        }
    }
});
