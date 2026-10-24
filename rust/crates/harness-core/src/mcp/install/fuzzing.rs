use super::executable;

use std::{ffi::OsString, path::Path};

/// Exercises executable resolution with explicit paths.
pub fn check(input: &str) {
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
}
