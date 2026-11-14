use std::{ffi::CStr, io, os::unix::ffi::OsStrExt, path::PathBuf};

/// The OS account home, independent of the caller's environment or login session.
pub(crate) fn user_home_directory() -> io::Result<PathBuf> {
    let user = super::effective_user();
    // Never switch between a login-session directory and a fallback: session
    // creation/removal must not replace an already-live service's authority.
    // NSS identifies the account independently of HOME and XDG overrides.
    // SAFETY: sysconf has no pointer arguments. This is a buffer chunk size;
    // ERANGE grows it to fit the actual account record, without a size limit.
    let chunk = usize::try_from(unsafe { libc::sysconf(libc::_SC_GETPW_R_SIZE_MAX) })
        .unwrap_or(1024)
        .max(1);
    let mut buffer = Vec::new();
    let mut length = chunk;
    loop {
        buffer
            .try_reserve_exact(length - buffer.len())
            .map_err(io::Error::other)?;
        buffer.resize(length, 0_u8);
        // SAFETY: zero is valid initial passwd storage; getpwuid_r fills its
        // fields and returns pointers borrowing the owned buffer on success.
        let mut record: libc::passwd = unsafe { std::mem::zeroed() };
        let mut found = std::ptr::null_mut();
        // SAFETY: record and found are writable, disjoint outputs; buffer owns
        // buffer.len() writable bytes and all three remain live for this call.
        let error = unsafe {
            libc::getpwuid_r(
                user,
                &raw mut record,
                buffer.as_mut_ptr().cast(),
                buffer.len(),
                &raw mut found,
            )
        };
        if error == libc::ERANGE {
            length = length.checked_mul(2).ok_or(io::ErrorKind::OutOfMemory)?;
            continue;
        }
        if error != 0 {
            return Err(io::Error::from_raw_os_error(error));
        }
        if found.is_null() || record.pw_dir.is_null() {
            return Err(io::ErrorKind::NotFound.into());
        }
        // SAFETY: successful getpwuid_r returns this NUL-terminated field
        // inside buffer, which remains alive until the owned path is copied.
        let home = unsafe { CStr::from_ptr(record.pw_dir) };
        let home = PathBuf::from(std::ffi::OsStr::from_bytes(home.to_bytes()));
        return if home.is_absolute() {
            Ok(home)
        } else {
            Err(io::ErrorKind::InvalidData.into())
        };
    }
}

/// Supply the OS per-user runtime directory inside a fresh user/mount namespace.
/// Mapping the caller to root does not create its login directory. The fixture
/// supplies private runtime and account-home mounts, leaving host contents intact.
///
/// # Errors
/// Returns the OS mount or directory error.
///
/// # Safety
/// The caller must own a fresh mount namespace with private propagation. Both
/// the runtime and account-home mounts are replaced until that namespace exits.
#[cfg(any(test, feature = "test-support"))]
pub unsafe fn initialize_isolated_user_runtime() -> io::Result<()> {
    // SAFETY: all strings are static NUL-terminated storage. The caller owns
    // an isolated mount namespace; this changes no host directory contents.
    if unsafe {
        libc::mount(
            c"tmpfs".as_ptr(),
            c"/run".as_ptr(),
            c"tmpfs".as_ptr(),
            libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC,
            c"mode=0755".as_ptr().cast(),
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    // Admission also uses the NSS home independently of environment overrides.
    // Cover it before any host authority can be created there. A missing home
    // already excludes that authority and must not be created on the host.
    let home = user_home_directory()?;
    if home.is_dir() {
        let target = std::ffi::CString::new(home.as_os_str().as_bytes())?;
        // SAFETY: target is owned NUL-terminated storage; the caller's private
        // mount namespace owns this overmount and its lifetime.
        if unsafe {
            libc::mount(
                c"tmpfs".as_ptr(),
                target.as_ptr(),
                c"tmpfs".as_ptr(),
                libc::MS_NOSUID | libc::MS_NODEV,
                c"mode=0700".as_ptr().cast(),
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    // /run exists even on minimal hosts that have no login-session subtree.
    // Its new private mount owns this target; never mkdir on the host's /run.
    super::create_directory(std::path::Path::new("/run/user"))?;
    super::create_directory(&PathBuf::from(format!(
        "/run/user/{}",
        super::effective_user()
    )))
}

/// Run an exact real-binary test with an isolated OS per-user runtime directory.
/// Returns `true` to the parent after the child has passed, and `false` in the
/// prepared child so its test body can run. No caller environment selects the
/// production service root.
///
/// # Errors
/// Returns namespace, mount, child-test or executable errors.
#[cfg(any(test, feature = "test-support"))]
pub fn with_isolated_user_runtime(test: &str) -> io::Result<bool> {
    const CASE: &str = "ACYCLIC_ISOLATED_USER_RUNTIME_TEST";
    if std::env::var_os(CASE).is_some_and(|case| case == test) {
        // SAFETY: this child is launched below by unshare with a fresh mount
        // namespace and private propagation, before any test body runs.
        unsafe {
            initialize_isolated_user_runtime()?;
        }
        return Ok(false);
    }
    let output = std::process::Command::new("unshare")
        .args([
            "--user",
            "--map-root-user",
            "--mount",
            "--net",
            "--propagation",
            "private",
            "--",
        ])
        // Preserve the original executable across unshare and any fixture
        // overmount, including when its pathname lives under /run/user.
        .arg("/proc/self/fd/0")
        .stdin(std::fs::File::open("/proc/self/exe")?)
        .args([test, "--exact", "--nocapture"])
        .env(CASE, test)
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !output.status.success()
        || !stdout
            .lines()
            .any(|line| line.starts_with("test result: ok. 1 passed; 0 failed; 0 ignored;"))
    {
        return Err(io::Error::other(format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    #[test]
    fn isolated_runtime_refuses_an_unmatched_test_filter() {
        assert!(
            super::with_isolated_user_runtime("private_file::linux::tests::no_such_test").is_err(),
            "a successful child with zero executed tests is not a pass"
        );
    }
}
