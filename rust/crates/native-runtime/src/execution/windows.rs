//! Native PSEC policy over the shared grant. No ACL propagation, filesystem
//! scan, alternate process owner or unconfined fallback is involved.
#![allow(unsafe_code, reason = "checked Windows PSEC and rooted NT object FFI")]

use super::{BoundPath, ExecutionAccess, ExecutionNetwork, Grant, refused, unavailable};
use std::{
    ffi::c_void,
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};
use windows_sys::Win32::Foundation::HANDLE;

mod objects;
pub(super) use objects::{open_beneath, validate_stdio};
mod specification;

pub(super) const REQUIRED_CAPABILITY: &str = "windows-psec-1.1";

type Create = unsafe extern "system" fn(*const c_void, u32, u32, *mut HANDLE) -> i32;
type Close = unsafe extern "system" fn(HANDLE);
type Version = unsafe extern "system" fn(u32, *mut u8, *mut u32) -> i32;

struct Api {
    create: Create,
    close: Close,
}
impl Api {
    fn get() -> io::Result<&'static Self> {
        static API: OnceLock<Option<Api>> = OnceLock::new();
        API.get_or_init(|| {
            use windows_sys::Win32::System::LibraryLoader::{GetProcAddress, LoadLibraryExW, LOAD_LIBRARY_SEARCH_SYSTEM32};
            let name: Vec<u16> = "processmodel.dll\0".encode_utf16().collect();
            // SAFETY: terminated system-only DLL name. The module is retained
            // for the process lifetime, keeping every published pointer valid.
            unsafe {
                let module = LoadLibraryExW(name.as_ptr(), std::ptr::null_mut(), LOAD_LIBRARY_SEARCH_SYSTEM32);
                if module.is_null() { return None; }
                let create = std::mem::transmute::<unsafe extern "system" fn() -> isize, Create>(
                    GetProcAddress(module, c"CreateProcessSecurityEnvironment".as_ptr().cast())?);
                let close = std::mem::transmute::<unsafe extern "system" fn() -> isize, Close>(
                    GetProcAddress(module, c"CloseProcessSecurityEnvironment".as_ptr().cast())?);
                let version = std::mem::transmute::<unsafe extern "system" fn() -> isize, Version>(
                    GetProcAddress(module, c"IsProcessSecurityEnvironmentVersionSupported".as_ptr().cast())?);
                let (mut supported, mut minor) = (0, 0);
                if version(1, &raw mut supported, &raw mut minor) < 0 || supported == 0 || minor < 1 { return None; }
                Some(Api { create, close })
            }
        }).as_ref().ok_or_else(|| unavailable(format!(
            "required-host-capability={REQUIRED_CAPABILITY}: native Process Security Environment 1.1 support (processmodel.dll) is required; update the Windows execution host")))
    }
}

pub(crate) struct Policy {
    environment: HANDLE,
    api: &'static Api,
    pub(super) access: ExecutionAccess,
    _roots: Arc<Grant>,
}

// SAFETY: the immutable kernel policy has no thread affinity. Arc owns its
// one close after every launch/tree retaining this policy has retired.
unsafe impl Send for Policy {}
unsafe impl Sync for Policy {}

impl std::fmt::Debug for Policy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowsPolicy")
            .field("access", &self.access)
            .finish_non_exhaustive()
    }
}

impl Policy {
    pub(super) fn new(grant: &Arc<Grant>, access: ExecutionAccess) -> io::Result<Self> {
        // PSEC's egress filter does not prohibit intra-container loopback or
        // socket creation. Private-network capability also permits inbound
        // traffic. Neither restricted contract can be represented exactly.
        if grant.network != ExecutionNetwork::On {
            return Err(unavailable(
                "Windows PSEC cannot enforce socket-free or outbound-only TCP execution grants",
            ));
        }
        let api = Api::get()?;
        for root in std::iter::once(&grant.workspace)
            .chain(grant.write.entries())
            .chain(grant.read.entries())
        {
            drop(root.bind_controlled(None)?);
        }
        let specification = specification::encode(grant, access)?;
        let size = u32::try_from(specification.len())
            .map_err(|_| refused("native PSEC specification exceeds address space"))?;
        let mut environment = std::ptr::null_mut();
        // SAFETY: the complete PSEC buffer and output remain live. Flags zero
        // leave process lifetime and termination with the existing tree owner.
        let status =
            unsafe { (api.create)(specification.as_ptr().cast(), size, 0, &raw mut environment) };
        if status < 0 {
            // FACILITY_WIN32 results carry the Win32 code naming the cause.
            let cause = if status.cast_unsigned() & 0xffff_0000 == 0x8007_0000 {
                io::Error::from_raw_os_error(status & 0xffff).to_string()
            } else {
                format!("HRESULT {status:#x}")
            };
            return Err(refused(format!(
                "Windows sandbox policy creation failed: {cause}"
            )));
        }
        if environment.is_null() {
            return Err(refused("Windows returned no security environment"));
        }
        Ok(Self {
            environment,
            api,
            access,
            _roots: Arc::clone(grant),
        })
    }

    pub(crate) fn handle(&self) -> HANDLE {
        self.environment
    }
}

impl Drop for Policy {
    fn drop(&mut self) {
        // SAFETY: this unique close retires the environment after all Arc
        // owners, including the physical process tree, have released it.
        unsafe { (self.api.close)(self.environment) };
    }
}

pub(super) struct Prepared {
    pub(super) policy: Arc<Policy>,
    _cwd: File,
}

impl Prepared {
    pub(super) fn new(
        grant: &Arc<Grant>,
        access: ExecutionAccess,
        retained: Option<&Arc<Policy>>,
        cwd: Option<&Path>,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<Self> {
        for root in std::iter::once(&grant.workspace)
            .chain(grant.write.entries())
            .chain(grant.read.entries())
        {
            drop(root.bind_controlled(meter)?);
        }
        let cwd = cwd.unwrap_or(&grant.workspace);
        let selected = grant.cwd_root(cwd)?;
        let root = selected.bind_controlled(meter)?;
        let relative = cwd.strip_prefix(&selected.path).map_err(refused)?;
        let cwd = if relative.as_os_str().is_empty() {
            root
        } else {
            open_beneath(&root, relative, true, meter)?
        };
        let policy = match retained.filter(|policy| policy.access == access) {
            Some(policy) => Arc::clone(policy),
            None => Arc::new(Policy::new(grant, access)?),
        };
        Ok(Self { policy, _cwd: cwd })
    }
}

/// Enumerates configured drive roots for the workspace-write read ceiling.
/// This queries the OS drive map once; it never enumerates directory contents.
pub fn ambient_read_roots() -> io::Result<Vec<PathBuf>> {
    use std::os::windows::ffi::OsStringExt;
    use windows_sys::Win32::Storage::FileSystem::GetLogicalDriveStringsW;
    // SAFETY: the sizing call has no output buffer.
    let required = unsafe { GetLogicalDriveStringsW(0, std::ptr::null_mut()) };
    if required == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut buffer = vec![0u16; required as usize];
    // SAFETY: sized writable buffer. A changing map refuses instead of retrying.
    let written = unsafe { GetLogicalDriveStringsW(required, buffer.as_mut_ptr()) };
    if written == 0 {
        return Err(io::Error::last_os_error());
    }
    if written >= required {
        return Err(refused("drive map changed during grant construction"));
    }
    Ok(available_read_roots(
        buffer[..written as usize]
            .split(|unit| *unit == 0)
            .filter(|part| !part.is_empty())
            .map(|part| PathBuf::from(std::ffi::OsString::from_wide(part))),
    ))
}

fn available_read_roots(paths: impl IntoIterator<Item = PathBuf>) -> Vec<PathBuf> {
    // Ambient drives are optional host reads, unlike explicit grant roots.
    // Use the same native binding to omit unavailable/locked/media-less roots.
    // Composition still strictly rebinds selected roots before granting them.
    paths
        .into_iter()
        .filter_map(|path| BoundPath::new(&path).ok().map(|root| root.path))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_paths_use_the_exact_win32_spelling() {
        let spell = |path: &str| specification::policy_path(Path::new(path));
        assert_eq!(spell(r"\\?\Q:\work\tree").unwrap(), r"Q:\work\tree");
        assert_eq!(spell(r"\\?\UNC\host\share\x").unwrap(), r"\\host\share\x");
        // Win32 normalisation would strip these, granting a different object.
        for widened in [r"\\?\C:\work\tree.", r"\\?\C:\work\tree ", r"\\?\C:\a\..\b"] {
            assert_eq!(
                spell(widened).unwrap_err().kind(),
                io::ErrorKind::PermissionDenied,
                "{widened}"
            );
        }
        assert_eq!(
            spell(r"\\?\Volume{785bdbce-c5b4-4576-8bdc-900d9a61e547}\x")
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
    }

    #[test]
    fn unavailable_ambient_roots_do_not_weaken_explicit_root_checks() {
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("unavailable");
        let available = available_read_roots([missing.clone(), root.path().to_path_buf()]);
        assert_eq!(available, [root.path().canonicalize().unwrap()]);
        assert!(
            super::super::ExecutionGrant::new(
                root.path(),
                ExecutionAccess::WorkspaceWrite,
                [missing],
                ExecutionNetwork::On,
            )
            .is_err()
        );
    }
}
