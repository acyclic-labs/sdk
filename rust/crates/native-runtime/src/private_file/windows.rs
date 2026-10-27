use std::{
    fs::File,
    io,
    os::windows::{
        ffi::OsStrExt as _,
        io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle},
    },
    path::Path,
};
use windows::{
    Win32::{
        Foundation::*,
        Security::*,
        Storage::FileSystem::*,
        System::Threading::{GetCurrentProcess, OpenProcessToken},
    },
    core::{BOOL, PCWSTR},
};

/// The OS user's profile, independent of process environment overrides.
#[cfg(not(any(test, feature = "test-support")))]
pub(super) fn user_data_directory() -> io::Result<std::path::PathBuf> {
    use std::os::windows::ffi::OsStringExt as _;
    use windows::Win32::{
        System::Com::CoTaskMemFree,
        UI::Shell::{FOLDERID_LocalAppData, KF_FLAG_DEFAULT, SHGetKnownFolderPath},
    };
    // SAFETY: the shell allocates this terminated path for the current token;
    // copy its exact UTF-16 units, then release it with the required allocator.
    let path = unsafe { SHGetKnownFolderPath(&FOLDERID_LocalAppData, KF_FLAG_DEFAULT, None) }
        .map_err(io::Error::other)?;
    let owned = unsafe { std::ffi::OsString::from_wide(path.as_wide()) };
    unsafe { CoTaskMemFree(Some(path.0.cast())) };
    Ok(owned.into())
}

// windows::core wraps Win32 status in HRESULT, whereas std::io classifies the
// underlying Win32 code. Normalize exactly that facility at this OS boundary.
pub(super) fn io_error(error: io::Error) -> io::Error {
    match error.raw_os_error().map(i32::cast_unsigned) {
        Some(code) if code & 0xffff_0000 == 0x8007_0000 => {
            io::Error::from_raw_os_error((code & 0xffff).cast_signed())
        }
        _ => error,
    }
}

fn user(process: HANDLE, buffer: &mut [usize; 16]) -> windows::core::Result<PSID> {
    let mut length = 0;
    // SAFETY: buffer is aligned and larger than TOKEN_USER plus SECURITY_MAX_SID_SIZE.
    unsafe {
        let mut handle = HANDLE::default();
        OpenProcessToken(process, TOKEN_QUERY, &mut handle)?;
        let handle = OwnedHandle::from_raw_handle(handle.0);
        GetTokenInformation(
            HANDLE(handle.as_raw_handle()),
            TokenUser,
            Some(buffer.as_mut_ptr().cast()),
            u32::try_from(std::mem::size_of_val(buffer))
                .map_err(|_| windows::core::Error::from(E_INVALIDARG))?,
            &mut length,
        )?;
        Ok((*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid)
    }
}

/// Creates a new file with a protected DACL granting only its current user access.
pub(super) fn create(path: &Path) -> io::Result<File> {
    let name: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    if name.strip_suffix(&[0]).is_none_or(|path| path.contains(&0)) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NUL in credential path",
        ));
    }
    with_security(|attributes| {
        // SAFETY: the encoded path and protected security descriptor live through the call.
        let handle = unsafe {
            CreateFileW(
                PCWSTR(name.as_ptr()),
                GENERIC_READ.0 | GENERIC_WRITE.0,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                Some(attributes),
                CREATE_NEW,
                FILE_ATTRIBUTE_NORMAL,
                None,
            )?
        };
        // SAFETY: the new handle is transferred exactly once to File.
        Ok(unsafe { File::from_raw_handle(handle.0) })
    })
}

pub(super) fn create_directory(path: &Path) -> io::Result<()> {
    let name: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    if name.strip_suffix(&[0]).is_none_or(|path| path.contains(&0)) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    with_security(|attributes| {
        // SAFETY: encoded path and descriptor remain live through CreateDirectoryW.
        unsafe {
            CreateDirectoryW(PCWSTR(name.as_ptr()), Some(attributes))?;
        }
        Ok(())
    })
}

fn with_security<T>(action: impl FnOnce(&SECURITY_ATTRIBUTES) -> io::Result<T>) -> io::Result<T> {
    let mut token = [0usize; 16];
    // SAFETY: GetCurrentProcess returns a borrowed pseudo-handle with no preconditions.
    let owner = user(unsafe { GetCurrentProcess() }, &mut token)?;
    let mut acl = [0usize; 32];
    let acl_pointer = acl.as_mut_ptr().cast::<ACL>();
    let mut descriptor = SECURITY_DESCRIPTOR::default();
    let security = PSECURITY_DESCRIPTOR((&mut descriptor as *mut SECURITY_DESCRIPTOR).cast());
    // SAFETY: SID, ACL and descriptor are aligned, initialized and live until CreateFileW returns.
    unsafe {
        InitializeAcl(
            acl_pointer,
            u32::try_from(std::mem::size_of_val(&acl)).map_err(io::Error::other)?,
            ACL_REVISION,
        )?;
        AddAccessAllowedAce(acl_pointer, ACL_REVISION, FILE_ALL_ACCESS.0, owner)?;
        InitializeSecurityDescriptor(security, 1)?;
        SetSecurityDescriptorOwner(security, Some(owner), false)?;
        SetSecurityDescriptorDacl(security, true, Some(acl_pointer), false)?;
        SetSecurityDescriptorControl(security, SE_DACL_PROTECTED, SE_DACL_PROTECTED)?;
        let attributes = SECURITY_ATTRIBUTES {
            nLength: u32::try_from(std::mem::size_of::<SECURITY_ATTRIBUTES>())
                .map_err(io::Error::other)?,
            lpSecurityDescriptor: security.0,
            bInheritHandle: false.into(),
        };
        action(&attributes)
    }
}

/// The security identifier of a process owner, obtained through the same token
/// query used to build private file and directory descriptors.
pub(super) fn process_user_id(process: *mut std::ffi::c_void) -> io::Result<Vec<u8>> {
    let mut buffer = [0usize; 16];
    let sid = user(HANDLE(process), &mut buffer)?;
    // SAFETY: user returned a SID inside the retained aligned buffer.
    let length = unsafe { GetLengthSid(sid) } as usize;
    // SAFETY: GetLengthSid describes the valid SID retained inside buffer above.
    Ok(unsafe { std::slice::from_raw_parts(sid.0.cast::<u8>(), length) }.to_vec())
}

/// Checks a file has a protected DACL granting full access only to its current user.
pub(super) fn is_private(file: &File) -> io::Result<bool> {
    check_security(file, false)
}

/// Tighten a trusted legacy descriptor only while exclusively holding the
/// original, unaliased inode owned by this process user. No bytes are read first.
pub(super) fn open_for_upgrade(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt as _;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .access_mode(GENERIC_READ.0 | READ_CONTROL.0 | WRITE_DAC.0)
        .share_mode(0)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT.0)
        .open(path)?;
    if !file.metadata()?.is_file() || !check_security(&file, true)? || !is_private(&file)? {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    Ok(file)
}

fn tighten(file: &File) -> io::Result<()> {
    use windows::Win32::Security::Authorization::{SE_FILE_OBJECT, SetSecurityInfo};
    with_security(|attributes| {
        let mut present = BOOL::default();
        let mut defaulted = BOOL::default();
        let mut acl = std::ptr::null_mut();
        // SAFETY: with_security retains the descriptor and ACL for this closure;
        // the caller retains a valid handle opened with WRITE_DAC.
        unsafe {
            GetSecurityDescriptorDacl(
                PSECURITY_DESCRIPTOR(attributes.lpSecurityDescriptor),
                &mut present,
                &mut acl,
                &mut defaulted,
            )?;
            SetSecurityInfo(
                HANDLE(file.as_raw_handle()),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                None,
                None,
                Some(acl),
                None,
            )
            .ok()?;
        }
        Ok(())
    })
}

fn check_security(file: &File, upgrade_legacy: bool) -> io::Result<bool> {
    use windows::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
    let mut information = BY_HANDLE_FILE_INFORMATION::default();
    // SAFETY: the held file handle and initialized output are live for this call.
    unsafe {
        GetFileInformationByHandle(HANDLE(file.as_raw_handle()), &mut information)?;
    }
    if information.nNumberOfLinks != 1
        || information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
    {
        return Ok(false);
    }
    let mut descriptor = PSECURITY_DESCRIPTOR::default();
    let mut acl = std::ptr::null_mut();
    let mut owner = PSID::default();
    // SAFETY: valid file handle and output pointers. GetSecurityInfo owns the returned descriptor.
    unsafe {
        GetSecurityInfo(
            HANDLE(file.as_raw_handle()),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | OWNER_SECURITY_INFORMATION,
            Some(&mut owner),
            None,
            Some(&mut acl),
            None,
            Some(&mut descriptor),
        )
        .ok()?;
        let check = (|| -> io::Result<bool> {
            let mut token = [0usize; 16];
            let current = user(GetCurrentProcess(), &mut token)?;
            let mut control = 0;
            let mut revision = 0;
            GetSecurityDescriptorControl(descriptor, &mut control, &mut revision)?;
            if !EqualSid(owner, current).is_ok() {
                return Ok(false);
            }
            if control & SE_DACL_PROTECTED.0 == 0 && upgrade_legacy {
                // Neither inherited nor explicit grants establish trust. The
                // legacy bytes must already exclude other users: the current
                // owner, SYSTEM and local administrators are the only trusted
                // readers. A missing/empty ACL or an unknown ACE fails closed.
                if acl.is_null() || (*acl).AceCount == 0 {
                    return Ok(false);
                }
                for index in 0..u32::from((*acl).AceCount) {
                    let mut ace = std::ptr::null_mut();
                    GetAce(acl, index, &mut ace)?;
                    if (*ace.cast::<ACE_HEADER>()).AceType != 0 {
                        return Ok(false);
                    }
                    let allowed = &*ace.cast::<ACCESS_ALLOWED_ACE>();
                    let sid = PSID((&allowed.SidStart as *const u32).cast_mut().cast());
                    if !(EqualSid(sid, current).is_ok()
                        || IsWellKnownSid(sid, WinLocalSystemSid).as_bool()
                        || IsWellKnownSid(sid, WinBuiltinAdministratorsSid).as_bool())
                    {
                        return Ok(false);
                    }
                }
                // The existing owner is authenticated before permissions change.
                // No publicly readable legacy bytes are silently adopted.
                tighten(file)?;
                return Ok(true);
            }
            if control & SE_DACL_PROTECTED.0 == 0 || acl.is_null() || (*acl).AceCount != 1 {
                return Ok(false);
            }
            let mut ace = std::ptr::null_mut();
            GetAce(acl, 0, &mut ace)?;
            let ace = &*ace.cast::<ACCESS_ALLOWED_ACE>();
            Ok(ace.Header.AceType == 0
                && ace.Header.AceFlags == 0
                && ace.Mask == FILE_ALL_ACCESS.0
                && EqualSid(
                    PSID((&ace.SidStart as *const u32).cast_mut().cast()),
                    current,
                )
                .is_ok())
        })();
        let _ = LocalFree(Some(HLOCAL(descriptor.0)));
        check
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows::Win32::Security::Authorization::{
        GetSecurityInfo, SE_FILE_OBJECT, SetSecurityInfo,
    };

    fn install_untrusted_acl(file: &File, kind: &str) -> io::Result<()> {
        let mut acl = [0usize; 32];
        let acl_pointer = acl.as_mut_ptr().cast::<ACL>();
        let mut world = [0usize; 16];
        let mut world_length =
            u32::try_from(std::mem::size_of_val(&world)).map_err(io::Error::other)?;
        let mut descriptor = PSECURITY_DESCRIPTOR::default();
        // SAFETY: aligned fixture buffers and the held file outlive every
        // security call. The returned descriptor is released below.
        unsafe {
            InitializeAcl(
                acl_pointer,
                u32::try_from(std::mem::size_of_val(&acl)).map_err(io::Error::other)?,
                ACL_REVISION,
            )?;
            if kind.ends_with("public") {
                let sid = PSID(world.as_mut_ptr().cast());
                CreateWellKnownSid(WinWorldSid, None, Some(sid), &mut world_length)?;
                AddAccessAllowedAceEx(
                    acl_pointer,
                    ACL_REVISION,
                    if kind == "inherited-public" {
                        INHERITED_ACE
                    } else {
                        ACE_FLAGS(0)
                    },
                    FILE_ALL_ACCESS.0,
                    sid,
                )?;
            }
            SetSecurityInfo(
                HANDLE(file.as_raw_handle()),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                None,
                None,
                Some(if kind == "missing" {
                    std::ptr::null_mut()
                } else {
                    acl_pointer
                }),
                None,
            )
            .ok()
            .map_err(|cause| io::Error::other(format!("{kind}: fixture ACL: {cause}")))?;
            let mut actual_acl = std::ptr::null_mut();
            GetSecurityInfo(
                HANDLE(file.as_raw_handle()),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                None,
                None,
                Some(&mut actual_acl),
                None,
                Some(&mut descriptor),
            )
            .ok()
            .map_err(|cause| io::Error::other(format!("{kind}: fixture inspection: {cause}")))?;
            let mut control = 0;
            let mut revision = 0;
            let result = GetSecurityDescriptorControl(descriptor, &mut control, &mut revision);
            if kind == "inherited-public" {
                assert!(!actual_acl.is_null());
                let mut ace = std::ptr::null_mut();
                GetAce(actual_acl, 0, &mut ace)?;
                assert_ne!(
                    u32::from((*ace.cast::<ACE_HEADER>()).AceFlags) & INHERITED_ACE.0,
                    0
                );
            }
            let _ = LocalFree(Some(HLOCAL(descriptor.0)));
            result?;
            if kind == "missing" {
                // Windows protects a null DACL when installing it. Its
                // absent grant list must still fail the strict reader.
                assert!(actual_acl.is_null());
            } else {
                // Negative control: the former upgrade guard accepted
                // unprotected public and empty ACLs without checking grants.
                assert_eq!(control & SE_DACL_PROTECTED.0, 0, "{kind}");
            }
        }
        Ok(())
    }

    #[test]
    fn owner_upgrade_rejects_explicit_shared_empty_and_missing_access_lists_before_decode()
    -> io::Result<()> {
        for kind in ["explicit-public", "inherited-public", "empty", "missing"] {
            let root = tempfile::tempdir()?;
            let path = root.path().join("owner");
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .access_mode(GENERIC_READ.0 | GENERIC_WRITE.0 | READ_CONTROL.0 | WRITE_DAC.0)
                .open(&path)
                .map_err(|cause| io::Error::other(format!("{kind}: fixture creation: {cause}")))?;
            file.write_all(b"untrusted owner")?;
            install_untrusted_acl(&file, kind)?;
            drop(file);
            assert!(
                crate::private_file::validate_destination_for_upgrade(&path).is_err(),
                "untrusted {kind} ACL reached credential admission"
            );
            assert!(
                crate::private_file::read_for_upgrade_with::<(), io::Error>(
                    &path,
                    100,
                    |_, _| panic!("untrusted {kind} ACL reached owner decoding"),
                )
                .is_err(),
                "{kind}"
            );
            // An empty ACL also denies ordinary deletion. Restore this owned
            // fixture through the same ACL setter before temporary cleanup.
            let restore = std::fs::OpenOptions::new()
                .read(true)
                .access_mode(READ_CONTROL.0 | WRITE_DAC.0)
                .open(&path)
                .map_err(|cause| io::Error::other(format!("{kind}: cleanup handle: {cause}")))?;
            tighten(&restore)
                .map_err(|cause| io::Error::other(format!("{kind}: cleanup ACL: {cause}")))?;
        }
        Ok(())
    }
}
