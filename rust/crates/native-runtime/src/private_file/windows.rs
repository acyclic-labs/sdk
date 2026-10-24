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
    core::PCWSTR,
};

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
        let check = (|| -> windows::core::Result<bool> {
            let mut token = [0usize; 16];
            let current = user(GetCurrentProcess(), &mut token)?;
            let mut control = 0;
            let mut revision = 0;
            GetSecurityDescriptorControl(descriptor, &mut control, &mut revision)?;
            if control & SE_DACL_PROTECTED.0 == 0
                || acl.is_null()
                || (*acl).AceCount != 1
                || !EqualSid(owner, current).is_ok()
            {
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
        Ok(check?)
    }
}
