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

fn user(buffer: &mut [usize; 16]) -> windows::core::Result<PSID> {
    let mut length = 0;
    // SAFETY: buffer is aligned and larger than TOKEN_USER plus SECURITY_MAX_SID_SIZE.
    unsafe {
        let mut handle = HANDLE::default();
        OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut handle)?;
        let handle = OwnedHandle::from_raw_handle(handle.0);
        GetTokenInformation(
            HANDLE(handle.as_raw_handle()),
            TokenUser,
            Some(buffer.as_mut_ptr().cast()),
            std::mem::size_of_val(buffer) as u32,
            &mut length,
        )?;
        Ok((*buffer.as_ptr().cast::<TOKEN_USER>()).User.Sid)
    }
}

/// Creates a new file with a protected DACL granting only its current user access.
pub fn create_owner_only_file(path: &Path) -> io::Result<File> {
    let name: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    if name[..name.len() - 1].contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NUL in credential path",
        ));
    }
    let mut token = [0usize; 16];
    let owner = user(&mut token)?;
    let mut acl = [0usize; 32];
    let acl_pointer = acl.as_mut_ptr().cast::<ACL>();
    let mut descriptor = SECURITY_DESCRIPTOR::default();
    let security = PSECURITY_DESCRIPTOR((&mut descriptor as *mut SECURITY_DESCRIPTOR).cast());
    // SAFETY: SID, ACL and descriptor are aligned, initialized and live until CreateFileW returns.
    unsafe {
        InitializeAcl(
            acl_pointer,
            std::mem::size_of_val(&acl) as u32,
            ACL_REVISION,
        )?;
        AddAccessAllowedAce(acl_pointer, ACL_REVISION, FILE_ALL_ACCESS.0, owner)?;
        InitializeSecurityDescriptor(security, 1)?;
        SetSecurityDescriptorOwner(security, Some(owner), false)?;
        SetSecurityDescriptorDacl(security, true, Some(acl_pointer), false)?;
        SetSecurityDescriptorControl(security, SE_DACL_PROTECTED, SE_DACL_PROTECTED)?;
        let attributes = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: security.0,
            bInheritHandle: false.into(),
        };
        let handle = CreateFileW(
            PCWSTR(name.as_ptr()),
            GENERIC_READ.0 | GENERIC_WRITE.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            Some(&attributes),
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL,
            None,
        )?;
        // SAFETY: the new handle is transferred exactly once to File.
        Ok(File::from_raw_handle(handle.0))
    }
}

/// Checks a file has a protected DACL granting full access only to its current user.
pub fn has_owner_only_permissions(file: &File) -> io::Result<bool> {
    use windows::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
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
            let current = user(&mut token)?;
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
