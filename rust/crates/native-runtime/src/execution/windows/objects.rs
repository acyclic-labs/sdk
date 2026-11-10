//! Root-relative object admission and inherited-stream fencing.
use super::*;
use std::os::windows::{
    ffi::OsStrExt,
    fs::MetadataExt,
    io::{AsRawHandle, FromRawHandle},
};
use windows_sys::{
    Wdk::{
        Foundation::{NtQueryObject, OBJECT_ATTRIBUTES},
        Storage::FileSystem::{
            FILE_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT,
            NtCreateFile,
        },
    },
    Win32::{
        Foundation::{RtlNtStatusToDosError, UNICODE_STRING},
        Storage::FileSystem::{
            FILE_ATTRIBUTE_REPARSE_POINT, FILE_READ_ATTRIBUTES, FILE_SHARE_READ, FILE_SHARE_WRITE,
            FILE_TRAVERSE, SYNCHRONIZE,
        },
        System::IO::IO_STATUS_BLOCK,
    },
};

pub(in crate::execution) fn open_beneath(
    root: &File,
    relative: &Path,
    directory: bool,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<File> {
    let mut parent = None::<File>;
    let mut components = relative.components().peekable();
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(refused("execution path is not a rooted descendant"));
        };
        let mut name: Vec<u16> = name.encode_wide().collect();
        if name.contains(&0) {
            return Err(refused("execution path contains NUL"));
        }
        let bytes = name
            .len()
            .checked_mul(2)
            .and_then(|size| u16::try_from(size).ok())
            .ok_or_else(|| refused("native path component exceeds address space"))?;
        let name = UNICODE_STRING {
            Length: bytes,
            MaximumLength: bytes,
            Buffer: name.as_mut_ptr(),
        };
        let attributes = OBJECT_ATTRIBUTES {
            Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
            RootDirectory: parent.as_ref().unwrap_or(root).as_raw_handle(),
            ObjectName: &raw const name,
            Attributes: 0x40,
            SecurityDescriptor: std::ptr::null(),
            SecurityQualityOfService: std::ptr::null(),
        };
        let options = FILE_OPEN_REPARSE_POINT
            | FILE_SYNCHRONOUS_IO_NONALERT
            | if directory || components.peek().is_some() {
                FILE_DIRECTORY_FILE
            } else {
                0
            };
        let traverse = if directory || components.peek().is_some() {
            FILE_TRAVERSE
        } else {
            0
        };
        let child =
            crate::meter::native_control_attempt(meter, crate::NativeOperationKind::Read, || {
                let mut handle = std::ptr::null_mut();
                let mut result = IO_STATUS_BLOCK::default();
                // SAFETY: a single relative UTF-16 component, original parent
                // handle and output storage remain live. Reparse points are opened
                // themselves, never followed; no delete sharing permits renaming.
                let status = unsafe {
                    NtCreateFile(
                        &raw mut handle,
                        FILE_READ_ATTRIBUTES | SYNCHRONIZE | traverse,
                        &raw const attributes,
                        &raw mut result,
                        std::ptr::null(),
                        0,
                        FILE_SHARE_READ | FILE_SHARE_WRITE,
                        FILE_OPEN,
                        options,
                        std::ptr::null(),
                        0,
                    )
                };
                if status < 0 {
                    return Err(nt_error(status));
                }
                // SAFETY: the successful call transferred one owned handle.
                Ok(unsafe { File::from_raw_handle(handle) })
            })?;
        let metadata =
            crate::meter::native_control_attempt(meter, crate::NativeOperationKind::Read, || {
                child.metadata()
            })?;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(refused("execution path traverses a reparse point"));
        }
        parent = Some(child);
    }
    parent.ok_or_else(|| refused("execution path has no descendant component"))
}

pub(in crate::execution) fn validate_stdio(handles: &[HANDLE]) -> io::Result<()> {
    for handle in handles {
        let mut needed = 0;
        // SAFETY: the sizing call has no buffer, only its required-size output.
        unsafe { NtQueryObject(*handle, 1, std::ptr::null_mut(), 0, &raw mut needed) };
        if needed < size_of::<UNICODE_STRING>() as u32 {
            return Err(refused("cannot establish confined stream identity"));
        }
        let mut buffer = vec![0usize; (needed as usize).div_ceil(size_of::<usize>())];
        // SAFETY: aligned native output storage of the exact required size.
        let status = unsafe {
            NtQueryObject(
                *handle,
                1,
                buffer.as_mut_ptr().cast(),
                needed,
                &raw mut needed,
            )
        };
        if status < 0 {
            return Err(nt_error(status));
        }
        // SAFETY: successful ObjectNameInformation begins with UNICODE_STRING.
        let name = unsafe { &*buffer.as_ptr().cast::<UNICODE_STRING>() };
        let start = name.Buffer as usize;
        let end = start
            .checked_add(usize::from(name.Length))
            .ok_or_else(|| refused("invalid stream identity"))?;
        let base = buffer.as_ptr() as usize;
        if name.Length % 2 != 0 || start < base || end > base + buffer.len() * size_of::<usize>() {
            return Err(refused("invalid stream identity"));
        }
        // SAFETY: the checked native name is wholly within the live output.
        let units =
            unsafe { std::slice::from_raw_parts(name.Buffer, usize::from(name.Length) / 2) };
        let name = String::from_utf16(units).map_err(|_| refused("invalid stream identity"))?;
        if name != "\\Device\\Null" && !name.starts_with("\\Device\\NamedPipe\\") {
            return Err(refused(
                "confined standard streams must be pipes or the null device",
            ));
        }
    }
    Ok(())
}

fn nt_error(status: i32) -> io::Error {
    // SAFETY: the pure system mapping accepts any NTSTATUS value.
    io::Error::from_raw_os_error(unsafe { RtlNtStatusToDosError(status) } as i32)
}
