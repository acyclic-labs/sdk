//! Proof of the actual constant callback bodies, independent of Windows ABI layout.
#![cfg(kani)]

// These values are never inspected. Opaque argument aliases let Kani cover
// the production bodies on Linux; the MSVC property covers the real ABI entry.
use std::ffi::c_void as GUID;
use std::ffi::c_void as PRJ_CALLBACK_DATA;
use std::primitive::i32 as HRESULT;
use std::primitive::usize as PCWSTR;
use std::primitive::usize as PRJ_DIR_ENTRY_BUFFER_HANDLE;

const HR_OK: HRESULT = 0;
const HR_FILE_NOT_FOUND: HRESULT = 0x8007_0002_u32.cast_signed();
const HR_UNEXPECTED: HRESULT = 0x8000_ffff_u32.cast_signed();

#[path = "../src/native_mount/projfs/constant_callbacks.rs"]
mod constant_callbacks;

#[kani::proof]
fn all_callback_arguments_are_ignored_and_each_status_is_constant() {
    let data = kani::any::<usize>() as *const PRJ_CALLBACK_DATA;
    let enumeration = kani::any::<usize>() as *const GUID;
    let expression: PCWSTR = kani::any();
    let buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE = kani::any();
    let offset: u64 = kani::any();
    let length: u32 = kani::any();
    // SAFETY: the production bodies do not dereference any arguments.
    unsafe {
        assert_eq!(constant_callbacks::end_directory(data, enumeration), HR_OK);
        assert_eq!(
            constant_callbacks::get_directory(data, enumeration, expression, buffer),
            HR_OK
        );
        assert_eq!(constant_callbacks::placeholder(data), HR_FILE_NOT_FOUND);
        assert_eq!(
            constant_callbacks::file_data(data, offset, length),
            HR_UNEXPECTED
        );
    }
}
