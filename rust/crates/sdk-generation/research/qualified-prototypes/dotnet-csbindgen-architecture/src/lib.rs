use std::os::raw::{c_int, c_uchar};

/// Opaque Rust-owned state. C# never receives or dereferences its layout.
#[repr(C)]
pub struct ActorTokenOpaque {
    _private: [u8; 0],
}

/// Explicit absence bit; zero is a valid `value` and is not a sentinel.
#[repr(C)]
pub struct OptionalU64 {
    pub present: c_uchar,
    pub _reserved: [u8; 7],
    pub value: u64,
}

const STATUS_OK: c_int = 0;
const STATUS_INVALID_ARGUMENT: c_int = 1;
const STATUS_NULL_OUTPUT: c_int = 2;

/// Status is the only failure channel across this toy C ABI.
#[no_mangle]
pub unsafe extern "C" fn actor_token_new(
    value: u64,
    out: *mut *mut ActorTokenOpaque,
) -> c_int {
    if out.is_null() {
        return STATUS_NULL_OUTPUT;
    }
    if value == 0 {
        *out = std::ptr::null_mut();
        return STATUS_INVALID_ARGUMENT;
    }

    let storage = Box::new(value);
    *out = Box::into_raw(storage).cast();
    STATUS_OK
}

/// The Rust side owns release semantics; the managed side only forwards once
/// through `SafeHandle.ReleaseHandle`.
#[no_mangle]
pub unsafe extern "C" fn actor_token_release(handle: *mut ActorTokenOpaque) {
    if !handle.is_null() {
        drop(Box::from_raw(handle.cast::<u64>()));
    }
}

#[no_mangle]
pub unsafe extern "C" fn actor_token_value(
    handle: *const ActorTokenOpaque,
    out: *mut u64,
) -> c_int {
    if handle.is_null() || out.is_null() {
        return STATUS_NULL_OUTPUT;
    }
    *out = *handle.cast::<u64>();
    STATUS_OK
}

#[no_mangle]
pub unsafe extern "C" fn actor_token_optional_value(
    handle: *const ActorTokenOpaque,
    out: *mut OptionalU64,
) -> c_int {
    if handle.is_null() || out.is_null() {
        return STATUS_NULL_OUTPUT;
    }
    *out = OptionalU64 {
        present: 1,
        _reserved: [0; 7],
        value: *handle.cast::<u64>(),
    };
    STATUS_OK
}
