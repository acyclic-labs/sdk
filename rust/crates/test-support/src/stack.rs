//! Deepest stack use of a callback on its calling thread.
//!
//! The thread's stack is painted with a pattern below the callback's first
//! frame; afterwards the deepest overwritten byte is the most stack any
//! frame of the callback, or anything it polled, ever used. The result is
//! the real frame depth on the target being built, including every poll
//! frame of the futures a callback drives, which a type size cannot show.

/// Bytes painted below the callback's first frame: the largest measurable use.
pub const PAINTED: usize = 512 << 10;
const PATTERN: u8 = 0xA5;

#[inline(never)]
fn paint<const BYTES: usize>() -> usize {
    let mut region = [PATTERN; BYTES];
    std::hint::black_box(&mut region);
    region.as_ptr() as usize
}

/// Runs `callback` on the calling thread and returns its result with the deepest
/// stack it used, in bytes. Exhausting the painted region returns `usize::MAX`,
/// so saturation cannot pass a budget equal to [`PAINTED`].
pub fn deepest_stack<T>(callback: impl FnOnce() -> T) -> (T, usize) {
    measure::<PAINTED, _>(callback)
}

#[inline(never)]
#[forbid(unsafe_code)]
fn measure<const BYTES: usize, T>(callback: impl FnOnce() -> T) -> (T, usize) {
    // Allocate the destination before painting: neither allocation nor scanning
    // should contribute to the callback's high-water mark.
    let mut snapshot = vec![0; BYTES];
    let lowest = paint::<BYTES>();
    let result = callback();
    // `lowest` names mapped stack memory, but paint's Rust allocation is dead.
    // Only the OS may read it; constructing/dereferencing a Rust pointer to that
    // expired allocation is undefined behaviour, even with read_volatile.
    snapshot_stack(lowest, &mut snapshot);
    let untouched = snapshot.iter().take_while(|byte| **byte == PATTERN).count();
    (
        result,
        if untouched == 0 {
            usize::MAX
        } else {
            BYTES - untouched
        },
    )
}

#[cfg(target_os = "linux")]
fn snapshot_stack(address: usize, output: &mut [u8]) {
    use std::os::unix::fs::FileExt as _;
    std::fs::File::open("/proc/self/mem")
        .unwrap()
        .read_exact_at(output, address as u64)
        .unwrap();
}

#[cfg(windows)]
fn snapshot_stack(address: usize, output: &mut [u8]) {
    use std::ffi::c_void;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn ReadProcessMemory(
            process: *mut c_void,
            address: *const c_void,
            output: *mut c_void,
            length: usize,
            read: *mut usize,
        ) -> i32;
    }
    let mut read = 0;
    // SAFETY: the OS interprets the source address; output and read are live,
    // writable allocations and the requested length is output's exact extent.
    let success = unsafe {
        ReadProcessMemory(
            GetCurrentProcess(),
            address as *const c_void,
            output.as_mut_ptr().cast(),
            output.len(),
            &mut read,
        )
    };
    assert_ne!(success, 0, "{}", std::io::Error::last_os_error());
    assert_eq!(read, output.len());
}

#[cfg(target_os = "macos")]
fn snapshot_stack(address: usize, output: &mut [u8]) {
    unsafe extern "C" {
        static mach_task_self_: u32;
        fn mach_vm_read_overwrite(
            task: u32,
            address: u64,
            size: u64,
            data: u64,
            read: *mut u64,
        ) -> i32;
    }
    let mut read = 0;
    // SAFETY: the kernel copies from this task's mapped stack into the live
    // output allocation; it never dereferences an expired Rust allocation.
    let status = unsafe {
        mach_vm_read_overwrite(
            mach_task_self_,
            address as u64,
            output.len() as u64,
            output.as_mut_ptr() as u64,
            &mut read,
        )
    };
    assert_eq!(status, 0, "mach_vm_read_overwrite failed");
    assert_eq!(read, output.len() as u64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[inline(never)]
    fn hold<const BYTES: usize>() -> u8 {
        let mut frame = [1_u8; BYTES];
        std::hint::black_box(&mut frame);
        frame[BYTES / 2]
    }

    #[test]
    fn a_held_frame_is_measured_and_an_empty_callback_is_not() {
        let ((), empty) = deepest_stack(|| ());
        let (_, held) = deepest_stack(hold::<{ 256 << 10 }>);
        assert!(empty < 16 << 10, "an empty callback used {empty} bytes");
        assert!(held >= 256 << 10, "a 256 KiB frame measured {held} bytes");
        assert!(held < 320 << 10, "a 256 KiB frame measured {held} bytes");
    }

    #[test]
    fn exhausting_the_snapshot_cannot_pass_the_budget() {
        let (_, used) = measure::<{ 64 << 10 }, _>(hold::<{ 128 << 10 }>);
        assert_eq!(used, usize::MAX, "an exhausted oracle must fail closed");
    }
}
