//! A mount-free kernel thread for the packet host's blocking notifier.
//! No libc state, TLS, owner callbacks or per-device thread is involved.

use super::*;
use core::sync::atomic::{AtomicI32, Ordering};

const STACK_BYTES: usize = 4096;
const STORAGE_BYTES: usize = 4096 + STACK_BYTES;
const FLAGS: usize = 0x0100 | 0x0200 | 0x0400 | 0x0800 | 0x10000 | 0x40000 | 0x200000 | 0x1000000;

#[cfg(target_arch = "x86_64")]
core::arch::global_asm!(
    ".globl acyclic_host_clone_thread",
    "acyclic_host_clone_thread:",
    "mov r10, rdx",
    "xor edx, edx",
    "xor r8d, r8d",
    "mov eax, 56",
    "syscall",
    "test rax, rax",
    "jnz 2f",
    "pop rdi",
    "pop rax",
    "call rax",
    "ud2",
    "2: ret",
);

#[cfg(target_arch = "aarch64")]
core::arch::global_asm!(
    ".globl acyclic_host_clone_thread",
    "acyclic_host_clone_thread:",
    "mov x4, x2",
    "mov x2, xzr",
    "mov x3, xzr",
    "mov x8, 220",
    "svc 0",
    "cbnz x0, 2f",
    "ldp x0, x16, [sp]",
    "blr x16",
    "brk #0",
    "2: ret",
);

unsafe extern "C" {
    fn acyclic_host_clone_thread(flags: usize, stack: *mut usize, tid: *mut i32) -> isize;
}

pub(crate) struct Thread {
    storage: *mut u8,
}

impl Thread {
    /// The argument and everything the entry borrows must remain live until
    /// this owner joins. The entry must terminate with `exit_thread`.
    pub(crate) unsafe fn spawn(
        entry: extern "C" fn(usize) -> !,
        argument: usize,
    ) -> Result<Self, Errno> {
        let storage = map(STORAGE_BYTES)?;
        // SAFETY: the mapping is exclusively owned and page-aligned. The
        // first page retains the kernel's clear-child-tid word; the second
        // owns the notifier's stack through its join receipt.
        let tid = unsafe {
            let tid = storage.cast::<AtomicI32>();
            tid.write(AtomicI32::new(1));
            &*tid
        };
        // SAFETY: the final aligned words of the fresh stack are the entry
        // trampoline's argument/function pair, never a parent return frame.
        let stack = unsafe {
            let stack = storage
                .add(STORAGE_BYTES - 2 * size_of::<usize>())
                .cast::<usize>();
            stack.write(argument);
            stack.add(1).write(entry as usize);
            stack
        };
        // SAFETY: the private trampoline switches stack only in the child;
        // CHILD_SETTID/CLEARTID retain the original join word until exit.
        let result = unsafe { acyclic_host_clone_thread(FLAGS, stack, tid.as_ptr()) };
        if result < 0 {
            // SAFETY: no thread was created, so the whole mapping is private.
            unsafe { unmap(storage, STORAGE_BYTES) };
            return Err(Errno(i32::try_from(-result).unwrap_or(EINVAL)));
        }
        Ok(Self { storage })
    }
}

impl Drop for Thread {
    fn drop(&mut self) {
        // SAFETY: this owner's mapping retains the original kernel join word.
        let tid = unsafe { &*self.storage.cast::<AtomicI32>() };
        loop {
            let observed = tid.load(Ordering::Acquire);
            if observed == 0 {
                break;
            }
            #[cfg(target_arch = "x86_64")]
            const FUTEX: usize = 202;
            #[cfg(target_arch = "aarch64")]
            const FUTEX: usize = 98;
            // SAFETY: FUTEX_WAIT joins the original clear-child-tid event.
            // It uses the shared operation, matching the kernel's exit wake.
            match unsafe { call(FUTEX, [tid.as_ptr().addr(), 0, observed as usize, 0, 0, 0]) } {
                Ok(_) | Err(Errno(EINTR | EAGAIN)) => {}
                Err(_) => panic!("packet notifier thread join failed"),
            }
        }
        // SAFETY: the kernel cleared the original tid after the thread's last
        // userspace access; neither its stack nor its argument can be borrowed.
        unsafe { unmap(self.storage, STORAGE_BYTES) };
    }
}

pub(crate) fn exit_thread() -> ! {
    #[cfg(target_arch = "x86_64")]
    const EXIT: usize = 60;
    #[cfg(target_arch = "aarch64")]
    const EXIT: usize = 93;
    // SAFETY: ends this one kernel thread, preserving the packet host.
    let _ = unsafe { call(EXIT, [0; 6]) };
    unreachable!("thread exit returned")
}
