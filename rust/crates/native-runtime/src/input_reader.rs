//! Cancellable Unix reads, with cancellation confined to a native C stack.
use crate::input_batch::InputBatch;
use std::ffi::{c_int, c_void};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;

type Publish = Box<dyn FnMut(io::Result<InputBatch>) -> bool + Send>;

struct Context {
    file: File,
    acquire: Box<dyn FnMut() -> Option<InputBatch> + Send>,
    publish: Publish,
    finish: Box<dyn FnMut() + Send>,
    pending: Option<InputBatch>,
}

#[repr(C)]
struct Ops {
    fd: c_int,
    context: *mut c_void,
    acquire: extern "C" fn(*mut c_void, *mut usize) -> *mut u8,
    publish: extern "C" fn(*mut c_void, isize, c_int) -> c_int,
    finish: extern "C" fn(*mut c_void),
}

unsafe extern "C" {
    fn acyclic_input_start(ops: *const Ops, handle: *mut usize) -> c_int;
    fn acyclic_input_stop(handle: usize) -> c_int;
}

extern "C" fn acquire(context: *mut c_void, length: *mut usize) -> *mut u8 {
    // SAFETY: the C worker is the sole accessor until stop joins it; Context
    // stays boxed at this address throughout that interval. Cancellation is
    // disabled in every callback. `length` points into the worker's C stack.
    let context = unsafe { &mut *context.cast::<Context>() };
    context.pending = (context.acquire)();
    match &mut context.pending {
        Some(bytes) => {
            let bytes = bytes.storage_mut();
            // SAFETY: the worker supplies its live size_t output parameter.
            unsafe { *length = bytes.len() };
            bytes.as_mut_ptr()
        }
        None => std::ptr::null_mut(),
    }
}

extern "C" fn publish(context: *mut c_void, count: isize, error: c_int) -> c_int {
    // SAFETY: same exclusive callback ownership as acquire; the native read
    // has finished before the C worker calls us with cancellation disabled.
    let context = unsafe { &mut *context.cast::<Context>() };
    let Some(bytes) = context.pending.take() else {
        return 0;
    };
    let result = if error != 0 {
        Err(io::Error::from_raw_os_error(error))
    } else if let Ok(count) = usize::try_from(count) {
        bytes.finish(count)
    } else {
        Err(io::ErrorKind::InvalidData.into())
    };
    c_int::from((context.publish)(result))
}

extern "C" fn finish(context: *mut c_void) {
    // SAFETY: the sole worker still owns callback access, before join.
    let context = unsafe { &mut *context.cast::<Context>() };
    (context.finish)();
}

/// One joinable native worker. Its initialized buffers are lent to read and
/// returned with their valid prefix length. No descriptor flags are changed.
/// Callbacks run on the worker, must not panic, and must not stop this worker.
pub struct InputReader {
    handle: usize,
    // Kept until join: includes the file and any buffer touched by read.
    context: usize,
}

impl InputReader {
    /// Start one persistent worker. Closing the caller's handoff must release
    /// a blocked acquire/publish callback before dropping the returned owner.
    pub fn spawn(
        file: File,
        acquire: impl FnMut() -> Option<InputBatch> + Send + 'static,
        publish: impl FnMut(io::Result<InputBatch>) -> bool + Send + 'static,
        finish: impl FnMut() + Send + 'static,
    ) -> io::Result<Self> {
        let context = Box::new(Context {
            file,
            acquire: Box::new(acquire),
            publish: Box::new(publish),
            finish: Box::new(finish),
            pending: None,
        });
        let fd = context.file.as_raw_fd();
        let context = Box::into_raw(context);
        let ops = Ops {
            fd,
            context: context.cast(),
            acquire: self::acquire,
            publish: self::publish,
            finish: self::finish,
        };
        let mut handle = 0;
        // SAFETY: C copies Ops before returning. The boxed Context and its
        // file/buffers outlive the worker because Drop joins before freeing.
        let error = unsafe { acyclic_input_start(&ops, &mut handle) };
        if error != 0 {
            // SAFETY: a failed start never creates a worker or retains Ops.
            drop(unsafe { Box::from_raw(context) });
            return Err(io::Error::from_raw_os_error(error));
        }
        Ok(Self {
            handle,
            context: context as usize,
        })
    }
}

impl Drop for InputReader {
    fn drop(&mut self) {
        // SAFETY: this is the unique owner of the successfully started native
        // worker; it joins once, before Rust drops the callback context.
        if unsafe { acyclic_input_stop(self.handle) } != 0 {
            // An impossible join failure must not release memory still owned
            // by a worker. In particular, a callback cannot join itself.
            std::process::abort();
        }
        // SAFETY: successful join ended all access to this unique allocation.
        drop(unsafe { Box::from_raw(self.context as *mut Context) });
    }
}

#[cfg(all(test, feature = "test-support"))]
#[path = "input_reader_counts.rs"]
mod counts;
