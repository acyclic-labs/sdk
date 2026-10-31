//! Exercise the real queued-callback owner across a Node Worker shutdown.

use std::{
    future::Future,
    io::Write,
    net::TcpStream,
    pin::Pin,
    task::{Context, Poll},
};

use napi::{Error, Result, Status, bindgen_prelude::Function};
use napi_derive::napi;

fn local_failure(error: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}

// This control calls the original Node reference API to reach its disposal path.
#[allow(unsafe_code)]
fn abort_queued(raw: napi::sys::napi_threadsafe_function, abort: bool) -> Result<()> {
    if !abort {
        return Ok(());
    }
    // The pending future retains the original handle throughout this call.
    // Acquire and release one test-owned reference to request queue disposal.
    let acquired = unsafe { napi::sys::napi_acquire_threadsafe_function(raw) };
    if acquired != napi::sys::Status::napi_ok {
        return Err(local_failure(format!(
            "callback acquire failed: {acquired}"
        )));
    }
    let released = unsafe {
        napi::sys::napi_release_threadsafe_function(
            raw,
            napi::sys::ThreadsafeFunctionReleaseMode::abort,
        )
    };
    if released != napi::sys::Status::napi_ok {
        return Err(local_failure(format!("callback abort failed: {released}")));
    }
    Ok(())
}

fn observe(
    mut pending: Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    port: u16,
    abort: impl FnOnce() -> Result<()>,
) -> Result<()> {
    // Poll on the owning JS thread before returning to JS. A queued callback
    // cannot execute until this JS stack yields; the worker exits on that stack.
    let waker = futures::task::noop_waker();
    if let Poll::Ready(result) = pending.as_mut().poll(&mut Context::from_waker(&waker)) {
        result?;
        return Err(local_failure("callback completed before queue admission"));
    }
    abort()?;
    // The socket is the test's completion event, owned by this one native task.
    // Producer loss closes it, so the observer cannot mistake EOF for success.
    let mut completion =
        TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).map_err(local_failure)?;
    std::thread::Builder::new()
        .name("callback-shutdown-control".into())
        .spawn(move || {
            if completion.write_all(b"queued\n").is_err() {
                return;
            }
            let result = futures::executor::block_on(pending);
            let status = match result {
                Ok(()) => "unexpected-success",
                Err(error) if error.status == Status::GenericFailure => "producer-lost",
                Err(error) if error.status == Status::PendingException => "environment-stopped",
                Err(error) => {
                    let _ = writeln!(
                        completion,
                        "unexpected-error {:?}: {}",
                        error.status, error.reason
                    );
                    return;
                }
            };
            let _ = writeln!(completion, "{status}");
        })
        .map_err(local_failure)?;
    Ok(())
}

/// Enqueue the original uncaught-error callback and observe its Rust completion.
#[napi]
pub fn queue_direct(callback: Function<(), ()>, port: u16, abort_queue: bool) -> Result<()> {
    let function = callback
        .build_threadsafe_function::<()>()
        .callee_handled::<false>()
        .build()?;
    let raw = function.raw();
    observe(
        Box::pin(async move { function.call_async_catch(()).await }),
        port,
        move || abort_queued(raw, abort_queue),
    )
}

/// Enqueue the original error-first callback and observe its Rust completion.
#[napi]
pub fn queue_error_first(callback: Function<(), ()>, port: u16, abort_queue: bool) -> Result<()> {
    let function = callback
        .build_threadsafe_function::<()>()
        .callee_handled::<true>()
        .build()?;
    let raw = function.raw();
    observe(
        Box::pin(async move { function.call_async_catch(Ok(())).await }),
        port,
        move || abort_queued(raw, abort_queue),
    )
}
