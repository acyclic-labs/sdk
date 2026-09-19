//! Apple positional I/O through Dispatch I/O.

#![allow(unsafe_code)]

use crate::{Cancellation, OwnedRead, OwnedWrite};
use block2::RcBlock;
use bytes::Bytes;
use dispatch2::{
    DispatchData, DispatchIO, DispatchIOCloseFlags, DispatchIOStreamType, DispatchQoS,
    DispatchQueue, GlobalQueueIdentifier,
};
use std::ffi::c_int;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd as _;
use std::sync::{Arc, Condvar, Mutex};

struct Completion<T> {
    result: Mutex<Option<io::Result<T>>>,
    ready: Condvar,
}

impl<T> Completion<T> {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            result: Mutex::new(None),
            ready: Condvar::new(),
        })
    }

    fn finish(&self, result: io::Result<T>) {
        *self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
        self.ready.notify_one();
    }

    fn wait(&self) -> io::Result<T> {
        let mut result = self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while result.is_none() {
            result = self
                .ready
                .wait(result)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        result
            .take()
            .ok_or_else(|| io::Error::other("missing Dispatch I/O result"))?
    }
}

fn queue() -> dispatch2::DispatchRetained<DispatchQueue> {
    DispatchQueue::global_queue(GlobalQueueIdentifier::QualityOfService(
        DispatchQoS::UserInitiated,
    ))
}

struct RegisteredChannel<'a> {
    cancellation: &'a Cancellation,
    channel: &'a DispatchIO,
}

impl<'a> RegisteredChannel<'a> {
    fn new(cancellation: &'a Cancellation, channel: &'a DispatchIO) -> Self {
        cancellation.register_apple(channel);
        Self {
            cancellation,
            channel,
        }
    }
}

impl Drop for RegisteredChannel<'_> {
    fn drop(&mut self) {
        self.cancellation.clear_apple(self.channel);
    }
}

pub(super) fn read_batch(
    file: &File,
    reads: &[OwnedRead],
    cancellation: &Cancellation,
) -> io::Result<Vec<Bytes>> {
    let offsets = reads
        .iter()
        .map(|read| {
            i64::try_from(read.offset).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "read offset is too large")
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    let queue = queue();
    let mut completions = Vec::new();
    completions.try_reserve_exact(reads.len())?;
    let mut channels = Vec::new();
    channels.try_reserve_exact(reads.len())?;
    for (read, offset) in reads.iter().zip(offsets) {
        // SAFETY: `file` holds a live descriptor for the duration of this call.
        let descriptor = unsafe { libc::dup(file.as_raw_fd()) };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        // A channel created from a descriptor treats its current cursor as
        // offset zero. `dup` shares that cursor, so reset the duplicate before
        // handing it to Dispatch; positional reads must ignore caller cursor.
        // SAFETY: `descriptor` is live and seekable because the public API
        // accepts regular files.
        if unsafe { libc::lseek(descriptor, 0, libc::SEEK_SET) } < 0 {
            let failure = io::Error::last_os_error();
            // SAFETY: Dispatch does not own the descriptor until construction.
            unsafe {
                libc::close(descriptor);
            }
            return Err(failure);
        }
        let cleanup = RcBlock::new(move |_: c_int| {
            // SAFETY: Dispatch invokes this cleanup handler exactly once for
            // the descriptor owned by this channel.
            unsafe {
                libc::close(descriptor);
            }
        });
        // SAFETY: the duplicated descriptor remains live until Dispatch runs
        // the cleanup handler after the channel and its operations close.
        let channel = unsafe {
            DispatchIO::new(
                DispatchIOStreamType::DISPATCH_IO_RANDOM,
                descriptor,
                &queue,
                &cleanup,
            )
        };
        channel.set_low_water(read.length);
        cancellation.register_apple(&channel);
        let completion = Completion::new();
        let callback = Arc::clone(&completion);
        let collected = Arc::new(Mutex::new(Vec::with_capacity(read.length)));
        let callback_bytes = Arc::clone(&collected);
        let requested = read.length;
        let handler = RcBlock::new(move |done: u8, data: *mut DispatchData, error: c_int| {
            if error != 0 {
                callback.finish(Err(io::Error::from_raw_os_error(error)));
                return;
            }
            if !data.is_null() {
                // SAFETY: Dispatch guarantees data is live for this handler invocation.
                let mut bytes = callback_bytes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let remaining = requested.saturating_sub(bytes.len());
                let delivered = unsafe { &*data }.to_vec();
                let take = remaining.min(delivered.len());
                if let Some(selected) = delivered.get(..take) {
                    bytes.extend_from_slice(selected);
                }
            }
            if done != 0 {
                let bytes = std::mem::take(
                    &mut *callback_bytes
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner),
                );
                callback.finish(Ok(Bytes::from(bytes)));
            }
        });
        let handler = unsafe {
            std::mem::transmute::<
                *mut block2::Block<dyn Fn(u8, *mut DispatchData, c_int)>,
                dispatch2::dispatch_io_handler_t,
            >(RcBlock::as_ptr(&handler))
        };
        // SAFETY: channel, queue, and copied handler stay live through the operation.
        unsafe {
            channel.read(offset, read.length, &queue, handler);
        }
        channels.push(channel);
        completions.push(completion);
    }
    let results = completions
        .into_iter()
        .map(|completion| completion.wait())
        .collect();
    for channel in &channels {
        cancellation.clear_apple(channel);
        channel.close(DispatchIOCloseFlags(0));
    }
    results
}

fn write_all_batch(
    file: &File,
    writes: &[OwnedWrite],
    cancellation: &Cancellation,
) -> io::Result<()> {
    let offsets = writes
        .iter()
        .map(|write| {
            i64::try_from(write.offset).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "write offset is too large")
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    let queue = queue();
    let cleanup = RcBlock::new(|_: c_int| {});
    // SAFETY: the file descriptor remains live until every operation and channel completes.
    let channel = unsafe {
        DispatchIO::new(
            DispatchIOStreamType::DISPATCH_IO_RANDOM,
            file.as_raw_fd(),
            &queue,
            &cleanup,
        )
    };
    let _registered = RegisteredChannel::new(cancellation, &channel);
    let mut completions = Vec::new();
    completions.try_reserve_exact(writes.len())?;
    for (write, offset) in writes.iter().zip(offsets) {
        let completion = Completion::new();
        let callback = Arc::clone(&completion);
        let handler = RcBlock::new(
            move |done: u8, _remaining: *mut DispatchData, error: c_int| {
                if done != 0 {
                    callback.finish(if error == 0 {
                        Ok(())
                    } else {
                        Err(io::Error::from_raw_os_error(error))
                    });
                }
            },
        );
        let handler = unsafe {
            std::mem::transmute::<
                *mut block2::Block<dyn Fn(u8, *mut DispatchData, c_int)>,
                dispatch2::dispatch_io_handler_t,
            >(RcBlock::as_ptr(&handler))
        };
        let data = DispatchData::from_bytes(&write.bytes);
        // SAFETY: channel, data, queue, and copied handler stay live through completion.
        unsafe {
            channel.write(offset, &data, &queue, handler);
        }
        completions.push(completion);
    }
    let mut first_error = None;
    for completion in completions {
        if let Err(error) = completion.wait() {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn write_all_batch_owned(
    file: &File,
    writes: Vec<OwnedWrite>,
    cancellation: &Cancellation,
) -> io::Result<()> {
    write_all_batch(file, &writes, cancellation)
}
