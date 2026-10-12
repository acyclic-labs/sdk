//! Owned completion batches on one Windows proactor.
use super::io::{overlapped_range, reopen_overlapped};
use crate::{NativeFilePayload, OwnedRead, OwnedWrite};
use bytes::Bytes;
use compio_buf::IntoInner as _;
use compio_driver::{
    BorrowedFd, Key, OwnedFd, Proactor, PushEntry, SharedFd,
    op::{ReadAt, WriteAt},
};
use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex, Weak, mpsc},
    task::{Wake, Waker},
};
use windows_sys::Win32::Storage::FileSystem::{FILE_GENERIC_READ, FILE_GENERIC_WRITE};

const DRIVER_QUEUE: usize = 1024;
const INTAKE_QUANTUM: usize = 64;
const COMPLETION_QUANTUM: usize = 256;

#[derive(Clone, Debug)]
enum DriverFile {
    Reopened(SharedFd<OwnedFd>),
    Original(Arc<NativeFilePayload>),
}
impl compio_driver::AsFd for DriverFile {
    fn as_fd(&self) -> BorrowedFd<'_> {
        match self {
            Self::Reopened(file) => file.as_fd(),
            Self::Original(file) => BorrowedFd::from(file.as_handle()),
        }
    }
}
type ReadOp = ReadAt<Vec<u8>, DriverFile>;
type WriteOp = WriteAt<Bytes, DriverFile>;
type ReadFinish = Box<dyn FnOnce(io::Result<Vec<Bytes>>) + Send>;
type WriteFinish = Box<dyn FnOnce(io::Result<()>) + Send>;

enum Job {
    Read(Arc<NativeFilePayload>, bool, Vec<OwnedRead>, ReadFinish),
    Write(Arc<NativeFilePayload>, bool, Vec<OwnedWrite>, WriteFinish),
}
#[derive(Clone)]
struct Driver {
    sender: mpsc::SyncSender<Job>,
    wake: Waker,
}
static DRIVER: Mutex<Option<Driver>> = Mutex::new(None);

fn driver() -> io::Result<Driver> {
    let mut slot = DRIVER
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(driver) = slot.as_ref() {
        return Ok(driver.clone());
    }
    let (sender, receiver) = mpsc::sync_channel(DRIVER_QUEUE);
    let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("acyclic-compio-windows".into())
        .spawn(move || {
            let proactor = match Proactor::new() {
                Ok(proactor) => proactor,
                Err(error) => {
                    let _ = ready_sender.send(Err(error));
                    return;
                }
            };
            if ready_sender.send(Ok(proactor.waker())).is_err() {
                return;
            }
            // A driver failure cannot release a buffer still visible to the kernel.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                DriverState::new(proactor, receiver).run()
            }));
            std::process::abort();
        })?;
    let wake = ready_receiver
        .recv()
        .map_err(|_| io::Error::other("completion owner exited during startup"))??;
    let driver = Driver { sender, wake };
    *slot = Some(driver.clone());
    Ok(driver)
}
pub(crate) fn submit_read(
    file: Arc<NativeFilePayload>,
    overlapped: bool,
    reads: Vec<OwnedRead>,
    finish: ReadFinish,
) -> io::Result<()> {
    let driver = driver()?;
    driver
        .sender
        .send(Job::Read(file, overlapped, reads, finish))
        .map_err(|_| io::Error::other("completion owner disconnected"))?;
    driver.wake.wake_by_ref();
    Ok(())
}
pub(crate) fn submit_write(
    file: Arc<NativeFilePayload>,
    overlapped: bool,
    writes: Vec<OwnedWrite>,
    finish: WriteFinish,
) -> io::Result<()> {
    let driver = driver()?;
    driver
        .sender
        .send(Job::Write(file, overlapped, writes, finish))
        .map_err(|_| io::Error::other("completion owner disconnected"))?;
    driver.wake.wake_by_ref();
    Ok(())
}

enum Pending {
    Read {
        batch: u64,
        index: usize,
        key: Key<ReadOp>,
        length: usize,
    },
    Write {
        batch: u64,
        index: usize,
        key: Key<WriteOp>,
        offset: u64,
        file: DriverFile,
    },
}
enum BatchResult {
    Reads(Vec<Option<Bytes>>, ReadFinish),
    Writes(WriteFinish),
}
struct Batch {
    remaining: usize,
    first_error: Option<io::Error>,
    result: BatchResult,
}
struct CompletionWake {
    id: u64,
    ready: mpsc::Sender<u64>,
}
impl Wake for CompletionWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.ready.send(self.id);
    }
}

struct DriverState {
    proactor: Proactor,
    incoming: mpsc::Receiver<Job>,
    ready_sender: mpsc::Sender<u64>,
    ready: mpsc::Receiver<u64>,
    pending: HashMap<u64, Pending>,
    batches: HashMap<u64, Batch>,
    attached_original: HashMap<usize, Weak<NativeFilePayload>>,
    next_id: u64,
}
impl DriverState {
    fn new(proactor: Proactor, incoming: mpsc::Receiver<Job>) -> Self {
        let (ready_sender, ready) = mpsc::channel();
        Self {
            proactor,
            incoming,
            ready_sender,
            ready,
            pending: HashMap::new(),
            batches: HashMap::new(),
            attached_original: HashMap::new(),
            next_id: 1,
        }
    }
    fn id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = id.checked_add(1).unwrap_or_else(|| std::process::abort());
        id
    }
    fn run(mut self) -> io::Result<()> {
        loop {
            let mut completed = 0;
            while completed < COMPLETION_QUANTUM {
                let Ok(id) = self.ready.try_recv() else {
                    break;
                };
                self.complete(id);
                completed += 1;
            }
            let mut admitted = 0;
            while admitted < INTAKE_QUANTUM {
                let Ok(job) = self.incoming.try_recv() else {
                    break;
                };
                match job {
                    Job::Read(file, overlapped, reads, finish) => {
                        self.submit_read_job(&file, overlapped, &reads, finish)
                    }
                    Job::Write(file, overlapped, writes, finish) => {
                        self.submit_write_job(&file, overlapped, writes, finish)
                    }
                }
                admitted += 1;
            }
            let timeout = (completed == COMPLETION_QUANTUM || admitted == INTAKE_QUANTUM)
                .then_some(std::time::Duration::ZERO);
            if let Err(error) = self.proactor.poll(timeout) {
                // An empty nonblocking IOCP poll is ordinary readiness, not driver failure.
                if timeout != Some(std::time::Duration::ZERO)
                    || error.kind() != io::ErrorKind::TimedOut
                {
                    return Err(error);
                }
            }
        }
    }
    fn attach(
        &mut self,
        file: &Arc<NativeFilePayload>,
        overlapped: bool,
        access: u32,
    ) -> io::Result<DriverFile> {
        if !overlapped {
            let reopened = reopen_overlapped(file, access)?;
            self.proactor.attach(reopened.as_raw_handle().cast())?;
            return Ok(DriverFile::Reopened(SharedFd::new(OwnedFd::from(reopened))));
        }
        let identity = Arc::as_ptr(file) as usize;
        match self
            .attached_original
            .get(&identity)
            .and_then(Weak::upgrade)
        {
            Some(attached) if Arc::ptr_eq(&attached, file) => {}
            Some(_) => std::process::abort(),
            None => {
                // The unsafe constructor guarantees a sole, unattached overlapped handle.
                self.proactor
                    .attach(file.as_raw_handle().cast())
                    .unwrap_or_else(|_| std::process::abort());
                self.attached_original
                    .insert(identity, Arc::downgrade(file));
                if self.attached_original.len() >= 4096 {
                    self.attached_original
                        .retain(|_, weak| weak.strong_count() > 0);
                }
            }
        }
        Ok(DriverFile::Original(Arc::clone(file)))
    }
    fn submit_read_job(
        &mut self,
        file: &Arc<NativeFilePayload>,
        overlapped: bool,
        reads: &[OwnedRead],
        finish: ReadFinish,
    ) {
        let prepared = reads
            .iter()
            .map(|read| {
                overlapped_range(read.offset, read.length)?;
                let mut buffer = Vec::new();
                buffer
                    .try_reserve_exact(read.length)
                    .map_err(io::Error::other)?;
                Ok(buffer)
            })
            .collect::<io::Result<Vec<_>>>();
        let buffers = match prepared {
            Ok(buffers) => buffers,
            Err(error) => {
                finish(Err(error));
                return;
            }
        };
        if reads.iter().all(|read| read.length == 0) {
            finish(Ok(vec![Bytes::new(); reads.len()]));
            return;
        }
        let file = match self.attach(file, overlapped, FILE_GENERIC_READ) {
            Ok(file) => file,
            Err(error) => {
                finish(Err(error));
                return;
            }
        };
        let batch = self.id();
        self.batches.insert(
            batch,
            Batch {
                remaining: reads.len(),
                first_error: None,
                result: BatchResult::Reads(vec![None; reads.len()], finish),
            },
        );
        for (index, (read, buffer)) in reads.iter().zip(buffers).enumerate() {
            if read.length == 0 {
                self.done(batch, index, Ok(Some(Bytes::new())));
                continue;
            }
            match self
                .proactor
                .push(ReadAt::new(file.clone(), read.offset, buffer))
            {
                PushEntry::Ready(result) => self.read_done(batch, index, read.length, result),
                PushEntry::Pending(key) => {
                    let id = self.id();
                    let wake = Waker::from(Arc::new(CompletionWake {
                        id,
                        ready: self.ready_sender.clone(),
                    }));
                    self.proactor.update_waker(&key, &wake);
                    self.pending.insert(
                        id,
                        Pending::Read {
                            batch,
                            index,
                            key,
                            length: read.length,
                        },
                    );
                }
            }
        }
    }
    fn submit_write_job(
        &mut self,
        file: &Arc<NativeFilePayload>,
        overlapped: bool,
        writes: Vec<OwnedWrite>,
        finish: WriteFinish,
    ) {
        if writes.is_empty() {
            finish(Ok(()));
            return;
        }
        if let Err(error) = writes
            .iter()
            .try_for_each(|write| overlapped_range(write.offset, write.bytes.len()).map(drop))
        {
            finish(Err(error));
            return;
        }
        let file = match self.attach(file, overlapped, FILE_GENERIC_WRITE) {
            Ok(file) => file,
            Err(error) => {
                finish(Err(error));
                return;
            }
        };
        let batch = self.id();
        self.batches.insert(
            batch,
            Batch {
                remaining: writes.len(),
                first_error: None,
                result: BatchResult::Writes(finish),
            },
        );
        for (index, write) in writes.into_iter().enumerate() {
            self.write_piece(batch, index, file.clone(), write.offset, write.bytes);
        }
    }
    fn write_piece(
        &mut self,
        batch: u64,
        index: usize,
        file: DriverFile,
        mut offset: u64,
        mut bytes: Bytes,
    ) {
        loop {
            match self
                .proactor
                .push(WriteAt::new(file.clone(), offset, bytes))
            {
                PushEntry::Pending(key) => {
                    let id = self.id();
                    let wake = Waker::from(Arc::new(CompletionWake {
                        id,
                        ready: self.ready_sender.clone(),
                    }));
                    self.proactor.update_waker(&key, &wake);
                    self.pending.insert(
                        id,
                        Pending::Write {
                            batch,
                            index,
                            key,
                            offset,
                            file,
                        },
                    );
                    return;
                }
                PushEntry::Ready(result) => {
                    let Some((written, suffix)) = self.write_done(batch, index, result) else {
                        return;
                    };
                    offset += written as u64;
                    bytes = suffix;
                }
            }
        }
    }
    fn complete(&mut self, id: u64) {
        let Some(pending) = self.pending.remove(&id) else {
            return;
        };
        match pending {
            Pending::Read {
                batch,
                index,
                key,
                length,
            } => match self.proactor.pop(key) {
                PushEntry::Pending(key) => {
                    self.pending.insert(
                        id,
                        Pending::Read {
                            batch,
                            index,
                            key,
                            length,
                        },
                    );
                }
                PushEntry::Ready(result) => self.read_done(batch, index, length, result),
            },
            Pending::Write {
                batch,
                index,
                key,
                offset,
                file,
            } => match self.proactor.pop(key) {
                PushEntry::Pending(key) => {
                    self.pending.insert(
                        id,
                        Pending::Write {
                            batch,
                            index,
                            key,
                            offset,
                            file,
                        },
                    );
                }
                PushEntry::Ready(result) => {
                    if let Some((written, suffix)) = self.write_done(batch, index, result) {
                        self.write_piece(batch, index, file, offset + written as u64, suffix);
                    }
                }
            },
        }
    }
    fn write_done(
        &mut self,
        batch: u64,
        index: usize,
        result: compio_buf::BufResult<usize, WriteOp>,
    ) -> Option<(usize, Bytes)> {
        let (count, op) = result.into_parts();
        let bytes = op.into_inner();
        let result = match count {
            Ok(count) if count == bytes.len() => Ok(None),
            Ok(0) => Err(io::ErrorKind::WriteZero.into()),
            Ok(count) if count < bytes.len() => return Some((count, bytes.slice(count..))),
            Ok(_) => Err(io::Error::other("write completion exceeded buffer")),
            Err(error) => Err(error),
        };
        self.done(batch, index, result);
        None
    }
    fn read_done(
        &mut self,
        batch: u64,
        index: usize,
        length: usize,
        result: compio_buf::BufResult<usize, ReadOp>,
    ) {
        let (count, op) = result.into_parts();
        let mut buffer = op.into_inner();
        let result = count.and_then(|count| {
            if count > length || count > buffer.capacity() {
                return Err(io::Error::other("read completion exceeded buffer"));
            }
            // SAFETY: terminal completion initialized count bytes in the owned buffer.
            unsafe {
                buffer.set_len(count);
            }
            Ok(Some(Bytes::from(buffer)))
        });
        self.done(batch, index, result);
    }
    fn done(&mut self, batch: u64, index: usize, result: io::Result<Option<Bytes>>) {
        let entry = self
            .batches
            .get_mut(&batch)
            .unwrap_or_else(|| std::process::abort());
        match result {
            Ok(Some(bytes)) => match &mut entry.result {
                BatchResult::Reads(values, _) => {
                    *values
                        .get_mut(index)
                        .unwrap_or_else(|| std::process::abort()) = Some(bytes)
                }
                BatchResult::Writes(_) => unreachable!("read result in write batch"),
            },
            Err(error) if entry.first_error.is_none() => entry.first_error = Some(error),
            Ok(None) | Err(_) => {}
        }
        entry.remaining -= 1;
        if entry.remaining != 0 {
            return;
        }
        let entry = self
            .batches
            .remove(&batch)
            .unwrap_or_else(|| std::process::abort());
        match entry.result {
            BatchResult::Reads(values, finish) => finish(entry.first_error.map_or_else(
                || {
                    values
                        .into_iter()
                        .collect::<Option<Vec<_>>>()
                        .ok_or_else(|| io::Error::other("completed read omitted a value"))
                },
                Err,
            )),
            BatchResult::Writes(finish) => finish(entry.first_error.map_or(Ok(()), Err)),
        }
    }
}
