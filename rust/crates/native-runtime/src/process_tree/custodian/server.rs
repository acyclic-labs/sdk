//! One completion port services every live Job; no per-Job thread is created.
use super::*;
use std::collections::HashMap;

struct Input {
    activate: bool,
    key: usize,
    name: String,
}
struct Job {
    handle: OwnedHandle,
    completion: OwnedHandle,
    activated: bool,
    members: Members,
}

pub(super) fn serve() -> io::Result<()> {
    let port = setup()?;
    let mut server = Server::default();
    loop {
        let mut message = 0;
        let mut key = 0;
        let mut overlapped: *mut OVERLAPPED = std::ptr::null_mut();
        // SAFETY: the retained port and writable outputs.
        if unsafe {
            GetQueuedCompletionStatus(
                port,
                &raw mut message,
                &raw mut key,
                &raw mut overlapped,
                INFINITE,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let changed = server.dispatch(key, message, overlapped, port)?;
        server.retire_changed(changed)?;
        if server.owner_exited && server.jobs.is_empty() {
            return Ok(());
        }
    }
}

fn setup() -> io::Result<HANDLE> {
    let mut input = BufReader::new(io::stdin());
    let mut line = String::new();
    input.read_line(&mut line)?;
    let owner = line.trim_end().parse::<usize>().map_err(io::Error::other)?;
    if owner == 0 {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    // SAFETY: the host duplicated this synchronize handle into the daemon.
    let owner =
        unsafe { OwnedHandle::from_raw_handle(std::ptr::with_exposed_provenance_mut(owner)) };
    // SAFETY: a new owned completion port, not associated with a file.
    let port = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 1) };
    if port.is_null() {
        return Err(io::Error::last_os_error());
    }
    PORT.store(port.expose_provenance(), Ordering::Relaxed);
    let port_value = port.expose_provenance();
    std::thread::spawn(move || {
        let port: HANDLE = std::ptr::with_exposed_provenance_mut(port_value);
        // SAFETY: the owned host handle and daemon-lifetime port.
        unsafe {
            WaitForSingleObject(owner.as_raw_handle(), INFINITE);
            PostQueuedCompletionStatus(port, 0, OWNER_EXITED, std::ptr::null());
        }
    });
    std::thread::spawn(move || {
        let _ = read_commands(input, port_value);
        let port: HANDLE = std::ptr::with_exposed_provenance_mut(port_value);
        // SAFETY: EOF or protocol failure closes admission and fences all Jobs.
        unsafe { PostQueuedCompletionStatus(port, 0, OWNER_EXITED, std::ptr::null()) };
    });
    ready()?;
    Ok(port)
}

#[derive(Default)]
struct Server {
    jobs: HashMap<usize, Job>,
    owners: HashMap<u32, usize>,
    owner_exited: bool,
}
impl Server {
    fn dispatch(
        &mut self,
        key: usize,
        message: u32,
        overlapped: *mut OVERLAPPED,
        port: HANDLE,
    ) -> io::Result<Option<usize>> {
        let changed = if key == COMMAND {
            // SAFETY: read_commands transferred this exact Box in the packet.
            let command = unsafe { Box::from_raw(overlapped.cast::<Input>()) };
            if self.owner_exited {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            apply(&mut self.jobs, &command, port)?;
            ready()?;
            Some(command.key)
        } else if key == OWNER_EXITED {
            self.owner_exited = true;
            for (key, job) in &mut self.jobs {
                job.activated = true;
                // SAFETY: each owned Job is fenced when the host exits.
                if unsafe { TerminateJobObject(job.handle.as_raw_handle(), 1) } == 0 {
                    return Err(io::Error::last_os_error());
                }
                for pid in super::super::custody::members(job.handle.as_raw_handle())? {
                    admit(job, *key, pid, &mut self.owners)?;
                }
            }
            None
        } else if key == MEMBER_EXITED {
            let owner = self.owners.remove(&message);
            if let Some(job) = owner.and_then(|key| self.jobs.get_mut(&key)) {
                job.members.release(message);
            }
            Some(owner.unwrap_or(MEMBER_EXITED))
        } else if message == JOB_OBJECT_MSG_NEW_PROCESS {
            if let Some(job) = self.jobs.get_mut(&key) {
                admit(
                    job,
                    key,
                    u32::try_from(overlapped.addr()).map_err(io::Error::other)?,
                    &mut self.owners,
                )?;
            }
            Some(key)
        } else {
            Some(key)
        };
        Ok(changed)
    }
    fn retire_changed(&mut self, changed: Option<usize>) -> io::Result<()> {
        if let Some(key) = changed {
            retire(&mut self.jobs, key)?;
        } else {
            // This full pass occurs only at host death, never per tool call.
            let keys: Vec<_> = self.jobs.keys().copied().collect();
            for key in keys {
                retire(&mut self.jobs, key)?;
            }
        }
        Ok(())
    }
}

fn admit(job: &mut Job, key: usize, pid: u32, owners: &mut HashMap<u32, usize>) -> io::Result<()> {
    job.members.admit(job.handle.as_raw_handle(), pid)?;
    if job.members.0.contains_key(&pid) {
        owners.insert(pid, key);
    }
    Ok(())
}

fn retire(jobs: &mut HashMap<usize, Job>, key: usize) -> io::Result<()> {
    let done = match jobs.get(&key) {
        Some(job) => {
            job.activated
                && job.members.is_empty()
                && active_processes(job.handle.as_raw_handle())? == 0
        }
        None => false,
    };
    if done {
        let job = jobs.remove(&key).ok_or(io::ErrorKind::NotFound)?;
        // Publish completion only after every pinned member has signaled.
        drop(job.handle);
        // SAFETY: the owned completion event remains live through publication.
        if unsafe { SetEvent(job.completion.as_raw_handle()) } == 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn apply(jobs: &mut HashMap<usize, Job>, input: &Input, port: HANDLE) -> io::Result<()> {
    if input.activate {
        if let Some(job) = jobs.get_mut(&input.key) {
            job.activated = true;
        }
        return Ok(());
    }
    if jobs.contains_key(&input.key) {
        return Err(io::ErrorKind::AlreadyExists.into());
    }
    let name: Vec<u16> = input
        .name
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    // JOB_OBJECT_SET_ATTRIBUTES | JOB_OBJECT_QUERY | JOB_OBJECT_TERMINATE.
    // SAFETY: a Job name pinned by its registering parent until acknowledgement.
    let raw = unsafe { OpenJobObjectW(0x0002 | 0x0004 | 0x0008, 0, name.as_ptr()) };
    if raw.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: OpenJobObjectW transferred this fresh handle.
    let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
    let completion = completion_name(&name);
    // SAFETY: the registering parent pins the fresh completion event too.
    let raw = unsafe { OpenEventW(EVENT_ALL_ACCESS, 0, completion.as_ptr()) };
    if raw.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: OpenEventW transferred this fresh handle.
    let completion = unsafe { OwnedHandle::from_raw_handle(raw) };
    let association = JOBOBJECT_ASSOCIATE_COMPLETION_PORT {
        CompletionKey: std::ptr::with_exposed_provenance_mut(input.key),
        CompletionPort: port,
    };
    // SAFETY: the owned still-empty Job, port and initialized ABI structure.
    if unsafe {
        SetInformationJobObject(
            handle.as_raw_handle(),
            JobObjectAssociateCompletionPortInformation,
            (&raw const association).cast(),
            u32::try_from(size_of::<JOBOBJECT_ASSOCIATE_COMPLETION_PORT>())
                .map_err(io::Error::other)?,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    jobs.insert(
        input.key,
        Job {
            handle,
            completion,
            activated: false,
            members: Members::default(),
        },
    );
    Ok(())
}

fn ready() -> io::Result<()> {
    let mut output = io::stdout().lock();
    writeln!(output, "{READY}")?;
    output.flush()
}

fn read_commands(mut input: impl io::BufRead, port: usize) -> io::Result<()> {
    loop {
        let mut line = String::new();
        if input.read_line(&mut line)? == 0 {
            return Ok(());
        }
        let mut fields = line.trim_end().splitn(3, ' ');
        let activate = match fields.next() {
            Some("A") => true,
            Some("R") => false,
            _ => return Err(io::ErrorKind::InvalidData.into()),
        };
        let key = fields
            .next()
            .ok_or(io::ErrorKind::InvalidData)?
            .parse::<usize>()
            .map_err(io::Error::other)?;
        if key >= COMMAND {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let name = fields.next().unwrap_or("").to_owned();
        if !activate && !name.starts_with("Local\\acyclic-process-tree-") {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let command = Box::into_raw(Box::new(Input {
            activate,
            key,
            name,
        }));
        let port: HANDLE = std::ptr::with_exposed_provenance_mut(port);
        // SAFETY: transfers the Box to this daemon's single port consumer.
        if unsafe { PostQueuedCompletionStatus(port, 0, COMMAND, command.cast()) } == 0 {
            // SAFETY: the failed post transferred no ownership.
            drop(unsafe { Box::from_raw(command) });
            return Err(io::Error::last_os_error());
        }
    }
}
