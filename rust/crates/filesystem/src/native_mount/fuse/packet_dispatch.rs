//! Standard FUSE operations over the existing projection state and source.

use super::packet::{PacketInput, PacketOutput, PacketRequest};
use super::*;

/// A source failure stays in its original core's terminal receipt. The packet
/// path carries that owner, rather than dropping counters or copying the error.
pub(super) enum PacketFailure {
    Local(NativeMountError),
    Source {
        _core: Arc<ProjectionCore>,
        errno: i32,
        completion: PacketCompletion,
    },
}

impl PacketFailure {
    pub(super) fn from_callback(
        core: &Arc<ProjectionCore>,
        failure: (NativeMountError, crate::WorkCounters),
    ) -> Self {
        if crate::native_mount::shared::outcome::stops_mount(&failure.0) {
            Self::source(core, failure)
        } else {
            Self::Local(failure.0)
        }
    }

    pub(super) fn source(
        core: &Arc<ProjectionCore>,
        failure: (NativeMountError, crate::WorkCounters),
    ) -> Self {
        let errno = callback_errno(&failure.0);
        core.retain_handler_failure(failure);
        Self::Source {
            _core: Arc::clone(core),
            errno,
            completion: PacketCompletion::no_reply(),
        }
    }

    pub(super) fn errno(&self) -> i32 {
        match self {
            Self::Local(error) => callback_errno(error),
            Self::Source { errno, .. } => *errno,
        }
    }

    pub(super) fn is_terminal(&self) -> bool {
        match self {
            Self::Local(error) => crate::native_mount::shared::outcome::stops_mount(error),
            Self::Source { .. } => true,
        }
    }

    pub(super) fn retaining_work(mut self, work: Option<crate::NativeMaterializationWork>) -> Self {
        if let Self::Source { completion, .. } = &mut self {
            completion._work = work;
        }
        self
    }

    pub(super) fn retaining_view(
        mut self,
        view: crate::native_mount::shared::view_table::ViewLease<FuseProjection>,
    ) -> Self {
        if let Self::Source { completion, .. } = &mut self {
            completion._view = Some(view);
        }
        self
    }

    pub(super) fn retaining_completion(mut self, retained: PacketCompletion) -> Self {
        if let Self::Source { completion, .. } = &mut self {
            completion.views = retained.views;
        }
        self
    }

    pub(super) fn into_native_error(self) -> NativeMountError {
        match self {
            Self::Local(error) => error,
            // Its original typed failure and counters remain in the retained
            // core receipt, consumed by the existing retirement owner.
            other => other.errno().into(),
        }
    }
}

impl From<i32> for PacketFailure {
    fn from(errno: i32) -> Self {
        Self::Local(errno.into())
    }
}

impl From<NativeMountError> for PacketFailure {
    fn from(error: NativeMountError) -> Self {
        Self::Local(error)
    }
}

/// Retains a shared view through the transport's physical reply publication.
pub(super) struct PacketCompletion {
    pub(super) length: Option<usize>,
    _view: Option<crate::native_mount::shared::view_table::ViewLease<FuseProjection>>,
    _work: Option<crate::NativeMaterializationWork>,
    views: Option<Box<PacketView>>,
}

struct PacketView {
    _view: crate::native_mount::shared::view_table::ViewLease<FuseProjection>,
    _work: crate::NativeMaterializationWork,
    _allocation: crate::NativeAllocation,
    next: Option<Box<Self>>,
}

impl Drop for PacketView {
    fn drop(&mut self) {
        // A large readdirplus reply must not recurse while retiring its leases.
        while let Some(mut next) = self.next.take() {
            self.next = next.next.take();
        }
    }
}

impl PacketCompletion {
    pub(super) fn reply(length: usize) -> Self {
        Self {
            length: Some(length),
            _view: None,
            _work: None,
            views: None,
        }
    }
    pub(super) fn no_reply() -> Self {
        Self {
            length: None,
            _view: None,
            _work: None,
            views: None,
        }
    }
    pub(super) fn retaining(
        mut self,
        view: crate::native_mount::shared::view_table::ViewLease<FuseProjection>,
    ) -> Self {
        self._view = Some(view);
        self
    }

    pub(super) fn retaining_work(mut self, work: crate::NativeMaterializationWork) -> Self {
        self._work = Some(work);
        self
    }

    fn retaining_optional_work(mut self, work: Option<crate::NativeMaterializationWork>) -> Self {
        self._work = work;
        self
    }

    pub(super) fn retain_view(
        &mut self,
        view: crate::native_mount::shared::view_table::ViewLease<FuseProjection>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let allocation = work.claim(
            std::mem::size_of::<PacketView>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        self.views = Some(Box::new(PacketView {
            _view: view,
            _work: work.clone(),
            _allocation: allocation,
            next: self.views.take(),
        }));
        Ok(())
    }
}

impl FuseProjection {
    /// The caller retains request/reply storage through physical completion.
    /// Session negotiation and source-derived identity are owned by the session.
    pub(super) fn dispatch_packet(
        &self,
        request: &PacketRequest<'_>,
        response: &mut [u8],
        owner: (u32, u32),
    ) -> Result<PacketCompletion, PacketFailure> {
        let mut no_reply = [0; super::packet::OUTPUT_HEADER];
        let response = if matches!(request.opcode, 2 | 42) {
            no_reply.as_mut_slice()
        } else {
            response
        };
        let mut output = PacketOutput::new(response, minimum_payload(request)?)?;
        let mut retained_work = None;
        let result = self.core.request(|work| {
            retained_work = Some(work.clone());
            self.packet_operation(request, &mut output, owner, work)
        });
        match result {
            Ok(true) => Ok(PacketCompletion::no_reply().retaining_optional_work(retained_work)),
            Ok(false) => Ok(
                PacketCompletion::reply(output.finish(request.unique, None)?)
                    .retaining_optional_work(retained_work),
            ),
            Err(failure) if crate::native_mount::shared::outcome::stops_mount(&failure.0) => {
                Err(PacketFailure::source(&self.core, failure).retaining_work(retained_work))
            }
            Err((error, _)) => {
                // FORGET has no reply even on a failed source admission. Surface
                // that failure to the session owner rather than inventing a reply.
                if matches!(request.opcode, 2 | 42) {
                    return Err(error.into());
                }
                Ok(PacketCompletion::reply(
                    output.finish(request.unique, Some(callback_errno(&error)))?,
                )
                .retaining_optional_work(retained_work))
            }
        }
    }

    fn packet_operation(
        &self,
        request: &PacketRequest<'_>,
        output: &mut PacketOutput<'_>,
        owner: (u32, u32),
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        match request.opcode {
            1 | 10 | 11 | 12 | 45 | 13 => self.packet_namespace(request, output, work),
            6 | 8 | 9 | 35 => self.packet_create(request, output, owner, work),
            3 | 4 | 5 | 17 | 21 | 22 | 23 | 24 => self.packet_metadata(request, output, work),
            14 | 27 | 18 | 25 | 29 | 20 | 30 => self.packet_open(request, output, work),
            15 | 28 | 44 | 16 => self.packet_data(request, output, work),
            43 | 46 | 47 => self.packet_allocation(request, output, work),
            2 | 42 => self.packet_forget(request, work),
            _ => Err(libc::ENOSYS.into()),
        }
    }

    fn packet_namespace(
        &self,
        request: &PacketRequest<'_>,
        output: &mut PacketOutput<'_>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let mut input = PacketInput::new(request.body);
        let inode = request.inode;
        match request.opcode {
            1 => {
                let name = input.name()?;
                input.end()?;
                let entry = self.lookup_entry(inode, name, work)?;
                output.entry(&entry.attr, &entry.ttl)?;
            }
            10 | 11 => {
                let name = input.name()?;
                input.end()?;
                self.remove_name(inode, name, work)?;
            }
            12 | 45 => {
                let parent = input.u64()?;
                let flags = if request.opcode == 45 {
                    let flags = input.u32()?;
                    input.take(4)?;
                    flags
                } else {
                    0
                };
                let name = input.name()?;
                let new_name = input.name()?;
                input.end()?;
                self.rename_name(inode, name, parent, new_name, flags, work)?;
            }
            13 => {
                let source = input.u64()?;
                let name = input.name()?;
                input.end()?;
                let entry = self.link_name(source, inode, name, work)?;
                output.entry(&entry.attr, &entry.ttl)?;
            }
            _ => return Err(libc::ENOSYS.into()),
        }
        Ok(false)
    }

    fn packet_create(
        &self,
        request: &PacketRequest<'_>,
        output: &mut PacketOutput<'_>,
        owner: (u32, u32),
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let mut input = PacketInput::new(request.body);
        let inode = request.inode;
        match request.opcode {
            6 => {
                let name = input.name()?;
                let target = input.name()?;
                input.end()?;
                let metadata = create_metadata(&self.core.clock, 0o777, S_IFLNK, owner.0, owner.1);
                let entry = self.create_node(
                    inode,
                    name,
                    |path, work| {
                        let target = fuse_source_chunk(target.as_bytes(), work)?;
                        Ok(self
                            .core
                            .source
                            .create_symbolic_link(path, target, metadata, work)?)
                    },
                    work,
                )?;
                output.entry(&entry.attr, &entry.ttl)?;
            }
            8 | 9 => {
                let mode = input.u32()?;
                let rdev = if request.opcode == 8 { input.u32()? } else { 0 };
                let umask = input.u32()?;
                if request.opcode == 8 {
                    input.take(4)?;
                }
                let name = input.name()?;
                input.end()?;
                let kind = if request.opcode == 9 {
                    S_IFDIR
                } else {
                    mode & S_IFMT
                };
                let metadata =
                    create_metadata(&self.core.clock, mode & !umask, kind, owner.0, owner.1);
                let entry = self.create_node(
                    inode,
                    name,
                    |path, work| {
                        if request.opcode == 9 {
                            return Ok(self.core.source.create_directory(path, metadata, work)?);
                        }
                        let (kind, device) = match kind {
                            S_IFIFO => (MountNodeKind::Fifo, None),
                            S_IFSOCK => (MountNodeKind::Socket, None),
                            S_IFCHR => (
                                MountNodeKind::CharacterDevice,
                                Some(super::super::device::split_device(u64::from(rdev))),
                            ),
                            S_IFBLK => (
                                MountNodeKind::BlockDevice,
                                Some(super::super::device::split_device(u64::from(rdev))),
                            ),
                            _ => return Err(libc::EOPNOTSUPP.into()),
                        };
                        Ok(self
                            .core
                            .source
                            .create_special(path, kind, device, metadata, work)?)
                    },
                    work,
                )?;
                output.entry(&entry.attr, &entry.ttl)?;
            }
            35 => {
                let flags = input.u32()? as i32;
                let mode = input.u32()?;
                let umask = input.u32()?;
                let open_flags = input.u32()?;
                let name = input.name()?;
                input.end()?;
                if open_flags != 0 {
                    return Err(libc::EOPNOTSUPP.into());
                }
                let (entry, handle, flags) =
                    self.create_file(owner, inode, name, mode & !umask, flags, work)?;
                output.entry(&entry.attr, &entry.ttl)?;
                output.opened(handle, flags)?;
            }
            _ => return Err(libc::ENOSYS.into()),
        }
        Ok(false)
    }

    fn packet_metadata(
        &self,
        request: &PacketRequest<'_>,
        output: &mut PacketOutput<'_>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let mut input = PacketInput::new(request.body);
        let inode = request.inode;
        match request.opcode {
            3 => {
                let flags = input.u32()?;
                input.take(4)?;
                let handle = input.u64()?;
                input.end()?;
                if flags & !1 != 0 {
                    return Err(libc::EINVAL.into());
                }
                let (attr, ttl) = self.node_attributes(
                    inode,
                    (flags & 1 != 0 && handle != 0).then_some(handle),
                    work,
                )?;
                output.attributes(&attr, &ttl)?;
            }
            4 => {
                let (handle, changes) = decode_setattr(input)?;
                let (attr, ttl) = self.set_attributes(inode, handle, &changes, work)?;
                output.attributes(&attr, &ttl)?;
            }
            5 => {
                input.end()?;
                let link = self.read_link(inode, work)?;
                output.bytes(&link.bytes)?;
            }
            17 => {
                input.end()?;
                output.unknown_capacity()?;
            }
            21 => {
                // SETXATTR_EXT is not negotiated; the 7.31 argument has two words.
                let size = usize::try_from(input.u32()?).map_err(|_| libc::EOVERFLOW)?;
                let flags = input.u32()? as i32;
                let name = input.name()?;
                let value = input.take(size)?;
                input.end()?;
                self.write_named_attribute(inode, name, value, flags, 0, work)?;
            }
            22 | 23 => {
                let size = input.u32()?;
                input.take(4)?;
                if request.opcode == 22 {
                    let name = input.name()?;
                    input.end()?;
                    let value = self.read_named_attribute(inode, name, work)?;
                    encode_xattr(output, &value.bytes, size)?;
                } else {
                    input.end()?;
                    let value = self.list_named_attributes(inode, work)?;
                    encode_xattr(output, &value, size)?;
                }
            }
            24 => {
                let name = input.name()?;
                input.end()?;
                self.remove_named_attribute(inode, name, work)?;
            }
            _ => return Err(libc::ENOSYS.into()),
        }
        Ok(false)
    }

    fn packet_open(
        &self,
        request: &PacketRequest<'_>,
        output: &mut PacketOutput<'_>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let mut input = PacketInput::new(request.body);
        let inode = request.inode;
        match request.opcode {
            14 | 27 => {
                let flags = input.u32()?;
                let open_flags = input.u32()?;
                input.end()?;
                if open_flags != 0 {
                    return Err(libc::EOPNOTSUPP.into());
                }
                let (handle, flags) = if request.opcode == 27 {
                    (0, self.open_directory(inode, work)?)
                } else {
                    let _names = self.core.names()?;
                    self.open_node(inode, flags as i32, work)?
                };
                output.opened(handle, flags)?;
            }
            18 | 25 | 29 => {
                let handle = input.u64()?;
                input.take(16)?;
                input.end()?;
                if request.opcode != 29 {
                    self.flush_handle(inode, handle, false, request.opcode == 18, work)?;
                }
            }
            20 | 30 => {
                let handle = input.u64()?;
                input.take(8)?;
                input.end()?;
                if request.opcode == 20 {
                    self.flush_handle(inode, handle, true, false, work)?;
                } else {
                    self.source_write(work, || Ok(self.core.source.flush(work)?))?;
                }
            }
            _ => return Err(libc::ENOSYS.into()),
        }
        Ok(false)
    }

    fn packet_data(
        &self,
        request: &PacketRequest<'_>,
        output: &mut PacketOutput<'_>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let mut input = PacketInput::new(request.body);
        let inode = request.inode;
        match request.opcode {
            15 | 28 | 44 => {
                let handle = input.u64()?;
                let offset = input.u64()?;
                let size = input.u32()?;
                input.take(20)?;
                input.end()?;
                output.limit_payload(size as usize)?;
                if request.opcode == 15 {
                    let bytes = self.read_handle(inode, handle, offset, size, work)?;
                    output.bytes(&bytes.bytes)?;
                } else {
                    output.plus = request.opcode == 44;
                    let mut listing = Some(&mut *output);
                    let result = self.list_directory(inode, offset, &mut listing, work);
                    // A partial emission must still report fatal work failures to
                    // the session while preserving its canonical directory state.
                    if let Err(error) = result {
                        if !output.complete
                            || crate::native_mount::shared::outcome::stops_mount(&error)
                        {
                            return Err(error);
                        }
                    }
                }
            }
            16 => {
                let handle = input.u64()?;
                let offset = input.u64()?;
                let size = usize::try_from(input.u32()?).map_err(|_| libc::EOVERFLOW)?;
                input.take(20)?;
                let data = input.take(size)?;
                input.end()?;
                let written = self.write_handle(inode, handle, offset, data, work)?;
                output.u32(written)?;
                output.u32(0)?;
            }
            _ => return Err(libc::ENOSYS.into()),
        }
        Ok(false)
    }

    fn packet_allocation(
        &self,
        request: &PacketRequest<'_>,
        output: &mut PacketOutput<'_>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let mut input = PacketInput::new(request.body);
        let inode = request.inode;
        match request.opcode {
            43 => {
                let handle = input.u64()?;
                let offset = input.u64()?;
                let length = input.u64()?;
                let mode = input.u32()? as i32;
                input.take(4)?;
                input.end()?;
                self.allocate(inode, handle, offset, length, mode, work)?;
            }
            46 => {
                let handle = input.u64()?;
                let offset = input.u64()? as i64;
                let whence = input.u32()? as i32;
                input.take(4)?;
                input.end()?;
                let offset = self.seek(inode, handle, offset, whence, work)?;
                output.u64(u64::try_from(offset).map_err(|_| libc::EOVERFLOW)?)?;
            }
            47 => {
                let handle = input.u64()?;
                let offset = input.u64()?;
                let destination = input.u64()?;
                let destination_handle = input.u64()?;
                let destination_offset = input.u64()?;
                let length = input.u64()?;
                let flags = input.u64()?;
                input.end()?;
                let written = self.copy_range(
                    inode,
                    handle,
                    offset,
                    destination,
                    destination_handle,
                    destination_offset,
                    length,
                    flags,
                    work,
                )?;
                output.u32(written)?;
                output.u32(0)?;
            }
            _ => return Err(libc::ENOSYS.into()),
        }
        Ok(false)
    }

    fn packet_forget(
        &self,
        request: &PacketRequest<'_>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        let mut input = PacketInput::new(request.body);
        let inode = request.inode;
        match request.opcode {
            2 => {
                let count = input.u64()?;
                input.end()?;
                self.core
                    .state_to_finish()
                    .release_lookup_reference(inode, count);
                Ok(true)
            }
            42 => {
                let count = usize::try_from(input.u32()?).map_err(|_| libc::EOVERFLOW)?;
                input.take(4)?;
                let bytes = count.checked_mul(16).ok_or(libc::EOVERFLOW)?;
                if input.remaining().len() != bytes {
                    return Err(libc::EPROTO.into());
                }
                let mut state = self.core.state_to_finish();
                for _ in 0..count {
                    work.charge(Items.delta(1))?;
                    state.release_lookup_reference(input.u64()?, input.u64()?);
                }
                Ok(true)
            }
            _ => Err(libc::ENOSYS.into()),
        }
    }
}

fn minimum_payload(request: &PacketRequest<'_>) -> Result<usize, i32> {
    Ok(match request.opcode {
        1 | 6 | 8 | 9 | 13 => 128,
        3 | 4 => 104,
        14 | 27 => 16,
        35 => 144,
        16 | 46 | 47 => 8,
        17 => 80,
        15 | 28 | 44 => {
            let mut input = PacketInput::new(request.body);
            input.take(16)?;
            usize::try_from(input.u32()?).map_err(|_| libc::EOVERFLOW)?
        }
        22 | 23 => {
            let mut input = PacketInput::new(request.body);
            match input.u32()? {
                0 => 8,
                size => usize::try_from(size).map_err(|_| libc::EOVERFLOW)?,
            }
        }
        _ => 0,
    })
}

fn encode_xattr(output: &mut PacketOutput<'_>, value: &[u8], size: u32) -> Result<(), i32> {
    if size == 0 {
        output.u32(u32::try_from(value.len()).map_err(|_| libc::EOVERFLOW)?)?;
        output.u32(0)
    } else if value.len() <= size as usize {
        output.bytes(value)
    } else {
        Err(libc::ERANGE)
    }
}

fn decode_setattr(mut input: PacketInput<'_>) -> Result<(Option<u64>, AttributeChanges), i32> {
    let valid = input.u32()?;
    input.take(4)?;
    let handle = input.u64()?;
    let size = input.u64()?;
    input.take(8)?;
    let atime = input.u64()?;
    let mtime = input.u64()?;
    let ctime = input.u64()?;
    let atimensec = input.u32()?;
    let mtimensec = input.u32()?;
    let ctimensec = input.u32()?;
    let mode = input.u32()?;
    input.take(4)?;
    let uid = input.u32()?;
    let gid = input.u32()?;
    input.take(4)?;
    input.end()?;
    // Reject attributes whose semantics were not negotiated, rather than
    // accepting a mutation whose requested effects would be discarded.
    if valid & !0x7ff != 0 {
        return Err(libc::EOPNOTSUPP);
    }
    let atime = decode_time(valid, 1 << 4, 1 << 7, atime, atimensec)?;
    let mtime = decode_time(valid, 1 << 5, 1 << 8, mtime, mtimensec)?;
    let changed = if valid & (1 << 10) != 0 {
        Some(timestamp(ctime, ctimensec)?)
    } else {
        None
    };
    Ok((
        (valid & (1 << 6) != 0 && handle != 0).then_some(handle),
        AttributeChanges {
            mode: (valid & 1 != 0).then_some(mode),
            uid: (valid & 2 != 0).then_some(uid),
            gid: (valid & 4 != 0).then_some(gid),
            size: (valid & 8 != 0).then_some(size),
            atime,
            mtime,
            changed,
            crtime: None,
            flags: None,
        },
    ))
}

fn decode_time(
    valid: u32,
    supplied: u32,
    now: u32,
    secs: u64,
    nanos: u32,
) -> Result<Option<TimeOrNow>, i32> {
    if valid & now != 0 {
        Ok(Some(TimeOrNow::Now))
    } else if valid & supplied != 0 {
        Ok(Some(TimeOrNow::SpecificTime(timestamp(secs, nanos)?)))
    } else {
        Ok(None)
    }
}

fn timestamp(secs: u64, nanos: u32) -> Result<SystemTime, i32> {
    if nanos >= 1_000_000_000 {
        return Err(libc::EINVAL);
    }
    let signed = secs as i64;
    let value = if signed >= 0 {
        SystemTime::UNIX_EPOCH.checked_add(Duration::new(secs, nanos))
    } else {
        SystemTime::UNIX_EPOCH
            .checked_sub(Duration::from_secs(signed.unsigned_abs()))
            .and_then(|value| value.checked_add(Duration::from_nanos(u64::from(nanos))))
    };
    value.ok_or(libc::EOVERFLOW)
}
