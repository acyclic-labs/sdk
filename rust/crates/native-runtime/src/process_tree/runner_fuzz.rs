//! Exercises the static runner's actual decoder without launching a program.

#[path = "../execution/instruction.rs"]
mod entry;
#[path = "../../runner/request.rs"]
mod request;

// Only memory backing differs from the static image: the actual decoder is
// included unchanged, while this adapter uses the host allocator on any OS.
mod sys {
    pub(super) type Fd = i32;
    pub(super) type Pid = i32;
    #[derive(Clone, Copy, Debug)]
    pub(super) struct Errno(pub(super) i32);
    pub(super) const ENOMEM: i32 = 12;
    pub(super) const EINVAL: i32 = 22;

    fn layout(length: usize) -> Result<std::alloc::Layout, Errno> {
        std::alloc::Layout::from_size_align(length, 8).map_err(|_| Errno(ENOMEM))
    }

    pub(super) fn map(length: usize) -> Result<*mut u8, Errno> {
        // SAFETY: a checked layout; zeroed backing matches anonymous mmap.
        let pointer = unsafe { std::alloc::alloc_zeroed(layout(length)?) };
        if pointer.is_null() {
            Err(Errno(ENOMEM))
        } else {
            Ok(pointer)
        }
    }

    pub(super) unsafe fn remap(base: *mut u8, old: usize, length: usize) -> Result<*mut u8, Errno> {
        // SAFETY: the decoder retains this complete original allocation.
        let pointer = unsafe { std::alloc::realloc(base, layout(old)?, layout(length)?.size()) };
        if pointer.is_null() {
            return Err(Errno(ENOMEM));
        }
        // SAFETY: reserve only grows; initialize the new tail before forming
        // the decoder's writable slice, just as anonymous mmap does.
        unsafe {
            pointer.add(old).write_bytes(0, length - old);
        }
        Ok(pointer)
    }

    pub(super) unsafe fn release(base: *mut u8, length: usize) {
        // SAFETY: the completed decoder has surrendered its whole backing.
        unsafe {
            // SAFETY: successful map/remap validated this original size and alignment.
            let layout = std::alloc::Layout::from_size_align_unchecked(length, 8);
            std::alloc::dealloc(base, layout);
        }
    }
}

impl Drop for request::Mapped {
    fn drop(&mut self) {
        if !self.base.is_null() {
            // SAFETY: the decoder has surrendered this complete host allocation.
            unsafe { sys::release(self.base, self.length) };
        }
    }
}

type Check = Result<(), &'static str>;

fn require(valid: bool, reason: &'static str) -> Check {
    if valid { Ok(()) } else { Err(reason) }
}

fn word<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], &'static str> {
    bytes
        .get(offset..)
        .and_then(<[u8]>::first_chunk::<N>)
        .copied()
        .ok_or("decoded field outside original request")
}

/// Decodes a descriptor-count byte followed by the actual captured runner body.
/// Invalid inputs exercise the real decoder's refusal path.
///
/// # Errors
///
/// Returns an error if an accepted request violates the original descriptor,
/// field, pointer or instruction invariants. Fuzz and test callers assert it.
pub fn fuzz_runner_request(input: &[u8]) -> Result<(), &'static str> {
    let Some((&count, bytes)) = input.split_first() else {
        return Ok(());
    };
    let mut body = request::Mapped::EMPTY;
    if body.reserve(bytes.len()).is_err() {
        return Ok(());
    }
    for (target, byte) in body.bytes().iter_mut().zip(bytes) {
        *target = *byte;
    }
    let mut descriptors = [0; request::DESCRIPTORS];
    for (descriptor, value) in descriptors.iter_mut().zip(10..) {
        *descriptor = value;
    }
    let received = request::Received {
        descriptors,
        count: usize::from(count),
        length: bytes.len(),
    };
    match request::spec(&received, &mut body) {
        Ok(spec) => {
            fields(&spec, &received, bytes)?;
            pointers(&spec, &mut body, bytes.len())
        }
        Err(error) => require(
            matches!(error.0, sys::EINVAL | sys::ENOMEM),
            "unexpected decoder refusal",
        ),
    }
}

fn fields(spec: &request::Spec, received: &request::Received, bytes: &[u8]) -> Check {
    let flags = *bytes.get(3).ok_or("decoded request has no flags")?;
    require(flags & !63 == 0, "unknown request flags accepted")?;
    require(flags & 9 != 9, "a boundary selected a second cwd")?;
    require(
        flags & 24 != 24,
        "a release action entered a guest boundary",
    )?;
    let slots = bytes
        .get(..3)
        .ok_or("decoded request has no stream slots")?;
    let sent = slots.iter().filter(|&&slot| slot == 1).count();
    require(
        received.count == 3 + sent + 2 * usize::from(flags & 8 != 0) + usize::from(flags & 16 != 0),
        "descriptor roles did not consume every original descriptor",
    )?;
    require(
        received.count <= request::DESCRIPTORS,
        "descriptor count outside received array",
    )?;
    require(
        (spec.directory, spec.reply, spec.here) == (if flags & 32 != 0 { -1 } else { 10 }, 11, 12),
        "original descriptor roles changed",
    )?;
    require(
        spec.uid == (flags & 2 != 0).then_some(u32::from_ne_bytes(word(bytes, 4)?)),
        "uid changed",
    )?;
    require(
        spec.gid == (flags & 4 != 0).then_some(u32::from_ne_bytes(word(bytes, 8)?)),
        "gid changed",
    )?;
    require(
        spec.owner.cast_unsigned() == u32::from_ne_bytes(word(bytes, 12)?),
        "owner changed",
    )?;
    require(
        spec.mask == u32::from_ne_bytes(word(bytes, 16)?),
        "mask changed",
    )?;
    require(
        spec.signals == u64::from_ne_bytes(word(bytes, 20)?),
        "signal mask changed",
    )?;
    require(
        spec.ignored == u64::from_ne_bytes(word(bytes, 28)?),
        "ignored dispositions changed",
    )?;
    for (index, limit) in spec.limits.iter().enumerate() {
        require(
            *limit
                == [
                    u64::from_ne_bytes(word(bytes, 36 + index * 16)?),
                    u64::from_ne_bytes(word(bytes, 44 + index * 16)?),
                ],
            "resource limit changed",
        )?;
    }
    let mut descriptor = 13;
    for (slot, original) in spec.slots.iter().zip(slots) {
        let kind = match slot {
            request::Slot::Null => 0,
            request::Slot::Closed => 2,
            request::Slot::Descriptor(fd) => {
                require(*fd == descriptor, "stream descriptor role changed")?;
                descriptor += 1;
                1
            }
        };
        require(kind == *original, "stream kind changed")?;
    }
    if let Some(policy) = spec.boundary.as_ref() {
        require(policy.ruleset == descriptor, "ruleset descriptor changed")?;
        require(
            spec.completion_report == descriptor + 1,
            "report descriptor changed",
        )?;
        descriptor += 2;
    }
    require(
        spec.activity == (flags & 16 != 0).then_some(descriptor),
        "activity descriptor changed",
    )
}

fn pointers(spec: &request::Spec, body: &mut request::Mapped, length: usize) -> Check {
    let base = body.base;
    let start = base.addr();
    let end = start
        .checked_add(body.length)
        .ok_or("mapping address overflow")?;
    let original = body
        .bytes()
        .get(..length)
        .ok_or("mapping lost original bytes")?;
    let string = |pointer: *const core::ffi::c_char| -> Check {
        let offset = pointer
            .addr()
            .checked_sub(start)
            .ok_or("pointer before input")?;
        require(
            original
                .get(offset..)
                .is_some_and(|suffix| suffix.contains(&0)),
            "string outside original request",
        )
    };
    string(spec.program)?;
    // SAFETY: the string oracle proved the original NUL-terminated range.
    let program = unsafe { core::ffi::CStr::from_ptr(spec.program) };
    require(
        spec.search != program.to_bytes().contains(&b'/'),
        "program search mode changed",
    )?;
    if let Some(cwd) = spec.cwd {
        string(cwd)?;
    }
    if let Some(path) = spec.path {
        string(path)?;
    }
    for vector in [spec.argv, spec.envp] {
        let mut address = vector.addr();
        loop {
            require(
                address >= start + length,
                "pointer vector overlaps original bytes",
            )?;
            let next = address
                .checked_add(core::mem::size_of::<*const core::ffi::c_char>())
                .ok_or("vector address overflow")?;
            require(next <= end, "pointer slot outside mapping")?;
            require(
                address.is_multiple_of(core::mem::align_of::<*const core::ffi::c_char>()),
                "unaligned pointer slot",
            )?;
            // SAFETY: checked aligned slot, retaining the allocation's provenance.
            let pointer = unsafe {
                base.with_addr(address)
                    .cast::<*const core::ffi::c_char>()
                    .read()
            };
            if pointer.is_null() {
                break;
            }
            string(pointer)?;
            address = next;
        }
    }
    if let Some(policy) = &spec.boundary {
        instructions(policy, original, start)?;
    }
    Ok(())
}

fn instructions(policy: &request::Boundary, original: &[u8], start: usize) -> Check {
    require(
        policy.count > 0 && policy.count <= usize::from(u16::MAX),
        "invalid instruction count",
    )?;
    let offset = policy
        .filter
        .addr()
        .checked_sub(start)
        .ok_or("policy before input")?;
    let length = policy.count * core::mem::size_of::<entry::Instruction>();
    require(
        original
            .get(offset..)
            .is_some_and(|suffix| suffix.len() >= length),
        "policy outside original input",
    )?;
    require(
        policy
            .filter
            .addr()
            .is_multiple_of(core::mem::align_of::<entry::Instruction>()),
        "unaligned policy",
    )?;
    for index in 0..policy.count {
        // SAFETY: the complete aligned original range was checked above.
        let instruction = unsafe { policy.filter.add(index).read() };
        let [a, b, jt, jf, e, f, g, h] = word(original, offset + index * 8)?;
        require(
            instruction.code == u16::from_ne_bytes([a, b]),
            "instruction opcode changed",
        )?;
        require(
            (instruction.jt, instruction.jf) == (jt, jf),
            "instruction branch changed",
        )?;
        require(
            instruction.k == u32::from_ne_bytes([e, f, g, h]),
            "instruction argument changed",
        )?;
    }
    Ok(())
}
