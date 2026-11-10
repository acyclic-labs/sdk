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

    fn layout(length: usize) -> std::alloc::Layout {
        std::alloc::Layout::from_size_align(length, 8).expect("checked mapped length")
    }

    pub(super) fn map(length: usize) -> Result<*mut u8, Errno> {
        // SAFETY: a checked layout; zeroed backing matches anonymous mmap.
        let pointer = unsafe { std::alloc::alloc_zeroed(layout(length)) };
        if pointer.is_null() {
            Err(Errno(ENOMEM))
        } else {
            Ok(pointer)
        }
    }

    pub(super) unsafe fn remap(base: *mut u8, old: usize, length: usize) -> Result<*mut u8, Errno> {
        // SAFETY: the decoder retains this complete original allocation.
        let pointer = unsafe { std::alloc::realloc(base, layout(old), length) };
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
            std::alloc::dealloc(base, layout(length));
        }
    }
}

/// Decodes one descriptor-count byte followed by a captured runner body.
/// Successful pointers must stay inside the original mapped request, and
/// every descriptor role must consume exactly one original descriptor.
pub fn fuzz_runner_request(input: &[u8]) {
    let Some((&count, bytes)) = input.split_first() else {
        return;
    };
    let mut body = request::Mapped::EMPTY;
    if body.reserve(bytes.len()).is_err() {
        return;
    }
    body.bytes()[..bytes.len()].copy_from_slice(bytes);
    let received = request::Received {
        descriptors: core::array::from_fn(|index| 10 + index as i32),
        count: usize::from(count),
        length: bytes.len(),
    };
    let decoded = request::spec(&received, &mut body);
    if let Ok(spec) = decoded {
        let flags = bytes[3];
        assert_eq!(flags & !31, 0);
        assert_ne!(flags & 9, 9, "a boundary cannot select a second cwd");
        assert_ne!(
            flags & 24,
            24,
            "a release action cannot enter a guest boundary"
        );
        let sent = bytes[..3].iter().filter(|&&slot| slot == 1).count();
        assert_eq!(
            received.count,
            3 + sent + usize::from(flags & 8 != 0) + usize::from(flags & 16 != 0),
            "descriptor roles consumed every original descriptor"
        );
        assert!(received.count <= request::DESCRIPTORS);
        assert_eq!((spec.directory, spec.reply, spec.here), (10, 11, 12));
        let u32_at = |offset| u32::from_ne_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let u64_at = |offset| u64::from_ne_bytes(bytes[offset..offset + 8].try_into().unwrap());
        assert_eq!(spec.uid, (flags & 2 != 0).then(|| u32_at(4)));
        assert_eq!(spec.gid, (flags & 4 != 0).then(|| u32_at(8)));
        assert_eq!(spec.owner.cast_unsigned(), u32_at(12));
        assert_eq!(spec.mask, u32_at(16));
        assert_eq!(spec.signals, u64_at(20));
        assert_eq!(spec.ignored, u64_at(28));
        for (index, limit) in spec.limits.iter().enumerate() {
            assert_eq!(*limit, [u64_at(36 + index * 16), u64_at(44 + index * 16)]);
        }
        let mut descriptor = 13;
        for (slot, original) in spec.slots.iter().zip(&bytes[..3]) {
            match (slot, original) {
                (request::Slot::Null, 0) | (request::Slot::Closed, 2) => {}
                (request::Slot::Descriptor(fd), 1) => {
                    assert_eq!(*fd, descriptor);
                    descriptor += 1;
                }
                _ => panic!("decoded stream changed its original role"),
            }
        }
        if let Some(policy) = spec.boundary.as_ref() {
            assert_eq!(policy.ruleset, descriptor);
            descriptor += 1;
        }
        assert_eq!(spec.activity, (flags & 16 != 0).then_some(descriptor));
        let start = body.base.addr();
        let end = start + body.length;
        let original = &body.bytes()[..bytes.len()];
        let string = |pointer: *const core::ffi::c_char| {
            let offset = pointer
                .addr()
                .checked_sub(start)
                .expect("pointer before input");
            assert!(offset < bytes.len(), "string outside original request");
            assert!(original[offset..].contains(&0));
        };
        string(spec.program);
        // SAFETY: the string oracle checked its original NUL-terminated range.
        assert_eq!(
            spec.search,
            !unsafe { core::ffi::CStr::from_ptr(spec.program) }
                .to_bytes()
                .contains(&b'/')
        );
        if let Some(cwd) = spec.cwd {
            string(cwd);
        }
        if let Some(path) = spec.path {
            string(path);
        }
        for vector in [spec.argv, spec.envp] {
            let mut address = vector.addr();
            loop {
                assert!(address >= start + bytes.len());
                assert!(address + core::mem::size_of::<*const core::ffi::c_char>() <= end);
                assert!(address.is_multiple_of(core::mem::align_of::<*const core::ffi::c_char>()));
                // SAFETY: the checked aligned pointer slot is in this mapping.
                let pointer = unsafe { (address as *const *const core::ffi::c_char).read() };
                if pointer.is_null() {
                    break;
                }
                string(pointer);
                address += core::mem::size_of_val(&pointer);
            }
        }
        if let Some(policy) = spec.boundary {
            assert!(policy.count > 0 && policy.count <= usize::from(u16::MAX));
            assert!(policy.filter.addr() >= start);
            assert!(
                policy.filter.addr() + policy.count * core::mem::size_of::<entry::Instruction>()
                    <= start + bytes.len()
            );
            assert!(
                policy
                    .filter
                    .addr()
                    .is_multiple_of(core::mem::align_of::<entry::Instruction>())
            );
            for index in 0..policy.count {
                // SAFETY: the complete aligned original policy range was
                // checked above, and remapping finished before decoding it.
                let instruction = unsafe { policy.filter.add(index).read() };
                let offset = policy.filter.addr() - start + index * 8;
                let bytes = &original[offset..offset + 8];
                assert_eq!(
                    instruction.code,
                    u16::from_ne_bytes(bytes[..2].try_into().unwrap())
                );
                assert_eq!((instruction.jt, instruction.jf), (bytes[2], bytes[3]));
                assert_eq!(
                    instruction.k,
                    u32::from_ne_bytes(bytes[4..].try_into().unwrap())
                );
            }
        }
    } else if let Err(error) = decoded {
        assert!(matches!(error.0, sys::EINVAL | sys::ENOMEM));
    }
    if !body.base.is_null() {
        // SAFETY: spec has returned; no request pointer is used again. This
        // iteration owns the whole original backing, including any remap move.
        unsafe {
            sys::release(body.base, body.length);
        }
    }
}
