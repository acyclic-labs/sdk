//! Exercises the static runner's actual decoder without launching a program.

#[path = "../execution/linux/entry.rs"]
mod entry;
#[path = "../../runner/request.rs"]
mod request;
#[path = "../../runner/sys.rs"]
mod sys;

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
            3 + sent + usize::from(flags & 8 != 0) + usize::from(flags & 16 != 0)
        );
        assert!(received.count <= request::DESCRIPTORS);
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
        }
    }
    if !body.base.is_null() {
        // SAFETY: spec has returned; no request pointer is used again. This
        // iteration owns the whole original mmap, including any mremap move.
        assert_eq!(unsafe { libc::munmap(body.base.cast(), body.length) }, 0);
    }
}
