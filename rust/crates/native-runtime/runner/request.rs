//! The runner's actual descriptor/request decoder and mapped argument storage.

use super::{entry, sys};
use core::ffi::{CStr, c_char};
use sys::{Errno, Fd, Pid};

/// Descriptors one request carries: custody directory, reply, the owner's
/// working directory, up to three streams and one boundary or activity handle.
pub(super) const DESCRIPTORS: usize = 7;
/// Stream slots in a request.
const NULL: u8 = 0;
const SENT: u8 = 1;
const CLOSED: u8 = 2;
/// Request flags.
const HAS_CWD: u8 = 1;
const HAS_UID: u8 = 2;
const HAS_GID: u8 = 4;
const HAS_BOUNDARY: u8 = 8;
const ON_RELEASE: u8 = 16;
/// Resource limits a request carries, as soft and hard pairs.
const LIMITS: usize = 16;
/// The fixed fields before a request's strings: slots, flags, user, group,
/// owner, file-creation mask, limits, and the argument and environment
/// counts.
const FIXED: usize = 3 + 1 + 4 + 4 + 4 + 4 + 8 + 8 + LIMITS * 16 + 8 + 8;
// The native instruction slice is borrowed directly from this wire offset.
const _: () = assert!((FIXED + 4).is_multiple_of(core::mem::align_of::<entry::Instruction>()));
/// Memory the runner maps for itself.
pub(super) struct Mapped {
    pub(super) base: *mut u8,
    pub(super) length: usize,
}

impl Mapped {
    pub(super) const EMPTY: Self = Self {
        base: core::ptr::null_mut(),
        length: 0,
    };

    /// Grows to at least `length` bytes, keeping the contents.
    pub(super) fn reserve(&mut self, length: usize) -> Result<(), Errno> {
        if length <= self.length {
            return Ok(());
        }
        let length = length
            .checked_next_power_of_two()
            .ok_or(Errno(sys::ENOMEM))?
            .max(1 << 16);
        self.base = if self.base.is_null() {
            sys::map(length)?
        } else {
            // SAFETY: this whole mapping, which nothing else references.
            unsafe { sys::remap(self.base, self.length, length)? }
        };
        self.length = length;
        Ok(())
    }

    pub(super) fn bytes(&mut self) -> &mut [u8] {
        if self.base.is_null() {
            return &mut [];
        }
        // SAFETY: this mapping is readable and writable for its length.
        unsafe { core::slice::from_raw_parts_mut(self.base, self.length) }
    }
}

/// One received request: its descriptors and body length.
pub(super) struct Received {
    pub(super) descriptors: [Fd; DESCRIPTORS],
    pub(super) count: usize,
    pub(super) length: usize,
}

pub(super) fn word(bytes: &[u8]) -> usize {
    let mut word = [0; 8];
    if let Some(bytes) = bytes.get(..8) {
        word.copy_from_slice(bytes);
    }
    usize::from_ne_bytes(word)
}

pub(super) fn array4(bytes: &[u8]) -> [u8; 4] {
    let mut array = [0; 4];
    if let Some(bytes) = bytes.get(..4) {
        array.copy_from_slice(bytes);
    }
    array
}

/// Where the program's standard stream comes from.
#[derive(Clone, Copy)]
pub(super) enum Slot {
    /// The runner's null device, already in place.
    Null,
    Descriptor(Fd),
    Closed,
}

/// A request, ready to exec: pointers into the runner's request memory.
pub(super) struct Spec {
    pub(super) reply: Fd,
    pub(super) directory: Fd,
    pub(super) here: Fd,
    pub(super) owner: Pid,
    pub(super) slots: [Slot; 3],
    pub(super) cwd: Option<*const c_char>,
    pub(super) uid: Option<u32>,
    pub(super) gid: Option<u32>,
    /// The owner's file-creation mask and resource limits at the request.
    pub(super) mask: u32,
    pub(super) signals: u64,
    pub(super) ignored: u64,
    pub(super) limits: [[u64; 2]; LIMITS],
    pub(super) program: *const c_char,
    /// The program has no slash, so the child's `PATH` finds it.
    pub(super) search: bool,
    pub(super) path: Option<*const c_char>,
    pub(super) argv: *const *const c_char,
    pub(super) envp: *const *const c_char,
    pub(super) boundary: Option<Boundary>,
    pub(super) activity: Option<Fd>,
}

/// Borrowed native policy in this original request's mapping. Remapping is
/// complete before these pointers are formed, just like argv and envp.
pub(super) struct Boundary {
    pub(super) ruleset: Fd,
    pub(super) filter: *const entry::Instruction,
    pub(super) count: usize,
}

/// Reads a request body and lays out its argument and environment vectors
/// after it.
pub(super) fn spec(received: &Received, body: &mut Mapped) -> Result<Spec, Errno> {
    let invalid = Errno(sys::EINVAL);
    let descriptor = |index: usize| {
        received
            .descriptors
            .get(index)
            .copied()
            .filter(|_| index < received.count)
            .ok_or(invalid)
    };
    let length = received.length;
    let (slots, flags, uid, gid, owner, mask, signals, ignored, limits, argc, envc) = {
        let mut reader = Reader(body.bytes().get(..length).ok_or(invalid)?);
        let slots = reader.take(3)?;
        let flags = *reader.take(1)?.first().ok_or(invalid)?;
        let uid = reader.word()?;
        let gid = reader.word()?;
        let owner = Pid::try_from(reader.word()?).map_err(|_| invalid)?;
        let mask = reader.word()?;
        let signals = reader.wide()?;
        let ignored = reader.wide()?;
        let mut limits = [[0; 2]; LIMITS];
        for limit in &mut limits {
            *limit = [reader.wide()?, reader.wide()?];
        }
        let argc = reader.count()?;
        let envc = reader.count()?;
        let mut kinds = [0; 3];
        kinds.copy_from_slice(slots);
        (
            kinds, flags, uid, gid, owner, mask, signals, ignored, limits, argc, envc,
        )
    };
    // A confined request's original `here` descriptor is its bound cwd.
    // Reject a second path-based selection before any warden is created.
    if flags & !(HAS_CWD | HAS_UID | HAS_GID | HAS_BOUNDARY | ON_RELEASE) != 0
        || flags & (HAS_BOUNDARY | HAS_CWD) == (HAS_BOUNDARY | HAS_CWD)
        || flags & (HAS_BOUNDARY | ON_RELEASE) == (HAS_BOUNDARY | ON_RELEASE)
    {
        return Err(invalid);
    }
    let mut next = 3;
    let mut streams = [Slot::Null; 3];
    for (stream, kind) in streams.iter_mut().zip(slots) {
        *stream = match kind {
            NULL => Slot::Null,
            SENT => {
                next += 1;
                Slot::Descriptor(descriptor(next - 1)?)
            }
            CLOSED => Slot::Closed,
            _ => return Err(invalid),
        };
    }
    let (policy, strings) = if flags & HAS_BOUNDARY != 0 {
        let mut reader = Reader(body.bytes().get(FIXED..length).ok_or(invalid)?);
        let count = usize::try_from(reader.word()?).map_err(|_| invalid)?;
        // The kernel consumes a 16-bit instruction count. Reject malformed
        // requests before creating a warden; no truncation can broaden policy.
        if count == 0 || u16::try_from(count).is_err() {
            return Err(invalid);
        }
        reader.take(
            count
                .checked_mul(core::mem::size_of::<entry::Instruction>())
                .ok_or(invalid)?,
        )?;
        let start = FIXED + 4;
        let end = start
            .checked_add(count * core::mem::size_of::<entry::Instruction>())
            .ok_or(invalid)?;
        (Some((descriptor(next)?, start, count)), end)
    } else {
        (None, FIXED)
    };
    let activity = if flags & ON_RELEASE != 0 {
        Some(descriptor(next + usize::from(policy.is_some()))?)
    } else {
        None
    };
    if received.count != next + usize::from(policy.is_some()) + usize::from(activity.is_some()) {
        return Err(invalid);
    }
    // Every argument and environment entry needs at least its NUL byte.
    // Validate that input-backed bound before allocating pointer storage.
    let strings_required = argc
        .checked_add(envc)
        .and_then(|n| n.checked_add(1 + usize::from(flags & HAS_CWD != 0)))
        .ok_or(invalid)?;
    if argc == 0 || strings_required > length.checked_sub(strings).ok_or(invalid)? {
        return Err(invalid);
    }
    // The vectors follow the body, aligned for pointers.
    let vectors = length
        .checked_next_multiple_of(8)
        .ok_or(Errno(sys::ENOMEM))?;
    let pointers = argc
        .checked_add(envc)
        .and_then(|count| count.checked_add(2))
        .and_then(|count| count.checked_mul(core::mem::size_of::<*const c_char>()))
        .ok_or(Errno(sys::ENOMEM))?;
    body.reserve(vectors.checked_add(pointers).ok_or(Errno(sys::ENOMEM))?)?;
    let base = body.base;
    // Shared, and only the body: the vectors after it are written below.
    let bytes: &[u8] = body.bytes().get(..length).ok_or(invalid)?;
    let mut offset = strings;
    let mut string = || -> Result<*const c_char, Errno> {
        let rest = bytes.get(offset..).ok_or(invalid)?;
        let end = rest.iter().position(|byte| *byte == 0).ok_or(invalid)?;
        let start = offset;
        offset += end + 1;
        Ok(base.wrapping_add(start).cast_const().cast())
    };
    let program = string()?;
    let cwd = if flags & HAS_CWD != 0 {
        Some(string()?)
    } else {
        None
    };
    let mut path = None;
    let vector = base.wrapping_add(vectors).cast::<*const c_char>();
    for index in 0..argc {
        let argument = string()?;
        // SAFETY: within the pointer area reserved above.
        unsafe { vector.add(index).write(argument) };
    }
    // SAFETY: within the pointer area reserved above.
    unsafe { vector.add(argc).write(core::ptr::null()) };
    let environment = vector.wrapping_add(argc + 1);
    for index in 0..envc {
        let variable = string()?;
        // SAFETY: a NUL-terminated string in this request.
        if unsafe { CStr::from_ptr(variable) }
            .to_bytes()
            .starts_with(b"PATH=")
        {
            path = Some(variable.wrapping_add(5));
        }
        // SAFETY: within the pointer area reserved above.
        unsafe { environment.add(index).write(variable) };
    }
    // SAFETY: within the pointer area reserved above.
    unsafe { environment.add(envc).write(core::ptr::null()) };
    // SAFETY: the program is a NUL-terminated string in this request.
    let search = !unsafe { CStr::from_ptr(program) }
        .to_bytes()
        .contains(&b'/');
    // A frame contains exactly one complete request, with no ignored suffix.
    if offset != length {
        return Err(invalid);
    }
    Ok(Spec {
        reply: descriptor(1)?,
        directory: descriptor(0)?,
        here: descriptor(2)?,
        owner,
        slots: streams,
        cwd,
        uid: (flags & HAS_UID != 0).then_some(uid),
        gid: (flags & HAS_GID != 0).then_some(gid),
        mask,
        signals,
        ignored,
        limits,
        program,
        search,
        path,
        argv: vector.cast_const(),
        envp: environment.cast_const(),
        boundary: policy.map(|(ruleset, start, count)| Boundary {
            ruleset,
            filter: base.wrapping_add(start).cast(),
            count,
        }),
        activity,
    })
}

/// A cursor over a request body.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], Errno> {
        if length > self.0.len() {
            return Err(Errno(sys::EINVAL));
        }
        let (taken, rest) = self.0.split_at(length);
        self.0 = rest;
        Ok(taken)
    }

    fn word(&mut self) -> Result<u32, Errno> {
        Ok(u32::from_ne_bytes(array4(self.take(4)?)))
    }

    fn count(&mut self) -> Result<usize, Errno> {
        Ok(word(self.take(8)?))
    }

    fn wide(&mut self) -> Result<u64, Errno> {
        let mut wide = [0; 8];
        wide.copy_from_slice(self.take(8)?);
        Ok(u64::from_ne_bytes(wide))
    }
}
