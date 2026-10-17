pub(super) const PREFIX: &[u8] = b"fs/operation-windows-v1/";
pub(super) const SEPARATOR: &[u8] = b"/leases/";
pub(crate) const MAX_ENCODED_BYTES: usize = PREFIX.len() + 32 + SEPARATOR.len() + 32 + 1 + 20;

pub(super) fn length(expires_at_millis: u64) -> usize {
    let mut digits = 1_usize;
    let mut number = expires_at_millis;
    while number >= 10 {
        digits += 1;
        number /= 10;
    }
    PREFIX.len() + 32 + SEPARATOR.len() + 32 + 1 + digits
}

// `length` is obtained from the expiry before the caller admits any writes.
pub(super) fn write<const N: usize>(
    workspace: [u8; 16],
    lease: [u8; 16],
    expires_at_millis: u64,
    length: usize,
    text: &mut [u8; N],
) {
    const {
        assert!(N >= MAX_ENCODED_BYTES);
    };
    write_buffer(workspace, lease, expires_at_millis, length, text);
}

// The production entry above retains its compile-time capacity bound. This
// same formatter is instantiated at 116 bytes by the proof's negative control.
pub(super) fn write_buffer<const N: usize>(
    workspace: [u8; 16],
    lease: [u8; 16],
    expires_at_millis: u64,
    length: usize,
    text: &mut [u8; N],
) {
    text[..PREFIX.len()].copy_from_slice(PREFIX);
    let mut cursor = PREFIX.len();
    write_identity(
        workspace,
        text[cursor..cursor + workspace.len() * 2]
            .first_chunk_mut::<32>()
            .expect("workspace identity slice has its fixed capacity"),
        true,
    );
    cursor += workspace.len() * 2;
    text[cursor..cursor + SEPARATOR.len()].copy_from_slice(SEPARATOR);
    cursor += SEPARATOR.len();
    write_identity(
        lease,
        text[cursor..cursor + lease.len() * 2]
            .first_chunk_mut::<32>()
            .expect("lease identity slice has its fixed capacity"),
        false,
    );
    cursor += lease.len() * 2;
    text[cursor] = b'/';
    let mut number = expires_at_millis;
    for position in (cursor + 1..length).rev() {
        text[position] = b'0' + (number % 10) as u8;
        number /= 10;
    }
}

// A checked component contract proves every identity and prior output byte. The
// complete formatter can then use that verified write effect, rather than
// expanding the same fixed-width hex iterator in each caller and control.
#[cfg_attr(kani, kani::modifies(text))]
#[inline]
pub(super) fn write_identity(identity: [u8; 16], text: &mut [u8; 32], workspace: bool) {
    let result = hex::encode_to_slice(identity, text);
    if workspace {
        result.expect("fixed-width workspace lease path has its exact hex capacity");
    } else {
        result.expect("fixed-width lease identity has its exact hex capacity");
    }
}
