use super::*;
use crate::source_watch::CallbackWork;

pub(super) fn translate(
    mut bytes: &[u8],
    extended: bool,
    delivery: &Delivery,
    work: &mut CallbackWork<'_>,
) -> Result<(), DemandError> {
    while !bytes.is_empty() {
        let decoded = notification(bytes, extended);
        let Some((next, change, file)) = decoded else {
            return delivery.sink.host_changed(&[HostChange::Everything], work);
        };
        if let Some(file) = file {
            delivery
                .sink
                .host_changed(&[change, HostChange::File(file)], work)?;
        } else {
            delivery.sink.host_changed(&[change], work)?;
        }
        if next == 0 {
            return Ok(());
        }
        bytes = &bytes[next..];
    }
    delivery.sink.host_changed(&[HostChange::Everything], work)
}

fn notification(bytes: &[u8], extended: bool) -> Option<(usize, HostChange<'_>, Option<u64>)> {
    let (name_offset, length_offset) = if extended {
        (84usize, 80usize)
    } else {
        (12, 8)
    };
    let word = |offset| {
        Some(u32::from_le_bytes(
            bytes.get(offset..offset + 4)?.try_into().ok()?,
        ))
    };
    let next = word(0)? as usize;
    let action = word(4)?;
    let length = word(length_offset)? as usize;
    let end = name_offset.checked_add(length)?;
    if length % 2 != 0 || next != 0 && (next < end || next >= bytes.len() || next % 2 != 0) {
        return None;
    }
    let name = bytes.get(name_offset..end)?;
    if name.as_ptr().align_offset(align_of::<u16>()) != 0 {
        return None;
    }
    // SAFETY: the completed native buffer is aligned, initialized and little-endian.
    let path = unsafe { std::slice::from_raw_parts(name.as_ptr().cast::<u16>(), length / 2) };
    let file = if extended {
        Some(u64::from_le_bytes(bytes.get(64..72)?.try_into().ok()?))
    } else {
        None
    };
    Some((next, HostChange::WidePath { action, path }, file))
}
