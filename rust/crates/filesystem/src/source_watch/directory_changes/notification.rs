#![allow(unsafe_code, reason = "borrows the checked aligned native notification buffer")]
use windows::Win32::Storage::FileSystem::{FILE_NOTIFY_EXTENDED_INFORMATION, FILE_NOTIFY_INFORMATION};

pub(super) fn notification(bytes: &[u8], extended: bool) -> Option<(usize, u32, &[u16], Option<u64>)> {
    let (name_offset, length_offset) = if extended {
        (std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileName),
            std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileNameLength))
    } else {
        (std::mem::offset_of!(FILE_NOTIFY_INFORMATION, FileName),
            std::mem::offset_of!(FILE_NOTIFY_INFORMATION, FileNameLength))
    };
    let word = |at| bytes.get(at..at + 4).and_then(|value| <[u8; 4]>::try_from(value).ok()).map(u32::from_le_bytes);
    let next = word(0)? as usize;
    let action = word(4)?;
    let length = word(length_offset)? as usize;
    let end = name_offset.checked_add(length)?;
    let name = bytes.get(name_offset..end)?;
    if length % 2 != 0 || name.as_ptr().align_offset(std::mem::align_of::<u16>()) != 0 { return None; }
    if next != 0 && (next < end || next > bytes.len()) { return None; }
    let file = if extended {
        let at = std::mem::offset_of!(FILE_NOTIFY_EXTENDED_INFORMATION, FileId);
        Some(u64::from_le_bytes(bytes.get(at..at + 8)?.try_into().ok()?))
    } else { None };
    // SAFETY: name is aligned and has a checked even byte length inside the completed page.
    let path = unsafe { std::slice::from_raw_parts(name.as_ptr().cast::<u16>(), length / 2) };
    Some((next, action, path, file))
}

#[cfg(test)]
mod tests {
    use super::notification;
    fn record(extended: bool, action: u32, file: u64, name: &str, last: bool) -> Vec<u8> {
        let name = name
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        let mut bytes = vec![0_u8; if extended { 84 } else { 12 }];
        bytes[4..8].copy_from_slice(&action.to_le_bytes());
        let length = u32::try_from(name.len()).unwrap_or(u32::MAX).to_le_bytes();
        if extended {
            bytes[64..72].copy_from_slice(&file.to_le_bytes());
            bytes[80..84].copy_from_slice(&length);
        } else {
            bytes[8..12].copy_from_slice(&length);
        }
        bytes.extend_from_slice(&name);
        bytes.resize(bytes.len().next_multiple_of(8), 0);
        if !last {
            let next = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
            bytes[..4].copy_from_slice(&next.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn names_and_file_identity_survive_both_native_layouts() {
        for extended in [true, false] {
            let mut bytes = record(extended, 1, 7, r"d\created", false);
            bytes.extend(record(extended, 3, 9, "written", true));
            let (next, action, name, file) = notification(&bytes, extended).expect("created report");
            assert_eq!((action, file), (1, extended.then_some(7)));
            assert_eq!(name, r"d\created".encode_utf16().collect::<Vec<_>>());
            let (next, action, name, file) = notification(&bytes[next..], extended).expect("modified report");
            assert_eq!((next, action, file), (0, 3, extended.then_some(9)));
            assert_eq!(name, "written".encode_utf16().collect::<Vec<_>>());
        }
    }
    #[test]
    fn native_names_preserve_unpaired_surrogates() {
        for extended in [true, false] {
            let mut bytes = record(extended, 3, 0, "a", true);
            let name = if extended { 84 } else { 12 };
            bytes[name..name + 2].copy_from_slice(&0xD800_u16.to_le_bytes());
            assert_eq!(notification(&bytes, extended).expect("opaque UTF-16 report").2, &[0xD800]);
        }
    }
}
