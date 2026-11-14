//! Descriptor custody shared by the static runner and its socket oracle.
//!
//! The OS adapter supplies the original recvmsg bytes and closes descriptors;
//! there is no allocator, syscall implementation, or second decoder here.

pub(super) struct Handles<const N: usize> {
    pub(super) descriptors: [i32; N],
    pub(super) count: usize,
}

impl<const N: usize> Handles<N> {
    pub(super) fn one(self, close: impl FnMut(i32)) -> Result<i32, ()> {
        if self.count == 1 {
            Ok(self.descriptors[0])
        } else {
            self.close(close);
            Err(())
        }
    }

    pub(super) fn close(self, mut close: impl FnMut(i32)) {
        for descriptor in self.descriptors.into_iter().take(self.count) {
            close(descriptor);
        }
    }
}

/// Takes custody of every descriptor delivered by a 64-bit Linux recvmsg.
/// Rejection closes all of them, including an overfull descriptor array.
pub(super) fn decode<const N: usize>(
    control: &[u8],
    flags: i32,
    mut close: impl FnMut(i32),
) -> Result<Handles<N>, ()> {
    let mut handles = Handles {
        descriptors: [-1; N],
        count: 0,
    };
    let mut invalid = flags & (8 | 32) != 0; // MSG_CTRUNC | MSG_TRUNC
    let mut offset = 0;
    while let Some((header, payload)) = control
        .get(offset..)
        .and_then(|message| message.split_first_chunk::<16>())
    {
        // Kernel-generated cmsghdr: length, SOL_SOCKET, SCM_RIGHTS, then fds.
        let mut length = [0; 8];
        length.copy_from_slice(&header[..8]);
        let length = usize::from_ne_bytes(length);
        let mut level = [0; 4];
        level.copy_from_slice(&header[8..12]);
        let level = i32::from_ne_bytes(level);
        let mut kind = [0; 4];
        kind.copy_from_slice(&header[12..16]);
        let kind = i32::from_ne_bytes(kind);
        let Some(data) = length
            .checked_sub(16)
            .and_then(|length| payload.get(..length))
        else {
            invalid = true;
            break;
        };
        if level == 1 && kind == 1 {
            let (descriptors, remainder) = data.as_chunks::<4>();
            invalid |= !remainder.is_empty();
            for bytes in descriptors {
                let descriptor = i32::from_ne_bytes(*bytes);
                if let Some(slot) = handles.descriptors.get_mut(handles.count) {
                    *slot = descriptor;
                    handles.count += 1;
                } else {
                    close(descriptor);
                    invalid = true;
                }
            }
        }
        offset += length.next_multiple_of(8);
    }
    if invalid {
        handles.close(close);
        Err(())
    } else {
        Ok(handles)
    }
}
